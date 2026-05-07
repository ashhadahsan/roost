"""End-to-end throughput benchmark.

Measures jobs / second from enqueue → completed, plus p50/p99 enqueue→start
latency. Run against a local Postgres::

    export ROOST_DSN=postgresql://postgres:postgres@localhost/postgres
    roost init --apply
    python bench/throughput.py --jobs 50000 --concurrency 16
"""

from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import time
from typing import Any

from roost import AsyncRoost, JobInsert, Worker, job


@job("bench_noop")
async def bench_noop(idx: int, started_at: float) -> dict[str, Any]:
    return {"finished_at": time.time(), "started_at": started_at, "idx": idx}


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs", type=int, default=10_000)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--prefetch", type=int, default=64)
    parser.add_argument(
        "--dsn",
        default=os.environ.get("ROOST_DSN", "postgresql://postgres:postgres@localhost/postgres"),
    )
    args = parser.parse_args()

    async with AsyncRoost(args.dsn) as r:
        await r.setup_schema()
        # Drain leftovers from prior runs.
        async with (await r._ensure_pool()).acquire() as conn:  # noqa: SLF001
            await conn.execute("TRUNCATE roost.jobs")
            await conn.execute("TRUNCATE roost.workers")

        print(f"enqueueing {args.jobs} jobs…")
        t0 = time.time()
        # Use bulk in batches of 1000 for efficient insert.
        batch = 1_000
        now = time.time()
        for i in range(0, args.jobs, batch):
            payload = [
                JobInsert(
                    task="bench_noop",
                    args={"idx": j, "started_at": now},
                    queue="bench",
                )
                for j in range(i, min(i + batch, args.jobs))
            ]
            await r.enqueue_many(payload)
        enq_elapsed = time.time() - t0
        print(f"  enqueue: {enq_elapsed:.2f}s  ({args.jobs / enq_elapsed:.0f} jobs/s)")

        worker = Worker(
            args.dsn,
            queues=["bench"],
            concurrency=args.concurrency,
            prefetch=args.prefetch,
            poll_interval=0.05,
            run_cron=False,
            heartbeat_interval=2.0,
            orphan_reaper_interval=60.0,
        )
        run_task = asyncio.create_task(worker.run())

        # Wait until everything is done.
        async with (await r._ensure_pool()).acquire() as conn:  # noqa: SLF001
            t1 = time.time()
            while True:
                row = await conn.fetchval(
                    "SELECT COUNT(*) FROM roost.jobs WHERE state IN ('available','executing','retryable')"
                )
                if row == 0:
                    break
                if time.time() - t1 > 600:
                    raise RuntimeError("bench did not drain in 10 minutes")
                await asyncio.sleep(0.25)

            durations = await conn.fetch(
                "SELECT (attempted_at - inserted_at) AS lag, "
                "       (completed_at - attempted_at) AS dur "
                "  FROM roost.jobs WHERE state = 'completed'"
            )

        worker.request_stop()
        await asyncio.wait_for(run_task, timeout=30.0)

        process_elapsed = time.time() - t1
        completed = len(durations)
        lag_ms = sorted(d["lag"].total_seconds() * 1000.0 for d in durations)
        dur_ms = sorted(d["dur"].total_seconds() * 1000.0 for d in durations)

        def pct(values: list[float], p: float) -> float:
            if not values:
                return 0.0
            return values[int(min(len(values) - 1, len(values) * p))]

        print()
        print(f"  processed:    {completed}")
        print(f"  process time: {process_elapsed:.2f}s  ({completed / process_elapsed:.0f} jobs/s)")
        print(f"  enq->run lag: p50={statistics.median(lag_ms):.1f}ms  p99={pct(lag_ms, 0.99):.1f}ms")
        print(f"  handler dur:  p50={statistics.median(dur_ms):.2f}ms  p99={pct(dur_ms, 0.99):.2f}ms")


if __name__ == "__main__":
    asyncio.run(main())
