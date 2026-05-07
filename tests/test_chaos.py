"""Chaos tests: kill workers mid-job, verify the reaper recovers."""

from __future__ import annotations

import asyncio
import contextlib

import asyncpg
import pytest

from roost import Worker, job
from roost._core import repo


@pytest.mark.asyncio
async def test_killed_worker_jobs_are_recovered_by_reaper(
    fresh_dsn: str, async_conn: asyncpg.Connection
) -> None:
    """Simulate a SIGKILL: cancel the worker mid-handler so the row stays in
    ``executing``, then run a peer worker with an aggressive reaper window
    and verify the abandoned job runs to completion."""
    blocker = asyncio.Event()
    completed = asyncio.Event()
    runs = {"first": 0, "second": 0}

    @job("survives_crash")
    async def survives_crash() -> None:
        runs["first"] += 1
        if runs["first"] == 1:
            # First worker enters and "crashes" — never completes.
            await blocker.wait()
            return
        runs["second"] += 1
        completed.set()

    job_id = await repo.enqueue_async(async_conn, task="survives_crash", max_attempts=3)

    # Worker A picks the job, then we yank the rug.
    worker_a = Worker(
        fresh_dsn,
        queues=["default"],
        concurrency=1,
        run_cron=False,
        poll_interval=0.05,
        heartbeat_interval=60.0,
        orphan_reaper_interval=60.0,
    )
    a_task = asyncio.create_task(worker_a.run())

    deadline = asyncio.get_event_loop().time() + 5.0
    while runs["first"] == 0:
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError("first worker never picked the job")
        await asyncio.sleep(0.05)

    # Hard-cancel worker A *without* allowing graceful drain. This is the
    # rough analog of a SIGKILL — handlers don't finalize.
    a_task.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await asyncio.wait_for(a_task, timeout=2.0)

    # The job is stuck in 'executing'. Backdate so the reaper considers it stale.
    await async_conn.execute(
        "UPDATE roost.jobs SET attempted_at = now() - interval '1 hour' WHERE id = $1",
        job_id,
    )

    # Worker B with a tight reaper recovers the orphan.
    worker_b = Worker(
        fresh_dsn,
        queues=["default"],
        concurrency=1,
        run_cron=False,
        poll_interval=0.05,
        heartbeat_interval=60.0,
        orphan_reaper_interval=0.1,
        orphan_stale_after=1.0,
    )
    blocker.set()
    b_task = asyncio.create_task(worker_b.run())

    try:
        await asyncio.wait_for(completed.wait(), timeout=10.0)
    finally:
        worker_b.request_stop()
        await asyncio.wait_for(b_task, timeout=5.0)

    row = await async_conn.fetchrow("SELECT state, attempt FROM roost.jobs WHERE id = $1", job_id)
    assert row is not None
    assert row["state"] == "completed"
    assert row["attempt"] >= 2  # picked up at least twice
