"""Tests for the production-readiness pieces: orphan reaper, heartbeat,
shutdown timeout."""

from __future__ import annotations

import asyncio

import asyncpg
import pytest

from roost import AsyncRoost, Worker, job
from roost._core import repo


@pytest.mark.asyncio
async def test_reap_orphans_marks_stuck_jobs_retryable(
    async_conn: asyncpg.Connection,
) -> None:
    job_id = await repo.enqueue_async(async_conn, task="t", max_attempts=3)
    await async_conn.execute(
        """
        UPDATE roost.jobs
           SET state = 'executing',
               attempt = 1,
               attempted_at = now() - interval '10 minutes'
         WHERE id = $1
        """,
        job_id,
    )

    reaped = await repo.reap_orphans_async(async_conn, stale_after_seconds=60)
    assert reaped == [(job_id, "retryable")]

    row = await async_conn.fetchrow("SELECT state, errors FROM roost.jobs WHERE id = $1", job_id)
    assert row is not None
    assert row["state"] == "retryable"
    assert row["errors"]
    assert "WorkerCrash" in row["errors"][0]["error"]


@pytest.mark.asyncio
async def test_reap_orphans_discards_at_max_attempts(
    async_conn: asyncpg.Connection,
) -> None:
    job_id = await repo.enqueue_async(async_conn, task="t", max_attempts=1)
    await async_conn.execute(
        """
        UPDATE roost.jobs
           SET state = 'executing',
               attempt = 1,
               attempted_at = now() - interval '10 minutes'
         WHERE id = $1
        """,
        job_id,
    )
    reaped = await repo.reap_orphans_async(async_conn, stale_after_seconds=60)
    assert reaped == [(job_id, "discarded")]


@pytest.mark.asyncio
async def test_reap_orphans_skips_fresh_executing(
    async_conn: asyncpg.Connection,
) -> None:
    job_id = await repo.enqueue_async(async_conn, task="t")
    await async_conn.execute(
        "UPDATE roost.jobs SET state = 'executing', attempted_at = now() WHERE id = $1",
        job_id,
    )
    reaped = await repo.reap_orphans_async(async_conn, stale_after_seconds=60)
    assert reaped == []


@pytest.mark.asyncio
async def test_heartbeat_writes_and_clears(async_conn: asyncpg.Connection) -> None:
    await repo.heartbeat_async(
        async_conn,
        worker_id="w-1",
        hostname="host",
        pid=1234,
        queues=["default", "emails"],
        metadata={"concurrency": 4},
    )

    row = await async_conn.fetchrow(
        "SELECT hostname, pid, queues, metadata FROM roost.workers WHERE id = 'w-1'"
    )
    assert row is not None
    assert row["hostname"] == "host"
    assert row["pid"] == 1234
    assert list(row["queues"]) == ["default", "emails"]
    assert row["metadata"] == {"concurrency": 4}

    await repo.deregister_worker_async(async_conn, "w-1")
    n = await async_conn.fetchval("SELECT COUNT(*) FROM roost.workers")
    assert n == 0


@pytest.mark.asyncio
async def test_gc_workers_removes_stale_rows(async_conn: asyncpg.Connection) -> None:
    await repo.heartbeat_async(async_conn, worker_id="stale", hostname="h", pid=1, queues=["q"])
    await async_conn.execute(
        "UPDATE roost.workers SET last_seen_at = now() - interval '10 minutes' WHERE id = 'stale'"
    )
    await repo.heartbeat_async(async_conn, worker_id="fresh", hostname="h", pid=2, queues=["q"])
    n = await repo.gc_workers_async(async_conn, stale_after_seconds=60)
    assert n == 1
    survivors = [r["id"] for r in await async_conn.fetch("SELECT id FROM roost.workers")]
    assert survivors == ["fresh"]


@pytest.mark.asyncio
async def test_worker_writes_heartbeat_during_run(fresh_dsn: str, async_conn: asyncpg.Connection) -> None:
    @job("noop")
    async def noop() -> None:
        return None

    worker = Worker(
        fresh_dsn,
        concurrency=1,
        run_cron=False,
        poll_interval=0.05,
        heartbeat_interval=0.05,
        orphan_reaper_interval=60.0,
    )
    task = asyncio.create_task(worker.run())

    try:
        deadline = asyncio.get_event_loop().time() + 5.0
        while asyncio.get_event_loop().time() < deadline:
            row = await async_conn.fetchrow("SELECT id FROM roost.workers WHERE id = $1", worker.id)
            if row is not None:
                break
            await asyncio.sleep(0.05)
        else:
            raise AssertionError("worker never registered a heartbeat")

        assert row is not None
    finally:
        worker.request_stop()
        await asyncio.wait_for(task, timeout=5.0)

    # On graceful shutdown the row is removed
    n = await async_conn.fetchval("SELECT COUNT(*) FROM roost.workers WHERE id = $1", worker.id)
    assert n == 0


@pytest.mark.asyncio
async def test_shutdown_drains_within_timeout(fresh_dsn: str) -> None:
    """Long-running handlers are cancelled when the drain timeout elapses."""
    started = asyncio.Event()
    cancelled = asyncio.Event()

    @job("forever")
    async def forever() -> None:
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async with AsyncRoost(fresh_dsn) as r:
        await r.enqueue(forever)

        worker = r.worker(
            concurrency=1,
            run_cron=False,
            poll_interval=0.05,
            shutdown_timeout=0.5,
        )
        task = asyncio.create_task(worker.run())

        await asyncio.wait_for(started.wait(), timeout=5.0)
        worker.request_stop()
        await asyncio.wait_for(task, timeout=5.0)
        assert cancelled.is_set()
