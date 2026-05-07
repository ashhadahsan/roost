from __future__ import annotations

import asyncio

import asyncpg
import pytest

from roost import AsyncRoost, fixed, job
from roost._core import repo


async def _wait_state(conn: asyncpg.Connection, job_id: int, target: str, timeout: float = 5.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        row = await conn.fetchrow("SELECT state FROM roost.jobs WHERE id = $1", job_id)
        if row is not None and row["state"] == target:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"job {job_id} never reached state {target!r}")


@pytest.mark.asyncio
async def test_failure_marks_retryable_with_backoff(fresh_dsn: str, async_conn: asyncpg.Connection) -> None:
    @job("boom")
    async def boom() -> None:
        raise RuntimeError("kaboom")

    async with AsyncRoost(fresh_dsn) as r:
        job_id = await r.enqueue(boom, max_attempts=3)

        worker = r.worker(concurrency=1, run_cron=False, poll_interval=0.05, retry_strategy=fixed(60.0))
        task = asyncio.create_task(worker.run())
        try:
            await _wait_state(async_conn, job_id, "retryable")
            row = await async_conn.fetchrow(
                "SELECT state, attempt, scheduled_at, errors FROM roost.jobs WHERE id = $1",
                job_id,
            )
            assert row is not None
            assert row["state"] == "retryable"
            assert row["attempt"] == 1
            errors = row["errors"]
            assert isinstance(errors, list) and len(errors) == 1
            assert "kaboom" in errors[0]["error"]
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5.0)


@pytest.mark.asyncio
async def test_failure_discarded_when_attempts_exhausted(
    fresh_dsn: str, async_conn: asyncpg.Connection
) -> None:
    @job("always_bad")
    async def always_bad() -> None:
        raise ValueError("nope")

    async with AsyncRoost(fresh_dsn) as r:
        job_id = await r.enqueue(always_bad, max_attempts=1)
        worker = r.worker(concurrency=1, run_cron=False, poll_interval=0.05)
        task = asyncio.create_task(worker.run())
        try:
            await _wait_state(async_conn, job_id, "discarded")
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5.0)


@pytest.mark.asyncio
async def test_promote_retryable_brings_due_jobs_back(async_conn: asyncpg.Connection) -> None:
    job_id = await repo.enqueue_async(async_conn, task="t")
    await async_conn.execute(
        "UPDATE roost.jobs SET state = 'retryable', scheduled_at = now() - interval '1 second' WHERE id = $1",
        job_id,
    )
    promoted = await repo.promote_retryable_async(async_conn)
    assert promoted == 1
    row = await async_conn.fetchrow("SELECT state FROM roost.jobs WHERE id = $1", job_id)
    assert row is not None and row["state"] == "available"
