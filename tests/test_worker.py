from __future__ import annotations

import asyncio
from typing import Any

import asyncpg
import pytest

from roost import AsyncRoost, Worker, job
from roost._core import repo


async def _wait_until(predicate, timeout: float = 5.0, interval: float = 0.05) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError("predicate never satisfied within timeout")
        await asyncio.sleep(interval)


@pytest.mark.asyncio
async def test_worker_runs_async_handler_to_completion(fresh_dsn: str) -> None:
    fired = asyncio.Event()
    captured: dict[str, Any] = {}

    @job("greet")
    async def greet(name: str) -> None:
        captured["name"] = name
        fired.set()

    async with AsyncRoost(fresh_dsn) as r:
        await r.enqueue(greet, args={"name": "Ashhad"})

        worker = r.worker(concurrency=2, run_cron=False, poll_interval=0.1)
        task = asyncio.create_task(worker.run())
        try:
            await asyncio.wait_for(fired.wait(), timeout=5.0)
            assert captured["name"] == "Ashhad"

            async def _completed() -> bool:
                rows = await r.status()
                return (("default", "completed", 1)) in rows

            await _wait_until(_completed)
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5.0)


@pytest.mark.asyncio
async def test_two_workers_dont_double_process(fresh_dsn: str) -> None:
    """Concurrency-by-construction: SKIP LOCKED guarantees one job runs once."""
    counter = {"runs": 0}
    barrier = asyncio.Event()

    @job("once")
    async def once() -> None:
        counter["runs"] += 1
        barrier.set()

    async with AsyncRoost(fresh_dsn) as r:
        await r.enqueue(once)

        w1 = Worker(fresh_dsn, concurrency=2, run_cron=False, poll_interval=0.05)
        w2 = Worker(fresh_dsn, concurrency=2, run_cron=False, poll_interval=0.05)
        t1 = asyncio.create_task(w1.run())
        t2 = asyncio.create_task(w2.run())

        try:
            await asyncio.wait_for(barrier.wait(), timeout=5.0)
            await asyncio.sleep(0.5)  # give the other worker a chance to (mis)fire
            assert counter["runs"] == 1
        finally:
            w1.request_stop()
            w2.request_stop()
            await asyncio.gather(t1, t2)


@pytest.mark.asyncio
async def test_unknown_task_is_recorded_as_error(async_conn: asyncpg.Connection, fresh_dsn: str) -> None:
    job_id = await repo.enqueue_async(async_conn, task="nope", max_attempts=1)
    worker = Worker(fresh_dsn, concurrency=1, run_cron=False, poll_interval=0.05)

    task = asyncio.create_task(worker.run())
    try:

        async def _terminal() -> bool:
            row = await async_conn.fetchrow("SELECT state FROM roost.jobs WHERE id = $1", job_id)
            return row is not None and row["state"] in {"discarded", "retryable"}

        await _wait_until(_terminal)
        row = await async_conn.fetchrow("SELECT state, errors FROM roost.jobs WHERE id = $1", job_id)
        assert row is not None
        assert row["state"] == "discarded"
        errors = row["errors"]
        assert isinstance(errors, list) and len(errors) == 1
        assert "UnknownTaskError" in errors[0]["error"]
    finally:
        worker.request_stop()
        await asyncio.wait_for(task, timeout=5.0)
