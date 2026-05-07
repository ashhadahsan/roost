"""Tier 1 + 2 feature tests: bulk, priority, tags, timeout, cancel,
queue pause, requeue discarded, result storage, Pydantic args."""

from __future__ import annotations

import asyncio

import asyncpg
import pytest
from pydantic import BaseModel

from roost import AsyncRoost, JobInsert, Worker, job
from roost._core import repo


@pytest.mark.asyncio
async def test_bulk_enqueue_inserts_all_rows(async_conn: asyncpg.Connection) -> None:
    payload = [JobInsert(task=f"t{i}", queue="bulk", priority=i % 5) for i in range(50)]
    inserted = await repo.enqueue_many_async(async_conn, payload)
    assert inserted == 50
    n = await async_conn.fetchval("SELECT COUNT(*) FROM roost.jobs WHERE queue = 'bulk'")
    assert n == 50


@pytest.mark.asyncio
async def test_priority_ordering(fresh_dsn: str, async_conn: asyncpg.Connection) -> None:
    order: list[int] = []

    @job("ranked")
    async def ranked(idx: int) -> None:
        order.append(idx)

    async with AsyncRoost(fresh_dsn) as r:
        # Lower priority numbers run first (matches Oban semantics).
        await r.enqueue(ranked, args={"idx": 1}, priority=10)
        await r.enqueue(ranked, args={"idx": 2}, priority=0)
        await r.enqueue(ranked, args={"idx": 3}, priority=5)
        worker = r.worker(concurrency=1, run_cron=False, poll_interval=0.05)
        task = asyncio.create_task(worker.run())
        try:
            deadline = asyncio.get_event_loop().time() + 5.0
            while len(order) < 3:
                if asyncio.get_event_loop().time() > deadline:
                    raise AssertionError(f"only ran {order}")
                await asyncio.sleep(0.05)
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5.0)
    assert order == [2, 3, 1]


@pytest.mark.asyncio
async def test_tags_persisted(async_conn: asyncpg.Connection) -> None:
    job_id = await repo.enqueue_async(async_conn, task="t", tags=["billing", "hot"], queue="emails")
    row = await async_conn.fetchrow("SELECT tags FROM roost.jobs WHERE id = $1", job_id)
    assert row is not None
    assert list(row["tags"]) == ["billing", "hot"]


@pytest.mark.asyncio
async def test_timeout_enforces_cancellation(fresh_dsn: str, async_conn: asyncpg.Connection) -> None:
    @job("slow")
    async def slow() -> None:
        await asyncio.sleep(60)

    async with AsyncRoost(fresh_dsn) as r:
        job_id = await r.enqueue(slow, timeout_seconds=1, max_attempts=1)
        worker = r.worker(concurrency=1, run_cron=False, poll_interval=0.05)
        task = asyncio.create_task(worker.run())
        try:
            deadline = asyncio.get_event_loop().time() + 10.0
            while True:
                row = await async_conn.fetchrow("SELECT state FROM roost.jobs WHERE id = $1", job_id)
                if row is not None and row["state"] == "discarded":
                    break
                if asyncio.get_event_loop().time() > deadline:
                    raise AssertionError("job never timed out")
                await asyncio.sleep(0.1)
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5.0)


@pytest.mark.asyncio
async def test_paused_queue_blocks_fetch(fresh_dsn: str, async_conn: asyncpg.Connection) -> None:
    fired = asyncio.Event()

    @job("hello")
    async def hello() -> None:
        fired.set()

    async with AsyncRoost(fresh_dsn) as r:
        await r.pause_queue("default")
        await r.enqueue(hello)

        worker = r.worker(concurrency=1, run_cron=False, poll_interval=0.05)
        task = asyncio.create_task(worker.run())
        try:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(fired.wait(), timeout=0.5)
            await r.resume_queue("default")
            await asyncio.wait_for(fired.wait(), timeout=5.0)
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5.0)


@pytest.mark.asyncio
async def test_cancel_pending_job_immediately(fresh_dsn: str, async_conn: asyncpg.Connection) -> None:
    @job("never_picked")
    async def never_picked() -> None: ...

    async with AsyncRoost(fresh_dsn) as r:
        job_id = await r.enqueue(never_picked, scheduled_at=None, queue="paused_q")
        await r.pause_queue("paused_q")
        await r.cancel(job_id)
        row = await async_conn.fetchrow(
            "SELECT state, cancel_requested FROM roost.jobs WHERE id = $1", job_id
        )
        assert row is not None
        assert row["state"] == "cancelled"
        assert row["cancel_requested"] is True


@pytest.mark.asyncio
async def test_cancel_propagates_to_running_handler(fresh_dsn: str, async_conn: asyncpg.Connection) -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    @job("long_running")
    async def long_running() -> None:
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async with AsyncRoost(fresh_dsn) as r:
        job_id = await r.enqueue(long_running, max_attempts=1)
        worker = r.worker(concurrency=1, run_cron=False, poll_interval=0.05)
        task = asyncio.create_task(worker.run())
        try:
            await asyncio.wait_for(started.wait(), timeout=5.0)
            await r.cancel(job_id)
            await asyncio.wait_for(cancelled.wait(), timeout=5.0)

            deadline = asyncio.get_event_loop().time() + 5.0
            while True:
                row = await async_conn.fetchrow("SELECT state FROM roost.jobs WHERE id = $1", job_id)
                if row is not None and row["state"] == "cancelled":
                    break
                if asyncio.get_event_loop().time() > deadline:
                    raise AssertionError(f"row never reached cancelled (state={row['state']!r})")
                await asyncio.sleep(0.05)
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5.0)


@pytest.mark.asyncio
async def test_requeue_discarded(fresh_dsn: str, async_conn: asyncpg.Connection) -> None:
    async with AsyncRoost(fresh_dsn) as r:
        for _ in range(3):
            await repo.enqueue_async(async_conn, task="x")
        await async_conn.execute("UPDATE roost.jobs SET state = 'discarded'")
        n = await r.requeue_discarded()
        assert n == 3
        row = await async_conn.fetchrow("SELECT COUNT(*) AS n FROM roost.jobs WHERE state = 'available'")
        assert row is not None and row["n"] == 3


@pytest.mark.asyncio
async def test_result_storage(fresh_dsn: str, async_conn: asyncpg.Connection) -> None:
    @job("compute")
    async def compute(x: int, y: int) -> dict[str, int]:
        return {"sum": x + y}

    async with AsyncRoost(fresh_dsn) as r:
        job_id = await r.enqueue(compute, args={"x": 2, "y": 3})
        worker = r.worker(concurrency=1, run_cron=False, poll_interval=0.05)
        task = asyncio.create_task(worker.run())
        try:
            deadline = asyncio.get_event_loop().time() + 5.0
            while True:
                row = await async_conn.fetchrow("SELECT state, result FROM roost.jobs WHERE id = $1", job_id)
                if row is not None and row["state"] == "completed":
                    break
                if asyncio.get_event_loop().time() > deadline:
                    raise AssertionError("job never completed")
                await asyncio.sleep(0.05)
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5.0)
    assert row is not None
    assert row["result"] == {"sum": 5}


class GreetingArgs(BaseModel):
    name: str
    times: int = 1


@pytest.mark.asyncio
async def test_pydantic_args_model_validates(fresh_dsn: str, async_conn: asyncpg.Connection) -> None:
    captured: dict[str, list[str]] = {"names": []}

    @job("greet_typed", args_model=GreetingArgs)
    async def greet_typed(name: str, times: int = 1) -> None:
        for _ in range(times):
            captured["names"].append(name)

    async with AsyncRoost(fresh_dsn) as r:
        # Pass a Pydantic model directly
        await r.enqueue(greet_typed, args=GreetingArgs(name="ada", times=2))
        worker = r.worker(concurrency=1, run_cron=False, poll_interval=0.05)
        task = asyncio.create_task(worker.run())
        try:
            deadline = asyncio.get_event_loop().time() + 5.0
            while len(captured["names"]) < 2:
                if asyncio.get_event_loop().time() > deadline:
                    raise AssertionError(f"only got {captured}")
                await asyncio.sleep(0.05)
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5.0)
    assert captured["names"] == ["ada", "ada"]


@pytest.mark.asyncio
async def test_pydantic_args_model_rejects_invalid(fresh_dsn: str) -> None:
    @job("greet_strict", args_model=GreetingArgs)
    async def greet_strict(name: str, times: int = 1) -> None:  # pragma: no cover
        ...

    async with AsyncRoost(fresh_dsn) as r:
        # `name` missing — validation must reject at handler-call time.
        job_id = await r.enqueue("greet_strict", args={"times": 1}, max_attempts=1)
        worker = r.worker(concurrency=1, run_cron=False, poll_interval=0.05)
        task = asyncio.create_task(worker.run())
        try:
            deadline = asyncio.get_event_loop().time() + 5.0
            async with (await r._ensure_pool()).acquire() as conn:  # noqa: SLF001
                while True:
                    row = await conn.fetchrow("SELECT state, errors FROM roost.jobs WHERE id = $1", job_id)
                    if row is not None and row["state"] == "discarded":
                        break
                    if asyncio.get_event_loop().time() > deadline:
                        raise AssertionError("job never discarded")
                    await asyncio.sleep(0.05)
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5.0)
    assert "ValidationError" in row["errors"][0]["error"] or "name" in row["errors"][0]["error"]


@pytest.mark.asyncio
async def test_workers_listing(fresh_dsn: str) -> None:
    async with AsyncRoost(fresh_dsn) as r:
        worker = Worker(
            fresh_dsn,
            queues=["default"],
            concurrency=1,
            run_cron=False,
            poll_interval=0.05,
            heartbeat_interval=0.05,
        )
        task = asyncio.create_task(worker.run())
        try:
            deadline = asyncio.get_event_loop().time() + 5.0
            while True:
                rows = await r.list_workers()
                if any(w["id"] == worker.id for w in rows):
                    break
                if asyncio.get_event_loop().time() > deadline:
                    raise AssertionError("worker never registered")
                await asyncio.sleep(0.05)
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5.0)
