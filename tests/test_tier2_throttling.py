"""Tier 2 throttling: per-task rate limit + max concurrency."""

from __future__ import annotations

import asyncio

import asyncpg
import pytest

from roost._core import repo


@pytest.mark.asyncio
async def test_max_concurrency_gates_fetch(async_conn: asyncpg.Connection) -> None:
    # Three queued jobs, all the same task, with max_concurrency=1.
    for _ in range(3):
        await repo.enqueue_async(async_conn, task="capped")

    limits = {"capped": (None, 1)}  # rate=None, max_concurrency=1

    # First fetch should pull exactly one row even though we asked for 5.
    jobs = await repo.fetch_available_async(async_conn, ["default"], 5, task_limits=limits)
    assert len(jobs) == 1
    in_flight_id = jobs[0].id

    # Second fetch (still one executing) is blocked.
    jobs = await repo.fetch_available_async(async_conn, ["default"], 5, task_limits=limits)
    assert len(jobs) == 0

    # Mark the first one completed; next fetch resumes.
    await repo.mark_completed_async(async_conn, in_flight_id)
    jobs = await repo.fetch_available_async(async_conn, ["default"], 5, task_limits=limits)
    assert len(jobs) == 1


@pytest.mark.asyncio
async def test_max_concurrency_is_per_task(async_conn: asyncpg.Connection) -> None:
    # `capped` is throttled, `unlimited` is not — we should still pick the second.
    capped_id = await repo.enqueue_async(async_conn, task="capped")
    unlimited_id = await repo.enqueue_async(async_conn, task="unlimited")

    limits = {"capped": (None, 1)}

    jobs = await repo.fetch_available_async(async_conn, ["default"], 10, task_limits=limits)
    ids = {j.id for j in jobs}
    assert {capped_id, unlimited_id} == ids

    # Now both are 'executing'. Fetch again: capped is at limit, unlimited has no limit
    # (and no more rows). Result must be empty.
    jobs = await repo.fetch_available_async(async_conn, ["default"], 10, task_limits=limits)
    assert jobs == []


@pytest.mark.asyncio
async def test_rate_per_minute_gates_fetch(async_conn: asyncpg.Connection) -> None:
    # Backdate two attempts in the last minute. With rate=2 we hit the cap immediately.
    j1 = await repo.enqueue_async(async_conn, task="metered")
    j2 = await repo.enqueue_async(async_conn, task="metered")
    j3 = await repo.enqueue_async(async_conn, task="metered")

    await async_conn.execute(
        "UPDATE roost.jobs SET state = 'completed', completed_at = now(), "
        "attempted_at = now() - interval '5 seconds' WHERE id = ANY($1)",
        [j1, j2],
    )

    limits = {"metered": (2, None)}  # rate=2/min, no concurrency cap
    jobs = await repo.fetch_available_async(async_conn, ["default"], 5, task_limits=limits)
    # Cap is 2 in the last minute and we already have 2. j3 must not be picked.
    assert jobs == []

    # If the rate is 3/min, j3 sneaks through.
    limits = {"metered": (3, None)}
    jobs = await repo.fetch_available_async(async_conn, ["default"], 5, task_limits=limits)
    assert {j.id for j in jobs} == {j3}


@pytest.mark.asyncio
async def test_throttled_jobs_dont_leak_into_other_tasks(async_conn: asyncpg.Connection) -> None:
    a_id = await repo.enqueue_async(async_conn, task="a")
    await repo.enqueue_async(async_conn, task="a")
    b_id = await repo.enqueue_async(async_conn, task="b")

    limits = {"a": (None, 1)}
    jobs = await repo.fetch_available_async(async_conn, ["default"], 10, task_limits=limits)

    # Exactly one `a` (the lower-id one) and the single `b` should be picked.
    fetched_tasks = {j.task: j.id for j in jobs}
    assert fetched_tasks.get("a") == a_id
    assert fetched_tasks.get("b") == b_id


@pytest.mark.asyncio
async def test_no_limits_path_unchanged(async_conn: asyncpg.Connection) -> None:
    """Tasks with no limits behave as before — fetch returns everything available."""
    ids = []
    for _ in range(5):
        ids.append(await repo.enqueue_async(async_conn, task="loose"))

    jobs = await repo.fetch_available_async(async_conn, ["default"], 10)
    fetched = {j.id for j in jobs}
    assert fetched == set(ids)


@pytest.mark.asyncio
async def test_worker_respects_task_default_max_concurrency(fresh_dsn: str) -> None:
    """End-to-end: @job(max_concurrency=1) limits concurrent executions."""
    from roost import AsyncRoost, job

    in_flight = 0
    peak = 0
    lock = asyncio.Lock()
    started = asyncio.Event()
    release = asyncio.Event()

    @job("singleton", max_concurrency=1)
    async def singleton() -> None:
        nonlocal in_flight, peak
        async with lock:
            in_flight += 1
            peak = max(peak, in_flight)
        started.set()
        await release.wait()
        async with lock:
            in_flight -= 1

    async with AsyncRoost(fresh_dsn) as r:
        await r.enqueue(singleton)
        await r.enqueue(singleton)
        await r.enqueue(singleton)

        worker = r.worker(concurrency=4, run_cron=False, poll_interval=0.05)
        task = asyncio.create_task(worker.run())
        try:
            await asyncio.wait_for(started.wait(), timeout=5.0)
            await asyncio.sleep(0.4)  # confirm no second handler joined
            assert peak == 1
        finally:
            release.set()
            worker.request_stop()
            await asyncio.wait_for(task, timeout=10.0)
