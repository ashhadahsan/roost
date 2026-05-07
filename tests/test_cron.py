from __future__ import annotations

from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from roost._core import repo
from roost._core.cron import ADVISORY_LOCK_KEY, CronEntry, CronRegistry, _run_once


@pytest.mark.asyncio
async def test_run_once_enqueues_due_entries(async_conn: asyncpg.Connection) -> None:
    registry = CronRegistry()
    registry.register(CronEntry(name="every_minute", expression="* * * * *", task="say_hi", queue="default"))
    await _run_once(async_conn, registry)
    rows = await async_conn.fetch("SELECT task, queue FROM roost.jobs")
    assert len(rows) == 1
    assert rows[0]["task"] == "say_hi"
    assert rows[0]["queue"] == "default"


@pytest.mark.asyncio
async def test_run_once_does_not_double_enqueue_same_slot(
    async_conn: asyncpg.Connection,
) -> None:
    registry = CronRegistry()
    registry.register(CronEntry(name="hourly", expression="0 * * * *", task="hour"))
    await _run_once(async_conn, registry)
    await _run_once(async_conn, registry)
    n = await async_conn.fetchval("SELECT COUNT(*) FROM roost.jobs")
    assert n == 1


@pytest.mark.asyncio
async def test_advisory_lock_singleton(async_pool: asyncpg.Pool) -> None:
    c1 = await async_pool.acquire()
    c2 = await async_pool.acquire()
    try:
        assert await repo.cron_try_lock_async(c1, ADVISORY_LOCK_KEY) is True
        assert await repo.cron_try_lock_async(c2, ADVISORY_LOCK_KEY) is False
        await repo.cron_unlock_async(c1, ADVISORY_LOCK_KEY)
        assert await repo.cron_try_lock_async(c2, ADVISORY_LOCK_KEY) is True
        await repo.cron_unlock_async(c2, ADVISORY_LOCK_KEY)
    finally:
        await async_pool.release(c1)
        await async_pool.release(c2)


@pytest.mark.asyncio
async def test_cron_should_run_claims_only_once(async_conn: asyncpg.Connection) -> None:
    when = datetime.now(tz=timezone.utc).replace(microsecond=0) - timedelta(seconds=10)
    first = await repo.cron_should_run_async(async_conn, "weekly", when)
    second = await repo.cron_should_run_async(async_conn, "weekly", when)
    assert first is True
    assert second is False
    later = when + timedelta(seconds=60)
    third = await repo.cron_should_run_async(async_conn, "weekly", later)
    assert third is True
