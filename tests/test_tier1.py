"""Tier 1 tests: per-task defaults, cron timezone, wait_for, testing helpers."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import asyncpg
import pytest

from roost import AsyncRoost, JobFailed, JobOutcome, JobTimeoutError, Worker, job
from roost._core import repo
from roost._core.cron import CronEntry
from roost.testing import drain_pending, run_inline

# ---------------------------------------------------------------------------
# Per-task defaults
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_job_defaults_apply_when_caller_omits(fresh_dsn: str, async_conn: asyncpg.Connection) -> None:
    @job("ranked", queue="hot", priority=-5, tags=["billing"], timeout_seconds=42)
    async def ranked() -> None: ...

    async with AsyncRoost(fresh_dsn) as r:
        job_id = await r.enqueue(ranked)
    row = await async_conn.fetchrow(
        "SELECT queue, priority, tags, timeout_seconds FROM roost.jobs WHERE id = $1", job_id
    )
    assert row is not None
    assert row["queue"] == "hot"
    assert row["priority"] == -5
    assert list(row["tags"]) == ["billing"]
    assert row["timeout_seconds"] == 42


@pytest.mark.asyncio
async def test_explicit_kwargs_override_defaults(fresh_dsn: str, async_conn: asyncpg.Connection) -> None:
    @job("typed", queue="hot", priority=-5)
    async def typed() -> None: ...

    async with AsyncRoost(fresh_dsn) as r:
        job_id = await r.enqueue(typed, queue="cold", priority=99)
    row = await async_conn.fetchrow("SELECT queue, priority FROM roost.jobs WHERE id = $1", job_id)
    assert row is not None
    assert row["queue"] == "cold"
    assert row["priority"] == 99


# ---------------------------------------------------------------------------
# Cron timezone
# ---------------------------------------------------------------------------


def test_cron_timezone_returns_utc_aware_datetimes() -> None:
    # 9am Mon–Fri Pacific.
    entry = CronEntry(
        name="daily_la_morning",
        expression="0 9 * * 1-5",
        task="t",
        timezone_name="America/Los_Angeles",
    )
    # Pick a known weekday afternoon UTC.
    now = datetime(2026, 5, 6, 18, 30, tzinfo=timezone.utc)  # Wed 18:30 UTC = 11:30 LA
    nxt = entry.next_after(now)
    prev = entry.previous_or_at(now)

    assert nxt.tzinfo == timezone.utc
    assert prev.tzinfo == timezone.utc
    # 9am LA = 16:00 UTC during DST. Previous must be the same morning, next must be next weekday.
    assert prev <= now < nxt
    assert prev.hour in {16, 17}  # 16 in PDT, 17 in PST
    assert nxt.hour in {16, 17}


def test_cron_no_timezone_is_utc() -> None:
    entry = CronEntry(name="hourly", expression="0 * * * *", task="t")
    nxt = entry.next_after(datetime(2026, 5, 6, 12, 30, tzinfo=timezone.utc))
    assert nxt == datetime(2026, 5, 6, 13, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# wait_for_async
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_wait_for_returns_result_on_completion(fresh_dsn: str) -> None:
    @job("compute_sum")
    async def compute_sum(x: int, y: int) -> dict[str, int]:
        return {"sum": x + y}

    async with AsyncRoost(fresh_dsn) as r:
        job_id = await r.enqueue(compute_sum, args={"x": 7, "y": 8})
        worker = r.worker(concurrency=1, run_cron=False, poll_interval=0.05)
        task = asyncio.create_task(worker.run())
        try:
            outcome = await r.wait_for(job_id, timeout=5.0)
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5.0)

    assert isinstance(outcome, JobOutcome)
    assert outcome.state == "completed"
    assert outcome.result == {"sum": 15}


@pytest.mark.asyncio
async def test_wait_for_raises_on_discarded(fresh_dsn: str) -> None:
    @job("always_dies")
    async def always_dies() -> None:
        raise RuntimeError("boom")

    async with AsyncRoost(fresh_dsn) as r:
        job_id = await r.enqueue(always_dies, max_attempts=1)
        worker = r.worker(concurrency=1, run_cron=False, poll_interval=0.05)
        task = asyncio.create_task(worker.run())
        try:
            with pytest.raises(JobFailed) as excinfo:
                await r.wait_for(job_id, timeout=5.0)
            assert excinfo.value.state == "discarded"
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5.0)


@pytest.mark.asyncio
async def test_wait_for_times_out(fresh_dsn: str) -> None:
    @job("never_runs")
    async def never_runs() -> None: ...

    async with AsyncRoost(fresh_dsn) as r:
        job_id = await r.enqueue(never_runs)
        # No worker is started — the job stays available forever.
        with pytest.raises(JobTimeoutError):
            await r.wait_for(job_id, timeout=0.5)


# ---------------------------------------------------------------------------
# roost.testing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_inline_runs_one_job(async_conn: asyncpg.Connection) -> None:
    captured: list[int] = []

    @job("collect")
    async def collect(x: int) -> None:
        captured.append(x)

    job_id = await repo.enqueue_async(async_conn, task="collect", args={"x": 7})
    ran = await run_inline(async_conn)
    assert ran == job_id
    assert captured == [7]
    row = await async_conn.fetchrow("SELECT state FROM roost.jobs WHERE id = $1", job_id)
    assert row is not None
    assert row["state"] == "completed"


@pytest.mark.asyncio
async def test_drain_pending_runs_all(async_conn: asyncpg.Connection) -> None:
    captured: list[int] = []

    @job("drain_each")
    async def drain_each(idx: int) -> None:
        captured.append(idx)

    for i in range(5):
        await repo.enqueue_async(async_conn, task="drain_each", args={"idx": i})
    n = await drain_pending(async_conn, max_jobs=10)
    assert n == 5
    assert sorted(captured) == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_run_inline_returns_none_when_empty(async_conn: asyncpg.Connection) -> None:
    ran = await run_inline(async_conn)
    assert ran is None


# ---------------------------------------------------------------------------
# Smoke that the worker loop still respects defaults end-to-end
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_worker_uses_task_default_timeout(fresh_dsn: str, async_conn: asyncpg.Connection) -> None:
    @job("auto_timeout", timeout_seconds=1, max_attempts=1)
    async def auto_timeout() -> None:
        await asyncio.sleep(60)

    async with AsyncRoost(fresh_dsn) as r:
        job_id = await r.enqueue(auto_timeout)  # no explicit timeout — picks up default
        worker = Worker(fresh_dsn, concurrency=1, run_cron=False, poll_interval=0.05)
        task = asyncio.create_task(worker.run())
        try:
            deadline = asyncio.get_event_loop().time() + 10.0
            while True:
                row = await async_conn.fetchrow("SELECT state FROM roost.jobs WHERE id = $1", job_id)
                if row and row["state"] == "discarded":
                    break
                if asyncio.get_event_loop().time() > deadline:
                    raise AssertionError("timeout never fired")
                await asyncio.sleep(0.1)
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5.0)
