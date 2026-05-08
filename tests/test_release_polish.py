"""Pre-release polish: doctor, run_once, error codes, tasks introspection."""

from __future__ import annotations

import asyncio

import asyncpg
import pytest
from typer.testing import CliRunner

from roost import (
    AsyncRoost,
    DuplicateUniqueJobError,
    JobFailed,
    JobNotFoundError,
    JobTimeoutError,
    RoostError,
    SnoozeJob,
    UnknownTaskError,
    Worker,
    WorkerShutdown,
    job,
    tasks,
)
from roost._core import repo
from roost._core.doctor import run_checks_async
from roost.cli import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# Stable error codes
# ---------------------------------------------------------------------------


def test_every_error_has_a_stable_code() -> None:
    expected = {
        RoostError: "roost.error",
        UnknownTaskError: "roost.unknown-task",
        DuplicateUniqueJobError: "roost.duplicate-unique-job",
        JobNotFoundError: "roost.job-not-found",
        WorkerShutdown: "roost.worker-shutdown",
        SnoozeJob: "roost.snooze-job",
        JobFailed: "roost.job-failed",
        JobTimeoutError: "roost.job-timeout",
    }
    for cls, code in expected.items():
        assert cls.code == code, f"{cls.__name__} code drifted: {cls.code!r}"


# ---------------------------------------------------------------------------
# tasks introspection
# ---------------------------------------------------------------------------


def test_tasks_list_returns_registered_specs() -> None:
    @job("introspect_demo", queue="emails", priority=-1, max_attempts=7)
    async def introspect_demo() -> None: ...

    by_name = {spec.name: spec for spec in tasks.specs()}
    spec = by_name["introspect_demo"]
    assert spec.is_async is True
    assert spec.defaults.queue == "emails"
    assert spec.defaults.priority == -1
    assert spec.defaults.max_attempts == 7
    assert "introspect_demo" in tasks.names()
    assert tasks.get("introspect_demo") is spec
    assert tasks.get("nope") is None


# ---------------------------------------------------------------------------
# doctor
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_doctor_passes_on_clean_db(async_conn: asyncpg.Connection) -> None:
    # async_conn fixture already migrates to latest. With no workers running,
    # the workers check should fail; everything else passes.
    checks = await run_checks_async(async_conn, worker_stale_after_seconds=60)
    by_name = {c.name: c for c in checks}
    assert by_name["migrations"].ok is True
    assert by_name["notify_triggers"].ok is True
    # workers absent → not ok
    assert by_name["workers"].ok is False


@pytest.mark.asyncio
async def test_doctor_reports_worker_heartbeat(async_conn: asyncpg.Connection) -> None:
    await repo.heartbeat_async(async_conn, worker_id="w-1", hostname="h", pid=1, queues=["default"])
    checks = await run_checks_async(async_conn, worker_stale_after_seconds=60)
    by_name = {c.name: c for c in checks}
    assert by_name["workers"].ok is True
    assert "1/1" in by_name["workers"].detail


def test_doctor_cli_command(fresh_dsn: str) -> None:
    result = runner.invoke(app, ["doctor", "--dsn", fresh_dsn])
    # No workers running, so it should exit non-zero with the workers check failing.
    assert result.exit_code == 1
    assert "migrations" in result.stdout
    assert "notify_triggers" in result.stdout
    assert "workers" in result.stdout


# ---------------------------------------------------------------------------
# Worker.run_once() and roost run --once
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_worker_run_once_drains_and_exits(fresh_dsn: str, async_conn: asyncpg.Connection) -> None:
    captured: list[int] = []

    @job("run_once_drain")
    async def run_once_drain(idx: int) -> None:
        captured.append(idx)

    async with AsyncRoost(fresh_dsn) as r:
        for i in range(5):
            await r.enqueue(run_once_drain, args={"idx": i})

    worker = Worker(fresh_dsn, queues=["default"], concurrency=4, run_cron=False)
    processed = await worker.run_once()
    assert processed == 5
    assert sorted(captured) == [0, 1, 2, 3, 4]

    rows = await async_conn.fetch("SELECT state FROM roost.jobs")
    assert all(r["state"] == "completed" for r in rows)


@pytest.mark.asyncio
async def test_run_once_returns_zero_when_nothing_queued(fresh_dsn: str) -> None:
    worker = Worker(fresh_dsn, queues=["default"], concurrency=2, run_cron=False)
    processed = await asyncio.wait_for(worker.run_once(), timeout=5.0)
    assert processed == 0


# ---------------------------------------------------------------------------
# explain_job_async
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_explain_flags_paused_queue(async_conn: asyncpg.Connection) -> None:
    job_id = await repo.enqueue_async(async_conn, task="t", queue="paused-q")
    await repo.pause_queue_async(async_conn, "paused-q")
    explain = await repo.explain_job_async(async_conn, job_id)
    assert explain["found"] is True
    assert explain["queue_paused"] is True
    assert explain["scheduled_in_future"] is False
    assert explain["waiting_on_parents"] == []


@pytest.mark.asyncio
async def test_explain_flags_future_scheduled(async_conn: asyncpg.Connection) -> None:
    from datetime import datetime, timedelta, timezone

    when = datetime.now(tz=timezone.utc) + timedelta(hours=1)
    job_id = await repo.enqueue_async(async_conn, task="t", scheduled_at=when)
    explain = await repo.explain_job_async(async_conn, job_id)
    assert explain["scheduled_in_future"] is True


@pytest.mark.asyncio
async def test_explain_flags_pending_parents(async_conn: asyncpg.Connection) -> None:
    parent = await repo.enqueue_async(async_conn, task="p")
    child = await repo.enqueue_async(async_conn, task="c", depends_on=[parent])
    explain = await repo.explain_job_async(async_conn, child)
    assert explain["waiting_on_parents"] == [parent]

    # Once parent completes, no more waiting.
    await async_conn.execute(
        "UPDATE roost.jobs SET state='completed', completed_at = now() WHERE id = $1", parent
    )
    explain = await repo.explain_job_async(async_conn, child)
    assert explain["waiting_on_parents"] == []


@pytest.mark.asyncio
async def test_explain_returns_not_found(async_conn: asyncpg.Connection) -> None:
    explain = await repo.explain_job_async(async_conn, 99999)
    assert explain == {"found": False}
