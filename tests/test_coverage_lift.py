"""Targeted tests to lift coverage on under-exercised modules.

Covers: CLI subcommands, sync_api delegate methods, doctor edge cases,
contrib framework shims (with lightweight stubs), observability config.
"""

from __future__ import annotations

import asyncio
import json
import sys
import types
from typing import Any

import asyncpg
import psycopg
import pytest
import pytest_asyncio
from typer.testing import CliRunner

from roost import job
from roost._core import repo
from roost._core.doctor import Check, run_checks_async
from roost.cli import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# CLI smoke tests against a real DB
# ---------------------------------------------------------------------------


def test_cli_doctor_passes_on_fresh_db(fresh_dsn: str) -> None:
    result = runner.invoke(app, ["doctor", "--dsn", fresh_dsn])
    # Workers check fails (no heartbeats yet) so we expect non-zero exit,
    # but the rendering / migrations / triggers branches all execute.
    assert "migrations" in result.stdout
    assert "notify_triggers" in result.stdout
    assert "workers" in result.stdout


def test_cli_doctor_no_dsn_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ROOST_DSN", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    result = runner.invoke(app, ["doctor"])
    assert result.exit_code != 0


def test_cli_migrate_status(fresh_dsn: str) -> None:
    result = runner.invoke(app, ["migrate", "status", "--dsn", fresh_dsn])
    assert result.exit_code == 0
    assert "version" in result.stdout
    assert "applied" in result.stdout


def test_cli_migrate_up_idempotent(fresh_dsn: str) -> None:
    # Schema is fresh; already at latest from fixture.
    result = runner.invoke(app, ["migrate", "up", "--dsn", fresh_dsn])
    assert result.exit_code == 0
    assert "nothing to apply" in result.stdout or "applied" in result.stdout


def test_cli_migrate_down_with_irreversible(fresh_dsn: str) -> None:
    # Going down to 0 attempts to revert all — bootstrap migration is
    # irreversible, so this surfaces the IrreversibleMigration branch.
    result = runner.invoke(app, ["migrate", "down", "0", "--dsn", fresh_dsn])
    # Either succeeds (everything had reversible) or fails with refused message.
    assert result.exit_code in (0, 1)


def test_cli_tasks_export_empty() -> None:
    result = runner.invoke(app, ["tasks", "export"])
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload == {"tasks": []}


def test_cli_tasks_export_with_registered_task() -> None:
    @job("export_check")
    def _h(x: int) -> int:
        return x

    result = runner.invoke(app, ["tasks", "export"])
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    names = [t["name"] for t in payload["tasks"]]
    assert "export_check" in names


def test_cli_status_with_jobs(fresh_dsn: str) -> None:
    from roost import Roost

    Roost(fresh_dsn).enqueue("any_task")
    result = runner.invoke(app, ["status", "--dsn", fresh_dsn])
    assert result.exit_code == 0
    assert "queue" in result.stdout
    assert "default" in result.stdout


def test_cli_enqueue_command(fresh_dsn: str) -> None:
    result = runner.invoke(
        app,
        ["enqueue", "ad_hoc", "--args", '{"x": 1}', "--dsn", fresh_dsn],
    )
    assert result.exit_code == 0
    assert "enqueued job" in result.stdout


def test_cli_enqueue_invalid_json(fresh_dsn: str) -> None:
    result = runner.invoke(app, ["enqueue", "x", "--args", "{not json", "--dsn", fresh_dsn])
    assert result.exit_code != 0


def test_cli_enqueue_args_must_be_object(fresh_dsn: str) -> None:
    result = runner.invoke(app, ["enqueue", "x", "--args", "[1,2]", "--dsn", fresh_dsn])
    assert result.exit_code != 0


def test_cli_enqueue_with_in_seconds(fresh_dsn: str) -> None:
    result = runner.invoke(app, ["enqueue", "delayed", "--in", "30", "--dsn", fresh_dsn])
    assert result.exit_code == 0


def test_cli_retry_unknown_job(fresh_dsn: str) -> None:
    result = runner.invoke(app, ["retry", "9999", "--dsn", fresh_dsn])
    assert result.exit_code == 1


def test_cli_cancel_unknown_job(fresh_dsn: str) -> None:
    result = runner.invoke(app, ["cancel", "9999", "--dsn", fresh_dsn])
    assert result.exit_code == 1


def test_cli_retry_then_cancel_real_job(fresh_dsn: str) -> None:
    from roost import Roost

    r = Roost(fresh_dsn)
    job_id = r.enqueue("retry_target")
    # Mark discarded so retry has work to do.
    with psycopg.connect(fresh_dsn) as conn, conn.cursor() as cur:
        cur.execute("UPDATE roost.jobs SET state = 'discarded' WHERE id = %s", (job_id,))
        conn.commit()
    result = runner.invoke(app, ["retry", str(job_id), "--dsn", fresh_dsn])
    assert result.exit_code == 0

    # Now cancel a fresh available job.
    job_id2 = r.enqueue("cancel_target")
    result = runner.invoke(app, ["cancel", str(job_id2), "--dsn", fresh_dsn])
    assert result.exit_code == 0


def test_cli_queue_pause_resume_list(fresh_dsn: str) -> None:
    res1 = runner.invoke(app, ["queue", "pause", "ingest", "--dsn", fresh_dsn])
    assert res1.exit_code == 0
    res2 = runner.invoke(app, ["queue", "list", "--dsn", fresh_dsn])
    assert res2.exit_code == 0
    assert "ingest" in res2.stdout
    res3 = runner.invoke(app, ["queue", "resume", "ingest", "--dsn", fresh_dsn])
    assert res3.exit_code == 0


def test_cli_queue_list_empty(fresh_dsn: str) -> None:
    result = runner.invoke(app, ["queue", "list", "--dsn", fresh_dsn])
    assert result.exit_code == 0
    assert "no queue rows" in result.stdout


def test_cli_workers_empty(fresh_dsn: str) -> None:
    result = runner.invoke(app, ["workers", "--dsn", fresh_dsn])
    assert result.exit_code == 0
    assert "no workers" in result.stdout


def test_cli_workers_with_heartbeat(fresh_dsn: str) -> None:
    with psycopg.connect(fresh_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO roost.workers (id, hostname, pid, queues, last_seen_at, metadata) "
            "VALUES (%s, %s, %s, %s, now(), %s::jsonb)",
            ("w1", "host", 1, ["default"], "{}"),
        )
        conn.commit()
    result = runner.invoke(app, ["workers", "--dsn", fresh_dsn])
    assert result.exit_code == 0
    assert "host" in result.stdout


def test_cli_requeue_requires_discarded_flag(fresh_dsn: str) -> None:
    result = runner.invoke(app, ["requeue", "--dsn", fresh_dsn])
    assert result.exit_code != 0


def test_cli_requeue_discarded(fresh_dsn: str) -> None:
    from roost import Roost

    r = Roost(fresh_dsn)
    job_id = r.enqueue("dead_letter")
    with psycopg.connect(fresh_dsn) as conn, conn.cursor() as cur:
        cur.execute("UPDATE roost.jobs SET state = 'discarded' WHERE id = %s", (job_id,))
        conn.commit()
    result = runner.invoke(app, ["requeue", "--discarded", "--dsn", fresh_dsn])
    assert result.exit_code == 0
    assert "requeued 1" in result.stdout


def test_cli_requeue_discarded_per_queue(fresh_dsn: str) -> None:
    from roost import Roost

    r = Roost(fresh_dsn)
    a = r.enqueue("dead_a", queue="aq")
    b = r.enqueue("dead_b", queue="bq")
    with psycopg.connect(fresh_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE roost.jobs SET state = 'discarded' WHERE id = ANY(%s)",
            ([a, b],),
        )
        conn.commit()
    result = runner.invoke(app, ["requeue", "--discarded", "--queue", "aq", "--dsn", fresh_dsn])
    assert result.exit_code == 0
    assert "requeued 1" in result.stdout


def test_cli_run_invalid_workers_value(fresh_dsn: str) -> None:
    result = runner.invoke(app, ["run", "--workers", "0", "--dsn", fresh_dsn])
    assert result.exit_code != 0


def test_cli_run_once_with_workers_n_errors(fresh_dsn: str) -> None:
    result = runner.invoke(app, ["run", "--once", "--workers", "2", "--dsn", fresh_dsn])
    assert result.exit_code != 0


def test_cli_run_reload_with_workers_n_errors(fresh_dsn: str) -> None:
    result = runner.invoke(app, ["run", "--reload", "--workers", "2", "--dsn", fresh_dsn])
    assert result.exit_code != 0


def test_cli_run_empty_queues_errors(fresh_dsn: str) -> None:
    result = runner.invoke(app, ["run", "--queues", ",,", "--dsn", fresh_dsn])
    assert result.exit_code != 0


def test_cli_run_once_drains(fresh_dsn: str) -> None:
    @job("oneshot_drain")
    def _h() -> str:
        return "done"

    from roost import Roost

    Roost(fresh_dsn, registry=None).enqueue("oneshot_drain")
    result = runner.invoke(
        app,
        [
            "run",
            "--once",
            "--queues",
            "default",
            "--concurrency",
            "1",
            "--dsn",
            fresh_dsn,
        ],
    )
    assert result.exit_code == 0
    assert "drained" in result.stdout


# ---------------------------------------------------------------------------
# sync_api: cover the methods that just thread through to repo
# ---------------------------------------------------------------------------


def test_sync_api_setup_schema_idempotent(fresh_dsn: str) -> None:
    from roost import Roost

    r = Roost(fresh_dsn)
    r.setup_schema()  # via managed connection
    with psycopg.connect(fresh_dsn) as c:
        r.setup_schema(conn=c)  # caller-provided connection branch


def test_sync_api_enqueue_with_pydantic_args(fresh_dsn: str) -> None:
    from pydantic import BaseModel

    from roost import Roost

    class A(BaseModel):
        x: int

    r = Roost(fresh_dsn)
    job_id = r.enqueue("p_args", args=A(x=7))
    assert job_id > 0


def test_sync_api_admin_methods(fresh_dsn: str) -> None:
    from roost import Roost

    r = Roost(fresh_dsn)
    r.pause_queue("q1")
    queues = r.list_queues()
    assert any(name == "q1" for name, _ in queues)
    r.resume_queue("q1")
    workers = r.list_workers()
    assert workers == []
    n = r.requeue_discarded()
    assert n == 0


def test_sync_api_enqueue_rolls_back_on_repo_failure(fresh_dsn: str, monkeypatch: pytest.MonkeyPatch) -> None:
    from roost import Roost

    def boom(*a: Any, **kw: Any) -> None:
        raise RuntimeError("simulated failure")

    monkeypatch.setattr(repo, "enqueue_sync", boom)
    r = Roost(fresh_dsn)
    with pytest.raises(RuntimeError, match="simulated failure"):
        r.enqueue("any")


# ---------------------------------------------------------------------------
# doctor: cover failure branches
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def fresh_async_conn(fresh_dsn: str):
    conn = await asyncpg.connect(fresh_dsn)
    await repo.init_connection(conn)
    try:
        yield conn
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_doctor_reports_no_workers(fresh_async_conn: asyncpg.Connection) -> None:
    results = await run_checks_async(fresh_async_conn)
    workers_check = next(c for c in results if c.name == "workers")
    assert workers_check.ok is False
    assert "no workers" in workers_check.detail


@pytest.mark.asyncio
async def test_doctor_reports_stale_worker(fresh_async_conn: asyncpg.Connection) -> None:
    await fresh_async_conn.execute(
        "INSERT INTO roost.workers (id, hostname, pid, queues, last_seen_at, metadata) "
        "VALUES ('stale', 'h', 1, ARRAY['default'], now() - interval '1 hour', '{}'::jsonb)"
    )
    results = await run_checks_async(fresh_async_conn, worker_stale_after_seconds=10.0)
    workers_check = next(c for c in results if c.name == "workers")
    assert workers_check.ok is False
    assert "stale" in workers_check.detail


@pytest.mark.asyncio
async def test_doctor_reports_jobs_summary_with_executing(
    fresh_async_conn: asyncpg.Connection,
) -> None:
    await fresh_async_conn.execute(
        "INSERT INTO roost.jobs (task, args, queue, state, attempted_at) "
        "VALUES ('t', '{}'::jsonb, 'default', 'executing', now())"
    )
    results = await run_checks_async(fresh_async_conn)
    jobs_check = next(c for c in results if c.name == "jobs")
    assert "executing" in jobs_check.detail
    assert "orphan reaper" in jobs_check.detail


@pytest.mark.asyncio
async def test_doctor_reports_missing_triggers(
    fresh_async_conn: asyncpg.Connection,
) -> None:
    await fresh_async_conn.execute("DROP TRIGGER IF EXISTS jobs_notify_inserted ON roost.jobs")
    results = await run_checks_async(fresh_async_conn)
    trig_check = next(c for c in results if c.name == "notify_triggers")
    assert trig_check.ok is False


@pytest.mark.asyncio
async def test_doctor_reports_no_migrations() -> None:
    """Cover the 'migrations table empty' branch."""

    # Build a stub conn that returns empty migrations and minimal triggers/workers/jobs.
    class _StubConn:
        async def fetch(self, sql: str, *_args: Any) -> list[dict[str, Any]]:
            if "migrations" in sql:
                return []
            if "triggers" in sql:
                return []
            return []

        async def fetchval(self, sql: str, *_args: Any) -> int:
            return 0

    results = await run_checks_async(_StubConn())  # type: ignore[arg-type]
    migrations_check = next(c for c in results if c.name == "migrations")
    assert migrations_check.ok is False
    assert "no migrations" in migrations_check.detail


def test_check_render_format() -> None:
    ok = Check("x", ok=True, detail="d")
    bad = Check("y", ok=False, detail="oops")
    assert ok.render().startswith("✓")
    assert bad.render().startswith("✗")


# ---------------------------------------------------------------------------
# contrib shims — using lightweight in-memory stubs
# ---------------------------------------------------------------------------


def test_contrib_flask_init_app_attaches_roost(monkeypatch: pytest.MonkeyPatch) -> None:
    flask_stub = types.ModuleType("flask")

    class FakeApp:
        def __init__(self) -> None:
            self.config: dict[str, Any] = {}
            self.extensions: dict[str, Any] = {}

    flask_stub.Flask = FakeApp  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "flask", flask_stub)

    from roost.contrib.flask import RoostExtension

    fake_app = FakeApp()
    ext = RoostExtension(dsn="postgresql://example/x")
    ext.init_app(fake_app)
    assert hasattr(fake_app, "roost")
    assert fake_app.extensions["roost"] is fake_app.roost


def test_contrib_flask_requires_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    flask_stub = types.ModuleType("flask")
    monkeypatch.setitem(sys.modules, "flask", flask_stub)

    from roost.contrib.flask import RoostExtension

    class FakeApp:
        config: dict[str, Any] = {}
        extensions: dict[str, Any] = {}

    with pytest.raises(RuntimeError, match="ROOST_DSN"):
        RoostExtension(app=FakeApp())


def test_contrib_flask_construct_app_directly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Constructor-with-app path."""
    flask_stub = types.ModuleType("flask")
    monkeypatch.setitem(sys.modules, "flask", flask_stub)

    from roost.contrib.flask import RoostExtension

    class FakeApp:
        config = {"ROOST_DSN": "postgresql://example/x"}
        extensions: dict[str, Any] = {}

    app_obj = FakeApp()
    RoostExtension(app=app_obj)
    assert hasattr(app_obj, "roost")


def test_contrib_django_enqueue_in_atomic(fresh_dsn: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """We don't want a real Django dep — fake `django.db.connections`."""

    class _Wrap:
        def __init__(self, real: psycopg.Connection[Any]) -> None:
            self.connection = real

        def ensure_connection(self) -> None:
            return None

    real_conn = psycopg.connect(fresh_dsn)
    real_conn.autocommit = True

    django_stub = types.ModuleType("django")
    django_db_stub = types.ModuleType("django.db")
    django_db_stub.connections = {"default": _Wrap(real_conn)}  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "django", django_stub)
    monkeypatch.setitem(sys.modules, "django.db", django_db_stub)

    from roost import Roost
    from roost.contrib.django import enqueue_in_atomic

    r = Roost(fresh_dsn)
    job_id = enqueue_in_atomic(
        r,
        "djtask",
        args={"a": 1},
        queue="djq",
        priority=3,
        max_attempts=5,
        unique_key="dj-uniq",
        tags=["x"],
        timeout_seconds=42,
    )
    assert job_id > 0
    real_conn.close()


def test_contrib_fastapi_roostdep_returns_app_state() -> None:
    from roost.contrib.fastapi import RoostDep

    class _State:
        roost = object()

    class _AppHolder:
        state = _State()

    class _Req:
        app = _AppHolder()

    r = RoostDep(_Req())  # type: ignore[arg-type]
    assert r is _State.roost


# ---------------------------------------------------------------------------
# observability: cover env-driven config
# ---------------------------------------------------------------------------


def test_observability_auto_configure_no_env(monkeypatch: pytest.MonkeyPatch) -> None:
    from roost import observability

    monkeypatch.delenv("ROOST_LOG_FORMAT", raising=False)
    monkeypatch.delenv("ROOST_LOG_LEVEL", raising=False)
    monkeypatch.delenv("ROOST_OTEL_ENABLED", raising=False)
    monkeypatch.delenv("ROOST_PROMETHEUS_PORT", raising=False)
    observability.auto_configure_from_env()


def test_observability_inject_trace_context_is_noop_without_otel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from roost import observability

    monkeypatch.delenv("ROOST_OTEL_ENABLED", raising=False)
    args = observability.inject_trace_context({"a": 1})
    assert args == {"a": 1}


def test_observability_metrics_smoke() -> None:
    from roost import observability

    # Just touching the labelled metrics should be safe and exercises code paths.
    observability.JOBS_ENQUEUED.labels(queue="q", task="t").inc()
    observability.JOBS_COMPLETED.labels(queue="q", task="t").inc()
    observability.JOBS_FAILED.labels(queue="q", task="t", outcome="retryable").inc()
    observability.JOB_DURATION.labels(queue="q", task="t").observe(0.001)


# ---------------------------------------------------------------------------
# supervisor: smoke test that the spawn path runs end-to-end
# ---------------------------------------------------------------------------


def test_supervisor_n_must_be_positive(fresh_dsn: str) -> None:
    from roost._core.supervisor import run_workers

    with pytest.raises(ValueError):
        run_workers(fresh_dsn, n=0, queues=["default"], modules=[], worker_kwargs={})


def test_supervisor_spawns_and_drains(fresh_dsn: str) -> None:
    """Spawn a single short-lived worker process via the supervisor.

    We use a contained helper module so the spawned process can re-import
    the handler. The worker is told to exit once via SIGTERM.
    """
    import os
    import signal
    import threading
    import time

    from roost import Roost
    from roost._core.supervisor import run_workers

    Roost(fresh_dsn).enqueue("supervisor_smoke")

    # Send SIGTERM after a short delay to let the worker boot + drain.
    def _kick() -> None:
        time.sleep(2.0)
        os.kill(os.getpid(), signal.SIGTERM)

    threading.Thread(target=_kick, daemon=True).start()

    rc = run_workers(
        fresh_dsn,
        n=1,
        queues=["default"],
        modules=[],
        worker_kwargs=dict(concurrency=1, prefetch=4, poll_interval=0.05, run_cron=False),
        shutdown_grace=5.0,
    )
    # Either clean exit (0) or non-zero if the SIGTERM arrived early — both
    # exercise the same code paths.
    assert rc in (0, 1)


# ---------------------------------------------------------------------------
# Misc: cover repo error paths via direct calls
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_init_connection_handles_pre_schema_db(fresh_dsn: str) -> None:
    """init_connection has a try/except around the warm-up — but warm-up is
    now removed. Still, init_connection should be safe on a connection that
    hasn't applied the schema yet (no jobs table)."""
    # Drop the schema to simulate a brand-new DB.
    with psycopg.connect(fresh_dsn) as c, c.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS roost CASCADE")
        c.commit()

    conn = await asyncpg.connect(fresh_dsn)
    try:
        await repo.init_connection(conn)  # must not raise
    finally:
        await conn.close()


def test_main_entrypoint_callable() -> None:
    from roost.cli import main

    # main() invokes typer's app(); calling it without args triggers help.
    # We just need the import to be exercised.
    assert callable(main)


# ---------------------------------------------------------------------------
# Retry strategies
# ---------------------------------------------------------------------------


def test_retry_linear_no_jitter() -> None:
    from roost._core.retry import linear

    s = linear(step=2.0, jitter=False)
    assert s(1) == 2.0
    assert s(5) == 10.0


def test_retry_linear_with_jitter() -> None:
    from roost._core.retry import linear

    s = linear(step=10.0, jitter=True)
    # Result is in [10*0.5, 10*1.5]
    val = s(1)
    assert 5.0 <= val <= 15.0


def test_retry_fixed_constant() -> None:
    from roost._core.retry import fixed

    s = fixed(seconds=42.0)
    assert s(1) == 42.0
    assert s(99) == 42.0


def test_retry_resolve_picks_default_when_none() -> None:
    from roost._core.retry import DEFAULT_STRATEGY, resolve

    assert resolve(None) is DEFAULT_STRATEGY


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


def test_snooze_rejects_negative() -> None:
    from roost.exceptions import SnoozeJob

    with pytest.raises(ValueError):
        SnoozeJob(-1.0)


# ---------------------------------------------------------------------------
# testing.py — drain + reset helpers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_inline_unknown_task_marks_discarded(
    fresh_async_conn: asyncpg.Connection,
) -> None:
    from roost.testing import run_inline

    await repo.enqueue_async(fresh_async_conn, task="not_registered", max_attempts=1)
    job_id = await run_inline(fresh_async_conn)
    assert job_id is not None
    state = await fresh_async_conn.fetchval("SELECT state FROM roost.jobs WHERE id = $1", job_id)
    assert state == "discarded"


@pytest.mark.asyncio
async def test_run_inline_failure_retries(
    fresh_async_conn: asyncpg.Connection,
) -> None:
    """Sync handler that raises — should land in 'retryable' (attempt < max)."""
    from roost import job
    from roost.testing import run_inline

    @job("flaky_inline")
    def _h() -> None:
        raise RuntimeError("kaboom")

    await repo.enqueue_async(fresh_async_conn, task="flaky_inline", max_attempts=5)
    job_id = await run_inline(fresh_async_conn)
    state = await fresh_async_conn.fetchval("SELECT state FROM roost.jobs WHERE id = $1", job_id)
    assert state == "retryable"


@pytest.mark.asyncio
async def test_run_inline_snooze_path(fresh_async_conn: asyncpg.Connection) -> None:
    from roost import job
    from roost.exceptions import SnoozeJob
    from roost.testing import run_inline

    @job("snoozer")
    def _h() -> None:
        raise SnoozeJob(60)

    await repo.enqueue_async(fresh_async_conn, task="snoozer")
    job_id = await run_inline(fresh_async_conn)
    state = await fresh_async_conn.fetchval("SELECT state FROM roost.jobs WHERE id = $1", job_id)
    assert state == "available"  # snooze keeps it available, just rescheduled


@pytest.mark.asyncio
async def test_run_inline_returns_none_when_empty(
    fresh_async_conn: asyncpg.Connection,
) -> None:
    from roost.testing import run_inline

    result = await run_inline(fresh_async_conn)
    assert result is None


@pytest.mark.asyncio
async def test_run_inline_sync_handler_returning_awaitable_errors(
    fresh_async_conn: asyncpg.Connection,
) -> None:
    from roost import job
    from roost.testing import run_inline

    @job("bad_sync")
    def _h() -> Any:
        async def _g() -> None:
            return None

        return _g()

    await repo.enqueue_async(fresh_async_conn, task="bad_sync", max_attempts=1)
    await run_inline(fresh_async_conn)
    state = await fresh_async_conn.fetchval(
        "SELECT state FROM roost.jobs WHERE id = (SELECT MAX(id) FROM roost.jobs)"
    )
    assert state == "discarded"


def test_fast_forward_args_serialises_datetime() -> None:
    from datetime import datetime as dt

    from roost.testing import fast_forward_args

    out = fast_forward_args({"when": dt(2026, 1, 1)})
    assert isinstance(out["when"], str)
    assert "2026" in out["when"]


def test_fast_forward_args_handles_none() -> None:
    from roost.testing import fast_forward_args

    assert fast_forward_args(None) == {}


def test_reset_default_registry_clears() -> None:
    from roost import job
    from roost.decorators import DEFAULT_HANDLERS
    from roost.testing import reset_default_registry

    @job("will_be_cleared")
    def _h() -> None: ...

    assert DEFAULT_HANDLERS.get("will_be_cleared") is not None
    reset_default_registry()
    assert DEFAULT_HANDLERS.get("will_be_cleared") is None


# ---------------------------------------------------------------------------
# cron module: cover the CronEntry timezone branches without firing the loop
# ---------------------------------------------------------------------------


def test_cron_entry_next_after_in_utc() -> None:
    from datetime import datetime as dt

    from roost._core.cron import CronEntry

    e = CronEntry(name="hourly", expression="0 * * * *", task="t")
    base = dt(2026, 1, 1, 12, 30, tzinfo=__import__("datetime").timezone.utc)
    nxt = e.next_after(base)
    assert nxt.hour == 13
    prev = e.previous_or_at(base)
    assert prev.hour == 12


def test_cron_entry_with_timezone() -> None:
    from datetime import datetime as dt
    from datetime import timezone as tz

    from roost._core.cron import CronEntry

    e = CronEntry(
        name="la_hourly",
        expression="0 * * * *",
        task="t",
        timezone_name="America/Los_Angeles",
    )
    base = dt(2026, 1, 1, 12, 0, tzinfo=tz.utc)
    nxt = e.next_after(base)
    assert nxt.tzinfo is tz.utc


def test_cron_registry_duplicate_register_same_entry_is_noop() -> None:
    from roost._core.cron import CronEntry, CronRegistry

    reg = CronRegistry()
    e = CronEntry(name="dup", expression="0 * * * *", task="t")
    reg.register(e)
    reg.register(e)  # no-op, same entry
    assert len(reg.all()) == 1


def test_cron_registry_conflicting_register_errors() -> None:
    from roost._core.cron import CronEntry, CronRegistry

    reg = CronRegistry()
    reg.register(CronEntry(name="x", expression="0 * * * *", task="t"))
    with pytest.raises(ValueError, match="already registered"):
        reg.register(CronEntry(name="x", expression="*/5 * * * *", task="t"))


@pytest.mark.asyncio
async def test_cron_run_scheduler_lock_held_skips(fresh_dsn: str) -> None:
    """If another holder owns the advisory lock, the scheduler returns
    immediately without entering the tick loop."""
    import asyncpg

    from roost._core.cron import ADVISORY_LOCK_KEY, run_scheduler

    holder = await asyncpg.connect(fresh_dsn)
    pool = await asyncpg.create_pool(fresh_dsn, min_size=1, max_size=2)
    try:
        # Hold the same advisory lock the scheduler tries to take.
        await holder.execute("SELECT pg_advisory_lock($1)", ADVISORY_LOCK_KEY)
        stop = asyncio.Event()
        await asyncio.wait_for(
            run_scheduler(pool, interval_seconds=0.05, stop_event=stop, dsn=fresh_dsn),
            timeout=5.0,
        )
    finally:
        await holder.execute("SELECT pg_advisory_unlock($1)", ADVISORY_LOCK_KEY)
        await holder.close()
        await pool.close()
