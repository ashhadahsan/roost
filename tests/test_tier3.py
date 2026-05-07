"""Tier 3: metadata column, capped errors, archive table, scheduled-enqueue CLI."""

from __future__ import annotations

import asyncpg
import psycopg
import pytest
from typer.testing import CliRunner

from roost._core import repo
from roost.cli import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# metadata column
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_metadata_round_trips(async_conn: asyncpg.Connection) -> None:
    job_id = await repo.enqueue_async(
        async_conn,
        task="t",
        metadata={"trace_id": "abc-123", "tenant": "acme"},
    )
    row = await async_conn.fetchrow(
        "SELECT metadata FROM roost.jobs WHERE id = $1", job_id
    )
    assert row is not None
    assert row["metadata"] == {"trace_id": "abc-123", "tenant": "acme"}


def test_metadata_via_sync_facade(fresh_dsn: str) -> None:
    from roost import Roost

    r = Roost(fresh_dsn)
    job_id = r.enqueue("t", args={"x": 1}, metadata={"req_id": "r-1"})

    with psycopg.connect(fresh_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT metadata FROM roost.jobs WHERE id = %s", (job_id,))
        row = cur.fetchone()
        assert row is not None
        # psycopg returns JSONB as a Python object via the default adapter.
        assert row[0] == {"req_id": "r-1"}


# ---------------------------------------------------------------------------
# capped errors
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_errors_are_capped_to_last_n(async_conn: asyncpg.Connection) -> None:
    job_id = await repo.enqueue_async(async_conn, task="t", max_attempts=100)
    from datetime import datetime, timezone

    when = datetime.now(tz=timezone.utc)
    for i in range(15):
        await repo.mark_retryable_async(
            async_conn,
            job_id,
            when,
            {"attempt": i, "error": f"error #{i}", "trace": ""},
            error_cap=10,
        )
    row = await async_conn.fetchrow(
        "SELECT errors FROM roost.jobs WHERE id = $1", job_id
    )
    assert row is not None
    errors = row["errors"]
    assert len(errors) == 10  # only last 10 retained
    # Most recent error must be at the end
    assert errors[-1]["error"] == "error #14"
    assert errors[0]["error"] == "error #5"


@pytest.mark.asyncio
async def test_errors_below_cap_keep_all(async_conn: asyncpg.Connection) -> None:
    job_id = await repo.enqueue_async(async_conn, task="t", max_attempts=100)
    from datetime import datetime, timezone

    when = datetime.now(tz=timezone.utc)
    for i in range(3):
        await repo.mark_retryable_async(
            async_conn,
            job_id,
            when,
            {"attempt": i, "error": f"e{i}", "trace": ""},
            error_cap=10,
        )
    row = await async_conn.fetchrow(
        "SELECT errors FROM roost.jobs WHERE id = $1", job_id
    )
    assert row is not None
    assert [e["error"] for e in row["errors"]] == ["e0", "e1", "e2"]


# ---------------------------------------------------------------------------
# archive
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_archive_moves_old_terminal_jobs(async_conn: asyncpg.Connection) -> None:
    fresh_id = await repo.enqueue_async(async_conn, task="fresh")
    stale_id = await repo.enqueue_async(async_conn, task="stale")

    # Mark fresh as completed *now* and stale as completed an hour ago.
    await async_conn.execute(
        "UPDATE roost.jobs SET state = 'completed', completed_at = now() WHERE id = $1",
        fresh_id,
    )
    await async_conn.execute(
        "UPDATE roost.jobs SET state = 'completed', completed_at = now() - interval '1 hour' "
        "WHERE id = $1",
        stale_id,
    )

    moved = await repo.archive_terminal_async(async_conn, older_than_seconds=60)
    assert moved == 1

    # The fresh row stays in roost.jobs.
    row = await async_conn.fetchrow("SELECT id FROM roost.jobs WHERE id = $1", fresh_id)
    assert row is not None
    # The stale row is gone from roost.jobs and present in roost.jobs_archive.
    row = await async_conn.fetchrow("SELECT id FROM roost.jobs WHERE id = $1", stale_id)
    assert row is None
    arch = await async_conn.fetchrow(
        "SELECT id, state FROM roost.jobs_archive WHERE id = $1", stale_id
    )
    assert arch is not None
    assert arch["state"] == "completed"


# ---------------------------------------------------------------------------
# CLI: enqueue + requeue --queue
# ---------------------------------------------------------------------------


def test_cli_enqueue_drops_a_job(fresh_dsn: str) -> None:
    result = runner.invoke(
        app,
        [
            "enqueue",
            "send_email",
            "--args",
            '{"user_id": 7}',
            "--queue",
            "emails",
            "--in",
            "5",
            "--dsn",
            fresh_dsn,
        ],
    )
    assert result.exit_code == 0, result.stdout
    assert "enqueued job" in result.stdout

    with psycopg.connect(fresh_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT task, queue, args FROM roost.jobs LIMIT 1")
        row = cur.fetchone()
        assert row is not None
        assert row[0] == "send_email"
        assert row[1] == "emails"
        assert row[2] == {"user_id": 7}


def test_cli_enqueue_rejects_non_object_args(fresh_dsn: str) -> None:
    result = runner.invoke(
        app,
        ["enqueue", "x", "--args", "[1, 2, 3]", "--dsn", fresh_dsn],
    )
    assert result.exit_code != 0


def test_cli_requeue_filtered_by_queue(fresh_dsn: str) -> None:
    with psycopg.connect(fresh_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO roost.jobs (task, queue, state) VALUES "
            "('a', 'q1', 'discarded'), ('b', 'q1', 'discarded'), ('c', 'q2', 'discarded')"
        )
        conn.commit()

    result = runner.invoke(
        app, ["requeue", "--discarded", "--queue", "q1", "--dsn", fresh_dsn]
    )
    assert result.exit_code == 0
    assert "requeued 2 discarded job(s)" in result.stdout

    with psycopg.connect(fresh_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT queue, state FROM roost.jobs ORDER BY queue, id")
        rows = cur.fetchall()
    assert rows == [("q1", "available"), ("q1", "available"), ("q2", "discarded")]


# ---------------------------------------------------------------------------
# Migration v3 schema check
# ---------------------------------------------------------------------------


def test_archive_table_exists(fresh_dsn: str) -> None:
    with psycopg.connect(fresh_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('roost.jobs_archive')")
        row = cur.fetchone()
        assert row is not None and row[0] == "roost.jobs_archive"


def test_metadata_column_exists(fresh_dsn: str) -> None:
    with psycopg.connect(fresh_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = 'roost' AND table_name = 'jobs' AND column_name = 'metadata'"
        )
        row = cur.fetchone()
        assert row is not None and row[0] == "jsonb"
