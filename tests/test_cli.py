from __future__ import annotations

import psycopg
from typer.testing import CliRunner

from roost.cli import app

runner = CliRunner()


def test_version() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip()


def test_init_prints_sql() -> None:
    result = runner.invoke(app, ["init"])
    assert result.exit_code == 0
    assert "CREATE SCHEMA IF NOT EXISTS roost" in result.stdout
    assert "roost.notify_inserted" in result.stdout
    assert "roost.notify_updated" in result.stdout


def test_init_apply_against_real_db(fresh_dsn: str) -> None:
    # `fresh_dsn` already applies the schema; calling apply again must be idempotent.
    result = runner.invoke(app, ["init", "--apply", "--dsn", fresh_dsn])
    assert result.exit_code == 0
    with psycopg.connect(fresh_dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('roost.jobs')")
        row = cur.fetchone()
        assert row is not None and row[0] == "roost.jobs"


def test_status_against_real_db(fresh_dsn: str) -> None:
    result = runner.invoke(app, ["status", "--dsn", fresh_dsn])
    assert result.exit_code == 0
    assert "no jobs" in result.stdout


def test_init_requires_dsn_when_apply_without_env(monkeypatch) -> None:
    monkeypatch.delenv("ROOST_DSN", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    result = runner.invoke(app, ["init", "--apply"])
    assert result.exit_code != 0
