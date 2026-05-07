"""Numbered migrations with up/down pairs.

Migrations are declared inline in :data:`MIGRATIONS` so the package ships
as a single source tree (no resource files), and a one-time pass during
``roost init --apply`` brings the database current.

Adding a migration: append a :class:`Migration` to :data:`MIGRATIONS` with
the next sequential ``version``. Each migration has ``up`` (always
required) and ``down`` (optional). Rolling back through an unset ``down``
raises :class:`IrreversibleMigration`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    import asyncpg
    import psycopg


class IrreversibleMigration(RuntimeError):
    """Raised when ``roost migrate down`` would cross a migration with no ``down`` SQL."""


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    up: str
    down: str | None = None


# Bootstrap table that tracks which migrations have been applied.
# Always created first; never appears in the MIGRATIONS list.
_BOOTSTRAP_SQL = """
CREATE SCHEMA IF NOT EXISTS roost;
CREATE TABLE IF NOT EXISTS roost.migrations (
    version     INTEGER      PRIMARY KEY,
    name        TEXT         NOT NULL,
    applied_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
"""


# ---------------------------------------------------------------------------
# v1 — initial schema (formerly INITIAL_SCHEMA)
# ---------------------------------------------------------------------------

_V1_UP = """
CREATE TABLE IF NOT EXISTS roost.jobs (
    id                BIGSERIAL    PRIMARY KEY,
    queue             TEXT         NOT NULL DEFAULT 'default',
    task              TEXT         NOT NULL,
    args              JSONB        NOT NULL DEFAULT '{}'::jsonb,
    state             TEXT         NOT NULL DEFAULT 'available',
    priority          SMALLINT     NOT NULL DEFAULT 0,
    attempt           INTEGER      NOT NULL DEFAULT 0,
    max_attempts      INTEGER      NOT NULL DEFAULT 20,
    scheduled_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    attempted_at      TIMESTAMPTZ,
    completed_at      TIMESTAMPTZ,
    cancelled_at      TIMESTAMPTZ,
    discarded_at      TIMESTAMPTZ,
    errors            JSONB        NOT NULL DEFAULT '[]'::jsonb,
    unique_key        TEXT,
    inserted_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    tags              TEXT[]       NOT NULL DEFAULT ARRAY[]::text[],
    timeout_seconds   INTEGER,
    cancel_requested  BOOLEAN      NOT NULL DEFAULT false,
    result            JSONB
);

CREATE INDEX IF NOT EXISTS jobs_fetch_idx
    ON roost.jobs (queue, priority, scheduled_at, id)
    WHERE state = 'available';

CREATE UNIQUE INDEX IF NOT EXISTS jobs_unique_idx
    ON roost.jobs (unique_key)
    WHERE unique_key IS NOT NULL
      AND state IN ('available', 'executing', 'retryable');

CREATE INDEX IF NOT EXISTS jobs_tags_idx
    ON roost.jobs USING GIN (tags);

CREATE TABLE IF NOT EXISTS roost.cron_runs (
    name         TEXT         PRIMARY KEY,
    last_run_at  TIMESTAMPTZ  NOT NULL DEFAULT 'epoch'::timestamptz
);

CREATE TABLE IF NOT EXISTS roost.workers (
    id              TEXT         PRIMARY KEY,
    hostname        TEXT         NOT NULL,
    pid             INTEGER      NOT NULL,
    queues          TEXT[]       NOT NULL DEFAULT ARRAY[]::text[],
    started_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    metadata        JSONB        NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS workers_last_seen_idx
    ON roost.workers (last_seen_at);

CREATE TABLE IF NOT EXISTS roost.queues (
    name        TEXT         PRIMARY KEY,
    paused_at   TIMESTAMPTZ,
    metadata    JSONB        NOT NULL DEFAULT '{}'::jsonb,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION roost.notify_inserted() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('roost_inserted', NEW.queue);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS jobs_notify_inserted ON roost.jobs;
CREATE TRIGGER jobs_notify_inserted
    AFTER INSERT ON roost.jobs
    FOR EACH ROW EXECUTE FUNCTION roost.notify_inserted();

CREATE OR REPLACE FUNCTION roost.notify_updated() RETURNS trigger AS $$
BEGIN
    IF NEW.state IS DISTINCT FROM OLD.state THEN
        PERFORM pg_notify('roost_updated', NEW.id::text);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS jobs_notify_updated ON roost.jobs;
CREATE TRIGGER jobs_notify_updated
    AFTER UPDATE ON roost.jobs
    FOR EACH ROW EXECUTE FUNCTION roost.notify_updated();

CREATE OR REPLACE FUNCTION roost.notify_cancel_requested() RETURNS trigger AS $$
BEGIN
    IF NEW.cancel_requested = true AND COALESCE(OLD.cancel_requested, false) = false THEN
        PERFORM pg_notify('roost_cancel_requested', NEW.id::text);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS jobs_notify_cancel_requested ON roost.jobs;
CREATE TRIGGER jobs_notify_cancel_requested
    AFTER UPDATE ON roost.jobs
    FOR EACH ROW EXECUTE FUNCTION roost.notify_cancel_requested();
"""

_V1_DOWN = """
DROP TRIGGER IF EXISTS jobs_notify_cancel_requested ON roost.jobs;
DROP TRIGGER IF EXISTS jobs_notify_updated ON roost.jobs;
DROP TRIGGER IF EXISTS jobs_notify_inserted ON roost.jobs;
DROP FUNCTION IF EXISTS roost.notify_cancel_requested();
DROP FUNCTION IF EXISTS roost.notify_updated();
DROP FUNCTION IF EXISTS roost.notify_inserted();
DROP TABLE IF EXISTS roost.queues;
DROP TABLE IF EXISTS roost.workers;
DROP TABLE IF EXISTS roost.cron_runs;
DROP TABLE IF EXISTS roost.jobs;
"""


_V2_UP = """
ALTER TABLE roost.jobs
    ADD COLUMN IF NOT EXISTS depends_on BIGINT[] NOT NULL DEFAULT ARRAY[]::bigint[];

CREATE INDEX IF NOT EXISTS jobs_depends_on_idx
    ON roost.jobs USING GIN (depends_on)
    WHERE state = 'available';
"""

_V2_DOWN = """
DROP INDEX IF EXISTS roost.jobs_depends_on_idx;
ALTER TABLE roost.jobs DROP COLUMN IF EXISTS depends_on;
"""


MIGRATIONS: list[Migration] = [
    Migration(version=1, name="initial", up=_V1_UP, down=_V1_DOWN),
    Migration(version=2, name="job_dependencies", up=_V2_UP, down=_V2_DOWN),
]


def latest_version() -> int:
    return max((m.version for m in MIGRATIONS), default=0)


# ---------------------------------------------------------------------------
# Async runtime
# ---------------------------------------------------------------------------


async def applied_versions_async(conn: asyncpg.Connection) -> list[int]:
    rows = await conn.fetch("SELECT version FROM roost.migrations ORDER BY version")
    return [int(r["version"]) for r in rows]


async def apply_pending_async(conn: asyncpg.Connection) -> list[int]:
    """Run any migrations whose version > the highest applied version. Returns versions applied."""
    await conn.execute(_BOOTSTRAP_SQL)
    applied = set(await applied_versions_async(conn))
    ran: list[int] = []
    for migration in sorted(MIGRATIONS, key=lambda m: m.version):
        if migration.version in applied:
            continue
        async with conn.transaction():
            await conn.execute(migration.up)
            await conn.execute(
                "INSERT INTO roost.migrations (version, name) VALUES ($1, $2)",
                migration.version,
                migration.name,
            )
        ran.append(migration.version)
    return ran


async def rollback_to_async(conn: asyncpg.Connection, target: int) -> list[int]:
    """Run ``down`` for every applied migration whose version > ``target``.

    Raises :class:`IrreversibleMigration` if any rolled-back migration has no
    ``down`` SQL.
    """
    await conn.execute(_BOOTSTRAP_SQL)
    applied = await applied_versions_async(conn)
    to_revert = sorted([v for v in applied if v > target], reverse=True)
    by_version = {m.version: m for m in MIGRATIONS}
    reverted: list[int] = []
    for version in to_revert:
        migration = by_version.get(version)
        if migration is None:
            # Applied historically but no longer in the package. Refuse rather than
            # corrupt state.
            raise IrreversibleMigration(
                f"version {version} is recorded as applied but not in MIGRATIONS — "
                "downgrade the package or restore the migration definition first"
            )
        if migration.down is None:
            raise IrreversibleMigration(f"migration {version} ({migration.name}) has no down SQL")
        async with conn.transaction():
            await conn.execute(migration.down)
            await conn.execute("DELETE FROM roost.migrations WHERE version = $1", version)
        reverted.append(version)
    return reverted


# ---------------------------------------------------------------------------
# Sync helpers (psycopg)
# ---------------------------------------------------------------------------


def applied_versions_sync(conn: psycopg.Connection) -> list[int]:
    with conn.cursor() as cur:
        cur.execute("SELECT version FROM roost.migrations ORDER BY version")
        return [int(r[0]) for r in cur.fetchall()]


def apply_pending_sync(conn: psycopg.Connection) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(_BOOTSTRAP_SQL)
    conn.commit()
    applied = set(applied_versions_sync(conn))
    ran: list[int] = []
    for migration in sorted(MIGRATIONS, key=lambda m: m.version):
        if migration.version in applied:
            continue
        try:
            with conn.cursor() as cur:
                cur.execute(migration.up)
                cur.execute(
                    "INSERT INTO roost.migrations (version, name) VALUES (%s, %s)",
                    (migration.version, migration.name),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        ran.append(migration.version)
    return ran


def rollback_to_sync(conn: psycopg.Connection, target: int) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(_BOOTSTRAP_SQL)
    conn.commit()
    applied = applied_versions_sync(conn)
    to_revert = sorted([v for v in applied if v > target], reverse=True)
    by_version = {m.version: m for m in MIGRATIONS}
    reverted: list[int] = []
    for version in to_revert:
        migration = by_version.get(version)
        if migration is None:
            raise IrreversibleMigration(f"version {version} is recorded as applied but not in MIGRATIONS")
        if migration.down is None:
            raise IrreversibleMigration(f"migration {version} ({migration.name}) has no down SQL")
        try:
            with conn.cursor() as cur:
                cur.execute(migration.down)
                cur.execute("DELETE FROM roost.migrations WHERE version = %s", (version,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        reverted.append(version)
    return reverted


__all__ = [
    "MIGRATIONS",
    "IrreversibleMigration",
    "Migration",
    "applied_versions_async",
    "applied_versions_sync",
    "apply_pending_async",
    "apply_pending_sync",
    "latest_version",
    "rollback_to_async",
    "rollback_to_sync",
]
