"""Schema as plain SQL — no Alembic dependency.

Two NOTIFY channels are part of `roost-web`'s public contract:
  - ``roost_inserted`` — payload = queue name. Fired on every INSERT.
  - ``roost_updated``  — payload = job id. Fired on every state transition.

Changing this file requires a sibling migration entry in ``MIGRATIONS`` and
typically a coordinated change in the ``roost-web`` repo.
"""

from __future__ import annotations

SCHEMA_VERSION = 1

INITIAL_SCHEMA: str = """
CREATE SCHEMA IF NOT EXISTS roost;

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

CREATE INDEX IF NOT EXISTS jobs_tags_idx
    ON roost.jobs USING GIN (tags);

CREATE INDEX IF NOT EXISTS jobs_fetch_idx
    ON roost.jobs (queue, priority, scheduled_at, id)
    WHERE state = 'available';

CREATE UNIQUE INDEX IF NOT EXISTS jobs_unique_idx
    ON roost.jobs (unique_key)
    WHERE unique_key IS NOT NULL
      AND state IN ('available', 'executing', 'retryable');

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

CREATE TABLE IF NOT EXISTS roost.schema_version (
    version INTEGER PRIMARY KEY
);
INSERT INTO roost.schema_version (version) VALUES (1)
    ON CONFLICT (version) DO NOTHING;
"""


MIGRATIONS: list[tuple[int, str]] = [
    (1, INITIAL_SCHEMA),
]


def migration_sql() -> str:
    """Concatenate all migrations into a single idempotent script."""
    return "\n\n".join(sql for _, sql in MIGRATIONS).strip() + "\n"


__all__ = ["INITIAL_SCHEMA", "MIGRATIONS", "SCHEMA_VERSION", "migration_sql"]
