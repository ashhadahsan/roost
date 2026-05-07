"""All DB I/O lives here. Single source of truth for SQL.

Both the async (asyncpg) and sync (psycopg) facades route through this module.
The two flavors share the same SQL with placeholder syntax differences only
(``$N`` for asyncpg, ``%s`` for psycopg).

The most important contract: ``enqueue_*`` always takes the caller's
connection / cursor. We never open one ourselves.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

from roost._core.job import Job
from roost.exceptions import JobNotFoundError

if TYPE_CHECKING:  # pragma: no cover
    import asyncpg
    import psycopg


# ---------------------------------------------------------------------------
# SQL strings — placeholder agnostic where possible
# ---------------------------------------------------------------------------

_ENQUEUE_BASE_COLS = (
    "task, args, queue, priority, max_attempts, scheduled_at, unique_key, tags, timeout_seconds, depends_on"
)

_INSERT_PLAIN_PG = f"""
INSERT INTO roost.jobs ({_ENQUEUE_BASE_COLS})
VALUES ($1, $2::jsonb, $3, $4, $5, $6, $7, $8::text[], $9, $10::bigint[])
RETURNING id
"""

_INSERT_UNIQUE_PG = f"""
WITH attempted AS (
    INSERT INTO roost.jobs ({_ENQUEUE_BASE_COLS})
    VALUES ($1, $2::jsonb, $3, $4, $5, $6, $7, $8::text[], $9, $10::bigint[])
    ON CONFLICT (unique_key)
      WHERE unique_key IS NOT NULL AND state IN ('available','executing','retryable')
    DO NOTHING
    RETURNING id, true AS inserted
)
SELECT id, inserted FROM attempted
UNION ALL
SELECT j.id, false AS inserted
  FROM roost.jobs j
 WHERE j.unique_key = $7
   AND j.state IN ('available','executing','retryable')
   AND NOT EXISTS (SELECT 1 FROM attempted)
 LIMIT 1
"""

_INSERT_PLAIN_PSY = f"""
INSERT INTO roost.jobs ({_ENQUEUE_BASE_COLS})
VALUES (%s, %s::jsonb, %s, %s, %s, %s, %s, %s::text[], %s, %s::bigint[])
RETURNING id
"""

_INSERT_UNIQUE_PSY = f"""
WITH attempted AS (
    INSERT INTO roost.jobs ({_ENQUEUE_BASE_COLS})
    VALUES (%s, %s::jsonb, %s, %s, %s, %s, %s, %s::text[], %s, %s::bigint[])
    ON CONFLICT (unique_key)
      WHERE unique_key IS NOT NULL AND state IN ('available','executing','retryable')
    DO NOTHING
    RETURNING id, true AS inserted
)
SELECT id, inserted FROM attempted
UNION ALL
SELECT j.id, false AS inserted
  FROM roost.jobs j
 WHERE j.unique_key = %s
   AND j.state IN ('available','executing','retryable')
   AND NOT EXISTS (SELECT 1 FROM attempted)
 LIMIT 1
"""

_FETCH_AVAILABLE_PG = """
WITH limits AS (
    SELECT t.task,
           t.rate_per_minute,
           t.max_concurrency
      FROM unnest($3::text[], $4::int[], $5::int[])
        AS t(task, rate_per_minute, max_concurrency)
), candidates AS (
    SELECT j.id,
           j.task,
           j.priority,
           j.scheduled_at,
           l.rate_per_minute,
           l.max_concurrency,
           (SELECT COUNT(*) FROM roost.jobs e
             WHERE e.task = j.task AND e.state = 'executing') AS exec_now,
           (SELECT COUNT(*) FROM roost.jobs r
             WHERE r.task = j.task
               AND r.attempted_at >= now() - interval '1 minute') AS rate_now
      FROM roost.jobs j
      LEFT JOIN limits l ON l.task = j.task
     WHERE j.state = 'available'
       AND j.queue = ANY($1::text[])
       AND j.scheduled_at <= now()
       AND j.queue NOT IN (SELECT name FROM roost.queues WHERE paused_at IS NOT NULL)
       AND (
           cardinality(j.depends_on) = 0
           OR NOT EXISTS (
               SELECT 1 FROM roost.jobs p
                WHERE p.id = ANY(j.depends_on)
                  AND p.state <> 'completed'
           )
       )
), ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY task ORDER BY priority, scheduled_at, id) AS rn_task
      FROM candidates
), allowed AS (
    SELECT id, priority, scheduled_at
      FROM ranked
     WHERE (max_concurrency IS NULL OR exec_now + rn_task <= max_concurrency)
       AND (rate_per_minute IS NULL OR rate_now + rn_task <= rate_per_minute)
), picked AS (
    SELECT j.id
      FROM roost.jobs j
      JOIN allowed a ON a.id = j.id
     WHERE j.state = 'available'
     ORDER BY j.priority ASC, j.scheduled_at ASC, j.id ASC
     FOR UPDATE SKIP LOCKED
     LIMIT $2
)
UPDATE roost.jobs j
   SET state = 'executing',
       attempt = attempt + 1,
       attempted_at = now()
  FROM picked
 WHERE j.id = picked.id
RETURNING j.*
"""

_MARK_COMPLETED_PG = """
UPDATE roost.jobs
   SET state = 'completed',
       completed_at = now(),
       result = COALESCE($2::jsonb, result)
 WHERE id = $1
"""

_MARK_RETRYABLE_PG = """
UPDATE roost.jobs
   SET state = 'retryable',
       scheduled_at = $2,
       errors = errors || $3::jsonb
 WHERE id = $1
"""

_MARK_DISCARDED_PG = """
UPDATE roost.jobs
   SET state = 'discarded',
       discarded_at = now(),
       errors = errors || $2::jsonb
 WHERE id = $1
"""

_RESET_TO_AVAILABLE_PG = """
UPDATE roost.jobs
   SET state = 'available',
       scheduled_at = $2
 WHERE id = $1
"""

_SNOOZE_PG = """
UPDATE roost.jobs
   SET state = 'available',
       scheduled_at = $2,
       attempt = GREATEST(attempt - 1, 0)
 WHERE id = $1
"""

_PROMOTE_RETRYABLE_PG = """
UPDATE roost.jobs
   SET state = 'available'
 WHERE state = 'retryable'
   AND scheduled_at <= now()
"""

_RETRY_BY_ID_PG = """
UPDATE roost.jobs
   SET state = 'available',
       scheduled_at = now()
 WHERE id = $1
   AND state IN ('retryable', 'discarded', 'cancelled', 'completed')
RETURNING id
"""

_CANCEL_BY_ID_PG = """
UPDATE roost.jobs
   SET state = 'cancelled',
       cancelled_at = now()
 WHERE id = $1
   AND state IN ('available', 'retryable', 'executing')
RETURNING id
"""

_STATUS_COUNTS_PG = """
SELECT queue, state, COUNT(*)::bigint AS n
  FROM roost.jobs
 GROUP BY queue, state
 ORDER BY queue, state
"""

_REAP_ORPHANS_PG = """
WITH stale AS (
    SELECT id
      FROM roost.jobs
     WHERE state = 'executing'
       AND attempted_at < now() - ($1::interval)
     FOR UPDATE SKIP LOCKED
)
UPDATE roost.jobs j
   SET state = CASE WHEN j.attempt >= j.max_attempts THEN 'discarded' ELSE 'retryable' END,
       discarded_at = CASE WHEN j.attempt >= j.max_attempts THEN now() ELSE j.discarded_at END,
       scheduled_at = CASE WHEN j.attempt >= j.max_attempts THEN j.scheduled_at ELSE now() END,
       errors = j.errors || $2::jsonb
  FROM stale
 WHERE j.id = stale.id
RETURNING j.id, j.state
"""

_HEARTBEAT_UPSERT_PG = """
INSERT INTO roost.workers (id, hostname, pid, queues, last_seen_at, metadata)
VALUES ($1, $2, $3, $4, now(), $5::jsonb)
ON CONFLICT (id) DO UPDATE
   SET last_seen_at = now(),
       queues = EXCLUDED.queues,
       metadata = EXCLUDED.metadata
"""

_WORKER_DEREGISTER_PG = """
DELETE FROM roost.workers WHERE id = $1
"""

_WORKER_GC_PG = """
DELETE FROM roost.workers
 WHERE last_seen_at < now() - ($1::interval)
"""

_LIST_WORKERS_PG = """
SELECT id, hostname, pid, queues, started_at, last_seen_at, metadata
  FROM roost.workers
 ORDER BY last_seen_at DESC
"""

_QUEUE_PAUSE_PG = """
INSERT INTO roost.queues (name, paused_at, updated_at)
VALUES ($1, now(), now())
ON CONFLICT (name) DO UPDATE
   SET paused_at = COALESCE(roost.queues.paused_at, EXCLUDED.paused_at),
       updated_at = now()
"""

_QUEUE_RESUME_PG = """
INSERT INTO roost.queues (name, paused_at, updated_at)
VALUES ($1, NULL, now())
ON CONFLICT (name) DO UPDATE
   SET paused_at = NULL,
       updated_at = now()
"""

_QUEUE_LIST_PG = """
SELECT name, paused_at, metadata, updated_at
  FROM roost.queues
 ORDER BY name
"""

_REQUEST_CANCEL_PG = """
UPDATE roost.jobs
   SET cancel_requested = true
 WHERE id = $1
   AND state IN ('available', 'retryable', 'executing')
RETURNING id, state
"""

_REQUEUE_DISCARDED_PG = """
UPDATE roost.jobs
   SET state = 'available',
       scheduled_at = now(),
       attempt = 0,
       cancel_requested = false
 WHERE state = 'discarded'
"""

_BULK_INSERT_PG = f"""
INSERT INTO roost.jobs ({_ENQUEUE_BASE_COLS})
VALUES ($1, $2::jsonb, $3, $4, $5, $6, $7, $8::text[], $9, $10::bigint[])
ON CONFLICT (unique_key)
  WHERE unique_key IS NOT NULL AND state IN ('available','executing','retryable')
DO NOTHING
"""

_CANCEL_BLOCKED_DEPENDENTS_PG = """
WITH blocked AS (
    SELECT j.id
      FROM roost.jobs j
     WHERE j.state = 'available'
       AND cardinality(j.depends_on) > 0
       AND EXISTS (
           SELECT 1 FROM roost.jobs p
            WHERE p.id = ANY(j.depends_on)
              AND p.state IN ('discarded', 'cancelled')
       )
     FOR UPDATE SKIP LOCKED
)
UPDATE roost.jobs j
   SET state = 'cancelled',
       cancelled_at = now(),
       errors = j.errors || $1::jsonb
  FROM blocked
 WHERE j.id = blocked.id
RETURNING j.id
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _coerce_scheduled_at(scheduled_at: datetime | None) -> datetime:
    if scheduled_at is None:
        return _utcnow()
    if scheduled_at.tzinfo is None:
        return scheduled_at.replace(tzinfo=timezone.utc)
    return scheduled_at


def _args_dict(args: dict[str, Any] | None) -> dict[str, Any]:
    return args or {}


def _args_json(args: dict[str, Any] | None) -> str:
    return json.dumps(args or {}, default=str, separators=(",", ":"))


def _record_to_job(record: Any) -> Job:
    """Convert an asyncpg Record or psycopg row mapping to a Job."""
    data = dict(record)
    return Job.model_validate(data)


# ---------------------------------------------------------------------------
# Async (asyncpg)
# ---------------------------------------------------------------------------


async def init_connection(conn: asyncpg.Connection) -> None:
    """Register codecs so JSONB columns round-trip as dict/list, not text.

    Call this on every asyncpg connection used to fetch from ``roost.jobs``,
    or pass it as ``init=`` when constructing a :class:`asyncpg.Pool`.
    """
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
        format="text",
    )
    await conn.set_type_codec(
        "json",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
        format="text",
    )


async def apply_schema_async(conn: asyncpg.Connection) -> None:
    """Bring the schema fully up to date by running pending migrations."""
    from roost._core.migrations import apply_pending_async

    await apply_pending_async(conn)


async def enqueue_async(
    conn: asyncpg.Connection,
    *,
    task: str,
    args: dict[str, Any] | None = None,
    queue: str = "default",
    priority: int = 0,
    max_attempts: int = 20,
    scheduled_at: datetime | None = None,
    unique_key: str | None = None,
    tags: list[str] | None = None,
    timeout_seconds: int | None = None,
    depends_on: list[int] | None = None,
) -> int:
    """Insert a job using ``conn`` — typically the caller's transaction.

    Returns the id of the inserted (or existing, on unique conflict) job.
    """
    args_value = _args_dict(args)
    when = _coerce_scheduled_at(scheduled_at)
    tags_value = list(tags or [])
    depends_value = [int(x) for x in (depends_on or [])]

    if unique_key is None:
        row = await conn.fetchrow(
            _INSERT_PLAIN_PG,
            task,
            args_value,
            queue,
            priority,
            max_attempts,
            when,
            None,
            tags_value,
            timeout_seconds,
            depends_value,
        )
        assert row is not None
        return cast(int, row["id"])

    row = await conn.fetchrow(
        _INSERT_UNIQUE_PG,
        task,
        args_value,
        queue,
        priority,
        max_attempts,
        when,
        unique_key,
        tags_value,
        timeout_seconds,
        depends_value,
    )
    assert row is not None, "unique INSERT CTE must return a row"
    return cast(int, row["id"])


@dataclass(frozen=True, slots=True)
class JobInsert:
    """Bulk-enqueue payload."""

    task: str
    args: dict[str, Any] | None = None
    queue: str = "default"
    priority: int = 0
    max_attempts: int = 20
    scheduled_at: datetime | None = None
    unique_key: str | None = None
    tags: list[str] | None = None
    timeout_seconds: int | None = None
    depends_on: list[int] | None = None


async def enqueue_many_async(conn: asyncpg.Connection, jobs: list[JobInsert]) -> int:
    """Bulk-insert jobs in a single round-trip via ``executemany``.

    Returns the number of submitted rows. The conflict policy matches
    :func:`enqueue_async` — duplicates against active ``unique_key`` rows
    are silently skipped (so the count is "submitted", not "unique inserted").

    Note: ids are not returned. If you need the id back, use ``enqueue_async``.
    """
    if not jobs:
        return 0
    rows = [
        (
            j.task,
            _args_dict(j.args),
            j.queue,
            j.priority,
            j.max_attempts,
            _coerce_scheduled_at(j.scheduled_at),
            j.unique_key,
            list(j.tags or []),
            j.timeout_seconds,
            [int(x) for x in (j.depends_on or [])],
        )
        for j in jobs
    ]
    await conn.executemany(_BULK_INSERT_PG, rows)
    return len(rows)


async def fetch_available_async(
    conn: asyncpg.Connection,
    queues: list[str],
    limit: int,
    *,
    task_limits: dict[str, tuple[int | None, int | None]] | None = None,
) -> list[Job]:
    """Pick up to ``limit`` jobs from the listed queues and mark them ``executing``.

    ``task_limits`` is ``{task: (rate_per_minute, max_concurrency)}`` — pass
    ``None`` for either field to skip that gate. Tasks not in the mapping are
    unrestricted.
    """
    tasks: list[str] = []
    rates: list[int | None] = []
    concs: list[int | None] = []
    for task, (rate, conc) in (task_limits or {}).items():
        tasks.append(task)
        rates.append(rate)
        concs.append(conc)
    rows = await conn.fetch(_FETCH_AVAILABLE_PG, queues, limit, tasks, rates, concs)
    return [_record_to_job(r) for r in rows]


async def mark_completed_async(conn: asyncpg.Connection, job_id: int, *, result: Any | None = None) -> None:
    """Mark ``job_id`` completed; optionally store ``result`` in the row."""
    await conn.execute(_MARK_COMPLETED_PG, job_id, result)


async def mark_retryable_async(
    conn: asyncpg.Connection, job_id: int, scheduled_at: datetime, error: dict[str, Any]
) -> None:
    await conn.execute(_MARK_RETRYABLE_PG, job_id, scheduled_at, [error])


async def mark_discarded_async(conn: asyncpg.Connection, job_id: int, error: dict[str, Any]) -> None:
    await conn.execute(_MARK_DISCARDED_PG, job_id, [error])


async def reset_to_available_async(conn: asyncpg.Connection, job_id: int, scheduled_at: datetime) -> None:
    await conn.execute(_RESET_TO_AVAILABLE_PG, job_id, scheduled_at)


async def snooze_async(conn: asyncpg.Connection, job_id: int, scheduled_at: datetime) -> None:
    await conn.execute(_SNOOZE_PG, job_id, scheduled_at)


async def promote_retryable_async(conn: asyncpg.Connection) -> int:
    res = await conn.execute(_PROMOTE_RETRYABLE_PG)
    # asyncpg returns "UPDATE N"
    try:
        return int(res.rsplit(" ", 1)[-1])
    except (ValueError, AttributeError):
        return 0


async def retry_job_async(conn: asyncpg.Connection, job_id: int) -> None:
    row = await conn.fetchrow(_RETRY_BY_ID_PG, job_id)
    if row is None:
        raise JobNotFoundError(f"job {job_id} not found or not in a retryable state")


async def cancel_job_async(conn: asyncpg.Connection, job_id: int) -> None:
    """Mark a job cancelled and request in-flight cancellation if executing.

    The accompanying NOTIFY (``roost_cancel_requested``) lets running workers
    cancel the inflight task. If the job is ``available`` or ``retryable``,
    it never runs.
    """
    async with conn.transaction():
        row = await conn.fetchrow(_REQUEST_CANCEL_PG, job_id)
        if row is None:
            raise JobNotFoundError(f"job {job_id} not found or not cancellable")
        # If not currently executing, also flip the row to cancelled now.
        if row["state"] != "executing":
            await conn.execute(_CANCEL_BY_ID_PG, job_id)


async def request_cancel_async(conn: asyncpg.Connection, job_id: int) -> str | None:
    """Set ``cancel_requested = true`` and return the current state.

    Returns ``None`` if the job is already terminal.
    """
    row = await conn.fetchrow(_REQUEST_CANCEL_PG, job_id)
    if row is None:
        return None
    return cast(str, row["state"])


async def finalize_cancel_async(conn: asyncpg.Connection, job_id: int) -> None:
    """Move an in-flight job into the ``cancelled`` terminal state."""
    await conn.execute(_CANCEL_BY_ID_PG, job_id)


async def status_counts_async(
    conn: asyncpg.Connection,
) -> list[tuple[str, str, int]]:
    rows = await conn.fetch(_STATUS_COUNTS_PG)
    return [(r["queue"], r["state"], int(r["n"])) for r in rows]


async def pause_queue_async(conn: asyncpg.Connection, name: str) -> None:
    await conn.execute(_QUEUE_PAUSE_PG, name)


async def resume_queue_async(conn: asyncpg.Connection, name: str) -> None:
    await conn.execute(_QUEUE_RESUME_PG, name)


async def list_queues_async(
    conn: asyncpg.Connection,
) -> list[tuple[str, datetime | None]]:
    rows = await conn.fetch(_QUEUE_LIST_PG)
    return [(r["name"], r["paused_at"]) for r in rows]


async def list_workers_async(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    rows = await conn.fetch(_LIST_WORKERS_PG)
    return [dict(r) for r in rows]


async def requeue_discarded_async(conn: asyncpg.Connection) -> int:
    res = await conn.execute(_REQUEUE_DISCARDED_PG)
    try:
        return int(res.rsplit(" ", 1)[-1])
    except (ValueError, AttributeError):
        return 0


async def cron_try_lock_async(conn: asyncpg.Connection, key: int) -> bool:
    val = await conn.fetchval("SELECT pg_try_advisory_lock($1)", key)
    return bool(val)


async def cron_unlock_async(conn: asyncpg.Connection, key: int) -> None:
    await conn.execute("SELECT pg_advisory_unlock($1)", key)


async def cancel_blocked_dependents_async(
    conn: asyncpg.Connection,
) -> list[int]:
    """Cancel jobs whose parents ended in ``discarded`` or ``cancelled``.

    Returns the list of cancelled ids.
    """
    error_payload = [
        {
            "at": _utcnow().isoformat(),
            "error": "BlockedDependency: a parent job ended in a non-completed state",
            "trace": "",
        }
    ]
    rows = await conn.fetch(_CANCEL_BLOCKED_DEPENDENTS_PG, error_payload)
    return [int(r["id"]) for r in rows]


async def reap_orphans_async(
    conn: asyncpg.Connection, *, stale_after_seconds: float
) -> list[tuple[int, str]]:
    """Recover jobs stuck in ``executing`` past the staleness window.

    Returns ``[(job_id, new_state), …]``. Jobs whose attempt count is at
    ``max_attempts`` go to ``discarded``; the rest go to ``retryable`` and
    are scheduled to run again immediately.
    """
    interval = timedelta(seconds=max(stale_after_seconds, 0.0))
    error_payload = [
        {
            "at": _utcnow().isoformat(),
            "error": "WorkerCrash: job left in executing state past staleness window",
            "trace": "",
        }
    ]
    rows = await conn.fetch(_REAP_ORPHANS_PG, interval, error_payload)
    return [(int(r["id"]), str(r["state"])) for r in rows]


async def heartbeat_async(
    conn: asyncpg.Connection,
    *,
    worker_id: str,
    hostname: str,
    pid: int,
    queues: list[str],
    metadata: dict[str, Any] | None = None,
) -> None:
    await conn.execute(
        _HEARTBEAT_UPSERT_PG,
        worker_id,
        hostname,
        pid,
        queues,
        metadata or {},
    )


async def deregister_worker_async(conn: asyncpg.Connection, worker_id: str) -> None:
    await conn.execute(_WORKER_DEREGISTER_PG, worker_id)


async def gc_workers_async(conn: asyncpg.Connection, *, stale_after_seconds: float) -> int:
    interval = timedelta(seconds=max(stale_after_seconds, 0.0))
    res = await conn.execute(_WORKER_GC_PG, interval)
    try:
        return int(res.rsplit(" ", 1)[-1])
    except (ValueError, AttributeError):
        return 0


async def cron_should_run_async(conn: asyncpg.Connection, name: str, due_at: datetime) -> bool:
    """Atomically claim the next due slot for a cron entry.

    Returns True iff the caller should enqueue a job for ``due_at``. Uses an
    UPSERT against ``roost.cron_runs`` keyed on ``name`` and bumps
    ``last_run_at`` to ``due_at`` only when the existing value is older.
    """
    row = await conn.fetchrow(
        """
        INSERT INTO roost.cron_runs (name, last_run_at)
        VALUES ($1, $2)
        ON CONFLICT (name) DO UPDATE
           SET last_run_at = EXCLUDED.last_run_at
         WHERE roost.cron_runs.last_run_at < EXCLUDED.last_run_at
        RETURNING last_run_at
        """,
        name,
        due_at,
    )
    return row is not None


# ---------------------------------------------------------------------------
# Sync (psycopg)
# ---------------------------------------------------------------------------


def apply_schema_sync(conn: psycopg.Connection[Any]) -> None:
    """Bring the schema fully up to date by running pending migrations."""
    from roost._core.migrations import apply_pending_sync

    apply_pending_sync(conn)


def enqueue_sync(
    conn: psycopg.Connection[Any],
    *,
    task: str,
    args: dict[str, Any] | None = None,
    queue: str = "default",
    priority: int = 0,
    max_attempts: int = 20,
    scheduled_at: datetime | None = None,
    unique_key: str | None = None,
    tags: list[str] | None = None,
    timeout_seconds: int | None = None,
    depends_on: list[int] | None = None,
) -> int:
    args_json = _args_json(args)
    when = _coerce_scheduled_at(scheduled_at)
    tags_value = list(tags or [])
    depends_value = [int(x) for x in (depends_on or [])]

    with conn.cursor() as cur:
        if unique_key is None:
            cur.execute(
                _INSERT_PLAIN_PSY,
                (
                    task,
                    args_json,
                    queue,
                    priority,
                    max_attempts,
                    when,
                    None,
                    tags_value,
                    timeout_seconds,
                    depends_value,
                ),
            )
            row = cur.fetchone()
            assert row is not None
            return cast(int, row[0])

        cur.execute(
            _INSERT_UNIQUE_PSY,
            (
                task,
                args_json,
                queue,
                priority,
                max_attempts,
                when,
                unique_key,
                tags_value,
                timeout_seconds,
                depends_value,
                unique_key,
            ),
        )
        row = cur.fetchone()
        assert row is not None
        return cast(int, row[0])


def status_counts_sync(
    conn: psycopg.Connection[Any],
) -> list[tuple[str, str, int]]:
    with conn.cursor() as cur:
        cur.execute(_STATUS_COUNTS_PG)
        return [(r[0], r[1], int(r[2])) for r in cur.fetchall()]


def retry_job_sync(conn: psycopg.Connection[Any], job_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(_RETRY_BY_ID_PG.replace("$1", "%s"), (job_id,))
        if cur.fetchone() is None:
            raise JobNotFoundError(f"job {job_id} not found or not in a retryable state")


def cancel_job_sync(conn: psycopg.Connection[Any], job_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(_REQUEST_CANCEL_PG.replace("$1", "%s"), (job_id,))
        row = cur.fetchone()
        if row is None:
            raise JobNotFoundError(f"job {job_id} not found or not cancellable")
        if row[1] != "executing":
            cur.execute(_CANCEL_BY_ID_PG.replace("$1", "%s"), (job_id,))


def pause_queue_sync(conn: psycopg.Connection[Any], name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(_QUEUE_PAUSE_PG.replace("$1", "%s"), (name,))


def resume_queue_sync(conn: psycopg.Connection[Any], name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(_QUEUE_RESUME_PG.replace("$1", "%s"), (name,))


def list_queues_sync(
    conn: psycopg.Connection[Any],
) -> list[tuple[str, datetime | None]]:
    with conn.cursor() as cur:
        cur.execute(_QUEUE_LIST_PG)
        return [(r[0], r[1]) for r in cur.fetchall()]


def list_workers_sync(conn: psycopg.Connection[Any]) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(_LIST_WORKERS_PG)
        cols = [d[0] for d in cur.description] if cur.description else []
        return [dict(zip(cols, r, strict=False)) for r in cur.fetchall()]


def requeue_discarded_sync(conn: psycopg.Connection[Any]) -> int:
    with conn.cursor() as cur:
        cur.execute(_REQUEUE_DISCARDED_PG)
        return cur.rowcount or 0
