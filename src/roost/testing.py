"""Test helpers — run handlers without spinning up a worker.

Two patterns:

* :func:`run_inline` — pop one job off the queue, run its handler in-process,
  finalize the row. Use in app tests when you want enqueue→handle to behave
  synchronously without a real worker loop.

* :func:`drain_pending` — repeatedly :func:`run_inline` until no available
  jobs remain or ``max_jobs`` is hit.

These bypass concurrency control intentionally — they assume the test owns
the database. Don't run them against a database where workers are also
draining queues.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import traceback
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

from roost._core import repo
from roost._core.retry import resolve
from roost.decorators import DEFAULT_HANDLERS, HandlerRegistry
from roost.exceptions import SnoozeJob, UnknownTaskError

if TYPE_CHECKING:  # pragma: no cover
    import asyncpg


async def run_inline(
    conn: asyncpg.Connection,
    *,
    queues: list[str] | None = None,
    registry: HandlerRegistry | None = None,
) -> int | None:
    """Fetch one available job and run its handler synchronously.

    Returns the executed job's id, or ``None`` if no job was available.
    On success the row is marked ``completed``; on failure it follows the
    same retry/discard policy as the worker (using the default backoff).
    """
    reg = registry or DEFAULT_HANDLERS
    jobs = await repo.fetch_available_async(conn, queues or ["default"], 1)
    if not jobs:
        return None
    job = jobs[0]
    spec = reg.get(job.task)
    try:
        if spec is None:
            raise UnknownTaskError(f"no handler registered for task '{job.task}'")
        args = dict(job.args or {})
        # Strip private trace carrier if present.
        args.pop("__roost_trace", None)
        if spec.is_async:
            result = await spec.func(**args)
        else:
            result = spec.func(**args)
            if inspect.isawaitable(result):
                raise TypeError(
                    f"sync handler {spec.func.__qualname__} returned an awaitable — "
                    "use `async def` for async handlers"
                )
        await repo.mark_completed_async(conn, job.id, result=result)
        return int(job.id)
    except SnoozeJob as snooze:
        when = datetime.now(tz=timezone.utc) + timedelta(seconds=snooze.seconds)
        await repo.snooze_async(conn, job.id, when)
        return int(job.id)
    except BaseException as exc:  # noqa: BLE001
        error_payload = {
            "attempt": job.attempt,
            "at": datetime.now(tz=timezone.utc).isoformat(),
            "error": f"{type(exc).__name__}: {exc}",
            "trace": "".join(traceback.format_exception(exc)).strip(),
        }
        if job.attempt >= job.max_attempts:
            await repo.mark_discarded_async(conn, job.id, error_payload)
        else:
            delay = float(resolve(None)(job.attempt))
            when = datetime.now(tz=timezone.utc) + timedelta(seconds=delay)
            await repo.mark_retryable_async(conn, job.id, when, error_payload)
        return int(job.id)


async def drain_pending(
    conn: asyncpg.Connection,
    *,
    queues: list[str] | None = None,
    registry: HandlerRegistry | None = None,
    max_jobs: int = 1000,
    promote_retryable: bool = True,
) -> int:
    """Run available jobs in a tight loop. Returns how many were processed.

    When ``promote_retryable=True`` (the default), jobs in ``retryable``
    state with ``scheduled_at <= now()`` are promoted to ``available``
    before each pass — useful when a test wants to follow a job through
    its retry path quickly.
    """
    processed = 0
    while processed < max_jobs:
        if promote_retryable:
            await repo.promote_retryable_async(conn)
        ran = await run_inline(conn, queues=queues, registry=registry)
        if ran is None:
            return processed
        processed += 1
    return processed


def fast_forward_args(args: dict[str, Any] | None) -> dict[str, Any]:
    """Round-trip ``args`` through JSON the same way the worker would.

    Lets tests compare what the handler will see against what they
    enqueued: e.g. ``datetime`` becomes an ISO string.
    """
    return cast(dict[str, Any], json.loads(json.dumps(args or {}, default=str)))


def reset_default_registry() -> None:
    """Clear ``DEFAULT_HANDLERS`` — useful as a pytest fixture teardown."""
    DEFAULT_HANDLERS.clear()


__all__ = [
    "drain_pending",
    "fast_forward_args",
    "reset_default_registry",
    "run_inline",
]


# Quiet ``asyncio`` import — used for type narrowing only.
_ = asyncio
