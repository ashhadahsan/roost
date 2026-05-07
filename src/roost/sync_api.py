"""Sync public facade — for Django / Flask / plain-Python codebases.

Uses ``psycopg`` directly. Never drives async-from-sync via ``asyncio.run``
inside library code — that's a footgun and the explicit non-goal.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from roost import observability
from roost._core import repo
from roost.decorators import DEFAULT_HANDLERS, HandlerRegistry, task_name

if TYPE_CHECKING:  # pragma: no cover
    import psycopg


def _coerce_args(args: dict[str, Any] | BaseModel | None) -> dict[str, Any]:
    if args is None:
        return {}
    if isinstance(args, BaseModel):
        return args.model_dump()
    return args


class Roost:
    """Synchronous entry point.

    Most users only call :meth:`enqueue` from this class — workers run via
    the CLI or :class:`roost.AsyncRoost.worker` (the worker loop itself is
    asyncio-based; sync code can drive it via ``asyncio.run``).
    """

    def __init__(
        self,
        dsn: str,
        *,
        registry: HandlerRegistry | None = None,
    ) -> None:
        self.dsn = dsn
        self.registry = registry or DEFAULT_HANDLERS

    @contextmanager
    def _connect(self) -> Iterator[psycopg.Connection[Any]]:
        import psycopg

        with psycopg.connect(self.dsn) as conn:
            yield conn

    # ------------------------------------------------------------------
    # schema
    # ------------------------------------------------------------------

    def setup_schema(self, conn: psycopg.Connection[Any] | None = None) -> None:
        if conn is not None:
            repo.apply_schema_sync(conn)
            conn.commit()
            return
        with self._connect() as managed:
            repo.apply_schema_sync(managed)
            managed.commit()

    # ------------------------------------------------------------------
    # enqueue
    # ------------------------------------------------------------------

    def enqueue(
        self,
        task: str | Callable[..., Any],
        *,
        args: dict[str, Any] | BaseModel | None = None,
        queue: str = "default",
        priority: int = 0,
        max_attempts: int = 20,
        scheduled_at: datetime | None = None,
        unique_key: str | None = None,
        tags: list[str] | None = None,
        timeout_seconds: int | None = None,
        conn: psycopg.Connection[Any] | None = None,
    ) -> int:
        name = task_name(task) if callable(task) else task
        args_dict = observability.inject_trace_context(_coerce_args(args))
        observability.JOBS_ENQUEUED.labels(queue=queue, task=name).inc()
        kwargs: dict[str, Any] = dict(
            task=name,
            args=args_dict,
            queue=queue,
            priority=priority,
            max_attempts=max_attempts,
            scheduled_at=scheduled_at,
            unique_key=unique_key,
            tags=tags,
            timeout_seconds=timeout_seconds,
        )

        if conn is not None:
            return repo.enqueue_sync(conn, **kwargs)

        with self._connect() as managed:
            try:
                job_id = repo.enqueue_sync(managed, **kwargs)
                managed.commit()
                return job_id
            except Exception:
                managed.rollback()
                raise

    # ------------------------------------------------------------------
    # admin
    # ------------------------------------------------------------------

    def status(self) -> list[tuple[str, str, int]]:
        with self._connect() as conn:
            return repo.status_counts_sync(conn)

    def retry(self, job_id: int) -> None:
        with self._connect() as conn:
            repo.retry_job_sync(conn, job_id)
            conn.commit()

    def cancel(self, job_id: int) -> None:
        with self._connect() as conn:
            repo.cancel_job_sync(conn, job_id)
            conn.commit()

    def pause_queue(self, name: str) -> None:
        with self._connect() as conn:
            repo.pause_queue_sync(conn, name)
            conn.commit()

    def resume_queue(self, name: str) -> None:
        with self._connect() as conn:
            repo.resume_queue_sync(conn, name)
            conn.commit()

    def list_queues(self) -> list[tuple[str, datetime | None]]:
        with self._connect() as conn:
            return repo.list_queues_sync(conn)

    def list_workers(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            return repo.list_workers_sync(conn)

    def requeue_discarded(self) -> int:
        with self._connect() as conn:
            n = repo.requeue_discarded_sync(conn)
            conn.commit()
            return n
