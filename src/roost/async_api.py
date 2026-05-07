"""Async public facade — primary API for FastAPI / Starlette / asyncio apps."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import datetime
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from roost import observability
from roost._core import repo
from roost._core.repo import JobInsert
from roost._core.retry import BackoffStrategy
from roost.decorators import DEFAULT_HANDLERS, HandlerRegistry, TaskDefaults, task_name
from roost.worker import Worker

if TYPE_CHECKING:  # pragma: no cover
    import asyncpg


_SENTINEL: Any = object()


def _coerce_args(args: dict[str, Any] | BaseModel | None) -> dict[str, Any]:
    if args is None:
        return {}
    if isinstance(args, BaseModel):
        return args.model_dump()
    return args


def _apply_task_defaults(
    defaults: TaskDefaults,
    *,
    queue: str,
    priority: int,
    max_attempts: int,
    tags: list[str] | None,
    timeout_seconds: int | None,
    queue_default: str,
    priority_default: int,
    max_attempts_default: int,
) -> tuple[str, int, int, list[str] | None, int | None]:
    """Pick the registered task's defaults when the caller didn't override.

    The "did the caller override?" check is done via comparison to the
    facade's own kwarg defaults — there's no way to detect missing kwargs
    after the call so we approximate: if the caller passed the same value
    the facade defaults to, we treat it as "not overridden" and use the
    task default.
    """
    final_queue = defaults.queue if (queue == queue_default and defaults.queue) else queue
    final_priority = (
        defaults.priority if (priority == priority_default and defaults.priority is not None) else priority
    )
    final_max_attempts = (
        defaults.max_attempts
        if (max_attempts == max_attempts_default and defaults.max_attempts is not None)
        else max_attempts
    )
    final_tags = list(defaults.tags) if (tags is None and defaults.tags) else tags
    final_timeout = defaults.timeout_seconds if (timeout_seconds is None) else timeout_seconds
    return final_queue, final_priority, final_max_attempts, final_tags, final_timeout


class AsyncRoost:
    """Entry point for async apps.

    Lazily opens an internal :class:`asyncpg.Pool` on first use. Pass
    ``conn=`` to :meth:`enqueue` to participate in the caller's transaction —
    that's the load-bearing primitive.
    """

    def __init__(
        self,
        dsn: str,
        *,
        registry: HandlerRegistry | None = None,
    ) -> None:
        self.dsn = dsn
        self.registry = registry or DEFAULT_HANDLERS
        self._pool: asyncpg.Pool | None = None

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    async def _ensure_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            import asyncpg

            self._pool = await asyncpg.create_pool(
                self.dsn, min_size=1, max_size=10, init=repo.init_connection
            )
        return self._pool

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def setup_schema(self, conn: asyncpg.Connection | None = None) -> None:
        """Apply the migration SQL. Idempotent."""
        if conn is not None:
            await repo.apply_schema_async(conn)
            return
        pool = await self._ensure_pool()
        async with pool.acquire() as managed:
            await repo.apply_schema_async(managed)

    # ------------------------------------------------------------------
    # enqueue
    # ------------------------------------------------------------------

    async def enqueue(
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
        depends_on: list[int] | None = None,
        metadata: dict[str, Any] | None = None,
        conn: asyncpg.Connection | None = None,
    ) -> int:
        """Insert a job. Pass ``conn=`` to enqueue inside the caller's txn.

        ``task`` may be a registered task name or a function decorated with
        ``@job(...)``. ``args`` accepts a dict or a Pydantic model — models
        are dumped via ``model_dump()`` so types like ``UUID`` and ``datetime``
        round-trip cleanly. ``metadata`` is an out-of-band JSONB column for
        trace ids / request ids / tenant ids that aren't handler input.
        """
        name = task_name(task) if callable(task) else task
        args_dict = observability.inject_trace_context(_coerce_args(args))

        spec = self.registry.get(name)
        if spec is not None:
            queue, priority, max_attempts, tags, timeout_seconds = _apply_task_defaults(
                spec.defaults,
                queue=queue,
                priority=priority,
                max_attempts=max_attempts,
                tags=tags,
                timeout_seconds=timeout_seconds,
                queue_default="default",
                priority_default=0,
                max_attempts_default=20,
            )

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
            depends_on=depends_on,
            metadata=metadata,
        )
        observability.JOBS_ENQUEUED.labels(queue=queue, task=name).inc()

        if conn is not None:
            return await repo.enqueue_async(conn, **kwargs)

        pool = await self._ensure_pool()
        async with pool.acquire() as managed:
            return await repo.enqueue_async(managed, **kwargs)

    async def enqueue_many(
        self,
        jobs: list[JobInsert],
        *,
        conn: asyncpg.Connection | None = None,
    ) -> int:
        """Bulk-insert in a single round-trip. Returns the submitted count."""
        if conn is not None:
            return await repo.enqueue_many_async(conn, jobs)
        pool = await self._ensure_pool()
        async with pool.acquire() as managed:
            return await repo.enqueue_many_async(managed, jobs)

    # ------------------------------------------------------------------
    # admin
    # ------------------------------------------------------------------

    async def status(self) -> list[tuple[str, str, int]]:
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            return await repo.status_counts_async(conn)

    async def retry(self, job_id: int) -> None:
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            await repo.retry_job_async(conn, job_id)

    async def cancel(self, job_id: int) -> None:
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            await repo.cancel_job_async(conn, job_id)

    async def pause_queue(self, name: str) -> None:
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            await repo.pause_queue_async(conn, name)

    async def resume_queue(self, name: str) -> None:
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            await repo.resume_queue_async(conn, name)

    async def list_queues(self) -> list[tuple[str, datetime | None]]:
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            return await repo.list_queues_async(conn)

    async def list_workers(self) -> list[dict[str, Any]]:
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            return await repo.list_workers_async(conn)

    async def requeue_discarded(self) -> int:
        pool = await self._ensure_pool()
        async with pool.acquire() as conn:
            return await repo.requeue_discarded_async(conn)

    async def wait_for(
        self,
        job_id: int,
        *,
        timeout: float | None = 30.0,
        poll_interval: float = 1.0,
        raise_on_failure: bool = True,
    ) -> Any:
        """Block until ``job_id`` reaches a terminal state.

        Returns a :class:`roost.JobOutcome`. By default raises
        :class:`roost.JobFailed` when the job ended in ``discarded`` or
        ``cancelled`` (set ``raise_on_failure=False`` to suppress).
        """
        from roost._core.wait import wait_for_async

        return await wait_for_async(
            self.dsn,
            job_id,
            timeout=timeout,
            poll_interval=poll_interval,
            raise_on_failure=raise_on_failure,
        )

    # ------------------------------------------------------------------
    # worker
    # ------------------------------------------------------------------

    def worker(
        self,
        *,
        queues: Iterable[str] = ("default",),
        concurrency: int = 4,
        prefetch: int | None = None,
        poll_interval: float = 1.0,
        retry_strategy: BackoffStrategy | None = None,
        run_cron: bool = True,
        heartbeat_interval: float = 15.0,
        orphan_reaper_interval: float = 30.0,
        orphan_stale_after: float = 5 * 60.0,
        shutdown_timeout: float = 30.0,
        listen_reconnect_delay: float = 1.0,
        error_cap: int = 20,
        archive_after_seconds: float | None = None,
        archive_interval: float = 60.0,
        startup_max_retries: int = 30,
        startup_retry_delay: float = 1.0,
    ) -> Worker:
        """Construct a :class:`Worker` bound to this Roost's DSN."""
        return Worker(
            self.dsn,
            queues=queues,
            concurrency=concurrency,
            prefetch=prefetch,
            poll_interval=poll_interval,
            retry_strategy=retry_strategy,
            registry=self.registry,
            run_cron=run_cron,
            heartbeat_interval=heartbeat_interval,
            orphan_reaper_interval=orphan_reaper_interval,
            orphan_stale_after=orphan_stale_after,
            shutdown_timeout=shutdown_timeout,
            listen_reconnect_delay=listen_reconnect_delay,
            error_cap=error_cap,
            archive_after_seconds=archive_after_seconds,
            archive_interval=archive_interval,
            startup_max_retries=startup_max_retries,
            startup_retry_delay=startup_retry_delay,
        )

    async def __aenter__(self) -> AsyncRoost:
        await self._ensure_pool()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()
