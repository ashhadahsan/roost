"""Worker loop: fetch ready jobs, dispatch handlers, retry on failure.

This module is the most critical part of the runtime. The design goals:

* No double-processing. ``FOR UPDATE SKIP LOCKED`` makes the row claim
  atomic; concurrency control lives in SQL, not the application.
* No silent loss. Jobs only leave ``executing`` via ``completed``,
  ``retryable``, ``discarded``, ``cancelled``, or via the orphan reaper.
* Crash-tolerant. SIGKILL'd workers leave their rows in ``executing``;
  the reaper running in any peer worker drains them after a configurable
  staleness window.
* Resilient to transient Postgres failures. Network blips and listener
  drops are caught, logged, and retried with backoff — they never kill
  the worker process.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import os
import signal
import socket
import traceback
import uuid
from collections.abc import Callable, Iterable
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import structlog

from roost import observability
from roost._core import repo
from roost._core.cron import run_scheduler
from roost._core.notify import CHANNEL_CANCEL_REQUESTED, CHANNEL_INSERTED
from roost._core.retry import BackoffStrategy, resolve
from roost.decorators import DEFAULT_HANDLERS, HandlerRegistry, HandlerSpec
from roost.exceptions import SnoozeJob, UnknownTaskError

if TYPE_CHECKING:  # pragma: no cover
    import asyncpg

_log = structlog.get_logger(__name__)


class Worker:
    """A single-process Roost worker.

    Multiple workers can run against the same database — concurrency control
    is enforced by ``FOR UPDATE SKIP LOCKED`` at the SQL level.
    """

    def __init__(
        self,
        dsn: str,
        *,
        queues: Iterable[str] = ("default",),
        concurrency: int = 4,
        prefetch: int | None = None,
        poll_interval: float = 1.0,
        retry_strategy: BackoffStrategy | None = None,
        registry: HandlerRegistry | None = None,
        run_cron: bool = True,
        heartbeat_interval: float = 15.0,
        orphan_reaper_interval: float = 30.0,
        orphan_stale_after: float = 5 * 60.0,
        shutdown_timeout: float = 30.0,
        listen_reconnect_delay: float = 1.0,
    ) -> None:
        self.dsn = dsn
        self.queues = list(queues)
        if not self.queues:
            raise ValueError("queues must not be empty")
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        self.concurrency = concurrency
        self.prefetch = prefetch if prefetch is not None else concurrency
        self.poll_interval = poll_interval
        self.retry_strategy = resolve(retry_strategy)
        self.registry = registry or DEFAULT_HANDLERS
        self.run_cron = run_cron
        self.heartbeat_interval = heartbeat_interval
        self.orphan_reaper_interval = orphan_reaper_interval
        self.orphan_stale_after = orphan_stale_after
        self.shutdown_timeout = shutdown_timeout
        self.listen_reconnect_delay = listen_reconnect_delay

        self.id = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self._stop = asyncio.Event()
        self._wakeup = asyncio.Event()
        self._inflight: set[asyncio.Task[None]] = set()
        # Map job_id -> running asyncio Task so cancel-requests can find it.
        self._running: dict[int, asyncio.Task[None]] = {}

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    async def run(self) -> None:
        import asyncpg

        pool = await asyncpg.create_pool(
            self.dsn,
            min_size=1,
            max_size=self.concurrency + 4,
            init=repo.init_connection,
        )

        background: list[asyncio.Task[None]] = []
        try:
            background.append(asyncio.create_task(self._listen_loop(), name="roost-listener"))
            background.append(asyncio.create_task(self._cancel_listen_loop(), name="roost-cancel"))
            background.append(asyncio.create_task(self._heartbeat_loop(pool), name="roost-heartbeat"))
            background.append(asyncio.create_task(self._reaper_loop(pool), name="roost-reaper"))
            if self.run_cron:
                background.append(
                    asyncio.create_task(
                        run_scheduler(pool, stop_event=self._stop, dsn=self.dsn),
                        name="roost-cron",
                    )
                )

            await self._main_loop(pool)
        finally:
            self._stop.set()
            for task in background:
                task.cancel()
            for task in background:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            await self._drain_inflight()
            with contextlib.suppress(Exception):
                async with pool.acquire() as conn:
                    await repo.deregister_worker_async(conn, self.id)
            await pool.close()
            _log.info("worker.stopped", id=self.id)

    def request_stop(self) -> None:
        self._stop.set()
        self._wakeup.set()

    def install_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self.request_stop)
            except NotImplementedError:  # pragma: no cover — Windows
                signal.signal(sig, lambda *_: self.request_stop())

    # ------------------------------------------------------------------
    # main loop
    # ------------------------------------------------------------------

    async def _main_loop(self, pool: asyncpg.Pool) -> None:
        backoff = 0.0
        while not self._stop.is_set():
            try:
                await self._promote_retryable(pool)
                picked = await self._fetch_batch(pool)
                backoff = 0.0
                if picked == 0:
                    await self._sleep_or_wakeup(self.poll_interval)
            except Exception as exc:  # pragma: no cover — defensive
                backoff = min(max(backoff * 2, 1.0), 30.0)
                _log.warning("worker.cycle_failed", error=str(exc), backoff=backoff)
                await self._sleep_or_wakeup(backoff)

    async def _promote_retryable(self, pool: asyncpg.Pool) -> None:
        async with pool.acquire() as conn:
            await repo.promote_retryable_async(conn)

    async def _fetch_batch(self, pool: asyncpg.Pool) -> int:
        free_slots = self.concurrency - len(self._inflight)
        if free_slots <= 0:
            await asyncio.sleep(0.05)
            return 0

        batch_size = min(self.prefetch, free_slots)
        async with pool.acquire() as conn, conn.transaction():
            jobs = await repo.fetch_available_async(conn, self.queues, batch_size)

        for job in jobs:
            task = asyncio.create_task(self._dispatch(pool, job), name=f"roost-job-{job.id}")
            self._inflight.add(task)
            self._running[job.id] = task

            def _cleanup(t: asyncio.Task[None], jid: int = job.id) -> None:
                self._inflight.discard(t)
                self._running.pop(jid, None)

            task.add_done_callback(_cleanup)

        return len(jobs)

    async def _drain_inflight(self) -> None:
        if not self._inflight:
            return
        _log.info(
            "worker.draining",
            inflight=len(self._inflight),
            timeout=self.shutdown_timeout,
        )
        try:
            await asyncio.wait_for(
                asyncio.gather(*self._inflight, return_exceptions=True),
                timeout=self.shutdown_timeout,
            )
        except asyncio.TimeoutError:
            still_running = [t for t in self._inflight if not t.done()]
            _log.warning(
                "worker.drain_timeout",
                cancelled=len(still_running),
                timeout=self.shutdown_timeout,
            )
            for t in still_running:
                t.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await asyncio.gather(*still_running, return_exceptions=True)

    # ------------------------------------------------------------------
    # dispatch
    # ------------------------------------------------------------------

    async def _dispatch(self, pool: asyncpg.Pool, job: Any) -> None:
        spec = self.registry.get(job.task)
        labels = {"queue": job.queue, "task": job.task}
        started = asyncio.get_running_loop().time()
        try:
            if spec is None:
                raise UnknownTaskError(f"no handler registered for task '{job.task}'")
            result = await self._invoke(spec, job)
            async with pool.acquire() as conn:
                await repo.mark_completed_async(conn, job.id, result=result)
            duration = asyncio.get_running_loop().time() - started
            observability.JOBS_COMPLETED.labels(**labels).inc()
            observability.JOB_DURATION.labels(**labels).observe(duration)
            _log.info(
                "job.completed",
                id=job.id,
                task=job.task,
                attempt=job.attempt,
                duration=round(duration, 4),
            )
        except SnoozeJob as snooze:
            when = datetime.now(tz=timezone.utc) + timedelta(seconds=snooze.seconds)
            async with pool.acquire() as conn:
                await repo.snooze_async(conn, job.id, when)
            _log.info("job.snoozed", id=job.id, task=job.task, seconds=snooze.seconds)
        except BaseException as exc:  # noqa: BLE001 — surfaced into errors[]
            await self._handle_failure(pool, job, exc)

    @staticmethod
    async def _invoke(spec: HandlerSpec, job: Any) -> Any:
        # Strip the trace carrier off args so it's not forwarded to the handler.
        args = dict(job.args or {})
        _, carrier = observability.strip_trace_context(args)
        timeout = job.timeout_seconds

        async def _run() -> Any:
            with observability.job_span(
                f"job:{job.task}",
                {
                    "roost.job.id": job.id,
                    "roost.job.queue": job.queue,
                    "roost.job.task": job.task,
                    "roost.job.attempt": job.attempt,
                },
                carrier,
            ):
                if spec.is_async:
                    return await spec.func(**args)
                return await asyncio.to_thread(_call_sync_handler, spec.func, args)

        if timeout and timeout > 0:
            return await asyncio.wait_for(_run(), timeout=float(timeout))
        return await _run()

    async def _handle_failure(self, pool: asyncpg.Pool, job: Any, exc: BaseException) -> None:
        next_attempt = job.attempt
        error_payload = {
            "attempt": next_attempt,
            "at": datetime.now(tz=timezone.utc).isoformat(),
            "error": f"{type(exc).__name__}: {exc}",
            "trace": "".join(traceback.format_exception(exc)).strip(),
        }
        try:
            async with pool.acquire() as conn:
                if isinstance(exc, asyncio.CancelledError):
                    # Re-fetch cancel_requested in case the row updated after dispatch.
                    requested = await conn.fetchval(
                        "SELECT cancel_requested FROM roost.jobs WHERE id = $1", job.id
                    )
                    if requested:
                        await repo.finalize_cancel_async(conn, job.id)
                        observability.JOBS_FAILED.labels(
                            queue=job.queue, task=job.task, outcome="cancelled"
                        ).inc()
                        _log.info("job.cancelled", id=job.id, task=job.task)
                        return
                if next_attempt >= job.max_attempts:
                    await repo.mark_discarded_async(conn, job.id, error_payload)
                    observability.JOBS_FAILED.labels(
                        queue=job.queue, task=job.task, outcome="discarded"
                    ).inc()
                    _log.warning(
                        "job.discarded",
                        id=job.id,
                        task=job.task,
                        attempt=next_attempt,
                        error=error_payload["error"],
                    )
                else:
                    delay = float(self.retry_strategy(next_attempt))
                    when = datetime.now(tz=timezone.utc) + timedelta(seconds=delay)
                    await repo.mark_retryable_async(conn, job.id, when, error_payload)
                    observability.JOBS_FAILED.labels(
                        queue=job.queue, task=job.task, outcome="retryable"
                    ).inc()
                    _log.info(
                        "job.retry_scheduled",
                        id=job.id,
                        task=job.task,
                        attempt=next_attempt,
                        delay=delay,
                        error=error_payload["error"],
                    )
        except Exception as inner:  # pragma: no cover — defensive
            _log.error(
                "job.failure_record_failed",
                id=job.id,
                task=job.task,
                error=str(inner),
            )

    # ------------------------------------------------------------------
    # background loops
    # ------------------------------------------------------------------

    async def _listen_loop(self) -> None:
        """Maintain a LISTEN connection. Reconnects on drop."""
        import asyncpg

        def _handler(_conn: object, _pid: int, _channel: str, payload: str) -> None:
            if payload in self.queues:
                self._wakeup.set()

        while not self._stop.is_set():
            conn: asyncpg.Connection | None = None
            try:
                conn = await asyncpg.connect(self.dsn)
                await repo.init_connection(conn)
                await conn.add_listener(CHANNEL_INSERTED, _handler)
                _log.info("listener.connected")
                # Hold the connection until shutdown or it dies.
                while not self._stop.is_set():
                    if conn.is_closed():
                        raise ConnectionError("listen connection closed")
                    await asyncio.sleep(self.listen_reconnect_delay)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _log.warning("listener.error", error=str(exc))
                await self._sleep_or_wakeup(min(self.listen_reconnect_delay * 5, 30.0))
            finally:
                if conn is not None:
                    with contextlib.suppress(Exception):
                        await conn.remove_listener(CHANNEL_INSERTED, _handler)
                    with contextlib.suppress(Exception):
                        await conn.close()

    async def _cancel_listen_loop(self) -> None:
        """Cancel in-flight handlers when ``cancel_requested`` flips to true."""
        import asyncpg

        loop = asyncio.get_running_loop()

        def _handler(_conn: object, _pid: int, _channel: str, payload: str) -> None:
            try:
                jid = int(payload)
            except (TypeError, ValueError):
                return
            task = self._running.get(jid)
            if task is not None and not task.done():
                _log.info("job.cancel_signaled", id=jid)
                loop.call_soon_threadsafe(task.cancel)

        while not self._stop.is_set():
            conn: asyncpg.Connection | None = None
            try:
                conn = await asyncpg.connect(self.dsn)
                await repo.init_connection(conn)
                await conn.add_listener(CHANNEL_CANCEL_REQUESTED, _handler)
                while not self._stop.is_set():
                    if conn.is_closed():
                        raise ConnectionError("cancel-listen connection closed")
                    await asyncio.sleep(self.listen_reconnect_delay)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _log.warning("cancel_listener.error", error=str(exc))
                await self._sleep_or_wakeup(min(self.listen_reconnect_delay * 5, 30.0))
            finally:
                if conn is not None:
                    with contextlib.suppress(Exception):
                        await conn.remove_listener(CHANNEL_CANCEL_REQUESTED, _handler)
                    with contextlib.suppress(Exception):
                        await conn.close()

    async def _heartbeat_loop(self, pool: asyncpg.Pool) -> None:
        hostname = socket.gethostname()
        pid = os.getpid()
        while not self._stop.is_set():
            try:
                async with pool.acquire() as conn:
                    await repo.heartbeat_async(
                        conn,
                        worker_id=self.id,
                        hostname=hostname,
                        pid=pid,
                        queues=self.queues,
                        metadata={
                            "concurrency": self.concurrency,
                            "inflight": len(self._inflight),
                        },
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _log.warning("worker.heartbeat_failed", error=str(exc))
            await self._sleep_or_wakeup(self.heartbeat_interval)

    async def _reaper_loop(self, pool: asyncpg.Pool) -> None:
        while not self._stop.is_set():
            try:
                async with pool.acquire() as conn:
                    reaped = await repo.reap_orphans_async(conn, stale_after_seconds=self.orphan_stale_after)
                    gced = await repo.gc_workers_async(
                        conn, stale_after_seconds=max(self.heartbeat_interval * 4, 60.0)
                    )
                if reaped:
                    _log.warning(
                        "worker.reaped_orphans",
                        count=len(reaped),
                        ids=[i for i, _ in reaped],
                    )
                if gced:
                    _log.info("worker.gc_workers", count=gced)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _log.warning("worker.reaper_failed", error=str(exc))
            await self._sleep_or_wakeup(self.orphan_reaper_interval)

    async def _sleep_or_wakeup(self, seconds: float) -> None:
        if seconds <= 0:
            return
        try:
            await asyncio.wait_for(self._wakeup.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass
        finally:
            self._wakeup.clear()


def _call_sync_handler(func: Callable[..., Any], args: dict[str, Any]) -> Any:
    result = func(**args)
    if inspect.isawaitable(result):
        raise TypeError(
            f"sync handler {func.__qualname__} returned an awaitable — use `async def` for async handlers"
        )
    return result


__all__ = ["Worker"]
