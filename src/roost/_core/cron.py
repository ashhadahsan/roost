"""Cluster-wide cron scheduler.

A single advisory lock guarantees only one scheduler is active per database
at a time — workers can be horizontally scaled without double-firing crons.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import structlog
from croniter import croniter

try:  # 3.9+ stdlib; we require 3.10+ so always present
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment,misc]

from roost._core import repo

if TYPE_CHECKING:  # pragma: no cover
    import asyncpg


_log = structlog.get_logger(__name__)

# A stable arbitrary 64-bit constant for the advisory lock so multiple
# Roost-using applications on the same database don't collide.
ADVISORY_LOCK_KEY = 0x52_4F_4F_53_54_43_52_4E  # 'ROOSTCRN'


@dataclass(frozen=True)
class CronEntry:
    name: str
    expression: str
    task: str
    args: dict[str, Any] = field(default_factory=dict)
    queue: str = "default"
    priority: int = 0
    max_attempts: int = 20
    timezone_name: str | None = None  # IANA name; e.g. "America/Los_Angeles". None == UTC.

    def _tz(self) -> Any:
        if self.timezone_name is None or ZoneInfo is None:
            return timezone.utc
        return ZoneInfo(self.timezone_name)

    def _localise(self, when: datetime) -> datetime:
        """Render ``when`` in the entry's local timezone for cron evaluation."""
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        return when.astimezone(self._tz())

    def next_after(self, now: datetime) -> datetime:
        itr = croniter(self.expression, self._localise(now))
        nxt = itr.get_next(datetime)
        if nxt.tzinfo is None:
            nxt = nxt.replace(tzinfo=self._tz())
        return nxt.astimezone(timezone.utc)

    def previous_or_at(self, now: datetime) -> datetime:
        itr = croniter(self.expression, self._localise(now))
        prev = itr.get_prev(datetime)
        if prev.tzinfo is None:
            prev = prev.replace(tzinfo=self._tz())
        return prev.astimezone(timezone.utc)


class CronRegistry:
    def __init__(self) -> None:
        self._entries: dict[str, CronEntry] = {}

    def register(self, entry: CronEntry) -> None:
        if entry.name in self._entries:
            existing = self._entries[entry.name]
            if existing == entry:
                return
            raise ValueError(f"cron name '{entry.name}' is already registered")
        self._entries[entry.name] = entry

    def all(self) -> list[CronEntry]:
        return list(self._entries.values())


# Module-global default registry. The decorators import this directly.
DEFAULT_REGISTRY = CronRegistry()


async def run_scheduler(
    pool: asyncpg.Pool,
    registry: CronRegistry | None = None,
    *,
    interval_seconds: float = 60.0,
    stop_event: asyncio.Event | None = None,
    dsn: str | None = None,
) -> None:
    """Long-running coroutine. Wakes every ``interval_seconds`` and enqueues
    any cron entries whose previous run is overdue.

    Holds the advisory lock on a dedicated connection (so we don't tie up a
    pool slot for the lifetime of the worker), and acquires fresh pool
    connections per tick for the actual work. If ``dsn`` is omitted the lock
    connection is borrowed from the pool — convenient but it costs a slot.
    """
    import asyncpg

    reg = registry or DEFAULT_REGISTRY
    stop_event = stop_event or asyncio.Event()

    lock_conn: asyncpg.Connection | None = None
    borrowed = False
    if dsn is not None:
        lock_conn = await asyncpg.connect(dsn)
        await repo.init_connection(lock_conn)
    else:
        # Fallback: borrow from the pool. Caller pays a slot.
        lock_conn = await pool.acquire()
        borrowed = True

    try:
        if lock_conn is None:
            return
        if not await repo.cron_try_lock_async(lock_conn, ADVISORY_LOCK_KEY):
            _log.info("cron.skip.lock_held")
            return

        _log.info("cron.lock_acquired")
        try:
            while not stop_event.is_set():
                try:
                    async with pool.acquire() as work_conn:
                        await _run_once(work_conn, reg)
                except Exception as exc:  # pragma: no cover — defensive
                    _log.warning("cron.tick_failed", error=str(exc))
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        finally:
            with contextlib.suppress(Exception):
                await repo.cron_unlock_async(lock_conn, ADVISORY_LOCK_KEY)
            _log.info("cron.lock_released")
    finally:
        if lock_conn is not None:
            if borrowed:
                with contextlib.suppress(Exception):
                    await pool.release(lock_conn)
            else:
                with contextlib.suppress(Exception):
                    await lock_conn.close()


async def _run_once(conn: asyncpg.Connection, registry: CronRegistry) -> None:
    now = datetime.now(tz=timezone.utc)
    for entry in registry.all():
        due_at = entry.previous_or_at(now)
        # croniter.get_prev returns naive sometimes; coerce to UTC.
        if due_at.tzinfo is None:
            due_at = due_at.replace(tzinfo=timezone.utc)
        if due_at > now:
            continue
        try:
            should_enqueue = await repo.cron_should_run_async(conn, entry.name, due_at)
        except Exception as exc:  # pragma: no cover — defensive
            _log.warning("cron.claim_failed", name=entry.name, error=str(exc))
            continue
        if not should_enqueue:
            continue
        try:
            await repo.enqueue_async(
                conn,
                task=entry.task,
                args=entry.args,
                queue=entry.queue,
                priority=entry.priority,
                max_attempts=entry.max_attempts,
                scheduled_at=now,
                unique_key=f"cron:{entry.name}:{int(due_at.timestamp())}",
            )
            _log.info("cron.enqueued", name=entry.name, due_at=due_at.isoformat())
        except Exception as exc:  # pragma: no cover — defensive
            _log.warning("cron.enqueue_failed", name=entry.name, error=str(exc))
