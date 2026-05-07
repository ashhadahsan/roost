"""LISTEN/NOTIFY helpers for the worker wakeup path."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    import asyncpg


CHANNEL_INSERTED = "roost_inserted"
CHANNEL_UPDATED = "roost_updated"
CHANNEL_CANCEL_REQUESTED = "roost_cancel_requested"


@asynccontextmanager
async def listen_inserts(
    conn: asyncpg.Connection,
    queues: Iterable[str],
    wakeup: asyncio.Event,
) -> AsyncIterator[None]:
    """Set ``wakeup`` whenever a NOTIFY arrives for one of ``queues``.

    Use as ``async with listen_inserts(conn, queues, ev): ...``.
    """

    queue_set = set(queues)

    def _handler(_conn: object, _pid: int, _channel: str, payload: str) -> None:
        if not queue_set or payload in queue_set:
            wakeup.set()

    await conn.add_listener(CHANNEL_INSERTED, _handler)
    try:
        yield
    finally:
        with contextlib.suppress(Exception):  # pragma: no cover — best-effort cleanup
            await conn.remove_listener(CHANNEL_INSERTED, _handler)
