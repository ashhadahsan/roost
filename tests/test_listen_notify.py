from __future__ import annotations

import asyncio

import asyncpg
import pytest

from roost._core import repo
from roost._core.notify import listen_inserts


async def _open(dsn: str) -> asyncpg.Connection:
    conn = await asyncpg.connect(dsn)
    await repo.init_connection(conn)
    return conn


@pytest.mark.asyncio
async def test_listener_wakes_up_on_insert(fresh_dsn: str) -> None:
    listen_conn = await _open(fresh_dsn)
    enqueue_conn = await _open(fresh_dsn)
    wakeup = asyncio.Event()
    try:
        async with listen_inserts(listen_conn, ["default"], wakeup):
            await repo.enqueue_async(enqueue_conn, task="ping")
            await asyncio.wait_for(wakeup.wait(), timeout=5.0)
    finally:
        await listen_conn.close()
        await enqueue_conn.close()


@pytest.mark.asyncio
async def test_listener_filters_by_queue(fresh_dsn: str) -> None:
    listen_conn = await _open(fresh_dsn)
    enqueue_conn = await _open(fresh_dsn)
    wakeup = asyncio.Event()
    try:
        async with listen_inserts(listen_conn, ["emails"], wakeup):
            await repo.enqueue_async(enqueue_conn, task="ping", queue="default")
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(wakeup.wait(), timeout=0.5)
            assert not wakeup.is_set()

            await repo.enqueue_async(enqueue_conn, task="ping", queue="emails")
            await asyncio.wait_for(wakeup.wait(), timeout=5.0)
    finally:
        await listen_conn.close()
        await enqueue_conn.close()
