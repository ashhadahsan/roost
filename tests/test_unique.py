from __future__ import annotations

import asyncpg
import pytest

from roost._core import repo


@pytest.mark.asyncio
async def test_unique_key_dedup_returns_existing_id(async_conn: asyncpg.Connection) -> None:
    first = await repo.enqueue_async(async_conn, task="t", unique_key="u-1")
    second = await repo.enqueue_async(async_conn, task="t", unique_key="u-1")
    assert first == second
    count = await async_conn.fetchval("SELECT COUNT(*) FROM roost.jobs")
    assert count == 1


@pytest.mark.asyncio
async def test_unique_key_allows_reuse_after_terminal_state(
    async_conn: asyncpg.Connection,
) -> None:
    first = await repo.enqueue_async(async_conn, task="t", unique_key="u-2")
    await async_conn.execute("UPDATE roost.jobs SET state = 'completed' WHERE id = $1", first)

    second = await repo.enqueue_async(async_conn, task="t", unique_key="u-2")
    assert second != first
    count = await async_conn.fetchval("SELECT COUNT(*) FROM roost.jobs")
    assert count == 2


@pytest.mark.asyncio
async def test_distinct_unique_keys_create_distinct_jobs(
    async_conn: asyncpg.Connection,
) -> None:
    a = await repo.enqueue_async(async_conn, task="t", unique_key="a")
    b = await repo.enqueue_async(async_conn, task="t", unique_key="b")
    assert a != b
