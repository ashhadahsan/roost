from __future__ import annotations

import asyncpg
import psycopg
import pytest

from roost import AsyncRoost, Roost, job
from roost._core import repo


@pytest.mark.asyncio
async def test_async_enqueue_in_caller_transaction_commits(
    async_conn: asyncpg.Connection,
) -> None:
    async with async_conn.transaction():
        job_id = await repo.enqueue_async(async_conn, task="ping")
    row = await async_conn.fetchrow("SELECT id, task, state FROM roost.jobs WHERE id = $1", job_id)
    assert row is not None
    assert row["task"] == "ping"
    assert row["state"] == "available"


@pytest.mark.asyncio
async def test_async_enqueue_rolls_back_with_caller_transaction(
    async_conn: asyncpg.Connection,
) -> None:
    """The load-bearing invariant: rollback must drop the inserted row."""
    txn = async_conn.transaction()
    await txn.start()
    job_id = await repo.enqueue_async(async_conn, task="ping")
    await txn.rollback()

    count = await async_conn.fetchval("SELECT COUNT(*) FROM roost.jobs")
    assert count == 0
    # the id we received was generated mid-transaction; it must not exist post-rollback
    survivor = await async_conn.fetchval("SELECT id FROM roost.jobs WHERE id = $1", job_id)
    assert survivor is None


def test_sync_enqueue_in_caller_transaction_rolls_back(
    sync_conn: psycopg.Connection,
) -> None:
    repo.enqueue_sync(sync_conn, task="ping")
    sync_conn.rollback()
    with sync_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM roost.jobs")
        row = cur.fetchone()
        assert row is not None and row[0] == 0


@pytest.mark.asyncio
async def test_async_facade_enqueue_with_callable(fresh_dsn: str) -> None:
    @job("send_email")
    async def send_email(user_id: int) -> None: ...

    async with AsyncRoost(fresh_dsn) as r:
        job_id = await r.enqueue(send_email, args={"user_id": 42}, queue="emails")
        rows = await r.status()
        assert (("emails", "available", 1)) in rows
        assert job_id > 0


def test_sync_facade_default_commit(fresh_dsn: str) -> None:
    @job("resize_image")
    def resize_image(image_id: int) -> None: ...

    r = Roost(fresh_dsn)
    job_id = r.enqueue(resize_image, args={"image_id": 9})
    assert job_id > 0
    counts = r.status()
    assert (("default", "available", 1)) in counts
