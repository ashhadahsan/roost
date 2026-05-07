"""Tier 2 tests: migrations + job chaining."""

from __future__ import annotations

import asyncio

import asyncpg
import psycopg
import pytest

from roost import AsyncRoost, job
from roost._core import repo
from roost._core.migrations import (
    MIGRATIONS,
    IrreversibleMigration,
    applied_versions_async,
    applied_versions_sync,
    apply_pending_async,
    rollback_to_async,
)

# ---------------------------------------------------------------------------
# Migrations
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_pending_records_each_migration(async_conn: asyncpg.Connection) -> None:
    # The conftest fixture already ran the migrations once; re-running should be a no-op.
    again = await apply_pending_async(async_conn)
    assert again == []
    versions = await applied_versions_async(async_conn)
    assert versions == sorted(m.version for m in MIGRATIONS)


@pytest.mark.asyncio
async def test_rollback_runs_down_in_reverse(async_conn: asyncpg.Connection) -> None:
    # Roll back to v1 — every migration above should have its down() applied,
    # including the chaining migration that adds depends_on.
    reverted = await rollback_to_async(async_conn, target=1)
    assert 2 in reverted  # chaining migration was reverted
    cols = [
        r["column_name"]
        for r in await async_conn.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'roost' AND table_name = 'jobs'"
        )
    ]
    assert "depends_on" not in cols

    # And re-apply gets us back to latest.
    applied = await apply_pending_async(async_conn)
    assert 2 in applied
    versions = await applied_versions_async(async_conn)
    assert versions == sorted(m.version for m in MIGRATIONS)


@pytest.mark.asyncio
async def test_rollback_refuses_irreversible(async_conn: asyncpg.Connection) -> None:
    # Pretend a fictional version 99 is recorded but absent from MIGRATIONS.
    await async_conn.execute("INSERT INTO roost.migrations (version, name) VALUES (99, 'fictional')")
    with pytest.raises(IrreversibleMigration):
        await rollback_to_async(async_conn, target=1)
    # Clean up so other tests aren't affected.
    await async_conn.execute("DELETE FROM roost.migrations WHERE version = 99")


def test_applied_versions_sync(fresh_dsn: str) -> None:
    with psycopg.connect(fresh_dsn) as conn:
        assert applied_versions_sync(conn) == sorted(m.version for m in MIGRATIONS)


# ---------------------------------------------------------------------------
# Chaining: depends_on
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dependent_job_blocked_until_parent_completes(
    fresh_dsn: str, async_conn: asyncpg.Connection
) -> None:
    fired_order: list[str] = []

    @job("parent_step")
    async def parent_step() -> None:
        await asyncio.sleep(0.05)
        fired_order.append("parent")

    @job("child_step")
    async def child_step() -> None:
        fired_order.append("child")

    async with AsyncRoost(fresh_dsn) as r:
        parent_id = await r.enqueue(parent_step)
        child_id = await r.enqueue(child_step, depends_on=[parent_id])

        # Sanity: the row carries the dependency.
        row = await async_conn.fetchrow("SELECT depends_on FROM roost.jobs WHERE id = $1", child_id)
        assert row is not None
        assert list(row["depends_on"]) == [parent_id]

        worker = r.worker(concurrency=1, run_cron=False, poll_interval=0.05)
        task = asyncio.create_task(worker.run())
        try:
            deadline = asyncio.get_event_loop().time() + 5.0
            while len(fired_order) < 2:
                if asyncio.get_event_loop().time() > deadline:
                    raise AssertionError(f"only ran: {fired_order}")
                await asyncio.sleep(0.05)
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5.0)

    assert fired_order == ["parent", "child"]


@pytest.mark.asyncio
async def test_dependent_skipped_while_parent_pending(async_conn: asyncpg.Connection) -> None:
    parent = await repo.enqueue_async(async_conn, task="p")
    child = await repo.enqueue_async(async_conn, task="c", depends_on=[parent])

    # Fetch must skip the child because its parent isn't completed.
    jobs = await repo.fetch_available_async(async_conn, ["default"], 10)
    fetched_ids = {j.id for j in jobs}
    assert parent in fetched_ids
    assert child not in fetched_ids


@pytest.mark.asyncio
async def test_blocked_dependent_is_cancelled_when_parent_dies(
    async_conn: asyncpg.Connection,
) -> None:
    parent = await repo.enqueue_async(async_conn, task="p")
    child = await repo.enqueue_async(async_conn, task="c", depends_on=[parent])

    # Move parent to discarded — child is now permanently blocked.
    await async_conn.execute(
        "UPDATE roost.jobs SET state = 'discarded', discarded_at = now() WHERE id = $1",
        parent,
    )

    cancelled = await repo.cancel_blocked_dependents_async(async_conn)
    assert child in cancelled

    row = await async_conn.fetchrow("SELECT state, errors FROM roost.jobs WHERE id = $1", child)
    assert row is not None
    assert row["state"] == "cancelled"
    assert row["errors"]
    assert "BlockedDependency" in row["errors"][-1]["error"]


@pytest.mark.asyncio
async def test_multiple_parents_all_must_complete(async_conn: asyncpg.Connection) -> None:
    p1 = await repo.enqueue_async(async_conn, task="p")
    p2 = await repo.enqueue_async(async_conn, task="p")
    child = await repo.enqueue_async(async_conn, task="c", depends_on=[p1, p2])

    # Only p1 completed — child still blocked.
    await async_conn.execute(
        "UPDATE roost.jobs SET state = 'completed', completed_at = now() WHERE id = $1", p1
    )
    fetched = await repo.fetch_available_async(async_conn, ["default"], 10)
    assert child not in {j.id for j in fetched}

    # Reset p1 + p2 to executing→completed flow so the next fetch sees only the child.
    await async_conn.execute(
        "UPDATE roost.jobs SET state = 'completed', completed_at = now() WHERE id = $1", p2
    )
    fetched = await repo.fetch_available_async(async_conn, ["default"], 10)
    assert {j.id for j in fetched} == {child}
