"""Shared pytest fixtures.

Spins up a real Postgres via ``testcontainers`` once per session. Each test
gets a freshly-applied schema in its own database to keep cross-test state
contained.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from typing import Any

import asyncpg
import psycopg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from roost._core.repo import apply_schema_sync
from roost._core.schema import migration_sql
from roost.decorators import DEFAULT_HANDLERS


@pytest.fixture(scope="session")
def pg_container() -> Iterator[PostgresContainer]:
    image = os.environ.get("ROOST_TEST_PG_IMAGE", "postgres:16-alpine")
    with PostgresContainer(image) as container:
        yield container


@pytest.fixture(scope="session")
def base_dsn(pg_container: PostgresContainer) -> str:
    """Asyncpg-compatible DSN — no SQLAlchemy ``+psycopg2`` prefix."""
    raw = pg_container.get_connection_url()
    # testcontainers returns ``postgresql+psycopg2://...`` — strip the driver tag.
    if raw.startswith("postgresql+"):
        raw = "postgresql://" + raw.split("://", 1)[1]
    return raw


def _admin_dsn(base: str) -> str:
    # PostgresContainer uses the `test`/`test` user with database `test`.
    # Keep it for admin operations (CREATE DATABASE).
    return base


@pytest.fixture
def fresh_dsn(base_dsn: str, request: pytest.FixtureRequest) -> Iterator[str]:
    """Create a fresh database per test, drop it on teardown."""
    safe = request.node.name.replace("-", "_").replace("[", "_").replace("]", "_")[:40]
    suffix = abs(hash(request.node.nodeid)) % 10_000
    db_name = f"roost_{safe}_{suffix}"

    with psycopg.connect(_admin_dsn(base_dsn), autocommit=True) as admin, admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)')
        cur.execute(f'CREATE DATABASE "{db_name}"')

    parts = base_dsn.rsplit("/", 1)
    dsn = f"{parts[0]}/{db_name}"

    with psycopg.connect(dsn) as conn:
        apply_schema_sync(conn)
        conn.commit()

    yield dsn

    with psycopg.connect(_admin_dsn(base_dsn), autocommit=True) as admin, admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)')


@pytest_asyncio.fixture
async def async_conn(fresh_dsn: str) -> AsyncIterator[asyncpg.Connection]:
    from roost._core.repo import init_connection

    conn = await asyncpg.connect(fresh_dsn)
    await init_connection(conn)
    try:
        yield conn
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def async_pool(fresh_dsn: str) -> AsyncIterator[asyncpg.Pool]:
    from roost._core.repo import init_connection

    pool = await asyncpg.create_pool(fresh_dsn, min_size=1, max_size=4, init=init_connection)
    try:
        yield pool
    finally:
        await pool.close()


@pytest.fixture
def sync_conn(fresh_dsn: str) -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(fresh_dsn) as conn:
        yield conn


@pytest.fixture(autouse=True)
def reset_default_registry() -> Iterator[None]:
    """Tests register handlers freely without leaking into one another."""
    snapshot = dict(DEFAULT_HANDLERS._handlers)  # noqa: SLF001
    DEFAULT_HANDLERS.clear()
    try:
        yield
    finally:
        DEFAULT_HANDLERS.clear()
        DEFAULT_HANDLERS._handlers.update(snapshot)  # noqa: SLF001


# Re-export migration SQL for tests that want to inspect it directly.
__all__ = ["migration_sql"]
