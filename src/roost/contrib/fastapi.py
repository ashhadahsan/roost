"""FastAPI integration helpers.

Two patterns most apps use:

1. **Singleton AsyncRoost** lives on ``app.state.roost`` (created in
   the lifespan). Inject it into request handlers via :func:`RoostDep`.

2. **Transactional enqueue** alongside the request's existing DB write.
   :func:`tx_roost_dep` returns a callable that yields ``(roost, conn)``
   inside an ``asyncpg`` transaction so business writes and the
   ``INSERT INTO roost.jobs`` commit atomically.

Example::

    from contextlib import asynccontextmanager
    import asyncpg
    from fastapi import FastAPI, Depends
    from roost import AsyncRoost
    from roost.contrib.fastapi import RoostDep

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.roost = AsyncRoost("postgresql://…")
        yield
        await app.state.roost.close()

    app = FastAPI(lifespan=lifespan)

    @app.post("/users")
    async def create_user(roost: AsyncRoost = Depends(RoostDep)):
        await roost.enqueue("send_welcome", args={"user_id": 42})
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    import asyncpg
    from fastapi import Request

    from roost.async_api import AsyncRoost


def RoostDep(request: Request) -> AsyncRoost:  # noqa: N802 — FastAPI dep convention
    """FastAPI dependency that returns the ``AsyncRoost`` on ``app.state.roost``."""
    roost = getattr(request.app.state, "roost", None)
    if roost is None:  # pragma: no cover — defensive
        raise RuntimeError("AsyncRoost not on app.state.roost — set it inside your lifespan handler")
    return roost  # type: ignore[no-any-return]


async def tx_roost_dep(
    request: Request,
) -> AsyncIterator[tuple[AsyncRoost, asyncpg.Connection]]:
    """Yield ``(roost, conn)`` inside an asyncpg transaction.

    Use this when you want the ``INSERT INTO roost.jobs`` to commit in
    the same transaction as your business writes::

        @app.post("/orders")
        async def create_order(deps = Depends(tx_roost_dep)):
            roost, conn = deps
            order_id = await conn.fetchval("INSERT INTO orders ... RETURNING id")
            await roost.enqueue("send_invoice", args={"order_id": order_id}, conn=conn)

    Requires ``app.state.roost`` to be an :class:`AsyncRoost` whose pool
    is open.
    """
    roost = RoostDep(request)
    pool = await roost._ensure_pool()  # noqa: SLF001 — facade method
    async with pool.acquire() as conn, conn.transaction():
        yield roost, conn


__all__ = ["RoostDep", "tx_roost_dep"]
