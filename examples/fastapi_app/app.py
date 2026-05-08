"""FastAPI app — transactional enqueue inside a request.

Run::

    docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=x postgres:16-alpine
    export DSN=postgresql://postgres:x@localhost/postgres
    roost init --apply --dsn $DSN

    # Terminal 1 — API
    uvicorn examples.fastapi_app.app:app --reload

    # Terminal 2 — worker
    ROOST_DSN=$DSN roost run --module examples.fastapi_app.app

    # Terminal 3 — exercise
    curl -X POST localhost:8000/users -H 'content-type: application/json' \\
         -d '{"email": "a@b.test"}'
    curl localhost:8000/jobs/<job_id>
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, EmailStr

from roost import AsyncRoost, JobFailed, JobOutcome, JobTimeoutError, job
from roost.contrib.fastapi import RoostDep, tx_roost_dep

DSN = os.environ.get("DATABASE_URL") or os.environ["ROOST_DSN"]


# ---------------------------------------------------------------------------
# Tasks — picked up by both the API process (registry) and the worker process
# ---------------------------------------------------------------------------


class WelcomeArgs(BaseModel):
    user_id: int


@job("send_welcome_email", args_model=WelcomeArgs, queue="emails", max_attempts=5)
async def send_welcome_email(user_id: int) -> dict[str, Any]:
    # In real code this would call your email provider.
    return {"sent_to": user_id, "ok": True}


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.users_pool = await asyncpg.create_pool(DSN, min_size=1, max_size=10)
    app.state.roost = AsyncRoost(DSN)
    # Auto-create the demo users table.
    async with app.state.users_pool.acquire() as c:
        await c.execute(
            "CREATE TABLE IF NOT EXISTS users ("
            "  id SERIAL PRIMARY KEY, email TEXT UNIQUE NOT NULL,"
            "  created_at TIMESTAMPTZ NOT NULL DEFAULT now())"
        )
    yield
    await app.state.roost.close()
    await app.state.users_pool.close()


app = FastAPI(lifespan=lifespan)


class CreateUserBody(BaseModel):
    email: EmailStr


@app.post("/users")
async def create_user(body: CreateUserBody, deps=Depends(tx_roost_dep)) -> dict[str, Any]:
    """Insert the user + enqueue the welcome email in one atomic transaction.

    If anything inside this block raises after the INSERT, the user creation
    *and* the queued job both roll back. That's the load-bearing primitive.
    """
    roost, conn = deps
    user_id = await conn.fetchval("INSERT INTO users (email) VALUES ($1) RETURNING id", body.email)
    job_id = await roost.enqueue(send_welcome_email, args=WelcomeArgs(user_id=user_id), conn=conn)
    return {"user_id": user_id, "job_id": job_id}


@app.get("/jobs/{job_id}")
async def show_job(job_id: int, roost: AsyncRoost = Depends(RoostDep)) -> dict[str, Any]:
    """Inspect a job by id — handy for the tutorial."""
    pool = await roost._ensure_pool()  # noqa: SLF001 — example reads internals
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, state, attempt, result, errors FROM roost.jobs WHERE id = $1",
            job_id,
        )
    if row is None:
        raise HTTPException(status_code=404)
    return dict(row)


@app.get("/jobs/{job_id}/wait")
async def wait_for_job(job_id: int, roost: AsyncRoost = Depends(RoostDep)) -> dict[str, Any]:
    """Block on a result Celery-style. Returns 504 on timeout, 502 on terminal failure."""
    try:
        outcome: JobOutcome = await roost.wait_for(job_id, timeout=15.0)
    except JobTimeoutError as exc:
        raise HTTPException(status_code=504, detail=str(exc)) from exc
    except JobFailed as exc:
        raise HTTPException(status_code=502, detail={"state": exc.state, "errors": exc.errors}) from exc
    return {"state": outcome.state, "result": outcome.result}
