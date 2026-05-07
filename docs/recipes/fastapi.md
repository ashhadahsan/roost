# FastAPI

```python
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI

from roost import AsyncRoost, job


@job("send_welcome_email")
async def send_welcome_email(user_id: int) -> None:
    ...


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool("postgresql://...")
    app.state.roost = AsyncRoost("postgresql://...")
    yield
    await app.state.roost.close()
    await app.state.pool.close()


app = FastAPI(lifespan=lifespan)


@app.post("/users")
async def create_user(email: str):
    pool = app.state.pool
    roost: AsyncRoost = app.state.roost
    async with pool.acquire() as conn:
        async with conn.transaction():
            user_id = await conn.fetchval(
                "INSERT INTO users (email) VALUES ($1) RETURNING id", email
            )
            await roost.enqueue(
                send_welcome_email,
                args={"user_id": user_id},
                conn=conn,
            )
    return {"id": user_id}
```

Run a worker process alongside the API:

```bash
roost run --module myapp.tasks --queues default --concurrency 8
```
