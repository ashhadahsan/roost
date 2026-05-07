"""Smallest possible end-to-end Roost example.

Run a Postgres locally, then::

    export ROOST_DSN=postgresql://postgres:x@localhost/postgres
    roost init --apply
    python examples/plain_python.py    # enqueues a job
    roost run --module examples.plain_python  # runs the worker (Ctrl-C to exit)
"""

from __future__ import annotations

import asyncio
import os
import sys

from roost import AsyncRoost, job


@job("hello")
async def hello(name: str) -> None:
    print(f"hello, {name}!")


async def main() -> None:
    dsn = os.environ.get("ROOST_DSN") or "postgresql://postgres:postgres@localhost/postgres"
    async with AsyncRoost(dsn) as r:
        job_id = await r.enqueue(hello, args={"name": "world"})
        print(f"enqueued job {job_id}")


if __name__ == "__main__":
    asyncio.run(main())
    sys.exit(0)
