"""ETL pipeline: extract → transform → load, three chained tasks per row.

Demonstrates:

* ``depends_on`` for sequential pipelines.
* ``rate_per_minute`` to throttle the source API politely.
* ``max_concurrency`` to bound load on the destination.
* ``timeout_seconds`` to prevent a stuck transform from blocking workers.

Run::

    roost init --apply
    roost run --module examples.etl_pipeline.etl --concurrency 16

    # In another shell:
    python -m examples.etl_pipeline.etl
"""

from __future__ import annotations

import asyncio
import os
import random
from typing import Any

from pydantic import BaseModel

from roost import AsyncRoost, job

DSN = os.environ.get("ROOST_DSN", "postgresql://postgres:postgres@localhost/postgres")


class ExtractArgs(BaseModel):
    source_id: int


class TransformArgs(BaseModel):
    source_id: int


class LoadArgs(BaseModel):
    source_id: int


@job(
    "etl_extract",
    args_model=ExtractArgs,
    queue="etl",
    rate_per_minute=120,  # be polite to the upstream API
    max_attempts=5,
    timeout_seconds=15,
    tags=["etl", "extract"],
)
async def etl_extract(source_id: int) -> dict[str, Any]:
    await asyncio.sleep(random.uniform(0.05, 0.2))
    # Pretend we fetched a row from an upstream system.
    return {"source_id": source_id, "rows": random.randint(10, 200)}


@job(
    "etl_transform",
    args_model=TransformArgs,
    queue="etl",
    max_concurrency=8,  # CPU-bound, don't oversubscribe
    timeout_seconds=30,
    max_attempts=3,
    tags=["etl", "transform"],
)
async def etl_transform(source_id: int) -> dict[str, Any]:
    await asyncio.sleep(random.uniform(0.1, 0.5))
    return {"source_id": source_id, "transformed": True}


@job(
    "etl_load",
    args_model=LoadArgs,
    queue="etl",
    max_concurrency=4,  # respect destination warehouse limits
    timeout_seconds=60,
    max_attempts=10,  # network blips happen
    tags=["etl", "load"],
)
async def etl_load(source_id: int) -> dict[str, Any]:
    await asyncio.sleep(random.uniform(0.2, 0.8))
    return {"source_id": source_id, "loaded": True}


async def schedule_pipeline(roost: AsyncRoost, source_ids: list[int]) -> list[int]:
    """For each source id, enqueue extract → transform → load chained on completion."""
    final_ids = []
    for sid in source_ids:
        ex = await roost.enqueue(etl_extract, args=ExtractArgs(source_id=sid))
        tr = await roost.enqueue(etl_transform, args=TransformArgs(source_id=sid), depends_on=[ex])
        ld = await roost.enqueue(etl_load, args=LoadArgs(source_id=sid), depends_on=[tr])
        final_ids.append(ld)
    return final_ids


async def main() -> None:
    async with AsyncRoost(DSN) as roost:
        ids = await schedule_pipeline(roost, list(range(1, 21)))
        print(f"enqueued {len(ids)} pipelines (final load job ids: {ids})")


if __name__ == "__main__":
    asyncio.run(main())
