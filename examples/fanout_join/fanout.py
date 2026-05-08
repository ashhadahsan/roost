"""Fan-out → join pattern using ``depends_on``.

Spawn N child jobs in parallel, then a single aggregate job that runs only
after every child has completed::

    [scrape A]  ┐
    [scrape B]  ├──► [aggregate]
    [scrape C]  ┘

If any child ends in ``discarded`` or ``cancelled``, the aggregate is
auto-cancelled by the worker's reaper with a ``BlockedDependency`` error
recorded — your aggregate never runs against half-data.

Run::

    roost init --apply
    roost run --module examples.fanout_join.fanout

    # In another shell:
    python -m examples.fanout_join.fanout
"""

from __future__ import annotations

import asyncio
import os
import random
from typing import Any

from pydantic import BaseModel

from roost import AsyncRoost, JobFailed, JobTimeoutError, job

DSN = os.environ.get("ROOST_DSN", "postgresql://postgres:postgres@localhost/postgres")


class ScrapeArgs(BaseModel):
    url: str


class AggregateArgs(BaseModel):
    label: str
    expected_pages: int


@job("scrape_page", args_model=ScrapeArgs, queue="default", max_attempts=3, timeout_seconds=30)
async def scrape_page(url: str) -> dict[str, Any]:
    await asyncio.sleep(random.uniform(0.1, 0.5))
    return {"url": url, "size_kb": random.randint(2, 200)}


@job(
    "aggregate_pages",
    args_model=AggregateArgs,
    queue="default",
    max_attempts=2,
)
async def aggregate_pages(label: str, expected_pages: int) -> dict[str, Any]:
    """Runs once every parent reaches `completed`.

    By the time we get here we know every page scraped successfully —
    if any parent had failed and exhausted its retries, this job would
    have been cancelled before reaching the worker.
    """
    return {"label": label, "aggregated": expected_pages}


async def main() -> None:
    urls = [f"https://example.test/page-{i}" for i in range(5)]

    async with AsyncRoost(DSN) as roost:
        # Fan out
        child_ids = []
        for url in urls:
            child_ids.append(await roost.enqueue(scrape_page, args=ScrapeArgs(url=url)))
        print(f"enqueued {len(child_ids)} child jobs: {child_ids}")

        # Join
        agg_id = await roost.enqueue(
            aggregate_pages,
            args=AggregateArgs(label="batch-1", expected_pages=len(urls)),
            depends_on=child_ids,
        )
        print(f"enqueued aggregate job: {agg_id} (depends_on={child_ids})")

        # Optional: wait for the aggregate
        try:
            outcome = await roost.wait_for(agg_id, timeout=60.0)
        except (JobTimeoutError, JobFailed) as exc:
            print(f"aggregate failed: {exc}")
            return
        print(f"aggregate result: {outcome.result}")


if __name__ == "__main__":
    asyncio.run(main())
