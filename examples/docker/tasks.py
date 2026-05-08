"""Tasks the worker registers."""

from __future__ import annotations

import asyncio
import random

from roost import job


@job("send_email", queue="emails", max_attempts=5)
async def send_email(user_id: int, template: str = "welcome") -> dict[str, int]:
    await asyncio.sleep(random.uniform(0.05, 0.5))
    return {"sent_to": user_id}


@job("export_report", queue="reports", max_attempts=2, timeout_seconds=30)
async def export_report(name: str, rows: int) -> None:
    await asyncio.sleep(random.uniform(0.5, 2.0))


@job("flaky_demo", queue="default", max_attempts=4)
async def flaky_demo(idx: int) -> None:
    if random.random() < 0.3:
        raise RuntimeError(f"flake on idx={idx}")
    await asyncio.sleep(0.05)
