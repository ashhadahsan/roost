"""Tier 4: hooks + tasks export CLI."""

from __future__ import annotations

import asyncio
import json

import pytest
from pydantic import BaseModel
from typer.testing import CliRunner

from roost import AsyncRoost, Hooks, JobFailed, job
from roost.cli import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# Hooks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_before_and_after_hooks_fire_on_success(fresh_dsn: str) -> None:
    captured: dict[str, object] = {}

    async def before(job, *, ctx):
        ctx["seen"] = True
        captured["before_job"] = job.id

    async def after(job, *, result, error, ctx):
        captured["after_job"] = job.id
        captured["after_result"] = result
        captured["after_error"] = error
        captured["ctx_passthrough"] = ctx.get("seen")

    @job("hello_hooks")
    async def hello_hooks() -> dict[str, str]:
        return {"hello": "world"}

    async with AsyncRoost(fresh_dsn) as r:
        job_id = await r.enqueue(hello_hooks)
        worker = r.worker(concurrency=1, run_cron=False, poll_interval=0.05, hooks=Hooks(before, after))
        task = asyncio.create_task(worker.run())
        try:
            await r.wait_for(job_id, timeout=5.0)
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5.0)

    assert captured.get("before_job") == job_id
    assert captured.get("after_job") == job_id
    assert captured.get("after_result") == {"hello": "world"}
    assert captured.get("after_error") is None
    assert captured.get("ctx_passthrough") is True


@pytest.mark.asyncio
async def test_after_hook_sees_error_on_failure(fresh_dsn: str) -> None:
    captured: dict[str, object] = {}

    async def after(job, *, result, error, ctx):
        captured["error_type"] = type(error).__name__ if error else None
        captured["result"] = result

    @job("explodes")
    async def explodes() -> None:
        raise ValueError("nope")

    async with AsyncRoost(fresh_dsn) as r:
        job_id = await r.enqueue(explodes, max_attempts=1)
        worker = r.worker(concurrency=1, run_cron=False, poll_interval=0.05, hooks=Hooks(after_job=after))
        task = asyncio.create_task(worker.run())
        try:
            with pytest.raises(JobFailed):
                await r.wait_for(job_id, timeout=5.0)
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5.0)

    assert captured.get("error_type") == "ValueError"
    assert captured.get("result") is None


@pytest.mark.asyncio
async def test_hook_failure_doesnt_break_handler(fresh_dsn: str) -> None:
    """A throwing hook must not poison the job — handler still runs to completion."""

    async def bad(job, *, ctx):
        raise RuntimeError("hook is broken")

    @job("survives_hook")
    async def survives_hook() -> str:
        return "ok"

    async with AsyncRoost(fresh_dsn) as r:
        job_id = await r.enqueue(survives_hook)
        worker = r.worker(concurrency=1, run_cron=False, poll_interval=0.05, hooks=Hooks(before_job=bad))
        task = asyncio.create_task(worker.run())
        try:
            outcome = await r.wait_for(job_id, timeout=5.0)
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, timeout=5.0)
    assert outcome.state == "completed"
    assert outcome.result == "ok"


# ---------------------------------------------------------------------------
# tasks export CLI
# ---------------------------------------------------------------------------


class _OrderArgs(BaseModel):
    order_id: int
    notes: str = ""


def test_tasks_export_emits_json_schema() -> None:
    @job(
        "process_order",
        args_model=_OrderArgs,
        queue="orders",
        priority=-3,
        timeout_seconds=30,
    )
    async def process_order(order_id: int, notes: str = "") -> None: ...

    result = runner.invoke(app, ["tasks", "export"])
    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)
    by_name = {t["name"]: t for t in payload["tasks"]}
    entry = by_name["process_order"]
    assert entry["defaults"]["queue"] == "orders"
    assert entry["defaults"]["priority"] == -3
    assert entry["defaults"]["timeout_seconds"] == 30
    assert entry["args_schema"]["properties"]["order_id"]["type"] == "integer"
    assert "order_id" in entry["args_schema"]["required"]


def test_tasks_export_handles_no_model() -> None:
    @job("no_model_task")
    async def no_model_task() -> None: ...

    result = runner.invoke(app, ["tasks", "export"])
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    by_name = {t["name"]: t for t in payload["tasks"]}
    assert by_name["no_model_task"]["args_schema"] is None
