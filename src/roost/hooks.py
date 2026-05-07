"""User-supplied event hooks around handler execution.

Hooks let host apps plug in extra observability, audit logging, custom
metrics, or shadow-mode dispatch without modifying Roost core.

Example::

    from roost import Hooks, Worker

    async def before(job, *, ctx):
        ctx["t0"] = time.monotonic()

    async def after(job, *, result, error, ctx):
        elapsed = time.monotonic() - ctx["t0"]
        my_metrics.observe(job.task, elapsed, error=error is not None)

    worker = Worker(dsn, hooks=Hooks(before_job=before, after_job=after))

The ``ctx`` dict is shared between ``before_job`` and ``after_job`` for the
same execution. Hooks are awaited; sync hooks raise ``TypeError``.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

BeforeHook = Callable[..., Awaitable[None]]
AfterHook = Callable[..., Awaitable[None]]


@dataclass(frozen=True)
class Hooks:
    """Bundle of optional hooks invoked around every handler call.

    Both hooks are async. The signature is ``(job, *, ctx)`` for
    ``before_job`` and ``(job, *, result, error, ctx)`` for
    ``after_job``. Either can be omitted; only set the ones you need.
    """

    before_job: BeforeHook | None = None
    after_job: AfterHook | None = None


async def call_before(hooks: Hooks | None, job: Any, ctx: dict[str, Any]) -> None:
    if hooks is None or hooks.before_job is None:
        return
    result = hooks.before_job(job, ctx=ctx)
    if not inspect.isawaitable(result):
        raise TypeError("Hooks.before_job must be async (return an awaitable)")
    await result


async def call_after(
    hooks: Hooks | None,
    job: Any,
    *,
    result: Any | None,
    error: BaseException | None,
    ctx: dict[str, Any],
) -> None:
    if hooks is None or hooks.after_job is None:
        return
    awaitable = hooks.after_job(job, result=result, error=error, ctx=ctx)
    if not inspect.isawaitable(awaitable):
        raise TypeError("Hooks.after_job must be async (return an awaitable)")
    await awaitable


__all__ = ["AfterHook", "BeforeHook", "Hooks", "call_after", "call_before"]
