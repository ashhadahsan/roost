"""``wait_for_async`` — block until a job reaches a terminal state.

Backed by ``LISTEN roost_updated`` so it reacts as fast as the trigger
fires; falls back to polling at ``poll_interval`` for resilience.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

from roost._core.notify import CHANNEL_UPDATED
from roost._core.repo import init_connection
from roost._core.states import TERMINAL_STATES

if TYPE_CHECKING:  # pragma: no cover
    import asyncpg


class JobTimeoutError(TimeoutError):
    """Raised when ``wait_for_async`` exceeds its timeout."""

    code: ClassVar[str] = "roost.job-timeout"


class JobFailed(RuntimeError):
    """Raised when the awaited job ended in ``discarded`` or ``cancelled``."""

    code: ClassVar[str] = "roost.job-failed"

    def __init__(self, job_id: int, state: str, errors: list[dict[str, Any]] | None = None):
        self.job_id = job_id
        self.state = state
        self.errors = errors or []
        last = self.errors[-1]["error"] if self.errors else "(no error recorded)"
        super().__init__(f"job {job_id} ended in state {state!r}: {last}")


@dataclass(frozen=True)
class JobOutcome:
    id: int
    state: str
    result: Any | None
    errors: list[dict[str, Any]]


async def wait_for_async(
    dsn: str,
    job_id: int,
    *,
    timeout: float | None = 30.0,
    poll_interval: float = 1.0,
    raise_on_failure: bool = True,
) -> JobOutcome:
    """Wait until ``job_id`` reaches a terminal state and return its row.

    Pass ``raise_on_failure=False`` to receive the :class:`JobOutcome` even
    when the job ended in ``discarded`` / ``cancelled`` (default: raise
    :class:`JobFailed`).

    Pass ``timeout=None`` to wait indefinitely.
    """
    import asyncpg

    deadline = None if timeout is None else asyncio.get_running_loop().time() + timeout
    wakeup = asyncio.Event()

    def _on_update(_conn: object, _pid: int, _channel: str, payload: str) -> None:
        try:
            updated = int(payload)
        except (TypeError, ValueError):
            return
        if updated == job_id:
            wakeup.set()

    listen_conn = await asyncpg.connect(dsn)
    poll_conn = await asyncpg.connect(dsn)
    await init_connection(listen_conn)
    await init_connection(poll_conn)

    try:
        await listen_conn.add_listener(CHANNEL_UPDATED, _on_update)

        # Initial check — the job may already be terminal.
        outcome = await _check_terminal(poll_conn, job_id)
        if outcome is not None:
            return _maybe_raise(outcome, raise_on_failure)

        while True:
            remaining = None if deadline is None else max(0.0, deadline - asyncio.get_running_loop().time())
            wait_for = poll_interval if remaining is None else min(remaining, poll_interval)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(wakeup.wait(), timeout=wait_for)
            wakeup.clear()

            outcome = await _check_terminal(poll_conn, job_id)
            if outcome is not None:
                return _maybe_raise(outcome, raise_on_failure)

            if deadline is not None and asyncio.get_running_loop().time() >= deadline:
                raise JobTimeoutError(f"job {job_id} did not finish within {timeout}s")
    finally:
        with contextlib.suppress(Exception):
            await listen_conn.remove_listener(CHANNEL_UPDATED, _on_update)
        with contextlib.suppress(Exception):
            await listen_conn.close()
        with contextlib.suppress(Exception):
            await poll_conn.close()


async def _check_terminal(conn: asyncpg.Connection, job_id: int) -> JobOutcome | None:
    row = await conn.fetchrow("SELECT id, state, result, errors FROM roost.jobs WHERE id = $1", job_id)
    if row is None:
        return None
    state = row["state"]
    if state not in TERMINAL_STATES:
        return None
    return JobOutcome(
        id=int(row["id"]),
        state=str(state),
        result=row["result"],
        errors=list(row["errors"] or []),
    )


def _maybe_raise(outcome: JobOutcome, raise_on_failure: bool) -> JobOutcome:
    if raise_on_failure and outcome.state in {"discarded", "cancelled"}:
        raise JobFailed(outcome.id, outcome.state, outcome.errors)
    return outcome


__all__ = ["JobFailed", "JobOutcome", "JobTimeoutError", "wait_for_async"]
