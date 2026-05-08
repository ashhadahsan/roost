"""Multi-process worker supervisor.

The asyncio :class:`roost.Worker` is single-process; each process runs
its own asyncio event loop and pulls jobs concurrently via
``FOR UPDATE SKIP LOCKED``. For CPU-bound handlers you usually want
multiple OS processes — that's what this supervisor provides.

Used by ``roost run --workers N`` (N > 1). Each child is a fresh
Python interpreter (``spawn`` context) so handler imports happen in
isolation; this is the same model uvicorn / gunicorn use.

Crash policy is intentionally "let it die" — the parent forwards
signals and waits for graceful drain; it does not restart children.
Pair with systemd / docker / kubernetes for restart semantics.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import logging
import multiprocessing as mp
import os
import signal
import sys
import time
from typing import Any

_log = logging.getLogger("roost.supervisor")


def _child_entry(
    dsn: str,
    queues: list[str],
    modules: list[str],
    worker_kwargs: dict[str, Any],
) -> None:
    """Run inside each spawned child. Re-imports user modules to populate
    the @job/@cron registries, builds a Worker, runs it forever."""
    # Re-establish a sane logging baseline in the child.
    from roost import observability

    observability.auto_configure_from_env()

    for mod in modules:
        if mod:
            importlib.import_module(mod)

    from roost.worker import Worker

    worker = Worker(dsn, queues=queues, **worker_kwargs)

    async def _main() -> None:
        loop = asyncio.get_running_loop()
        worker.install_signal_handlers(loop)
        await worker.run()

    asyncio.run(_main())


def run_workers(
    dsn: str,
    *,
    n: int,
    queues: list[str],
    modules: list[str],
    worker_kwargs: dict[str, Any],
    shutdown_grace: float = 60.0,
) -> int:
    """Spawn ``n`` worker processes and block until they all exit.

    Returns 0 if every child exited cleanly, 1 otherwise. On SIGINT or
    SIGTERM the parent forwards the signal to every alive child and
    waits up to ``shutdown_grace`` seconds before SIGKILL'ing stragglers.
    """
    if n < 1:
        raise ValueError("n must be >= 1")

    ctx = mp.get_context("spawn")
    procs: list[mp.process.BaseProcess] = []
    for i in range(n):
        proc = ctx.Process(
            target=_child_entry,
            args=(dsn, queues, modules, worker_kwargs),
            name=f"roost-worker-{i}",
            daemon=False,
        )
        proc.start()
        procs.append(proc)

    stopping = {"flag": False, "deadline": 0.0}

    def _shutdown(_signum: int, _frame: Any) -> None:
        if stopping["flag"]:
            # Second signal — accelerate to SIGKILL.
            for p in procs:
                if p.is_alive() and p.pid:
                    with contextlib.suppress(ProcessLookupError):
                        os.kill(p.pid, signal.SIGKILL)
            return
        stopping["flag"] = True
        stopping["deadline"] = time.monotonic() + shutdown_grace
        for p in procs:
            if p.is_alive() and p.pid:
                with contextlib.suppress(ProcessLookupError):
                    os.kill(p.pid, signal.SIGTERM)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    bad = 0
    for p in procs:
        try:
            p.join()
        except KeyboardInterrupt:  # pragma: no cover — covered by signal handler
            _shutdown(signal.SIGINT, None)
            p.join()
        if stopping["flag"] and time.monotonic() > stopping["deadline"] and p.is_alive():
            if p.pid:
                with contextlib.suppress(ProcessLookupError):
                    os.kill(p.pid, signal.SIGKILL)
            p.join(timeout=5.0)
        if p.exitcode not in (0, -signal.SIGTERM):
            bad += 1
            _log.warning(
                "supervisor.child_exited_nonzero",
                extra={"pid": p.pid, "exitcode": p.exitcode},
            )

    if bad:
        sys.stdout.flush()
    return 1 if bad else 0


__all__ = ["run_workers"]
