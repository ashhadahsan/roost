"""Health-check primitive for ``roost doctor``.

Reports on:

* DSN reachable
* Migrations applied + at the latest version known to the package
* The two NOTIFY triggers expected by ``roost-web`` exist
* At least one worker has heartbeated within ``stale_after_seconds``
* Counts of jobs in each state (so users see drift at a glance)

Returns a list of :class:`Check` records — the CLI prints them, tests
assert against them, and a future ``/api/v1/doctor`` endpoint can
return them as JSON.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING

from roost._core.migrations import MIGRATIONS, latest_version

if TYPE_CHECKING:  # pragma: no cover
    import asyncpg


_EXPECTED_TRIGGERS = (
    "jobs_notify_inserted",
    "jobs_notify_updated",
    "jobs_notify_cancel_requested",
)


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    detail: str

    def render(self) -> str:
        marker = "✓" if self.ok else "✗"
        return f"{marker} {self.name}: {self.detail}"


async def run_checks_async(
    conn: asyncpg.Connection,
    *,
    worker_stale_after_seconds: float = 60.0,
) -> list[Check]:
    """Run every diagnostic. Each check is independent — one failure
    does not short-circuit later checks."""
    checks: list[Check] = []

    # --- migrations -------------------------------------------------------
    try:
        applied = [
            int(r["version"])
            for r in await conn.fetch("SELECT version FROM roost.migrations ORDER BY version")
        ]
        target = latest_version()
        if not applied:
            checks.append(
                Check(
                    "migrations",
                    ok=False,
                    detail="no migrations applied — run `roost migrate up`",
                )
            )
        elif applied[-1] < target:
            checks.append(
                Check(
                    "migrations",
                    ok=False,
                    detail=f"at v{applied[-1]}, package expects v{target} — run `roost migrate up`",
                )
            )
        elif applied[-1] > target:
            checks.append(
                Check(
                    "migrations",
                    ok=False,
                    detail=(
                        f"DB is at v{applied[-1]} but this Roost only knows about v{target} "
                        "— upgrade the package or roll back the schema"
                    ),
                )
            )
        else:
            applied_names = ", ".join(m.name for m in MIGRATIONS if m.version in set(applied))
            checks.append(
                Check(
                    "migrations",
                    ok=True,
                    detail=f"at v{applied[-1]} — {applied_names}",
                )
            )
    except Exception as exc:
        checks.append(
            Check(
                "migrations",
                ok=False,
                detail=f"could not read roost.migrations: {exc}",
            )
        )

    # --- triggers ---------------------------------------------------------
    try:
        rows = await conn.fetch(
            """
            SELECT trigger_name FROM information_schema.triggers
             WHERE event_object_schema = 'roost'
               AND event_object_table = 'jobs'
            """
        )
        present = {r["trigger_name"] for r in rows}
        missing = [t for t in _EXPECTED_TRIGGERS if t not in present]
        if missing:
            checks.append(
                Check(
                    "notify_triggers",
                    ok=False,
                    detail=f"missing: {', '.join(missing)} — re-run `roost migrate up`",
                )
            )
        else:
            checks.append(
                Check(
                    "notify_triggers",
                    ok=True,
                    detail=f"all {len(_EXPECTED_TRIGGERS)} present",
                )
            )
    except Exception as exc:
        checks.append(
            Check(
                "notify_triggers",
                ok=False,
                detail=f"could not enumerate triggers: {exc}",
            )
        )

    # --- workers ----------------------------------------------------------
    try:
        n_fresh = await conn.fetchval(
            "SELECT COUNT(*) FROM roost.workers WHERE last_seen_at >= now() - ($1::interval)",
            timedelta(seconds=worker_stale_after_seconds),
        )
        n_total = await conn.fetchval("SELECT COUNT(*) FROM roost.workers")
        if n_fresh and n_fresh > 0:
            checks.append(
                Check(
                    "workers",
                    ok=True,
                    detail=f"{n_fresh}/{n_total} heartbeating within {int(worker_stale_after_seconds)}s",
                )
            )
        elif n_total and n_total > 0:
            checks.append(
                Check(
                    "workers",
                    ok=False,
                    detail=(
                        f"{n_total} stale row(s) — no heartbeat in last "
                        f"{int(worker_stale_after_seconds)}s. Are workers running?"
                    ),
                )
            )
        else:
            checks.append(
                Check(
                    "workers",
                    ok=False,
                    detail="no workers heartbeating — start one with `roost run`",
                )
            )
    except Exception as exc:
        checks.append(Check("workers", ok=False, detail=f"check failed: {exc}"))

    # --- queue counts (informational) ------------------------------------
    try:
        rows = await conn.fetch("SELECT state, COUNT(*)::bigint AS n FROM roost.jobs GROUP BY state")
        if not rows:
            checks.append(Check("jobs", ok=True, detail="no jobs yet"))
        else:
            summary = ", ".join(f"{r['state']}={r['n']}" for r in rows)
            stuck = sum(int(r["n"]) for r in rows if r["state"] == "executing")
            ok = True  # informational only — no explicit failure threshold
            detail = summary
            if stuck > 0:
                detail = f"{summary} (note: {stuck} executing — orphan reaper handles stalled rows)"
            checks.append(Check("jobs", ok=ok, detail=detail))
    except Exception as exc:
        checks.append(Check("jobs", ok=False, detail=f"check failed: {exc}"))

    return checks


__all__ = ["Check", "run_checks_async"]
