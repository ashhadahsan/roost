"""Backwards-compatible facade over :mod:`roost._core.migrations`.

The schema is now versioned via numbered migrations — see
:mod:`roost._core.migrations` to add new ones. This module preserves the
``migration_sql()`` API so ``roost init`` (no ``--apply``) still prints a
single idempotent script.

Two NOTIFY channels are part of ``roost-web``'s public contract:

* ``roost_inserted`` — payload = queue name. Fired on every INSERT.
* ``roost_updated`` — payload = job id. Fired on state transitions.
* ``roost_cancel_requested`` — payload = job id. Fired when ``cancel`` is requested.

Changing those signals requires a sibling change in the ``roost-web`` repo.
"""

from __future__ import annotations

from roost._core.migrations import MIGRATIONS, latest_version

SCHEMA_VERSION = latest_version()


def migration_sql() -> str:
    """Concatenate every migration's ``up`` plus the migrations bookkeeping table.

    Idempotent — uses ``IF NOT EXISTS``-style DDL throughout.
    """
    parts: list[str] = [
        "CREATE SCHEMA IF NOT EXISTS roost;",
        (
            "CREATE TABLE IF NOT EXISTS roost.migrations ("
            "version INTEGER PRIMARY KEY, "
            "name TEXT NOT NULL, "
            "applied_at TIMESTAMPTZ NOT NULL DEFAULT now()"
            ");"
        ),
    ]
    for migration in sorted(MIGRATIONS, key=lambda m: m.version):
        parts.append(migration.up)
        parts.append(
            "INSERT INTO roost.migrations (version, name) VALUES "
            f"({migration.version}, '{migration.name}') "
            "ON CONFLICT (version) DO NOTHING;"
        )
    return "\n\n".join(p.strip() for p in parts) + "\n"


__all__ = ["MIGRATIONS", "SCHEMA_VERSION", "migration_sql"]
