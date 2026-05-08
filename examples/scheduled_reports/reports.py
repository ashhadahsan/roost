"""Scheduled jobs — daily digest at 9am Pacific, weekly rollup Mondays at 06:00 UTC.

Run a worker pointed at this module. The cron scheduler runs once per
cluster (advisory lock) — you can have N workers and only one will fire
each entry.

    roost init --apply
    roost run --module examples.scheduled_reports.reports
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import structlog

from roost import cron, job

_log = structlog.get_logger("reports")


# ---------------------------------------------------------------------------
# Daily digest — 9am Pacific Mon-Fri (DST-aware via the cron timezone)
# ---------------------------------------------------------------------------


@cron(
    "0 9 * * 1-5",
    timezone="America/Los_Angeles",
    queue="reports",
    name="daily_digest",
    max_attempts=3,
)
async def daily_digest() -> dict[str, Any]:
    now = datetime.now(tz=timezone.utc)
    _log.info("daily_digest.fired", at=now.isoformat())
    # Real code: fetch yesterday's events, build summary, send via mailer.
    return {"sent_at": now.isoformat(), "rows_summarised": 0}


# ---------------------------------------------------------------------------
# Weekly rollup — every Monday at 06:00 UTC
# ---------------------------------------------------------------------------


@cron(
    "0 6 * * 1",
    queue="reports",
    name="weekly_rollup",
    max_attempts=2,
    args={"window_days": 7},
)
async def weekly_rollup(window_days: int = 7) -> None:
    _log.info("weekly_rollup.fired", window_days=window_days)
    # Real code: aggregate events from the last `window_days` and persist.


# ---------------------------------------------------------------------------
# A non-cron task that the digest could enqueue
# ---------------------------------------------------------------------------


@job("send_email_digest", queue="emails")
async def send_email_digest(recipient: str, summary_id: int) -> None:
    _log.info("digest_email.sent", to=recipient, summary_id=summary_id)
