"""Seed 50 demo jobs across queues + states so the dashboard isn't empty on first load."""

from __future__ import annotations

import os
import random

import psycopg

DSN = os.environ["ROOST_DSN"]

with psycopg.connect(DSN) as conn, conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM roost.jobs")
    row = cur.fetchone()
    if row and row[0] > 0:
        print(f"[seed] {row[0]} jobs already present, skipping")
    else:
        plans = [
            ("send_email", "emails", "available", 0),
            ("send_email", "emails", "completed", 1),
            ("send_email", "emails", "completed", 1),
            ("send_email", "emails", "executing", 1),
            ("send_email", "emails", "retryable", 2),
            ("export_report", "reports", "available", 0),
            ("export_report", "reports", "completed", 1),
            ("export_report", "reports", "discarded", 5),
            ("flaky_demo", "default", "available", 0),
            ("flaky_demo", "default", "available", 0),
            ("flaky_demo", "default", "completed", 1),
            ("flaky_demo", "default", "completed", 1),
            ("flaky_demo", "default", "discarded", 4),
            ("flaky_demo", "default", "retryable", 2),
        ]
        for plan, queue, state, attempt in plans * 4:
            cur.execute(
                "INSERT INTO roost.jobs (task, queue, state, attempt, args, tags) "
                "VALUES (%s, %s, %s, %s, %s::jsonb, %s::text[])",
                (
                    plan,
                    queue,
                    state,
                    attempt,
                    '{"demo": true}',
                    random.choice([["demo"], ["demo", "hot"], []]),
                ),
            )
        conn.commit()
        print(f"[seed] inserted {len(plans) * 4} demo jobs")
