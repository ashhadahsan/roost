"""Standalone uvicorn target for the dashboard service in docker-compose."""

from __future__ import annotations

import os

from roost_web import dashboard

DSN = os.environ["ROOST_DSN"]
app = dashboard(dsn=DSN, title="Roost — docker demo")
