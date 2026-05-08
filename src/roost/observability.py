"""Observability hooks: structlog config + optional OTel / Prometheus.

The OpenTelemetry and Prometheus integrations are *optional* — both detect
their dependencies at import time and degrade to no-ops when unavailable.
Install the extras to opt in::

    pip install pgroost[otel]      # opentelemetry-api
    pip install pgroost[metrics]   # prometheus-client
"""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import structlog


def configure_logging(*, level: str | int = "INFO", json: bool | None = None) -> None:
    """Wire ``structlog`` to a sensible default.

    JSON output by default in non-TTY environments (CI, Docker, prod);
    pretty key-value output otherwise. Idempotent — safe to call multiple
    times.
    """
    if json is None:
        json = not sys.stderr.isatty()

    if isinstance(level, str):
        level = logging.getLevelName(level.upper())

    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        timestamper,
        structlog.processors.StackInfoRenderer(),
    ]

    if json:
        renderer: Any = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        cache_logger_on_first_use=True,
    )


# ---------------------------------------------------------------------------
# OpenTelemetry
# ---------------------------------------------------------------------------

_TRACE_KEY = "__roost_trace"

try:
    from opentelemetry import context as _otel_context
    from opentelemetry import propagate as _otel_propagate
    from opentelemetry import trace as _otel_trace

    _OTEL_AVAILABLE = True
except Exception:  # pragma: no cover — optional dep
    _OTEL_AVAILABLE = False


def inject_trace_context(args: dict[str, Any] | None) -> dict[str, Any]:
    """Embed the current OTel trace context into ``args`` so the worker can resume it.

    No-op when OpenTelemetry is not installed. The carrier lives under the
    private ``__roost_trace`` key and is invisible to handler kwargs.
    """
    base = dict(args or {})
    if not _OTEL_AVAILABLE:
        return base
    carrier: dict[str, str] = {}
    _otel_propagate.inject(carrier)
    if carrier:
        base[_TRACE_KEY] = carrier
    return base


def strip_trace_context(args: dict[str, Any]) -> tuple[dict[str, Any], dict[str, str] | None]:
    """Pop the trace carrier off ``args`` (mutating). Returns ``(handler_kwargs, carrier)``."""
    carrier = args.pop(_TRACE_KEY, None) if args else None
    return args, carrier


@contextmanager
def job_span(name: str, attrs: dict[str, Any], carrier: dict[str, str] | None) -> Iterator[None]:
    """Open an OTel span around the handler call, restoring trace context if any."""
    if not _OTEL_AVAILABLE:
        yield
        return
    tracer = _otel_trace.get_tracer("roost")
    parent_ctx = _otel_propagate.extract(carrier) if carrier else None
    token = _otel_context.attach(parent_ctx) if parent_ctx else None
    try:
        with tracer.start_as_current_span(name, attributes=attrs):
            yield
    finally:
        if token is not None:
            _otel_context.detach(token)


# ---------------------------------------------------------------------------
# Prometheus
# ---------------------------------------------------------------------------

try:
    from prometheus_client import Counter, Histogram

    _PROM_AVAILABLE = True
except Exception:  # pragma: no cover — optional dep
    _PROM_AVAILABLE = False


class _NoopMetric:
    def labels(self, **_: Any) -> _NoopMetric:
        return self

    def inc(self, _amount: float = 1.0) -> None:
        return None

    def observe(self, _value: float) -> None:
        return None


if _PROM_AVAILABLE:
    JOBS_ENQUEUED: Any = Counter(
        "roost_jobs_enqueued_total",
        "Total jobs enqueued.",
        ["queue", "task"],
    )
    JOBS_COMPLETED: Any = Counter(
        "roost_jobs_completed_total",
        "Total jobs completed successfully.",
        ["queue", "task"],
    )
    JOBS_FAILED: Any = Counter(
        "roost_jobs_failed_total",
        "Total jobs that hit a terminal failure (retryable counted on each retry; discarded once).",
        ["queue", "task", "outcome"],  # outcome: retryable | discarded | cancelled
    )
    JOB_DURATION: Any = Histogram(
        "roost_job_duration_seconds",
        "Handler runtime in seconds.",
        ["queue", "task"],
        buckets=(0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0),
    )
else:  # pragma: no cover — optional dep
    JOBS_ENQUEUED = _NoopMetric()
    JOBS_COMPLETED = _NoopMetric()
    JOBS_FAILED = _NoopMetric()
    JOB_DURATION = _NoopMetric()


def metrics_enabled() -> bool:
    """``True`` when ``prometheus-client`` is importable."""
    return _PROM_AVAILABLE


def otel_enabled() -> bool:
    """``True`` when ``opentelemetry-api`` is importable."""
    return _OTEL_AVAILABLE


def auto_configure_from_env() -> None:
    """Apply log defaults from ``ROOST_LOG_LEVEL`` / ``ROOST_LOG_JSON``."""
    level = os.environ.get("ROOST_LOG_LEVEL", "INFO")
    raw_json = os.environ.get("ROOST_LOG_JSON")
    json_flag: bool | None = (
        None if raw_json is None else raw_json.strip().lower() in {"1", "true", "yes", "on"}
    )
    configure_logging(level=level, json=json_flag)


__all__ = [
    "JOBS_COMPLETED",
    "JOBS_ENQUEUED",
    "JOBS_FAILED",
    "JOB_DURATION",
    "auto_configure_from_env",
    "configure_logging",
    "inject_trace_context",
    "job_span",
    "metrics_enabled",
    "otel_enabled",
    "strip_trace_context",
]
