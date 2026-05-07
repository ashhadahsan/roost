# Observability

Roost ships with structured logging built in, plus optional OpenTelemetry and Prometheus integrations behind extras.

## Logging

```python
from roost.observability import configure_logging

configure_logging(level="INFO")  # JSON in non-TTY, console in TTY
```

`configure_logging` is idempotent. The CLI's `roost run` calls `auto_configure_from_env()` for you, so `ROOST_LOG_LEVEL=DEBUG` and `ROOST_LOG_JSON=1` work out of the box.

Example log line (JSON):

```json
{"event": "job.completed", "id": 42, "task": "send_email", "duration": 0.0123, "level": "info", "timestamp": "2026-05-07T20:32:11.842Z"}
```

## Tracing — OpenTelemetry

```bash
pip install roost[otel]
```

Once `opentelemetry-api` is importable, `enqueue` injects the active trace context into each job's `args` (under a private `__roost_trace` key the handler never sees), and the worker re-attaches it before invoking the handler. A span named `job:<task>` wraps every handler call with attributes for queue, task, attempt, and id.

If you've already configured an OTel SDK + exporter in your app, traces appear automatically.

## Metrics — Prometheus

```bash
pip install roost[metrics]
```

Counters and a histogram are exposed under the standard `prometheus_client` registry:

| Metric                          | Type      | Labels                              |
| ------------------------------- | --------- | ----------------------------------- |
| `roost_jobs_enqueued_total`     | Counter   | `queue`, `task`                     |
| `roost_jobs_completed_total`    | Counter   | `queue`, `task`                     |
| `roost_jobs_failed_total`       | Counter   | `queue`, `task`, `outcome`          |
| `roost_job_duration_seconds`    | Histogram | `queue`, `task`                     |

`outcome` is one of `retryable`, `discarded`, or `cancelled`. Wire `prometheus_client.start_http_server(8000)` in your app to expose them.

## Without the extras

When neither package is installed, the hooks are no-ops — no runtime cost, no import errors.
