# Grafana — Roost overview

Pre-built dashboard for the Prometheus metrics that ship with Roost (`pip install roost[metrics]`).

## What's in it

- **Top row** — instantaneous: enqueue rate, completion rate, terminal failures (5-min window), handler latency p50/p95/p99.
- **Middle row** — time-series stacks: throughput per task, failure rate per outcome (`retryable` / `discarded` / `cancelled`).
- **Bottom row** — handler duration heatmap so you can see the latency distribution shift over time.

The whole thing is one JSON file: [`roost-overview.json`](./roost-overview.json).

## Importing

1. Make sure Roost is exporting metrics. In your worker's process, expose them:

   ```python
   from prometheus_client import start_http_server
   start_http_server(8000)  # http://0.0.0.0:8000/metrics
   ```

2. Add a Prometheus scrape job for `:8000/metrics`.

3. In Grafana: `Dashboards` → `New` → `Import` → upload `roost-overview.json`.

4. Pick your Prometheus datasource when prompted.

## What's required from the metrics extra

The dashboard expects these series to exist:

| Series | Labels | Type |
| --- | --- | --- |
| `roost_jobs_enqueued_total` | `queue`, `task` | Counter |
| `roost_jobs_completed_total` | `queue`, `task` | Counter |
| `roost_jobs_failed_total` | `queue`, `task`, `outcome` | Counter |
| `roost_job_duration_seconds` | `queue`, `task` | Histogram |

These are emitted automatically by the Roost worker when `prometheus_client` is importable. With the extra not installed, the metrics are no-ops and this dashboard stays empty.
