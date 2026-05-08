# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-05-08

First public release.

### Distribution

- Published on PyPI as **`pgroost`** (the bare `roost` name was reserved). Install with `pip install pgroost`. The import path stays `import roost` and the CLI command stays `roost`.

### Added — pre-publish polish

- **`roost doctor`** — health-check command + `Check` records covering migration state, NOTIFY trigger presence, recent worker heartbeats, and a job-state summary. Returns non-zero exit on any failure.
- **`roost run --once`** + `Worker.run_once()` — drain currently-available jobs and exit. Useful for serverless / one-shot runners.
- **`roost run --workers N`** — multi-process supervisor via `multiprocessing` spawn context. Each child re-imports user modules into a fresh interpreter (uvicorn-style). Pair with systemd / docker / k8s for restart semantics.
- **Stable error codes** (`code: ClassVar[str]`) on every `RoostError` subclass plus `JobTimeoutError` / `JobFailed`. Lets users branch on errors programmatically instead of string-matching.
- **`roost.tasks`** module — `specs()`, `get(name)`, `names()` over the registered handler set.
- **Two-step claim** (`SELECT FOR UPDATE SKIP LOCKED` → `UPDATE` by ids) replacing the modifying-CTE pattern. Smaller contention surface, easier to reason about.
- **Server-side `scheduled_at` default** — `COALESCE($N::timestamptz, now())` in INSERT. Fixes a subtle clock-skew bug where Python `datetime.now()` on the client could produce timestamps slightly ahead of Postgres `now()` (e.g. Docker testcontainers, k8s pods with unsynced clocks), making newly enqueued rows briefly invisible to `WHERE scheduled_at <= now()`.
- **Test coverage at 86%**, 163 tests on real Postgres via `testcontainers` (no DB mocks).

### Added — v0.2 polish (post-initial-release)

- **Per-task defaults on `@job`** — `queue`, `priority`, `max_attempts`, `tags`, `timeout_seconds`, plus throttling: `rate_per_minute`, `max_concurrency`. Explicit enqueue kwargs always win.
- **Cron timezone support** — `@cron("0 9 * * 1-5", timezone="America/Los_Angeles")`. IANA names, DST-aware. Defaults to UTC.
- **`roost.contrib`** integrations: FastAPI (`RoostDep`, `tx_roost_dep`), Django (`enqueue_in_atomic`), Flask (`RoostExtension`).
- **`AsyncRoost.wait_for(job_id)`** — block until terminal, returns a `JobOutcome` with the stored result. Backed by `LISTEN roost_updated`.
- **`roost.testing`** — `run_inline` and `drain_pending` so app tests don't need testcontainers.
- **Migration framework** — numbered up/down migrations, `roost migrate up/down/status` CLI, `roost.migrations` bookkeeping table.
- **Job dependencies / chaining** — `enqueue(child, depends_on=[parent_id])`. Child waits for every parent to reach `completed`. A peer-worker reaper cancels children whose parent ended in `discarded` or `cancelled`.
- **Per-task rate limiting + max concurrency** — fetch SQL gates with a `ROW_NUMBER() PARTITION BY task` window so a single batch never over-picks. Best-effort under multi-worker contention.
- **`metadata` JSONB column** — out-of-band field for trace/request/tenant ids that aren't handler input.
- **Capped `errors[]`** — default 20, configurable per worker. Trim runs in SQL so the row never balloons under retry storms.
- **Auto-archive** — optional periodic move of terminal jobs older than N seconds into a `roost.jobs_archive` table.
- **Worker startup retries** — exponential backoff if Postgres isn't ready when the worker boots.
- **`roost enqueue`** CLI — adhoc operator enqueue: `roost enqueue task_name --args '{...}' --in 5m`.
- **`roost requeue --discarded --queue X`** — bulk dead-letter revive scoped to one queue.
- **Event hooks** — `Hooks(before_job, after_job)` plug into every dispatch with a shared per-execution `ctx` dict; throwing hooks never poison the handler.
- **`roost tasks export`** — emit a JSON manifest of registered tasks plus their Pydantic-derived JSON Schemas, useful for typed clients.
- **`roost run --reload`** — dev mode that watches imported handler modules and exits cleanly so a supervisor can restart. Requires `pip install pgroost[reload]`.

### Initial release

- Initial project scaffold (uv, src layout, ruff, mypy, pre-commit, MIT license).
- `roost.jobs` schema with `LISTEN`/`NOTIFY` triggers (`roost_inserted`, `roost_updated`, `roost_cancel_requested`).
- Transactional enqueue (async + sync) honoring the caller's connection.
- **Bulk enqueue** (`enqueue_many` + `JobInsert`) using ``executemany`` for one-round-trip inserts.
- **Per-job priorities, tags, timeouts, and result storage** exposed on the public facade.
- **Pydantic-typed args** via `@job(name, args_model=…)` — validation at handler-call time.
- Worker loop using `FOR UPDATE SKIP LOCKED` with retries, snoozing, and graceful shutdown.
- **Cancel propagation** — `roost cancel` aborts in-flight handlers via `LISTEN roost_cancel_requested`.
- **Per-job timeout enforcement** via `asyncio.wait_for` around the handler.
- **Result storage** — handler return values are persisted to `roost.jobs.result`.
- Backoff strategies: `exponential`, `linear`, `fixed`, plus custom callable hook.
- Cluster-singleton cron scheduler via Postgres advisory lock (now uses a dedicated lock connection).
- `@job` and `@cron` decorators backed by an in-process registry.
- Public facades `AsyncRoost` (asyncpg) and `Roost` (psycopg).
- **Operational primitives:** queue pause/resume, worker heartbeat table, orphan reaper, mass-requeue of discarded jobs.
- Typer CLI: `init`, `run`, `status`, `retry`, `cancel`, `workers`, `requeue --discarded`, `queue pause|resume|list`, `version`.
- **Observability:** structlog defaults (JSON in prod, pretty in dev), optional OpenTelemetry trace propagation (`pip install pgroost[otel]`), optional Prometheus metrics (`pip install pgroost[metrics]`).
- **Bench harness** (`bench/throughput.py`) measuring throughput + p50/p99 enqueue→start latency.
- **Chaos test** verifying SIGKILL'd worker jobs are recovered by the orphan reaper.
- testcontainers-based test suite covering enqueue atomicity, retries, cron, listen/notify, unique jobs, hardening, and feature surfaces (50+ tests).
- CI matrix on Python 3.10–3.13 × Postgres 13–16 and Read the Docs configuration.
- Sphinx + Furo documentation site with quickstart, concepts, and recipes.
- Community files: SECURITY.md, CODE_OF_CONDUCT.md, GitHub issue + PR templates, Dependabot config.

[Unreleased]: https://github.com/ashhadahsan/roost/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ashhadahsan/roost/releases/tag/v0.1.0
