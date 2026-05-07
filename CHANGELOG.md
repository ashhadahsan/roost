# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
- **Observability:** structlog defaults (JSON in prod, pretty in dev), optional OpenTelemetry trace propagation (`pip install roost[otel]`), optional Prometheus metrics (`pip install roost[metrics]`).
- **Bench harness** (`bench/throughput.py`) measuring throughput + p50/p99 enqueue→start latency.
- **Chaos test** verifying SIGKILL'd worker jobs are recovered by the orphan reaper.
- testcontainers-based test suite covering enqueue atomicity, retries, cron, listen/notify, unique jobs, hardening, and feature surfaces (50+ tests).
- CI matrix on Python 3.10–3.13 × Postgres 13–16 and Read the Docs configuration.
- Sphinx + Furo documentation site with quickstart, concepts, and recipes.
- Community files: SECURITY.md, CODE_OF_CONDUCT.md, GitHub issue + PR templates, Dependabot config.

[Unreleased]: https://github.com/ashhadahsan/roost/compare/v0.0.0...HEAD
