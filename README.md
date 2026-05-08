<!-- markdownlint-disable MD033 MD041 -->
<p align="center">
  <img src="docs/_static/logo.svg" alt="Roost" width="180"/>
</p>

<h1 align="center">Roost</h1>

<p align="center">
  <em>Postgres-backed background job queue for Python — Oban for Pythonistas.</em>
</p>

<p align="center">
  <a href="https://pypi.org/project/pgroost/"><img src="https://img.shields.io/pypi/v/pgroost.svg" alt="PyPI"/></a>
  <a href="https://pypi.org/project/pgroost/"><img src="https://img.shields.io/pypi/pyversions/pgroost.svg" alt="Python versions"/></a>
  <a href="https://github.com/ashhadahsan/roost/actions/workflows/ci.yml"><img src="https://github.com/ashhadahsan/roost/actions/workflows/ci.yml/badge.svg" alt="CI"/></a>
  <a href="https://pgroost.readthedocs.io/"><img src="https://readthedocs.org/projects/pgroost/badge/?version=latest" alt="Docs"/></a>
  <img src="https://img.shields.io/badge/coverage-86%25-brightgreen.svg" alt="Coverage 86%"/>
  <img src="https://img.shields.io/badge/tests-163%20passing-brightgreen.svg" alt="163 tests"/>
  <a href="LICENSE"><img src="https://img.shields.io/pypi/l/pgroost.svg" alt="MIT"/></a>
</p>

> 🚧 **Alpha** — under active development. APIs may change before `1.0`. Pin exactly.

## Why Roost?

- **One less piece of infra.** No Redis, no RabbitMQ. Your existing Postgres is the queue.
- **Transactional enqueue.** `INSERT INTO roost.jobs ...` commits in your transaction. Jobs cannot orphan or vanish.
- **Battle-tested concurrency.** `SELECT ... FOR UPDATE SKIP LOCKED` for safe parallel workers.
- **Real-time wakeups.** `LISTEN` / `NOTIFY` — no polling overhead, sub-second pickup.
- **Sync and async, first-class.** Twin facades over a single SQL surface — Django/Flask + FastAPI/Starlette without a glue layer.
- **Crash-tolerant.** Heartbeats + orphan reaper recover jobs from SIGKILL'd workers automatically.
- **Polished dashboard.** [`roost-web`](https://github.com/ashhadahsan/roost-web) mounts in three lines, live updates via SSE, no Node.js toolchain.

## Real numbers (from a laptop)

```text
Bulk enqueue:    15,289 jobs/sec   single async connection
Sustained drain:    822 jobs/sec   16 workers, local PG, noop handler
Dispatch overhead:  p50 3 ms       p99 51 ms — pure Roost overhead
                                   per job (excludes handler runtime)
```

Run `bench/throughput.py` against your own Postgres to see what you get.

## Quickstart

```bash
pip install pgroost                # install
export ROOST_DSN=postgresql://user:pass@localhost/app
roost init --apply                 # CLI command stays `roost`
```

> **Distribution vs import name:** the PyPI distribution is `pgroost` (the bare `roost` name is taken on PyPI). You still `import roost` and call the `roost` CLI — only the install line differs.

```python
# tasks.py
from roost import job

@job("send_welcome_email", queue="emails", max_attempts=5, timeout_seconds=30)
async def send_welcome_email(user_id: int) -> None:
    ...
```

```python
# inside a FastAPI / Starlette / Django handler — same conn, same txn
async with pool.acquire() as conn, conn.transaction():
    user_id = await conn.fetchval("INSERT INTO users ... RETURNING id")
    await roost.enqueue(send_welcome_email, args={"user_id": user_id}, conn=conn)
# both rows commit together — or roll back together. That's the whole point.
```

```bash
roost run --module tasks --queues emails,default --concurrency 8
roost doctor                       # health check
roost status                       # counts per state per queue
```

Read the [docs](https://pgroost.readthedocs.io/) and the [recipes](https://roost.ashhadahsan.com/recipes) for more.

## Feature matrix

| Feature                              | Status                                                  |
| ------------------------------------ | ------------------------------------------------------- |
| Transactional enqueue                | shipped                                                 |
| Async + sync facades                 | shipped                                                 |
| Bulk enqueue (`enqueue_many`)        | shipped                                                 |
| Worker, retries (3 strategies)       | shipped                                                 |
| Snoozing                             | shipped                                                 |
| Per-task timeouts                    | shipped                                                 |
| Cron with IANA timezones             | shipped                                                 |
| Unique jobs (partial idx)            | shipped                                                 |
| Job chaining (`depends_on`)          | shipped                                                 |
| Per-task rate limit                  | shipped                                                 |
| Per-task max concurrency             | shipped                                                 |
| Pydantic-typed args                  | shipped                                                 |
| Cancel propagation via NOTIFY        | shipped                                                 |
| Result storage + `wait_for`          | shipped                                                 |
| Worker heartbeats + orphan reaper    | shipped                                                 |
| Auto-archive + result TTL            | shipped                                                 |
| OpenTelemetry hooks (extra)          | shipped                                                 |
| Prometheus metrics (extra)           | shipped                                                 |
| Event hooks (`Hooks(before, after)`) | shipped                                                 |
| FastAPI / Django / Flask contrib     | shipped                                                 |
| `roost.testing` helpers              | shipped                                                 |
| Schema migrations + `roost migrate`  | shipped                                                 |
| Typer CLI (init/run/doctor/...)      | shipped                                                 |
| Drop-in dashboard                    | [`roost-web`](https://github.com/ashhadahsan/roost-web) |

## Compatibility

- **Python:** 3.10, 3.11, 3.12, 3.13.
- **PostgreSQL:** 13, 14, 15, 16. Tested every commit on the full matrix.
- **Drivers:** `asyncpg` (async), `psycopg[binary]` (sync).
- **Hosts:** any ASGI app — FastAPI, Starlette, Litestar, Quart. Django and Flask via the contrib helpers.

## Test suite

- **163 tests, 86% coverage**, real Postgres via `testcontainers` (no mocks of the DB layer).
- Run locally: `uv run pytest -q`. With coverage: `uv run --with pytest-cov pytest --cov=src/roost --cov-report=term`.
- Override the test image: `ROOST_TEST_PG_IMAGE=postgres:15-alpine uv run pytest -q`.

| Module                     | Coverage |
| -------------------------- | -------- |
| `roost.sync_api`           | 100%     |
| `roost._core.retry`        | 100%     |
| `roost.exceptions`         | 100%     |
| `roost.contrib.flask`      | 100%     |
| `roost.testing`            | 98%      |
| `roost.contrib.django`     | 96%      |
| `roost._core.wait`         | 95%      |
| `roost.hooks`              | 92%      |
| `roost._core.migrations`   | 91%      |
| `roost._core.repo`         | 88%      |
| `roost.observability`      | 85%      |
| `roost._core.doctor`       | 84%      |
| `roost.async_api`          | 83%      |
| `roost.cli`, `worker.py`   | 82%      |
| `roost.decorators`         | 81%      |
| `roost._core.cron`         | 80%      |

## Compared to other queues

See [the comparison page](https://roost.ashhadahsan.com/comparison) for a side-by-side feature matrix vs Celery, RQ, dramatiq, arq, procrastinate, and pgqueuer.

Quick read: pick **Roost** if you already run Postgres and want transactional enqueue + a polished dashboard. Pick **Celery / RQ** if you don't run Postgres and have Redis already. Pick **procrastinate / pgqueuer** if you want the same Postgres-only foundation with a smaller feature surface.

## Project structure

| Concern                                | File                                               |
| -------------------------------------- | -------------------------------------------------- |
| Schema + numbered migrations           | `src/roost/_core/migrations.py`                    |
| All DB I/O (single source of truth)    | `src/roost/_core/repo.py`                          |
| Worker loop, signals, heartbeats       | `src/roost/worker.py`                              |
| Backoff strategies                     | `src/roost/_core/retry.py`                         |
| Cron scheduler                         | `src/roost/_core/cron.py`                          |
| Public async API                       | `src/roost/async_api.py`                           |
| Public sync API                        | `src/roost/sync_api.py`                            |
| `@job` / `@cron` decorators            | `src/roost/decorators.py`                          |
| Hooks, observability                   | `src/roost/hooks.py`, `src/roost/observability.py` |
| Health-check primitives                | `src/roost/_core/doctor.py`                        |
| Test helpers                           | `src/roost/testing.py`                             |
| `roost.contrib.{fastapi,django,flask}` | `src/roost/contrib/`                               |
| CLI                                    | `src/roost/cli.py`                                 |

## Examples

The `examples/` directory has six runnable patterns:

- `docker/` — one-command stack (Postgres + worker + dashboard).
- `fastapi_app/` — transactional enqueue inside a FastAPI request, `wait_for` for sync results.
- `scheduled_reports/` — daily cron with timezone, weekly UTC rollup.
- `fanout_join/` — N parallel children + a single aggregator gated on `depends_on`.
- `etl_pipeline/` — chained extract → transform → load with rate limits and concurrency caps.
- `plain_python.py` — smallest possible enqueue + worker.

Each has its own README; start there.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md). Bugs + feature requests via GitHub issues. Security disclosures: see [SECURITY.md](SECURITY.md). All participants are expected to follow the [Code of Conduct](CODE_OF_CONDUCT.md).

## License

MIT — see [LICENSE](LICENSE).
