<!-- markdownlint-disable MD033 MD041 -->
<p align="center">
  <img src="docs/_static/logo.svg" alt="Roost" width="180"/>
</p>

<h1 align="center">Roost</h1>

<p align="center">
  <em>Postgres-backed background job queue for Python — Oban for Pythonistas.</em>
</p>

<p align="center">
  <a href="https://pypi.org/project/roost/"><img src="https://img.shields.io/pypi/v/roost.svg" alt="PyPI"/></a>
  <a href="https://pypi.org/project/roost/"><img src="https://img.shields.io/pypi/pyversions/roost.svg" alt="Python versions"/></a>
  <a href="https://github.com/ashhadahsan/roost/actions/workflows/ci.yml"><img src="https://github.com/ashhadahsan/roost/actions/workflows/ci.yml/badge.svg" alt="CI"/></a>
  <a href="https://roost.readthedocs.io/"><img src="https://readthedocs.org/projects/roost/badge/?version=latest" alt="Docs"/></a>
  <a href="LICENSE"><img src="https://img.shields.io/pypi/l/roost.svg" alt="MIT"/></a>
</p>

> 🚧 **Alpha** — under active development. APIs may change before `v0.1.0`. Pin exactly.

## Why Roost?

- **One less piece of infra.** No Redis, no RabbitMQ. Your existing Postgres is the queue.
- **Transactional enqueue.** `INSERT INTO roost.jobs ...` runs in the same transaction as your business writes. Jobs cannot orphan or vanish.
- **Battle-tested concurrency.** `SELECT ... FOR UPDATE SKIP LOCKED` for safe parallel workers.
- **Real-time wakeups.** `LISTEN` / `NOTIFY` — no polling overhead.
- **Sync and async.** First-class facades for Django/Flask and FastAPI/Starlette.
- **Crash-tolerant.** Built-in heartbeats and an orphan reaper recover jobs from killed workers.
- **Observable by SQL.** `SELECT * FROM roost.jobs WHERE state = 'discarded'`. A polished dashboard ships separately as [`roost-web`](https://github.com/ashhadahsan/roost-web).

## Quickstart

```bash
pip install roost
export ROOST_DSN=postgresql://user:pass@localhost/app
roost init --apply
```

```python
from roost import AsyncRoost, job

@job("send_welcome_email")
async def send_welcome_email(user_id: int) -> None:
    ...

roost = AsyncRoost("postgresql://...")

# inside a request handler — same connection, same transaction
async with pool.acquire() as conn, conn.transaction():
    user_id = await create_user(conn, email=email)
    await roost.enqueue(send_welcome_email, args={"user_id": user_id}, conn=conn)
```

```bash
roost run --module myapp.tasks --queues default --concurrency 4
```

Read the [docs](https://roost.readthedocs.io/) for the full picture.

## Feature matrix

| Feature                         | Status           |
| ------------------------------- | ---------------- |
| Transactional enqueue           | shipped          |
| Async + sync facades            | shipped          |
| Worker + retries (3 strategies) | shipped          |
| Snoozing                        | shipped          |
| Cron (cluster-singleton)        | shipped          |
| Unique jobs                     | shipped          |
| LISTEN/NOTIFY wakeups           | shipped          |
| Orphan reaper                   | shipped          |
| Worker heartbeats               | shipped          |
| Typer CLI                       | shipped          |
| `roost-web` dashboard           | separate repo    |

## Compatibility

- **Python:** 3.10, 3.11, 3.12, 3.13.
- **PostgreSQL:** 13, 14, 15, 16. Tested every commit on the full matrix.
- **Drivers:** `asyncpg` (async), `psycopg[binary]` (sync).

## Project structure

| Concern                           | File                               |
| --------------------------------- | ---------------------------------- |
| Schema, NOTIFY triggers           | `src/roost/_core/schema.py`        |
| All DB I/O                        | `src/roost/_core/repo.py`          |
| Worker loop, signals, heartbeats  | `src/roost/worker.py`              |
| Backoff strategies                | `src/roost/_core/retry.py`         |
| Cron scheduler                    | `src/roost/_core/cron.py`          |
| Public async API                  | `src/roost/async_api.py`           |
| Public sync API                   | `src/roost/sync_api.py`            |
| `@job` / `@cron` decorators       | `src/roost/decorators.py`          |
| CLI                               | `src/roost/cli.py`                 |

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md). Bugs and feature requests via GitHub
issues. Security disclosures: see [SECURITY.md](SECURITY.md). All participants
are expected to follow the [Code of Conduct](CODE_OF_CONDUCT.md).

## License

MIT — see [LICENSE](LICENSE).
