# Roost

> Postgres-backed background job queue for Python — Oban for Pythonistas.

```{warning}
Roost is in **alpha**. APIs may break before `v0.1.0`. Pin exactly.
```

## Why Roost?

- **No new infra.** Your existing Postgres is the queue. No Redis, no RabbitMQ, no Kafka.
- **Transactional enqueue.** Insert a job in the same transaction as your business writes — they commit or roll back together.
- **Battle-tested concurrency.** `SELECT … FOR UPDATE SKIP LOCKED` for safe parallel workers.
- **Real-time wakeups.** `LISTEN`/`NOTIFY` — no busy-polling.
- **Sync and async.** First-class twin facades over a single core.
- **Observable by SQL** today; by [`roost-web`](https://github.com/ashhadahsan/roost-web) tomorrow.

```{toctree}
:maxdepth: 2
:caption: Getting started

quickstart
```

```{toctree}
:maxdepth: 2
:caption: Concepts

concepts/transactional-enqueue
concepts/retries
concepts/cron
concepts/unique-jobs
concepts/operations
concepts/observability
```

```{toctree}
:maxdepth: 2
:caption: Recipes

recipes/fastapi
recipes/django
recipes/flask
```

```{toctree}
:maxdepth: 1
:caption: Reference

reference/api
changelog
```
