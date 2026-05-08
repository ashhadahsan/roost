# Roost — one-command Docker stack

Postgres + worker + dashboard in three containers. Includes a seed step that inserts ~50 demo jobs across queues and states so the UI isn't empty on first load.

## Run it

```bash
docker compose -f examples/docker/docker-compose.yml up
```

Then open:

- Dashboard: <http://localhost:8000/>
- Postgres: `postgresql://roost:roost@localhost:5432/roost`

## What you'll see

- **Overview** — counters per state per queue, plus a workers table that fills in within ~2 seconds of the worker starting.
- **Jobs** — a mix of `available`, `executing`, `completed`, `retryable`, and `discarded` rows from the seed.
- **`flaky_demo` task** is intentionally flaky (~30% failure rate) so you can watch retries happen live via SSE.

## Stop and clean up

```bash
docker compose -f examples/docker/docker-compose.yml down -v   # -v wipes the postgres volume
```

## What's in the stack

| Service | Image | Role |
| --- | --- | --- |
| `postgres` | `postgres:16-alpine` | The queue's storage. |
| `schema` | `python:3.12-slim` | One-shot. Runs `roost init --apply`. Exits zero. |
| `seed` | `python:3.12-slim` | One-shot. Inserts demo jobs. Exits zero. |
| `worker` | `python:3.12-slim` | Long-running. `roost run --module tasks --concurrency 4`. |
| `dashboard` | `python:3.12-slim` | Long-running. Serves `roost-web` on port 8000. |

`schema` and `seed` are `restart: "no"` and use `service_completed_successfully` healthcheck conditions so the worker and dashboard wait for them.

## Where to look next

- `tasks.py` — the three demo tasks (`send_email`, `export_report`, `flaky_demo`).
- `seed.py` — populates the jobs table with realistic-looking historical data.
- `standalone.py` — three lines that mount the dashboard.
