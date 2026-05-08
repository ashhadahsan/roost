# Roost examples

Six concrete patterns. Each subdirectory is self-contained and assumes the schema is applied (`roost init --apply --dsn ...`).

| Folder | Pattern | Demonstrates |
| --- | --- | --- |
| [`docker/`](./docker/) | One-command stack | Postgres + worker + dashboard via `docker compose up`. |
| [`plain_python.py`](./plain_python.py) | Hello world | Smallest possible enqueue + worker, no framework. |
| [`fastapi_app/`](./fastapi_app/) | Web request → job | Transactional enqueue inside `tx_roost_dep`, `wait_for` for synchronous results, Pydantic-typed args. |
| [`scheduled_reports/`](./scheduled_reports/) | Cron with timezone | Daily digest at 9am Pacific (DST-aware), weekly UTC rollup. |
| [`fanout_join/`](./fanout_join/) | Fan-out → join | N parallel children + a single aggregator gated on `depends_on`. |
| [`etl_pipeline/`](./etl_pipeline/) | Sequential ETL | Three chained tasks per row, with rate limits and per-task max-concurrency. |

## Trying them out

```bash
# 1. Local Postgres (Docker is easiest)
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=x postgres:16-alpine
export ROOST_DSN="postgresql://postgres:x@localhost/postgres"

# 2. Apply the schema
roost init --apply

# 3. Pick an example, then in another terminal start a worker pointed at it
roost run --module examples.fastapi_app.app --queues default,emails

# 4. Watch the dashboard fill up
pip install roost-web uvicorn
ROOST_DSN=$ROOST_DSN python -m uvicorn examples.docker.standalone:app --port 8000
open http://localhost:8000
```

Or use [`docker/`](./docker/) and skip steps 1–3 entirely:

```bash
docker compose -f examples/docker/docker-compose.yml up
open http://localhost:8000
```
