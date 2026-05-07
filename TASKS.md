# Roost — Task Plan

Working checklist derived from the master plan
(`~/.claude/plans/okay-create-a-detsailed-velvety-gizmo.md`).
Tick boxes as work lands. Conventional Commits for messages.

Legend: `[ ]` todo · `[~]` in progress · `[x]` done · `[!]` blocked

---

## Phase 0 — Project scaffolding (½ day)

- [x] Verify `roost` name is free on PyPI + GitHub (else fall back to `roostpy` / `pyroost` / `roostq` / `roostdb`)
- [x] Rename working directory to `/Users/ashhad/Projects/roost`
- [x] `git init`
- [x] `uv init --package --python 3.12`
- [x] Add core deps: `asyncpg`, `psycopg[binary]`, `typer`, `pydantic`, `croniter`, `structlog`
- [x] Add dev deps: `pytest`, `pytest-asyncio`, `testcontainers[postgres]`, `ruff`, `mypy`, `pre-commit`, `sphinx`, `furo`, `myst-parser`, `sphinx-autodoc-typehints`, `sphinx-copybutton`
- [x] Write `LICENSE` (MIT, © 2026 Ashhad Ahsan)
- [x] Write `.gitignore`
- [x] Write `.pre-commit-config.yaml` (ruff, ruff-format, mypy)
- [x] Write stub `README.md` with elevator pitch + alpha banner
- [x] Write `CLAUDE.md` (local-only — gitignored, do not commit)
- [x] Initial commit: `chore: initial scaffold`

**Done when:** `uv sync` works, `pre-commit run --all-files` passes on stubs.

---

## Phase 1 — Schema + transactional enqueue (1–2 days)

- [ ] `src/roost/_core/schema.py` — SQL constant: `roost.jobs` table, `jobs_fetch_idx`, `jobs_unique_idx`
- [ ] Add `notify_inserted` trigger (channel `roost_inserted`, payload = queue)
- [ ] Add `notify_updated` trigger (channel `roost_updated`, payload = id) — locked in early so `roost-web` doesn't need a v0.2 migration
- [ ] `src/roost/_core/states.py` — state Enum (`available`, `executing`, `completed`, `retryable`, `discarded`, `cancelled`)
- [ ] `src/roost/_core/job.py` — `Job` Pydantic / dataclass model
- [ ] `src/roost/_core/repo.py` — `enqueue(conn, task, args, *, queue, scheduled_at, priority, max_attempts, unique_key)` that takes an externally-managed conn/transaction (async via asyncpg + sync via psycopg)
- [ ] `src/roost/exceptions.py` — base + specific errors
- [ ] Tests: enqueue inside txn → rollback → no row; enqueue → commit → row visible
- [ ] Tests: unique_key collision honored within `(available, executing, retryable)` only

**Done when:** `roost.AsyncRoost.enqueue(...)` and sync twin work end-to-end against a real Postgres via testcontainers.

> ⚠️ Load-bearing invariant: enqueue MUST accept and use the caller's connection. Never open a new conn inside `enqueue()`.

---

## Phase 2 — Worker, retries, cron (3–4 days)

### Worker loop (`src/roost/worker.py`)
- [ ] Dedicated `LISTEN roost_inserted` connection, 1s poll fallback
- [ ] Fetch: `BEGIN; SELECT … FOR UPDATE SKIP LOCKED LIMIT prefetch; UPDATE state='executing', attempt=attempt+1, attempted_at=now(); COMMIT;`
- [ ] Dispatch to handler from `@job("name")` registry
- [ ] Success path: `state='completed', completed_at=now()`
- [ ] Failure path: append error JSON, compute next `scheduled_at` via backoff, `state='retryable'` or `'discarded'` if exhausted
- [ ] Graceful shutdown on SIGTERM/SIGINT — drain in-flight, then exit
- [ ] Concurrency knobs: `--queues`, `--concurrency`, `--prefetch`

### Retries (`src/roost/_core/retry.py`)
- [ ] `exponential(base=2, jitter=True)` — default
- [ ] `linear`
- [ ] `fixed`
- [ ] Custom callable hook

### Cron (`src/roost/_core/cron.py`)
- [ ] Scheduler coroutine waking every 60s
- [ ] Cluster-wide singleton via `pg_try_advisory_lock(SCHEDULER_KEY)`
- [ ] Insert next-due cron entries
- [ ] `@cron("*/5 * * * *", queue="emails")` decorator registers entries on import

### LISTEN/NOTIFY (`src/roost/_core/notify.py`)
- [ ] Helpers for asyncpg + psycopg listening channels (`roost_inserted`, `roost_updated`)

**Done when:** `roost run --queues default,emails` runs jobs, retries failures, fires cron on time.

---

## Phase 3 — Sync + async API surface (1 day)

- [ ] `src/roost/async_api.py` — `AsyncRoost` (asyncpg)
- [ ] `src/roost/sync_api.py` — `Roost` (psycopg, NEVER drives async via loop tricks)
- [ ] `src/roost/decorators.py` — `@job`, `@cron`
- [ ] `src/roost/__init__.py` — public re-exports only
- [ ] `src/roost/py.typed` — PEP 561 marker
- [ ] Tests covering both facades end-to-end

**Done when:** README quickstart code works for both async (FastAPI-style) and sync (Django-style).

---

## Phase 4 — CLI (½ day)

`src/roost/cli.py` (Typer):
- [ ] `roost init` — emit SQL to stdout; `--apply --dsn=…` to run it
- [ ] `roost run [--queues …] [--concurrency N] [--prefetch N]`
- [ ] `roost status` — counts per state per queue
- [ ] `roost retry <job_id>`
- [ ] `roost cancel <job_id>`
- [ ] `roost version`
- [ ] Register console script `roost = "roost.cli:app"` in `pyproject.toml`

**Done when:** `uv run roost --help` works.

---

## Phase 5 — Tests + CI (1–2 days)

- [ ] `tests/conftest.py` — `testcontainers[postgres]` real-PG fixture (session-scoped)
- [ ] `test_enqueue.py` — atomicity, unique_key
- [ ] `test_worker.py` — concurrency: N workers can't double-process a row
- [ ] `test_retries.py` — exponential / linear / fixed, max_attempts boundary
- [ ] `test_cron.py` — advisory-lock singleton, insertion cadence
- [ ] `test_unique.py`
- [ ] `test_listen_notify.py` — wakeup latency
- [ ] `test_cli.py` — smoke
- [ ] `.github/workflows/ci.yml` — matrix Python 3.10/3.11/3.12/3.13 × Postgres 13/14/15/16
- [ ] CI steps: `uv sync`, `ruff check`, `ruff format --check`, `mypy src`, `pytest -n auto`
- [ ] Coverage via `coverage.py` + Codecov badge

**Done when:** green CI, coverage ≥85%.

---

## Phase 6 — Documentation on Read the Docs (1 day)

- [ ] `docs/conf.py` — Sphinx + `furo` + `myst_parser` + autodoc + typehints + copybutton
- [ ] `.readthedocs.yaml` (Ubuntu 22.04, Python 3.12, install with `[docs]` extra)
- [ ] `pyproject.toml` — `[project.optional-dependencies] docs = […]`
- [ ] `docs/quickstart.md`
- [ ] `docs/concepts/transactional-enqueue.md` (the marketing piece)
- [ ] `docs/concepts/{retries,cron,unique-jobs}.md`
- [ ] `docs/recipes/{django,fastapi,flask}.md`
- [ ] `docs/reference/api.md` — autodoc-driven
- [ ] `docs/changelog.md`
- [ ] Import repo on readthedocs.org, set default branch

**Done when:** `roost.readthedocs.io` builds on push and on tag.

---

## Phase 7 — Logo & branding (½–1 day)

Concept: small bird perched on a branch, geometric mark. Palette: Postgres-blue `#336791` lead, `#f8f9fa` bg, warm accent `#e6a23c`. Wordmark in Inter / Geist.

- [ ] Generate 4–6 candidates (Canva / DALL·E / Midjourney)
- [ ] Pick a direction; refine to vector (Figma / Inkscape)
- [ ] Export: 512×512 PNG (PyPI), SVG (`docs/_static/logo.svg`), 1280×640 PNG (`.github/social-card.png`), 32×32 favicon (reused by `roost-web`)
- [ ] Add logo to README top + Sphinx `html_logo`

**Done when:** logo files committed; README, docs, PyPI page, social card all show the mark.

---

## Phase 8 — PyPI release v0.1.0 (½ day)

- [ ] Final pass on `pyproject.toml` (name, version, classifiers, urls, scripts)
- [ ] Reserve `roost` on PyPI by publishing a `0.0.0` placeholder (if not already)
- [ ] Configure PyPI Trusted Publisher (OIDC) for `release.yml`
- [ ] `.github/workflows/release.yml` — on `v*` tag: `uv build && uv publish` via OIDC
- [ ] Bump `version = "0.1.0"` in `pyproject.toml`
- [ ] Update `CHANGELOG.md`
- [ ] Tag `v0.1.0`, push tag → release.yml fires
- [ ] Smoke: `pip install roost` in fresh venv, `roost --version`

**Done when:** `roost==0.1.0` on PyPI, RTD shows v0.1.0 docs.

> ⚠️ Never run `uv publish` from a workstation. Tags are the only trigger.

---

## Phase 9 — Launch (ongoing)

- [ ] Announce: r/Python, Hacker News (Show HN), Python Weekly, Lobsters, network
- [ ] Triage incoming issues within 48h for the first month
- [ ] Open `ashhadahsan/roost-web` repo and start the dashboard track

---

## Verification gates before tagging v0.1.0

1. **Local sanity**
   - [ ] `uv sync && uv run pytest` — all green
   - [ ] `uv run pre-commit run --all-files` — clean
   - [ ] `uv build` produces wheel + sdist
2. **Real-world smoke**
   - [ ] Fresh venv: `pip install dist/roost-0.1.0-py3-none-any.whl`
   - [ ] `docker run -p 5432:5432 -e POSTGRES_PASSWORD=x postgres:16`
   - [ ] `roost init --apply --dsn postgresql://postgres:x@localhost/postgres`
   - [ ] `examples/plain_python.py` enqueues + worker runs end-to-end
   - [ ] Forced failure → `retryable` → re-runs
3. **CI matrix:** green on PG 13/14/15/16 × Python 3.10/3.11/3.12/3.13
4. **Docs:** `roost.readthedocs.io` builds and renders the logo
5. **PyPI:** `pip install roost` from a clean machine; `roost --version` → `0.1.0`
6. **Tag:** `git tag v0.1.0 && git push --tags` → `release.yml` succeeds; PyPI + RTD updated

---

## Parallel track — `roost-web` (separate repo, AFTER core ships)

> Lives at `ashhadahsan/roost-web`. PyPI `roost-web`. Depends on `roost>=0.1`. Mirrors `oban`/`oban_web`. Do **not** merge into core.

- [ ] Phase 0 — scaffold repo, link to core, share logo asset
- [ ] Phase 1 — read-only views (overview, jobs list, job detail) — Starlette + Jinja2 + HTMX
- [ ] Phase 2 — SSE live updates from `LISTEN roost_inserted` + `roost_updated`
- [ ] Phase 3 — admin actions (retry / cancel / pause queue) gated by `read_only` flag
- [ ] Phase 4 — workers + cron pages
- [ ] Phase 5 — tests (Playwright e2e + Starlette TestClient)
- [ ] Phase 6 — RTD docs + Tailwind compile in CI
- [ ] Phase 7 — logo reuse + screenshots in README
- [ ] Phase 8 — PyPI release `roost-web==0.1.0`

---

## Standing rules (from CLAUDE.md)

- ❌ No Redis/RabbitMQ/Kafka/any broker dep — Postgres-only is the pitch
- ❌ Never break transactional enqueue (caller's `conn` is honored)
- ❌ Never change `_core/schema.py` without a migration entry + a note about whether `roost-web` needs a sibling change
- ❌ Never merge `roost-web` into this repo
- ❌ Never publish manually — tags trigger `release.yml` via OIDC
- ✅ Conventional Commits: `feat:`, `fix:`, `docs:`, `chore:`, `refactor:`, `test:`
- ✅ Every PR must keep CI green on PG 13–16 × Python 3.10–3.13, coverage ≥85%
