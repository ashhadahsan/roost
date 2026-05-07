# Security policy

## Supported versions

Until Roost reaches `1.0`, only the latest minor release receives security
fixes. Pin exactly when running in production.

| Version | Status        |
| ------- | ------------- |
| `0.1.x` | active        |
| `< 0.1` | not supported |

## Reporting a vulnerability

**Please do not file a public GitHub issue for security problems.**

Email `ashhadahsan@gmail.com` with:

- a description of the issue,
- the affected version(s),
- a minimal reproducer if you have one,
- any disclosure deadline you require.

You'll receive an acknowledgment within three business days. We'll work
with you on a fix and a coordinated disclosure window — typically 30
days, longer if the fix needs cross-project coordination.

## Scope

In scope:

- The `roost` Python package and its CLI.
- Schema migrations in `roost._core.schema`.
- The release pipeline (`.github/workflows/release.yml`).

Out of scope (please report to the upstream project):

- Vulnerabilities in `asyncpg`, `psycopg`, `pydantic`, `typer`, or PostgreSQL itself.
- The `roost-web` dashboard — see <https://github.com/ashhadahsan/roost-web>.

## Hardening expectations for users

Roost stores task arguments verbatim in the `roost.jobs.args` JSONB column.
Treat that column with the same sensitivity as the data flowing into it:

- Don't pass secrets through `args`. Pass references (a key) and resolve
  them inside the handler.
- Restrict the database role used by application code to the minimum
  privileges needed (`SELECT`, `INSERT`, `UPDATE` on `roost.jobs` and the
  cron / workers tables).
- The worker process needs to talk to PostgreSQL — ensure that connection
  uses TLS in production.
