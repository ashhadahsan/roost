"""``roost`` CLI — operational commands for the queue."""

from __future__ import annotations

import asyncio
import importlib
import os
from typing import Annotated

import typer

from roost import __version__, observability
from roost._core.schema import migration_sql
from roost.async_api import AsyncRoost
from roost.exceptions import JobNotFoundError
from roost.sync_api import Roost
from roost.worker import Worker

app = typer.Typer(
    name="roost",
    help="Postgres-backed background job queue.",
    add_completion=False,
    no_args_is_help=True,
)


def _resolve_dsn(dsn: str | None) -> str:
    if dsn:
        return dsn
    env = os.environ.get("ROOST_DSN") or os.environ.get("DATABASE_URL")
    if env:
        return env
    raise typer.BadParameter("no DSN provided — pass --dsn or set ROOST_DSN / DATABASE_URL")


def _import_modules(modules: list[str]) -> None:
    """Import dotted module paths so their decorators register handlers/cron entries."""
    for mod in modules:
        if not mod:
            continue
        importlib.import_module(mod)


@app.command()
def version() -> None:
    """Print the installed Roost version."""
    typer.echo(__version__)


migrate_app = typer.Typer(name="migrate", help="Schema migrations.", no_args_is_help=True)
app.add_typer(migrate_app)


@migrate_app.command("up")
def migrate_up(
    dsn: Annotated[str | None, typer.Option(help="Postgres DSN (or set ROOST_DSN).")] = None,
) -> None:
    """Apply every pending migration."""
    import psycopg

    from roost._core.migrations import apply_pending_sync

    target = _resolve_dsn(dsn)
    with psycopg.connect(target) as conn:
        applied = apply_pending_sync(conn)
    if applied:
        typer.secho(f"applied: {applied}", fg=typer.colors.GREEN)
    else:
        typer.secho("nothing to apply — already at latest", fg=typer.colors.CYAN)


@migrate_app.command("down")
def migrate_down(
    target_version: int = typer.Argument(..., help="Roll back to (and including) this version."),
    dsn: Annotated[str | None, typer.Option(help="Postgres DSN (or set ROOST_DSN).")] = None,
) -> None:
    """Roll back to ``target_version`` (exclusive of versions above)."""
    import psycopg

    from roost._core.migrations import IrreversibleMigration, rollback_to_sync

    target = _resolve_dsn(dsn)
    try:
        with psycopg.connect(target) as conn:
            reverted = rollback_to_sync(conn, target_version)
    except IrreversibleMigration as exc:
        typer.secho(f"refused: {exc}", fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc
    if reverted:
        typer.secho(f"reverted: {reverted}", fg=typer.colors.YELLOW)
    else:
        typer.secho("no migrations above target — nothing to do", fg=typer.colors.CYAN)


@migrate_app.command("status")
def migrate_status(
    dsn: Annotated[str | None, typer.Option(help="Postgres DSN (or set ROOST_DSN).")] = None,
) -> None:
    """Show applied vs available migrations."""
    import psycopg

    from roost._core.migrations import MIGRATIONS, applied_versions_sync

    target = _resolve_dsn(dsn)
    try:
        with psycopg.connect(target) as conn:
            applied = set(applied_versions_sync(conn))
    except Exception as exc:  # pragma: no cover — defensive
        typer.secho(f"error: {exc}", fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc

    typer.echo(f"{'version':<8}  {'name':<30}  status")
    typer.echo(f"{'-' * 8}  {'-' * 30}  ------")
    for migration in sorted(MIGRATIONS, key=lambda m: m.version):
        marker = "applied" if migration.version in applied else "pending"
        typer.echo(f"{migration.version:<8}  {migration.name:<30}  {marker}")


@app.command()
def init(
    apply: Annotated[
        bool, typer.Option("--apply", help="Run the SQL against --dsn instead of printing.")
    ] = False,
    dsn: Annotated[str | None, typer.Option(help="Postgres DSN (or set ROOST_DSN).")] = None,
) -> None:
    """Print the schema migration SQL — pass ``--apply`` to run it."""
    sql = migration_sql()
    if not apply:
        typer.echo(sql)
        return

    target = _resolve_dsn(dsn)
    Roost(target).setup_schema()
    typer.secho(f"schema applied to {target}", fg=typer.colors.GREEN)


@app.command()
def run(
    queues: Annotated[str, typer.Option(help="Comma-separated queue names.")] = "default",
    concurrency: Annotated[int, typer.Option(help="In-flight jobs per worker process.")] = 4,
    prefetch: Annotated[int | None, typer.Option(help="Rows to fetch per poll.")] = None,
    poll_interval: Annotated[float, typer.Option(help="Seconds between polls when idle.")] = 1.0,
    no_cron: Annotated[bool, typer.Option("--no-cron", help="Skip cron scheduler in this worker.")] = False,
    module: Annotated[
        list[str] | None,
        typer.Option("--module", "-m", help="Dotted module to import (registers @job/@cron). Repeatable."),
    ] = None,
    dsn: Annotated[str | None, typer.Option(help="Postgres DSN (or set ROOST_DSN).")] = None,
) -> None:
    """Run a worker."""
    observability.auto_configure_from_env()

    target = _resolve_dsn(dsn)
    queue_list = [q.strip() for q in queues.split(",") if q.strip()]
    if not queue_list:
        raise typer.BadParameter("at least one queue is required")

    _import_modules(list(module or []))

    worker = Worker(
        target,
        queues=queue_list,
        concurrency=concurrency,
        prefetch=prefetch,
        poll_interval=poll_interval,
        run_cron=not no_cron,
    )

    async def _main() -> None:
        loop = asyncio.get_running_loop()
        worker.install_signal_handlers(loop)
        await worker.run()

    typer.secho(
        f"roost worker queues={queue_list} concurrency={concurrency} dsn={target}",
        fg=typer.colors.CYAN,
    )
    asyncio.run(_main())


@app.command()
def status(
    dsn: Annotated[str | None, typer.Option(help="Postgres DSN (or set ROOST_DSN).")] = None,
) -> None:
    """Print job counts grouped by queue and state."""
    target = _resolve_dsn(dsn)
    rows = Roost(target).status()
    if not rows:
        typer.echo("(no jobs)")
        return
    width_q = max(len("queue"), max(len(q) for q, _, _ in rows))
    width_s = max(len("state"), max(len(s) for _, s, _ in rows))
    typer.echo(f"{'queue'.ljust(width_q)}  {'state'.ljust(width_s)}  count")
    typer.echo(f"{'-' * width_q}  {'-' * width_s}  -----")
    for queue, state, count in rows:
        typer.echo(f"{queue.ljust(width_q)}  {state.ljust(width_s)}  {count}")


@app.command()
def retry(
    job_id: int = typer.Argument(..., help="Job id to retry."),
    dsn: Annotated[str | None, typer.Option(help="Postgres DSN (or set ROOST_DSN).")] = None,
) -> None:
    """Move a job back to ``available`` immediately."""
    target = _resolve_dsn(dsn)
    try:
        Roost(target).retry(job_id)
    except JobNotFoundError as exc:
        raise typer.Exit(code=1) from exc
    typer.secho(f"retry queued for job {job_id}", fg=typer.colors.GREEN)


@app.command()
def cancel(
    job_id: int = typer.Argument(..., help="Job id to cancel."),
    dsn: Annotated[str | None, typer.Option(help="Postgres DSN (or set ROOST_DSN).")] = None,
) -> None:
    """Cancel a pending job (or signal cancel to a running one)."""
    target = _resolve_dsn(dsn)
    try:
        Roost(target).cancel(job_id)
    except JobNotFoundError as exc:
        raise typer.Exit(code=1) from exc
    typer.secho(f"cancel signaled for job {job_id}", fg=typer.colors.YELLOW)


queues_app = typer.Typer(name="queue", help="Per-queue admin (pause/resume/list).", no_args_is_help=True)
app.add_typer(queues_app)


@queues_app.command("pause")
def queue_pause(
    name: str = typer.Argument(..., help="Queue name."),
    dsn: Annotated[str | None, typer.Option(help="Postgres DSN (or set ROOST_DSN).")] = None,
) -> None:
    """Pause ``name`` — workers stop fetching jobs from it until resumed."""
    target = _resolve_dsn(dsn)
    Roost(target).pause_queue(name)
    typer.secho(f"queue {name!r} paused", fg=typer.colors.YELLOW)


@queues_app.command("resume")
def queue_resume(
    name: str = typer.Argument(..., help="Queue name."),
    dsn: Annotated[str | None, typer.Option(help="Postgres DSN (or set ROOST_DSN).")] = None,
) -> None:
    """Resume ``name``."""
    target = _resolve_dsn(dsn)
    Roost(target).resume_queue(name)
    typer.secho(f"queue {name!r} resumed", fg=typer.colors.GREEN)


@queues_app.command("list")
def queue_list(
    dsn: Annotated[str | None, typer.Option(help="Postgres DSN (or set ROOST_DSN).")] = None,
) -> None:
    """List configured queues and their pause state."""
    target = _resolve_dsn(dsn)
    rows = Roost(target).list_queues()
    if not rows:
        typer.echo("(no queue rows yet)")
        return
    width = max(4, max(len(q) for q, _ in rows))
    typer.echo(f"{'name'.ljust(width)}  paused_at")
    typer.echo(f"{'-' * width}  ---------")
    for name, paused_at in rows:
        marker = paused_at.isoformat() if paused_at else "-"
        typer.echo(f"{name.ljust(width)}  {marker}")


@app.command()
def workers(
    dsn: Annotated[str | None, typer.Option(help="Postgres DSN (or set ROOST_DSN).")] = None,
) -> None:
    """List recently-seen workers from the heartbeat table."""
    target = _resolve_dsn(dsn)
    rows = Roost(target).list_workers()
    if not rows:
        typer.echo("(no workers heartbeating)")
        return
    typer.echo(f"{'id':<48}  {'host':<24}  pid    queues          last_seen_at")
    typer.echo(f"{'-' * 48}  {'-' * 24}  -----  --------------  ------------")
    for row in rows:
        wid = str(row.get("id", ""))[:48]
        host = str(row.get("hostname", ""))[:24]
        pid = row.get("pid", "")
        qs = ",".join(row.get("queues", []) or [])[:14]
        seen = row.get("last_seen_at")
        seen_s = seen.isoformat() if seen is not None else "-"
        typer.echo(f"{wid:<48}  {host:<24}  {pid!s:<5}  {qs:<14}  {seen_s}")


@app.command()
def requeue(
    discarded: Annotated[
        bool,
        typer.Option("--discarded", help="Mass-revive every discarded job to available."),
    ] = False,
    queue: Annotated[
        str | None,
        typer.Option("--queue", help="Limit the bulk action to this queue."),
    ] = None,
    dsn: Annotated[str | None, typer.Option(help="Postgres DSN (or set ROOST_DSN).")] = None,
) -> None:
    """Requeue discarded jobs in bulk. Use ``roost retry <id>`` for a single id."""
    import psycopg

    if not discarded:
        raise typer.BadParameter("pass --discarded to mass-requeue the dead-letter pile")
    target = _resolve_dsn(dsn)
    if queue is None:
        n = Roost(target).requeue_discarded()
    else:
        with psycopg.connect(target) as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE roost.jobs SET state = 'available', scheduled_at = now(), "
                "    attempt = 0, cancel_requested = false "
                " WHERE state = 'discarded' AND queue = %s",
                (queue,),
            )
            n = cur.rowcount or 0
            conn.commit()
    typer.secho(
        f"requeued {n} discarded job(s)" + (f" from queue {queue!r}" if queue else ""),
        fg=typer.colors.GREEN,
    )


@app.command()
def enqueue(
    task: str = typer.Argument(..., help="Registered task name."),
    args_json: Annotated[
        str,
        typer.Option("--args", help="JSON-encoded args dict. Example: --args '{\"x\": 1}'."),
    ] = "{}",
    queue_name: Annotated[str, typer.Option("--queue", help="Queue to enqueue into.")] = "default",
    in_seconds: Annotated[
        float | None,
        typer.Option(
            "--in",
            help="Seconds from now until the job becomes available. Negative => snooze.",
        ),
    ] = None,
    priority: Annotated[int, typer.Option(help="Job priority (lower runs first).")] = 0,
    max_attempts: Annotated[int, typer.Option(help="Maximum retry attempts.")] = 20,
    unique_key: Annotated[
        str | None, typer.Option(help="Dedup key. Active rows with same key are deduplicated.")
    ] = None,
    dsn: Annotated[str | None, typer.Option(help="Postgres DSN (or set ROOST_DSN).")] = None,
) -> None:
    """Ad-hoc enqueue from the command line — handy for ops + debugging."""
    import json
    from datetime import datetime, timedelta, timezone

    target = _resolve_dsn(dsn)
    try:
        parsed = json.loads(args_json)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"--args is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise typer.BadParameter("--args must encode a JSON object")
    when: datetime | None = None
    if in_seconds is not None:
        when = datetime.now(tz=timezone.utc) + timedelta(seconds=in_seconds)

    job_id = Roost(target).enqueue(
        task,
        args=parsed,
        queue=queue_name,
        priority=priority,
        max_attempts=max_attempts,
        scheduled_at=when,
        unique_key=unique_key,
    )
    typer.secho(
        f"enqueued job {job_id} task={task!r} queue={queue_name!r}",
        fg=typer.colors.GREEN,
    )


# Make ``AsyncRoost`` reachable for ``importlib`` users; suppresses unused-import warnings.
_ = AsyncRoost


def main() -> None:
    """Console-script entry point."""
    app()


if __name__ == "__main__":  # pragma: no cover
    main()
