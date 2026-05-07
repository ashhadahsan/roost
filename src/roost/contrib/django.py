"""Django integration helpers.

Use Django's existing connection so the ``INSERT INTO roost.jobs``
commits inside ``transaction.atomic()`` alongside your model writes.

Example::

    from django.db import transaction, connection
    from roost import Roost, job
    from roost.contrib.django import enqueue_in_atomic

    roost = Roost(settings.DATABASE_URL)

    @job("send_welcome")
    def send_welcome(user_id: int) -> None: ...

    def signup_view(request):
        with transaction.atomic():
            user = User.objects.create(email=...)
            enqueue_in_atomic(roost, send_welcome, args={"user_id": user.id})
        return HttpResponse("ok")

The helper pulls the active Django ORM connection's underlying psycopg
handle and passes it to ``Roost.enqueue(conn=...)``. Django and Roost
use the *same* transaction.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from roost.sync_api import Roost


def enqueue_in_atomic(
    roost: Roost,
    task: str | Callable[..., Any],
    *,
    args: dict[str, Any] | None = None,
    queue: str | None = None,
    priority: int | None = None,
    max_attempts: int | None = None,
    scheduled_at: Any | None = None,
    unique_key: str | None = None,
    tags: list[str] | None = None,
    timeout_seconds: int | None = None,
    using: str | None = None,
) -> int:
    """Enqueue a job using Django's active transaction.

    :param using: optional Django database alias (matches
        ``transaction.atomic(using=...)``). Defaults to ``"default"``.
    """
    try:
        from django.db import connections
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("django is not installed — install Django to use roost.contrib.django") from exc

    db_alias = using or "default"
    conn = connections[db_alias]
    # Ensure we have an open underlying connection.
    conn.ensure_connection()
    raw_conn = conn.connection  # psycopg connection
    if raw_conn is None:  # pragma: no cover
        raise RuntimeError(f"Django database alias '{db_alias}' has no active connection")

    kwargs: dict[str, Any] = {"args": args}
    if queue is not None:
        kwargs["queue"] = queue
    if priority is not None:
        kwargs["priority"] = priority
    if max_attempts is not None:
        kwargs["max_attempts"] = max_attempts
    if scheduled_at is not None:
        kwargs["scheduled_at"] = scheduled_at
    if unique_key is not None:
        kwargs["unique_key"] = unique_key
    if tags is not None:
        kwargs["tags"] = tags
    if timeout_seconds is not None:
        kwargs["timeout_seconds"] = timeout_seconds

    return roost.enqueue(task, conn=raw_conn, **kwargs)


__all__ = ["enqueue_in_atomic"]
