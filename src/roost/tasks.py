"""Public introspection over the registered task set.

Thin wrapper around :data:`roost.decorators.DEFAULT_HANDLERS` so users
don't need to reach into ``decorators`` for the common case::

    from roost import tasks

    for spec in tasks.specs():
        print(spec.name, spec.defaults.queue, spec.args_model)

    spec = tasks.get("send_welcome_email")
"""

from __future__ import annotations

from roost.decorators import DEFAULT_HANDLERS, HandlerSpec


def specs() -> list[HandlerSpec]:
    """Every registered task spec, sorted by name."""
    return DEFAULT_HANDLERS.specs()


def get(name: str) -> HandlerSpec | None:
    """Look up a single task by name. ``None`` if not registered."""
    return DEFAULT_HANDLERS.get(name)


def names() -> list[str]:
    """Just the registered task names, sorted."""
    return DEFAULT_HANDLERS.names()


__all__ = ["get", "names", "specs"]
