"""``@job`` and ``@cron`` decorators backed by module-level registries."""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar, cast

from pydantic import BaseModel

from roost._core.cron import DEFAULT_REGISTRY, CronEntry

F = TypeVar("F", bound=Callable[..., Any])


@dataclass(frozen=True)
class TaskDefaults:
    """Per-task enqueue defaults declared on ``@job(...)``.

    The first five (``queue`` … ``timeout_seconds``) are merged into
    ``enqueue`` calls — explicit kwargs always win.

    The throttling fields (``rate_per_minute``, ``max_concurrency``) are
    enforced at fetch time by the worker. Workers read the registry and
    pass the limits into the fetch SQL.
    """

    queue: str | None = None
    priority: int | None = None
    max_attempts: int | None = None
    tags: tuple[str, ...] | None = None
    timeout_seconds: int | None = None
    rate_per_minute: int | None = None
    max_concurrency: int | None = None


@dataclass(frozen=True)
class HandlerSpec:
    name: str
    func: Callable[..., Any]
    is_async: bool
    args_model: type[BaseModel] | None = None
    defaults: TaskDefaults = field(default_factory=TaskDefaults)


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, HandlerSpec] = {}

    def register(
        self,
        name: str,
        func: Callable[..., Any],
        *,
        args_model: type[BaseModel] | None = None,
        defaults: TaskDefaults | None = None,
    ) -> None:
        if name in self._handlers and self._handlers[name].func is not func:
            raise ValueError(f"task '{name}' is already registered to a different function")
        self._handlers[name] = HandlerSpec(
            name=name,
            func=func,
            is_async=inspect.iscoroutinefunction(func),
            args_model=args_model,
            defaults=defaults or TaskDefaults(),
        )

    def get(self, name: str) -> HandlerSpec | None:
        return self._handlers.get(name)

    def specs(self) -> list[HandlerSpec]:
        """Return every registered :class:`HandlerSpec`, sorted by name.

        Useful for building admin UIs or generating manifests::

            for spec in roost_handlers.specs():
                print(spec.name, spec.defaults.queue, spec.args_model)
        """
        return [self._handlers[name] for name in sorted(self._handlers)]

    def names(self) -> list[str]:
        return sorted(self._handlers)

    def clear(self) -> None:
        self._handlers.clear()


DEFAULT_HANDLERS = HandlerRegistry()


def job(
    name: str,
    *,
    args_model: type[BaseModel] | None = None,
    queue: str | None = None,
    priority: int | None = None,
    max_attempts: int | None = None,
    tags: list[str] | tuple[str, ...] | None = None,
    timeout_seconds: int | None = None,
    rate_per_minute: int | None = None,
    max_concurrency: int | None = None,
    registry: HandlerRegistry | None = None,
) -> Callable[[F], F]:
    """Register ``func`` as the handler for the task ``name``.

    Per-task defaults (``queue``, ``priority``, ``max_attempts``, ``tags``,
    ``timeout_seconds``) are applied to every enqueue of this task unless
    the caller passes an explicit kwarg.

    Pass ``args_model=`` (a Pydantic model) to validate enqueued args at
    handler-call time.

    The decorated function is returned untouched — it can still be called
    directly in tests.
    """

    target = registry or DEFAULT_HANDLERS
    defaults = TaskDefaults(
        queue=queue,
        priority=priority,
        max_attempts=max_attempts,
        tags=tuple(tags) if tags is not None else None,
        timeout_seconds=timeout_seconds,
        rate_per_minute=rate_per_minute,
        max_concurrency=max_concurrency,
    )

    def _decorate(func: F) -> F:
        if args_model is not None:
            wrapped = _wrap_with_validation(func, args_model)
            target.register(name, wrapped, args_model=args_model, defaults=defaults)
            wrapped.__roost_task_name__ = name  # type: ignore[attr-defined]
            return cast(F, wrapped)
        target.register(name, func, defaults=defaults)
        func.__roost_task_name__ = name  # type: ignore[attr-defined]
        return func

    return _decorate


def _wrap_with_validation(func: F, model: type[BaseModel]) -> Callable[..., Any]:
    """Validate inbound kwargs against ``model`` before calling ``func``."""

    if inspect.iscoroutinefunction(func):

        @functools.wraps(func)
        async def _async_wrapper(**kwargs: Any) -> Any:
            validated = model(**kwargs)
            return await func(**validated.model_dump())

        return _async_wrapper

    @functools.wraps(func)
    def _sync_wrapper(**kwargs: Any) -> Any:
        validated = model(**kwargs)
        return func(**validated.model_dump())

    return _sync_wrapper


def cron(
    expression: str,
    *,
    name: str | None = None,
    queue: str = "default",
    args: dict[str, Any] | None = None,
    priority: int = 0,
    max_attempts: int = 20,
    timezone: str | None = None,
    handler_registry: HandlerRegistry | None = None,
) -> Callable[[F], F]:
    """Register a function as a cron handler under ``expression``.

    ``timezone`` accepts an IANA name (``"America/Los_Angeles"``,
    ``"Europe/Berlin"``). Defaults to UTC. The cron expression is then
    interpreted in that local timezone, including DST.
    """
    handler_target = handler_registry or DEFAULT_HANDLERS

    def _decorate(func: F) -> F:
        task_name = name or getattr(func, "__roost_task_name__", None) or func.__name__
        handler_target.register(task_name, func)
        func.__roost_task_name__ = task_name  # type: ignore[attr-defined]
        DEFAULT_REGISTRY.register(
            CronEntry(
                name=task_name,
                expression=expression,
                task=task_name,
                args=dict(args or {}),
                queue=queue,
                priority=priority,
                max_attempts=max_attempts,
                timezone_name=timezone,
            )
        )
        return func

    return _decorate


def task_name(func: Callable[..., Any]) -> str:
    """Resolve the registered task name for ``func`` or raise."""
    name = getattr(func, "__roost_task_name__", None)
    if name is None:
        raise ValueError(f"function {func!r} is not a registered Roost task — did you forget @job?")
    return cast(str, name)
