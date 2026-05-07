from __future__ import annotations

import random
from typing import Protocol


class BackoffStrategy(Protocol):
    """Maps an attempt number (1-indexed) to seconds-until-next-attempt."""

    def __call__(self, attempt: int) -> float: ...


def exponential(base: float = 2.0, *, jitter: bool = True, cap: float = 24 * 60 * 60) -> BackoffStrategy:
    """``base ** attempt`` seconds, optionally jittered, capped at ``cap``.

    Defaults to Oban's behavior: ``2, 4, 8, 16, …`` capped at one day.
    """

    def _strategy(attempt: int) -> float:
        delay = min(base ** max(attempt, 1), cap)
        if jitter:
            delay *= 0.5 + random.random()  # noqa: S311 — jitter, not crypto
        return delay

    return _strategy


def linear(step: float = 60.0, *, jitter: bool = False) -> BackoffStrategy:
    """Constant linear growth: ``step * attempt`` seconds."""

    def _strategy(attempt: int) -> float:
        delay = step * max(attempt, 1)
        if jitter:
            delay *= 0.5 + random.random()  # noqa: S311
        return delay

    return _strategy


def fixed(seconds: float = 60.0) -> BackoffStrategy:
    """Always wait ``seconds``."""

    def _strategy(attempt: int) -> float:
        return seconds

    return _strategy


DEFAULT_STRATEGY: BackoffStrategy = exponential()


def resolve(strategy: BackoffStrategy | None) -> BackoffStrategy:
    """Pick ``strategy`` if provided, otherwise the package default.

    Plain ``Callable[[int], float]`` callables are structurally compatible
    with :class:`BackoffStrategy` and may be passed directly.
    """
    return strategy if strategy is not None else DEFAULT_STRATEGY
