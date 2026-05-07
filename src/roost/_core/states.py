from __future__ import annotations

from enum import Enum


class JobState(str, Enum):
    AVAILABLE = "available"
    EXECUTING = "executing"
    RETRYABLE = "retryable"
    COMPLETED = "completed"
    DISCARDED = "discarded"
    CANCELLED = "cancelled"


ACTIVE_STATES: frozenset[str] = frozenset(
    {JobState.AVAILABLE.value, JobState.EXECUTING.value, JobState.RETRYABLE.value}
)

TERMINAL_STATES: frozenset[str] = frozenset(
    {JobState.COMPLETED.value, JobState.DISCARDED.value, JobState.CANCELLED.value}
)
