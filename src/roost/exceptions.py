"""Public exception hierarchy.

Each subclass declares a stable ``code`` constant — SDK consumers should
match on the code, not on the message text. Codes use kebab-case and a
``roost.`` prefix so they don't collide with any other library's catalog.
"""

from __future__ import annotations

from typing import ClassVar


class RoostError(Exception):
    """Base class for all Roost errors. Carries a stable :attr:`code`."""

    code: ClassVar[str] = "roost.error"


class UnknownTaskError(RoostError):
    """Raised when a worker pulls a job whose task name is not in the registry."""

    code: ClassVar[str] = "roost.unknown-task"


class DuplicateUniqueJobError(RoostError):
    """Raised when an enqueue conflicts with an active job sharing the same unique_key."""

    code: ClassVar[str] = "roost.duplicate-unique-job"


class JobNotFoundError(RoostError):
    """Raised when an admin operation targets a job_id that does not exist."""

    code: ClassVar[str] = "roost.job-not-found"


class WorkerShutdown(RoostError):
    """Raised internally to interrupt the worker loop on graceful shutdown."""

    code: ClassVar[str] = "roost.worker-shutdown"


class SnoozeJob(RoostError):
    """Raise inside a job handler to reschedule the job without counting it as a failure."""

    code: ClassVar[str] = "roost.snooze-job"

    def __init__(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("snooze seconds must be >= 0")
        self.seconds = seconds
        super().__init__(f"snoozed for {seconds}s")
