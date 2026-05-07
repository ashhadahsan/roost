from __future__ import annotations


class RoostError(Exception):
    """Base class for all Roost errors."""


class UnknownTaskError(RoostError):
    """Raised when a worker pulls a job whose task name is not in the registry."""


class DuplicateUniqueJobError(RoostError):
    """Raised when an enqueue conflicts with an active job sharing the same unique_key."""


class JobNotFoundError(RoostError):
    """Raised when an admin operation targets a job_id that does not exist."""


class WorkerShutdown(RoostError):
    """Raised internally to interrupt the worker loop on graceful shutdown."""


class SnoozeJob(RoostError):
    """Raise inside a job handler to reschedule the job without counting it as a failure."""

    def __init__(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("snooze seconds must be >= 0")
        self.seconds = seconds
        super().__init__(f"snoozed for {seconds}s")
