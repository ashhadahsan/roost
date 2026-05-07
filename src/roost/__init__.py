"""Roost — Postgres-backed background job queue for Python.

Public API::

    from roost import AsyncRoost, Roost, job, cron

The async path uses asyncpg; the sync path uses psycopg. They share schema
and a single SQL surface defined in :mod:`roost._core.repo`.
"""

from __future__ import annotations

from roost._core.cron import CronEntry
from roost._core.job import Job
from roost._core.repo import JobInsert
from roost._core.retry import (
    BackoffStrategy,
    exponential,
    fixed,
    linear,
)
from roost._core.states import JobState
from roost._core.wait import JobFailed, JobOutcome, JobTimeoutError
from roost.async_api import AsyncRoost
from roost.decorators import HandlerRegistry, TaskDefaults, cron, job
from roost.exceptions import (
    DuplicateUniqueJobError,
    JobNotFoundError,
    RoostError,
    SnoozeJob,
    UnknownTaskError,
    WorkerShutdown,
)
from roost.sync_api import Roost
from roost.worker import Worker

__version__ = "0.1.0.dev0"

__all__ = [
    "AsyncRoost",
    "BackoffStrategy",
    "CronEntry",
    "DuplicateUniqueJobError",
    "HandlerRegistry",
    "Job",
    "JobFailed",
    "JobInsert",
    "JobNotFoundError",
    "JobOutcome",
    "JobState",
    "JobTimeoutError",
    "Roost",
    "RoostError",
    "SnoozeJob",
    "TaskDefaults",
    "UnknownTaskError",
    "Worker",
    "WorkerShutdown",
    "__version__",
    "cron",
    "exponential",
    "fixed",
    "job",
    "linear",
]
