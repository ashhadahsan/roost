from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class Job(BaseModel):
    """A row from ``roost.jobs``.

    Public read-only view; created internally by the repo. Users construct
    their own typed argument models and pass them through ``args``.
    """

    model_config = ConfigDict(frozen=True)

    id: int
    queue: str
    task: str
    args: dict[str, Any] = Field(default_factory=dict)
    state: str
    priority: int = 0
    attempt: int = 0
    max_attempts: int = 20
    scheduled_at: datetime
    attempted_at: datetime | None = None
    completed_at: datetime | None = None
    cancelled_at: datetime | None = None
    discarded_at: datetime | None = None
    errors: list[dict[str, Any]] = Field(default_factory=list)
    unique_key: str | None = None
    inserted_at: datetime
    tags: list[str] = Field(default_factory=list)
    timeout_seconds: int | None = None
    cancel_requested: bool = False
    result: Any | None = None
    depends_on: list[int] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
