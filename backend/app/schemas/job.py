"""Job Pydantic schemas."""

from datetime import datetime
from uuid import UUID
from typing import Any, Optional

from pydantic import BaseModel

from app.models.job import JobMode, JobStatus


class PipelineStepConfig(BaseModel):
    step: str
    config: dict[str, Any] = {}


class JobCreate(BaseModel):
    dataset_id: UUID
    mode: JobMode = JobMode.COMMON
    steps: list[PipelineStepConfig] = []
    config: Optional[dict] = None


class JobResponse(BaseModel):
    id: UUID
    dataset_id: UUID
    user_id: UUID
    mode: JobMode
    config: Optional[dict] = None
    workflow_steps: Optional[list] = None
    status: JobStatus
    progress: int
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    celery_task_id: Optional[str] = None

    model_config = {"from_attributes": True}


class JobResultResponse(BaseModel):
    job_id: UUID
    status: JobStatus
    total_rows_before: int = 0
    total_rows_after: int = 0
    total_rows_removed: int = 0
    duration_seconds: float = 0.0
    steps: list[dict] = []
    warnings: list[str] = []
    download_url: Optional[str] = None


class JobList(BaseModel):
    jobs: list[JobResponse]
    total: int
