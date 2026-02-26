"""Job Pydantic schemas."""

from datetime import datetime
from uuid import UUID
from typing import Optional

from pydantic import BaseModel

from app.models.job import JobMode, JobStatus


class JobCreate(BaseModel):
    dataset_id: UUID
    mode: JobMode
    config: Optional[dict] = None
    workflow_steps: Optional[list] = None


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


class JobList(BaseModel):
    jobs: list[JobResponse]
    total: int
