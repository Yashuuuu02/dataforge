"""Workflow Pydantic schemas."""

from datetime import datetime
from uuid import UUID
from typing import Optional

from pydantic import BaseModel


class WorkflowCreate(BaseModel):
    name: str
    description: Optional[str] = None
    steps: Optional[list] = None
    is_public: bool = False


class WorkflowUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    steps: Optional[list] = None
    is_public: Optional[bool] = None


class WorkflowResponse(BaseModel):
    id: UUID
    user_id: UUID
    name: str
    description: Optional[str] = None
    steps: Optional[list] = None
    is_public: bool
    use_count: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class WorkflowList(BaseModel):
    workflows: list[WorkflowResponse]
    total: int
