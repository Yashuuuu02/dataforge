"""Jobs API routes — create, list, get status, get results."""

import asyncio
import json
import logging
from datetime import datetime, timezone
from uuid import UUID

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.core.config import settings
from app.core.database import get_db
from app.core.minio_client import get_presigned_url
from app.models.job import Job, JobMode, JobStatus
from app.models.user import User
from app.schemas.job import JobCreate, JobList, JobResponse, JobResultResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/jobs", tags=["Jobs"])


@router.get("/", response_model=JobList)
async def list_jobs(
    skip: int = 0,
    limit: int = 50,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> JobList:
    """List all jobs for the current user."""
    query = (
        select(Job)
        .where(Job.user_id == current_user.id)
        .order_by(Job.started_at.desc().nullslast())
        .offset(skip)
        .limit(limit)
    )
    result = await db.execute(query)
    jobs = result.scalars().all()

    count_query = select(func.count()).select_from(Job).where(Job.user_id == current_user.id)
    total = (await db.execute(count_query)).scalar() or 0

    return JobList(jobs=[JobResponse.model_validate(j) for j in jobs], total=total)


@router.post("/", response_model=JobResponse, status_code=status.HTTP_201_CREATED)
async def create_job(
    payload: JobCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> JobResponse:
    """Create a new processing job and dispatch the pipeline."""
    # Convert steps to serializable list
    steps_list = [{"step": s.step, "config": s.config} for s in payload.steps]

    job = Job(
        dataset_id=payload.dataset_id,
        user_id=current_user.id,
        mode=payload.mode,
        config=payload.config or {},
        workflow_steps=steps_list,
        status=JobStatus.QUEUED,
        started_at=datetime.now(timezone.utc),
    )
    db.add(job)
    await db.flush()
    await db.refresh(job)

    # Dispatch Celery task
    if payload.mode == JobMode.COMMON:
        from pipeline.tasks.pipeline import run_common_pipeline
        task = run_common_pipeline.delay(str(job.id))
        job.celery_task_id = task.id
        await db.flush()

    return JobResponse.model_validate(job)


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> JobResponse:
    """Get a single job by ID."""
    result = await db.execute(
        select(Job).where(Job.id == job_id, Job.user_id == current_user.id)
    )
    job = result.scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return JobResponse.model_validate(job)


@router.get("/{job_id}/result", response_model=JobResultResponse)
async def get_job_result(
    job_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> JobResultResponse:
    """Get the result of a completed job with download URL."""
    result = await db.execute(
        select(Job).where(Job.id == job_id, Job.user_id == current_user.id)
    )
    job = result.scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    pipeline_result = (job.config or {}).get("pipeline_result", {})
    output_path = (job.config or {}).get("output_path")

    download_url = None
    if output_path and job.status == JobStatus.COMPLETED:
        try:
            download_url = get_presigned_url("dataforge-processed", output_path)
        except Exception:
            pass

    return JobResultResponse(
        job_id=job.id,
        status=job.status,
        total_rows_before=pipeline_result.get("total_rows_before", 0),
        total_rows_after=pipeline_result.get("total_rows_after", 0),
        total_rows_removed=pipeline_result.get("total_rows_removed", 0),
        duration_seconds=pipeline_result.get("duration_seconds", 0.0),
        steps=pipeline_result.get("steps", []),
        warnings=pipeline_result.get("warnings", []),
        download_url=download_url,
    )

@router.get("/{job_id}/insight")
async def get_job_insight(
    job_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get AI or heuristic insight report for a completed job."""
    result = await db.execute(
        select(Job).where(Job.id == job_id, Job.user_id == current_user.id)
    )
    job = result.scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status != JobStatus.COMPLETED:
        raise HTTPException(status_code=400, detail="Job is not completed yet")

    # If already stored on job config, return it
    job_config = job.config or {}
    if "insight_report" in job_config:
        return job_config["insight_report"]

    from ai.insight_reporter import InsightReporter
    from app.api.agent import get_llm_client
    
    # Needs to be generated
    pipeline_result = job_config.get("pipeline_result", {})
    # In a real app we'd load the dataset analysis here, mock it for now
    analysis_mock = {"row_count": pipeline_result.get("total_rows_before", 0)}

    llm = get_llm_client(current_user)
    reporter = InsightReporter(llm)
    
    report = await reporter.generate(pipeline_result, analysis_mock, job.mode.value)
    
    # Cache it on the job
    from dataclasses import asdict
    report_dict = asdict(report)
    job_config["insight_report"] = report_dict
    
    # Force mutation update so JSONB saves
    from sqlalchemy.orm.attributes import flag_modified
    job.config = job_config
    await db.commit()
    return report_dict

@router.post("/finetune", response_model=JobResponse)
async def create_finetune_job(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Dispatch a fine-tune job from the frontend wizard."""
    # Using raw request to avoid needing a strict schema for FinetuneConfig right now
    from pydantic import ValidationError
    data = await request.json()
    dataset_id = data.get("dataset_id")
    config = data.get("config", {})

    job = Job(
        dataset_id=dataset_id,
        user_id=current_user.id,
        mode=JobMode.FINETUNE,
        config={"finetune_config": config},
        workflow_steps=[], # Abstracted in config for finetune
        status=JobStatus.QUEUED,
        started_at=datetime.now(timezone.utc)
    )
    db.add(job)
    await db.flush()
    await db.refresh(job)

    from pipeline.tasks.finetune import run_finetune_pipeline
    task = run_finetune_pipeline.delay(str(job.id))
    job.celery_task_id = task.id
    
    await db.commit()
    return JobResponse.model_validate(job)

@router.get("/{job_id}/finetune-result")
async def get_finetune_result(
    job_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Retrieve fine-tune stats and pre-signed minio URLs."""
    result = await db.execute(select(Job).where(Job.id == job_id, Job.user_id == current_user.id))
    job = result.scalar_one_or_none()
    if not job: raise HTTPException(status_code=404)
    if job.status != JobStatus.COMPLETED: raise HTTPException(status_code=400)

    cfg = job.config or {}
    keys = cfg.get("minio_keys", {})
    stats = cfg.get("pipeline_result", {})
    
    urls = {}
    from app.core.minio_client import get_presigned_url
    if keys.get("train"): urls["train_url"] = get_presigned_url("dataforge-processed", keys["train"])
    if keys.get("val"): urls["val_url"] = get_presigned_url("dataforge-processed", keys["val"])
    if keys.get("config"): urls["config_url"] = get_presigned_url("dataforge-processed", keys["config"])
    
    return {"stats": stats, "urls": urls}

