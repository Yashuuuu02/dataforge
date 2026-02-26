"""Jobs API routes."""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.core.database import get_db
from app.models.job import Job
from app.models.user import User
from app.schemas.job import JobCreate, JobList, JobResponse

router = APIRouter(prefix="/jobs", tags=["Jobs"])


@router.get("/", response_model=JobList)
async def list_jobs(
    skip: int = 0,
    limit: int = 50,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> JobList:
    """List all jobs for the current user."""
    query = select(Job).where(Job.user_id == current_user.id).offset(skip).limit(limit)
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
    """Create a new processing job."""
    job = Job(
        dataset_id=payload.dataset_id,
        user_id=current_user.id,
        mode=payload.mode,
        config=payload.config or {},
        workflow_steps=payload.workflow_steps or [],
    )
    db.add(job)
    await db.flush()
    await db.refresh(job)

    # Dispatch Celery task (placeholder — worker integration in later phase)
    # from pipeline.workers.celery_app import run_job
    # task = run_job.delay(str(job.id))
    # job.celery_task_id = task.id
    # await db.flush()

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
