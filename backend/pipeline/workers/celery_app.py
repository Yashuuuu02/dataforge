"""Celery application and task definitions."""

import logging
import time

from celery import Celery

from app.core.config import settings

logger = logging.getLogger(__name__)

celery_app = Celery(
    "dataforge",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
)

# Auto-discover tasks in pipeline/tasks/
celery_app.autodiscover_tasks(["pipeline.tasks"])


@celery_app.task(name="test_task")
def test_task() -> str:
    """Simple test task to verify worker connectivity."""
    logger.info("Test task executed successfully")
    return "worker is alive"


@celery_app.task(name="run_job", bind=True)
def run_job(self, job_id: str) -> dict:
    """Placeholder job processing task.

    In future phases, this will:
    1. Load job config from DB
    2. Execute pipeline steps
    3. Update progress
    4. Save results
    """
    logger.info("Starting job: %s", job_id)

    # Simulate processing
    for progress in range(0, 101, 10):
        self.update_state(state="PROGRESS", meta={"progress": progress, "job_id": job_id})
        time.sleep(0.5)

    logger.info("Completed job: %s", job_id)
    return {"job_id": job_id, "status": "completed", "progress": 100}
