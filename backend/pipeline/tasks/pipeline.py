"""Celery task for running the common pipeline on a dataset."""

import json
import logging
import os
import tempfile
from typing import Optional

import redis

from pipeline.workers.celery_app import celery_app
from app.core.config import settings

logger = logging.getLogger(__name__)

_redis: Optional[redis.Redis] = None


def get_redis() -> redis.Redis:
    global _redis
    if _redis is None:
        _redis = redis.from_url(settings.REDIS_URL)
    return _redis


def publish_job_progress(job_id: str, progress: int, step: str, message: str, status: str = "running", step_result: Optional[dict] = None) -> None:
    """Publish job progress to Redis for WebSocket delivery."""
    r = get_redis()
    payload = {
        "job_id": job_id,
        "progress": progress,
        "step": step,
        "message": message,
        "status": status,
    }
    if step_result:
        payload["step_result"] = step_result
    r.publish(f"job:{job_id}", json.dumps(payload))


@celery_app.task(name="run_common_pipeline", bind=True)
def run_common_pipeline(self, job_id: str) -> dict:
    """Run the common processing pipeline on a dataset.

    Steps:
    1. Load job from DB
    2. Load dataset from MinIO
    3. Parse workflow_steps from job config
    4. Run PipelineRunner with progress callback
    5. Save processed DataFrame to MinIO (dataforge-processed)
    6. Update job status and stats in DB
    7. Create ProcessedDataset record
    8. Push completion event
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from app.models.job import Job, JobStatus, ProcessedDataset
    from app.models.dataset import Dataset

    engine = create_engine(settings.DATABASE_URL_SYNC)

    try:
        publish_job_progress(job_id, 2, "init", "Loading job...")

        # 1. Load job
        with Session(engine) as session:
            job = session.get(Job, job_id)
            if not job:
                return {"error": "Job not found"}

            job.status = JobStatus.RUNNING
            job.progress = 5
            session.commit()

            dataset_id = str(job.dataset_id)
            steps_config = job.workflow_steps or []
            session.expunge(job)

        # 2. Load dataset
        with Session(engine) as session:
            dataset = session.get(Dataset, dataset_id)
            if not dataset or not dataset.raw_file_path:
                _fail_job(engine, job_id, "Dataset not found or has no file")
                publish_job_progress(job_id, 0, "error", "Dataset not found", status="failed")
                return {"error": "Dataset not found"}

            raw_path = dataset.raw_file_path
            detected_format = dataset.detected_format or "csv"
            session.expunge(dataset)

        publish_job_progress(job_id, 10, "loading", "Downloading dataset from storage...")

        # 3. Download from MinIO
        from app.core.minio_client import download_file, upload_file as minio_upload
        try:
            file_data = download_file("dataforge-raw", raw_path)
        except Exception as exc:
            _fail_job(engine, job_id, f"Storage error: {exc}")
            publish_job_progress(job_id, 0, "error", str(exc), status="failed")
            return {"error": str(exc)}

        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{detected_format}") as tmp:
            tmp.write(file_data.read())
            tmp_path = tmp.name

        try:
            publish_job_progress(job_id, 15, "parsing", "Parsing dataset...")

            from pipeline.ingestion.file_handler import FileHandler
            df = FileHandler.parse(tmp_path, detected_format)

            if df.empty:
                _fail_job(engine, job_id, "Dataset could not be parsed")
                publish_job_progress(job_id, 0, "error", "Parse failed", status="failed")
                return {"error": "Parse failed"}

            # 4. Run pipeline
            publish_job_progress(job_id, 20, "pipeline", "Starting pipeline...")

            from pipeline.common.runner import PipelineRunner

            def progress_cb(progress: int, step: str, message: str) -> None:
                # Scale progress to 20-90 range
                scaled = 20 + int(progress * 0.7)
                publish_job_progress(job_id, scaled, step, message)
                # Update job progress in DB
                with Session(engine) as session:
                    j = session.get(Job, job_id)
                    if j:
                        j.progress = scaled
                        session.commit()

            runner = PipelineRunner()
            result = runner.run(df, steps_config, job_id=job_id, progress_callback=progress_cb)

            publish_job_progress(job_id, 92, "saving", "Saving processed dataset...")

            # 5. Save to MinIO
            processed_path = f"processed/{job_id}/output.parquet"
            with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as out_tmp:
                result.df.to_parquet(out_tmp.name, index=False)
                out_size = os.path.getsize(out_tmp.name)
                with open(out_tmp.name, "rb") as f:
                    minio_upload("dataforge-processed", processed_path, f, length=out_size)
                os.unlink(out_tmp.name)

            # 6. Update job
            steps_summary = []
            for i, sr in enumerate(result.steps_results):
                step_name = steps_config[i]["step"] if i < len(steps_config) else "unknown"
                steps_summary.append({
                    "step": step_name,
                    "rows_before": sr.rows_before,
                    "rows_after": sr.rows_after,
                    "rows_removed": sr.rows_removed,
                    "metadata": sr.metadata,
                    "warnings": sr.warnings,
                })

            with Session(engine) as session:
                j = session.get(Job, job_id)
                if j:
                    j.status = JobStatus.COMPLETED
                    j.progress = 100
                    j.config = {
                        **(j.config or {}),
                        "pipeline_result": {
                            "total_rows_before": result.total_rows_before,
                            "total_rows_after": result.total_rows_after,
                            "total_rows_removed": result.total_rows_removed,
                            "duration_seconds": result.duration_seconds,
                            "steps": steps_summary,
                            "warnings": result.warnings,
                        },
                        "output_path": processed_path,
                    }
                    session.commit()

            # 7. Create ProcessedDataset
            with Session(engine) as session:
                processed = ProcessedDataset(
                    job_id=job_id,
                    output_path=processed_path,
                    row_count=result.total_rows_after,
                    quality_score_avg=result.pipeline_stats.get("mean_quality_score"),
                )
                session.add(processed)
                session.commit()

            publish_job_progress(job_id, 100, "complete",
                                f"Pipeline complete: {result.total_rows_before} → {result.total_rows_after} rows ({result.duration_seconds}s)",
                                status="completed")

            return {
                "job_id": job_id,
                "status": "completed",
                "rows_before": result.total_rows_before,
                "rows_after": result.total_rows_after,
                "duration": result.duration_seconds,
            }

        finally:
            os.unlink(tmp_path)

    except Exception as exc:
        logger.exception("Pipeline failed for job %s: %s", job_id, exc)
        _fail_job(engine, job_id, str(exc))
        publish_job_progress(job_id, 0, "error", str(exc), status="failed")
        return {"error": str(exc)}


def _fail_job(engine, job_id: str, error_msg: str) -> None:
    from sqlalchemy.orm import Session
    from app.models.job import Job, JobStatus

    with Session(engine) as session:
        job = session.get(Job, job_id)
        if job:
            job.status = JobStatus.FAILED
            job.error_message = error_msg if hasattr(job, "error_message") else None
            session.commit()
