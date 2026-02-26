"""Celery task for fine-tune pipelines."""

import asyncio
import json
import logging
from uuid import UUID

from pipeline.workers.celery_app import celery_app
from app.core.database import SessionLocal
from app.models.job import Job, JobStatus
from app.core.minio_client import get_file_stream, upload_file
from ai.insight_reporter import InsightReporter

from pipeline.common.io import load_dataframe
from pipeline.modes.finetune.runner import FinetunePipelineRunner, FinetuneConfig

logger = logging.getLogger(__name__)

def sync_update_job(job_id: str, updates: dict):
    with SessionLocal() as db:
        job = db.query(Job).filter(Job.id == UUID(job_id)).first()
        if job:
            for k, v in updates.items():
                if k == "config":
                    # merge config
                    curr = job.config or {}
                    curr.update(v)
                    job.config = curr
                    from sqlalchemy.orm.attributes import flag_modified
                    flag_modified(job, "config")
                else:
                    setattr(job, k, v)
            db.commit()

@celery_app.task(bind=True, name="run_finetune_pipeline")
def run_finetune_pipeline(self, job_id: str) -> dict:
    logger.info(f"Starting finetune job {job_id}")
    sync_update_job(job_id, {"status": JobStatus.PROCESSING, "progress": 0})
    
    import redis
    from app.core.config import settings
    r = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    
    def report_progress(prog: int, msg: str):
        self.update_state(state="PROGRESS", meta={"progress": prog, "message": msg})
        sync_update_job(job_id, {"progress": prog})
        payload = json.dumps({"job_id": job_id, "progress": prog, "message": msg, "status": "processing"})
        r.publish(f"job:{job_id}", payload)

    try:
        # Load Job
        with SessionLocal() as db:
            job = db.query(Job).filter(Job.id == UUID(job_id)).first()
            if not job: raise ValueError("Job not found")
            dataset_id = str(job.dataset_id)
            user_id = str(job.user_id)
            raw_config = job.config or {}
        
        # Load Dataset
        report_progress(5, "Loading dataset from storage...")
        stream = get_file_stream("dataforge-raw", f"{user_id}/{dataset_id}")
        df = load_dataframe(stream, "dataset.csv") # Simplified, real app passes actual filename/format
        
        # Parse Config
        fc = FinetuneConfig(**raw_config.get("finetune_config", {}))
        
        # Run Pipeline
        runner = FinetunePipelineRunner()
        result = runner.run(df, fc, job_id, report_progress)
        
        # Upload results to MinIO
        report_progress(95, "Uploading results to storage...")
        
        train_key = f"{user_id}/{dataset_id}/finetune/{job_id}/train.jsonl"
        val_key = f"{user_id}/{dataset_id}/finetune/{job_id}/val.jsonl"
        cfg_key = f"{user_id}/{dataset_id}/finetune/{job_id}/training_config.json"
        
        train_path = result.output_files["train"]
        val_path = result.output_files.get("val")
        cfg_path = result.output_files["config"]
        
        upload_file("dataforge-processed", train_key, open(train_path, "rb"), len(train_df := result.train_df) if train_path else 0)
        if val_path: upload_file("dataforge-processed", val_key, open(val_path, "rb"), len(result.val_df))
        if cfg_path: upload_file("dataforge-processed", cfg_key, open(cfg_path, "rb"), 1000)
        
        # Generate insight synchronously mapped async
        loop = asyncio.get_event_loop()
        reporter = InsightReporter(None) # Heuristic only for background generic
        insight = loop.run_until_complete(reporter.generate(
            {"total_rows_before": len(df), "total_rows_after": result.total_examples, "total_rows_removed": len(df)-result.total_examples, "duration_seconds": 30},
            {"row_count": len(df)},
            "finetune"
        ))
        
        from dataclasses import asdict
        
        final_meta = {
             "pipeline_result": {
                 "train_examples": result.train_examples,
                 "val_examples": result.val_examples,
                 "avg_tokens": result.avg_tokens,
                 "estimated_training_time": result.estimated_training_time,
                 "output_format": result.output_format,
             },
             "insight_report": asdict(insight),
             "minio_keys": {
                 "train": train_key,
                 "val": val_key if val_path else None,
                 "config": cfg_key
             }
        }
        
        sync_update_job(job_id, {"status": JobStatus.COMPLETED, "progress": 100, "config": final_meta})
        
        r.publish(f"job:{job_id}", json.dumps({
            "job_id": job_id, "progress": 100, "message": "Finetuning complete!", "status": "completed"
        }))
        
        return {"job_id": job_id, "status": "completed"}
        
    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        sync_update_job(job_id, {"status": JobStatus.FAILED, "error_message": str(e)})
        r.publish(f"job:{job_id}", json.dumps({"job_id": job_id, "status": "failed", "error": str(e)}))
        raise e
