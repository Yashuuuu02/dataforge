"""Celery task for background ingestion processing."""

import json
import logging
import os
import tempfile
from typing import Optional

import redis

from pipeline.workers.celery_app import celery_app
from app.core.config import settings

logger = logging.getLogger(__name__)

# Redis client for progress publishing
_redis: Optional[redis.Redis] = None


def get_redis() -> redis.Redis:
    global _redis
    if _redis is None:
        _redis = redis.from_url(settings.REDIS_URL)
    return _redis


def publish_progress(dataset_id: str, progress: int, step: str, message: str, status: str = "processing") -> None:
    """Publish progress update to Redis for WebSocket delivery."""
    r = get_redis()
    payload = json.dumps({
        "dataset_id": dataset_id,
        "progress": progress,
        "step": step,
        "message": message,
        "status": status,
    })
    r.publish(f"ingestion:{dataset_id}", payload)


@celery_app.task(name="process_ingestion", bind=True)
def process_ingestion(self, dataset_id: str) -> dict:
    """Process an ingested file: parse, compute stats, update dataset record.

    Steps:
    1. Download raw file from MinIO to temp dir
    2. Detect format and parse with FileHandler
    3. Compute statistics (row/column counts, per-column stats, token estimate)
    4. Update Dataset record with results
    5. Clean up temp file
    """
    # Synchronous DB access for Celery worker
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from app.models.dataset import Dataset, DatasetStatus

    engine = create_engine(settings.DATABASE_URL_SYNC)

    try:
        publish_progress(dataset_id, 5, "starting", "Loading dataset record...")

        # 1. Load dataset record
        with Session(engine) as session:
            dataset = session.get(Dataset, dataset_id)
            if dataset is None:
                logger.error("Dataset not found: %s", dataset_id)
                return {"error": "Dataset not found"}

            dataset.status = DatasetStatus.PROCESSING
            session.commit()

            raw_file_path = dataset.raw_file_path
            detected_format = dataset.detected_format
            dataset_name = dataset.name

        if not raw_file_path:
            _fail_dataset(engine, dataset_id, "No raw file path")
            return {"error": "No raw file path"}

        publish_progress(dataset_id, 15, "downloading", "Downloading raw file from storage...")

        # 2. Download from MinIO to temp file
        from app.core.minio_client import download_file
        try:
            file_data = download_file("dataforge-raw", raw_file_path)
        except Exception as exc:
            _fail_dataset(engine, dataset_id, f"Storage error: {exc}")
            publish_progress(dataset_id, 0, "error", str(exc), status="failed")
            return {"error": str(exc)}

        # Write to temp file for parsing
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{detected_format}") as tmp:
            tmp.write(file_data.read())
            tmp_path = tmp.name

        try:
            publish_progress(dataset_id, 30, "parsing", f"Parsing {detected_format} file...")

            # 3. Parse with FileHandler
            from pipeline.ingestion.file_handler import FileHandler
            from pipeline.ingestion.validators import detect_format

            if not detected_format:
                detected_format = detect_format(tmp_path, dataset_name)

            df = FileHandler.parse(tmp_path, detected_format)

            if df.empty:
                _fail_dataset(engine, dataset_id, "File could not be parsed or is empty")
                publish_progress(dataset_id, 0, "error", "Parse failed", status="failed")
                return {"error": "Parse failed"}

            publish_progress(dataset_id, 60, "stats", "Computing statistics...")

            # 4. Compute statistics
            stats = _compute_stats(df)

            publish_progress(dataset_id, 85, "tokens", "Estimating token count...")

            # Estimate tokens
            estimated_tokens = _estimate_tokens(df)
            stats["estimated_tokens"] = estimated_tokens

            # Detect language
            detected_language = _detect_language(df)
            if detected_language:
                stats["detected_language"] = detected_language

            publish_progress(dataset_id, 95, "saving", "Saving results...")

            # 5. Update dataset record
            with Session(engine) as session:
                dataset = session.get(Dataset, dataset_id)
                if dataset:
                    dataset.status = DatasetStatus.READY
                    dataset.row_count = len(df)
                    dataset.column_count = len(df.columns)
                    dataset.detected_format = detected_format
                    dataset.stats = stats
                    dataset.error_message = None
                    session.commit()

            publish_progress(dataset_id, 100, "complete", "Ingestion complete", status="ready")

            logger.info("Ingestion complete for %s: %d rows, %d cols", dataset_id, len(df), len(df.columns))
            return {
                "dataset_id": dataset_id,
                "status": "ready",
                "row_count": len(df),
                "column_count": len(df.columns),
                "estimated_tokens": estimated_tokens,
            }

        finally:
            os.unlink(tmp_path)

    except Exception as exc:
        logger.exception("Ingestion failed for %s: %s", dataset_id, exc)
        _fail_dataset(engine, dataset_id, str(exc))
        publish_progress(dataset_id, 0, "error", str(exc), status="failed")
        return {"error": str(exc)}


def _fail_dataset(engine, dataset_id: str, error_msg: str) -> None:
    """Mark a dataset as failed."""
    from sqlalchemy.orm import Session
    from app.models.dataset import Dataset, DatasetStatus

    with Session(engine) as session:
        dataset = session.get(Dataset, dataset_id)
        if dataset:
            dataset.status = DatasetStatus.FAILED
            dataset.error_message = error_msg
            session.commit()


def _compute_stats(df) -> dict:
    """Compute per-column statistics."""
    import pandas as pd
    import numpy as np

    columns: list[dict] = []
    for col in df.columns:
        col_data = df[col]
        dtype_str = str(col_data.dtype)
        null_count = int(col_data.isnull().sum())
        total = len(col_data)

        stat: dict = {
            "name": str(col),
            "dtype": dtype_str,
            "null_count": null_count,
            "null_percentage": round(null_count / total * 100, 2) if total > 0 else 0,
            "unique_count": int(col_data.nunique()),
            "sample_values": [_safe_json(v) for v in col_data.dropna().head(5).tolist()],
        }

        # Numeric column stats
        if pd.api.types.is_numeric_dtype(col_data):
            stat["min"] = _safe_json(col_data.min())
            stat["max"] = _safe_json(col_data.max())
            stat["mean"] = _safe_json(col_data.mean())
            stat["std"] = _safe_json(col_data.std())

        columns.append(stat)

    return {
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": columns,
    }


def _estimate_tokens(df) -> int:
    """Estimate token count for the entire dataframe."""
    try:
        import tiktoken
        enc = tiktoken.get_encoding("cl100k_base")
        total = 0
        # Sample up to 1000 rows for estimation
        sample = df.head(1000)
        for _, row in sample.iterrows():
            text = " ".join(str(v) for v in row.values if v is not None)
            total += len(enc.encode(text))
        # Extrapolate
        if len(sample) < len(df):
            total = int(total * len(df) / len(sample))
        return total
    except Exception as exc:
        logger.warning("Token estimation failed: %s", exc)
        # Rough fallback: ~0.75 tokens per character
        total_chars = sum(df.astype(str).apply(lambda x: x.str.len().sum()))
        return int(total_chars * 0.75)


def _detect_language(df) -> Optional[str]:
    """Detect primary language from text columns."""
    try:
        from langdetect import detect
        text_cols = df.select_dtypes(include=["object"]).columns
        if len(text_cols) == 0:
            return None
        # Use first text column, sample up to 20 rows
        sample_text = " ".join(df[text_cols[0]].dropna().head(20).astype(str).tolist())
        if len(sample_text) < 20:
            return None
        return detect(sample_text)
    except Exception:
        return None


def _safe_json(value) -> any:
    """Convert numpy/pandas types to JSON-serializable Python types."""
    import numpy as np
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        v = float(value)
        if np.isnan(v) or np.isinf(v):
            return None
        return v
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, (np.ndarray,)):
        return value.tolist()
    return value
