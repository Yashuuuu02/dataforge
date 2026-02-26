"""HuggingFace connector — download datasets from the HuggingFace Hub."""

import logging
import os
import tempfile
from io import BytesIO
from typing import Optional

import pandas as pd

from app.core.minio_client import upload_file as minio_upload

logger = logging.getLogger(__name__)


class HuggingFaceConnector:
    """Download HuggingFace datasets and store in MinIO."""

    def __init__(self, hf_token: Optional[str] = None):
        self.hf_token = hf_token

    def download_dataset(
        self,
        dataset_id: str,
        config: Optional[str] = None,
        split: str = "train",
        minio_bucket: str = "dataforge-raw",
    ) -> dict:
        """Download a HuggingFace dataset and store as Parquet in MinIO.

        Args:
            dataset_id: HuggingFace dataset ID (e.g. "tatsu-lab/alpaca").
            config: Optional dataset config/subset.
            split: Dataset split (default: "train").
            minio_bucket: Target MinIO bucket.

        Returns: {minio_key, row_count, columns, size_bytes}
        """
        try:
            from datasets import load_dataset
        except ImportError:
            raise RuntimeError("huggingface 'datasets' library not installed")

        logger.info("Downloading HF dataset: %s (config=%s, split=%s)", dataset_id, config, split)

        kwargs: dict = {"split": split}
        if config:
            kwargs["name"] = config
        if self.hf_token:
            kwargs["token"] = self.hf_token

        ds = load_dataset(dataset_id, **kwargs)
        df = ds.to_pandas()

        logger.info("HF dataset loaded: %d rows, %d columns", len(df), len(df.columns))

        # Save as parquet to a temp file, then upload to MinIO
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
            df.to_parquet(tmp_path, index=False)

        try:
            size = os.path.getsize(tmp_path)
            safe_name = dataset_id.replace("/", "_")
            minio_key = f"hf-import/{safe_name}_{split}.parquet"

            with open(tmp_path, "rb") as f:
                minio_upload(minio_bucket, minio_key, f, length=size, content_type="application/parquet")
        finally:
            os.unlink(tmp_path)

        return {
            "minio_key": minio_key,
            "row_count": len(df),
            "columns": list(df.columns),
            "size_bytes": size,
        }
