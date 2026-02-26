"""Dataset Pydantic schemas — extended for Phase 2 ingestion."""

from datetime import datetime
from uuid import UUID
from typing import Any, Optional

from pydantic import BaseModel

from app.models.dataset import DatasetStatus


class DatasetCreate(BaseModel):
    name: str
    source_type: Optional[str] = None


class DatasetResponse(BaseModel):
    id: UUID
    user_id: UUID
    name: str
    source_type: Optional[str] = None
    raw_file_path: Optional[str] = None
    size_bytes: Optional[int] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    detected_format: Optional[str] = None
    status: DatasetStatus
    created_at: datetime
    error_message: Optional[str] = None

    model_config = {"from_attributes": True}


class DatasetList(BaseModel):
    datasets: list[DatasetResponse]
    total: int


# --- Upload responses ---

class DatasetUploadResponse(BaseModel):
    dataset_id: UUID
    name: str
    size_bytes: int
    detected_format: str
    status: DatasetStatus


class ChunkUploadRequest(BaseModel):
    upload_id: str
    chunk_index: int
    total_chunks: int


class ChunkUploadResponse(BaseModel):
    upload_id: str
    chunk_index: int
    total_chunks: int
    progress: float
    status: str  # "uploading" | "assembling" | "complete"
    dataset_id: Optional[UUID] = None


# --- Preview & Stats ---

class ColumnStats(BaseModel):
    name: str
    dtype: str
    null_count: int
    null_percentage: float
    unique_count: int
    sample_values: list[Any]
    # Numeric fields (optional)
    min: Optional[float] = None
    max: Optional[float] = None
    mean: Optional[float] = None
    std: Optional[float] = None


class DatasetPreview(BaseModel):
    dataset_id: UUID
    columns: list[str]
    dtypes: dict[str, str]
    row_count: int
    rows: list[dict[str, Any]]


class DatasetStatsResponse(BaseModel):
    dataset_id: UUID
    row_count: int
    column_count: int
    size_bytes: int
    estimated_tokens: Optional[int] = None
    detected_language: Optional[str] = None
    columns: list[ColumnStats]


# --- Connector requests ---

class S3ListRequest(BaseModel):
    bucket: str
    prefix: str = ""
    access_key: str
    secret_key: str
    region: str = "us-east-1"


class S3ImportRequest(BaseModel):
    bucket: str
    key: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"
    dataset_name: str


class S3FileInfo(BaseModel):
    key: str
    size: int
    last_modified: Optional[str] = None


class UrlScrapeRequest(BaseModel):
    urls: list[str]
    scrape_mode: str = "auto"  # "download" | "scrape" | "auto"
    dataset_name: Optional[str] = None


class HuggingFaceImportRequest(BaseModel):
    dataset_id: str  # e.g. "tatsu-lab/alpaca"
    config: Optional[str] = None
    split: Optional[str] = "train"
    hf_token: Optional[str] = None
    dataset_name: Optional[str] = None


class GDriveAuthResponse(BaseModel):
    auth_url: str


class GDriveImportRequest(BaseModel):
    auth_code: str
    file_id: str
    dataset_name: str
