"""Ingestion API routes — file uploads, preview, stats, and external connectors."""

import logging
import os
import tempfile
import uuid as uuid_mod
from io import BytesIO
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.core.database import get_db
from app.core.errors import (
    IngestionError,
    raise_dataset_not_found,
    raise_storage_error,
    raise_unsupported_format,
    raise_parse_error,
    SUPPORTED_FORMATS,
)
from app.core.minio_client import get_minio_client, upload_file as minio_upload, download_file as minio_download
from app.models.dataset import Dataset, DatasetStatus
from app.models.user import User
from app.schemas.dataset import (
    DatasetUploadResponse,
    ChunkUploadRequest,
    ChunkUploadResponse,
    DatasetPreview,
    DatasetStatsResponse,
    ColumnStats,
    S3ListRequest,
    S3ImportRequest,
    S3FileInfo,
    UrlScrapeRequest,
    HuggingFaceImportRequest,
    GDriveAuthResponse,
    GDriveImportRequest,
)
from pipeline.ingestion.validators import detect_format, validate_file, SUPPORTED_FORMATS as VALID_FORMATS

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ingestion", tags=["Ingestion"])

# Max streaming chunk size (1MB per read)
STREAM_CHUNK_SIZE = 1024 * 1024


# ────────────────────────────────────────────────────────
# File Upload
# ────────────────────────────────────────────────────────

@router.post("/upload", response_model=DatasetUploadResponse, status_code=status.HTTP_201_CREATED)
async def upload_file(
    file: UploadFile = File(...),
    dataset_name: str = Form(default=""),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> DatasetUploadResponse:
    """Upload a file for ingestion. Streams to MinIO without loading into memory."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")

    filename = file.filename
    name = dataset_name or os.path.splitext(filename)[0]

    # Validate format
    ext = os.path.splitext(filename)[1].lower().lstrip(".")
    # Write to temp file for validation
    with tempfile.NamedTemporaryFile(delete=False, suffix=f".{ext}") as tmp:
        tmp_path = tmp.name
        size = 0
        while True:
            chunk = await file.read(STREAM_CHUNK_SIZE)
            if not chunk:
                break
            tmp.write(chunk)
            size += len(chunk)

    try:
        validation = validate_file(tmp_path, filename)
        if not validation["is_valid"]:
            raise_unsupported_format(ext)

        detected_format = validation["format"]

        # Upload to MinIO (stream from temp file)
        minio_key = f"uploads/{uuid_mod.uuid4()}/{filename}"
        with open(tmp_path, "rb") as f:
            minio_upload("dataforge-raw", minio_key, f, length=size)

    finally:
        os.unlink(tmp_path)

    # Create dataset record
    dataset = Dataset(
        user_id=current_user.id,
        name=name,
        source_type="file_upload",
        raw_file_path=minio_key,
        size_bytes=size,
        detected_format=detected_format,
        status=DatasetStatus.PENDING,
    )
    db.add(dataset)
    await db.flush()
    await db.refresh(dataset)

    # Dispatch background ingestion task
    from pipeline.tasks.ingest import process_ingestion
    process_ingestion.delay(str(dataset.id))

    return DatasetUploadResponse(
        dataset_id=dataset.id,
        name=dataset.name,
        size_bytes=size,
        detected_format=detected_format,
        status=DatasetStatus.PENDING,
    )


# ────────────────────────────────────────────────────────
# Chunked Upload
# ────────────────────────────────────────────────────────

@router.post("/upload/chunk", response_model=ChunkUploadResponse)
async def upload_chunk(
    upload_id: str = Form(...),
    chunk_index: int = Form(...),
    total_chunks: int = Form(...),
    filename: str = Form(...),
    chunk_data: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> ChunkUploadResponse:
    """Upload a chunk for large file chunked upload. Assembles in MinIO."""
    # Store chunk as a separate object
    chunk_key = f"chunks/{upload_id}/chunk_{chunk_index:05d}"
    data = await chunk_data.read()
    minio_upload("dataforge-raw", chunk_key, BytesIO(data), length=len(data))

    progress = ((chunk_index + 1) / total_chunks) * 100

    # Check if all chunks are received
    if chunk_index + 1 >= total_chunks:
        # Assemble chunks
        assembled = BytesIO()
        total_size = 0
        client = get_minio_client()

        for i in range(total_chunks):
            key = f"chunks/{upload_id}/chunk_{i:05d}"
            try:
                response = client.get_object("dataforge-raw", key)
                chunk_bytes = response.read()
                assembled.write(chunk_bytes)
                total_size += len(chunk_bytes)
                response.close()
                response.release_conn()
            except Exception as exc:
                raise_storage_error(f"Missing chunk {i}: {exc}")

        # Upload assembled file
        assembled.seek(0)
        final_key = f"uploads/{upload_id}/{filename}"
        minio_upload("dataforge-raw", final_key, assembled, length=total_size)

        # Clean up chunks
        for i in range(total_chunks):
            key = f"chunks/{upload_id}/chunk_{i:05d}"
            try:
                client.remove_object("dataforge-raw", key)
            except Exception:
                pass

        # Validate and create dataset
        ext = os.path.splitext(filename)[1].lower().lstrip(".")
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{ext}") as tmp:
            assembled.seek(0)
            tmp.write(assembled.read())
            tmp_path = tmp.name

        try:
            validation = validate_file(tmp_path, filename)
            detected_fmt = validation["format"] if validation["is_valid"] else ext
        finally:
            os.unlink(tmp_path)

        dataset = Dataset(
            user_id=current_user.id,
            name=os.path.splitext(filename)[0],
            source_type="chunked_upload",
            raw_file_path=final_key,
            size_bytes=total_size,
            detected_format=detected_fmt,
            status=DatasetStatus.PENDING,
        )
        db.add(dataset)
        await db.flush()
        await db.refresh(dataset)

        # Dispatch processing
        from pipeline.tasks.ingest import process_ingestion
        process_ingestion.delay(str(dataset.id))

        return ChunkUploadResponse(
            upload_id=upload_id,
            chunk_index=chunk_index,
            total_chunks=total_chunks,
            progress=100.0,
            status="complete",
            dataset_id=dataset.id,
        )

    return ChunkUploadResponse(
        upload_id=upload_id,
        chunk_index=chunk_index,
        total_chunks=total_chunks,
        progress=round(progress, 1),
        status="uploading",
    )


# ────────────────────────────────────────────────────────
# Preview & Stats
# ────────────────────────────────────────────────────────

@router.get("/datasets/{dataset_id}/preview", response_model=DatasetPreview)
async def get_preview(
    dataset_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> DatasetPreview:
    """Return first 50 rows of the dataset as JSON."""
    result = await db.execute(
        select(Dataset).where(Dataset.id == dataset_id, Dataset.user_id == current_user.id)
    )
    dataset = result.scalar_one_or_none()
    if dataset is None:
        raise_dataset_not_found()

    if not dataset.raw_file_path:
        raise_parse_error("Dataset has no file path")

    # Download and parse
    try:
        file_data = minio_download("dataforge-raw", dataset.raw_file_path)
    except Exception as exc:
        raise_storage_error(f"Could not download file: {exc}")

    fmt = dataset.detected_format or "csv"
    with tempfile.NamedTemporaryFile(delete=False, suffix=f".{fmt}") as tmp:
        tmp.write(file_data.read())
        tmp_path = tmp.name

    try:
        from pipeline.ingestion.file_handler import FileHandler
        df = FileHandler.preview(tmp_path, fmt, n_rows=50)
    finally:
        os.unlink(tmp_path)

    if df.empty:
        raise_parse_error("Could not parse file for preview")

    # Convert to JSON-safe format
    rows = df.fillna("").to_dict(orient="records")
    dtypes = {col: str(dtype) for col, dtype in df.dtypes.items()}

    return DatasetPreview(
        dataset_id=dataset_id,
        columns=list(df.columns),
        dtypes=dtypes,
        row_count=len(rows),
        rows=rows,
    )


@router.get("/datasets/{dataset_id}/stats", response_model=DatasetStatsResponse)
async def get_stats(
    dataset_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> DatasetStatsResponse:
    """Return computed statistics for a dataset."""
    result = await db.execute(
        select(Dataset).where(Dataset.id == dataset_id, Dataset.user_id == current_user.id)
    )
    dataset = result.scalar_one_or_none()
    if dataset is None:
        raise_dataset_not_found()

    stats = dataset.stats or {}
    columns_raw = stats.get("columns", [])
    columns = [ColumnStats(**c) for c in columns_raw]

    return DatasetStatsResponse(
        dataset_id=dataset_id,
        row_count=dataset.row_count or 0,
        column_count=dataset.column_count or 0,
        size_bytes=dataset.size_bytes or 0,
        estimated_tokens=stats.get("estimated_tokens"),
        detected_language=stats.get("detected_language"),
        columns=columns,
    )


# ────────────────────────────────────────────────────────
# Connectors
# ────────────────────────────────────────────────────────

@router.get("/connectors")
async def list_connectors(
    current_user: User = Depends(get_current_user),
) -> dict:
    """List available ingestion connectors."""
    return {
        "connectors": [
            {"id": "file_upload", "name": "File Upload", "status": "available"},
            {"id": "s3", "name": "Amazon S3", "status": "available"},
            {"id": "url", "name": "URL / Web Scrape", "status": "available"},
            {"id": "huggingface", "name": "HuggingFace Hub", "status": "available"},
            {"id": "google_drive", "name": "Google Drive", "status": "available"},
        ]
    }


@router.post("/connect/s3")
async def s3_list(
    payload: S3ListRequest,
    current_user: User = Depends(get_current_user),
) -> dict:
    """List files in an S3 bucket at prefix."""
    try:
        from pipeline.ingestion.connectors.s3 import S3Connector
        connector = S3Connector(payload.access_key, payload.secret_key, payload.region)
        files = connector.list_objects(payload.bucket, payload.prefix)
        return {"files": [S3FileInfo(**f).model_dump() for f in files]}
    except Exception as exc:
        raise IngestionError("CONNECTOR_AUTH_FAILED", f"S3 connection failed: {exc}")


@router.post("/connect/s3/import", response_model=DatasetUploadResponse, status_code=status.HTTP_201_CREATED)
async def s3_import(
    payload: S3ImportRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> DatasetUploadResponse:
    """Import a file from S3 to DataForge."""
    try:
        from pipeline.ingestion.connectors.s3 import S3Connector
        connector = S3Connector(payload.access_key, payload.secret_key, payload.region)
        minio_key = connector.download_to_minio(payload.bucket, payload.key)
    except Exception as exc:
        raise IngestionError("CONNECTOR_AUTH_FAILED", f"S3 import failed: {exc}")

    filename = payload.key.split("/")[-1]
    ext = os.path.splitext(filename)[1].lower().lstrip(".")

    dataset = Dataset(
        user_id=current_user.id,
        name=payload.dataset_name,
        source_type="s3",
        raw_file_path=minio_key,
        detected_format=ext or "csv",
        status=DatasetStatus.PENDING,
    )
    db.add(dataset)
    await db.flush()
    await db.refresh(dataset)

    from pipeline.tasks.ingest import process_ingestion
    process_ingestion.delay(str(dataset.id))

    return DatasetUploadResponse(
        dataset_id=dataset.id,
        name=dataset.name,
        size_bytes=0,
        detected_format=dataset.detected_format or "csv",
        status=DatasetStatus.PENDING,
    )


@router.post("/connect/url", response_model=DatasetUploadResponse, status_code=status.HTTP_201_CREATED)
async def url_import(
    payload: UrlScrapeRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> DatasetUploadResponse:
    """Import data from URL(s) — auto-detects file download vs. web scrape."""
    try:
        from pipeline.ingestion.connectors.url_scraper import scrape_urls
        result = await scrape_urls(payload.urls, payload.scrape_mode)
    except Exception as exc:
        raise IngestionError("CONNECTOR_NOT_FOUND", f"URL import failed: {exc}")

    minio_key = result.get("minio_key")
    if not minio_key:
        raise IngestionError("PARSE_ERROR", "No content could be extracted from the provided URLs")

    name = payload.dataset_name or f"url-import-{uuid_mod.uuid4().hex[:8]}"
    fmt = "jsonl" if result["scraped_count"] > 0 else os.path.splitext(minio_key)[1].lstrip(".")

    dataset = Dataset(
        user_id=current_user.id,
        name=name,
        source_type="url",
        raw_file_path=minio_key,
        detected_format=fmt or "jsonl",
        status=DatasetStatus.PENDING,
    )
    db.add(dataset)
    await db.flush()
    await db.refresh(dataset)

    from pipeline.tasks.ingest import process_ingestion
    process_ingestion.delay(str(dataset.id))

    return DatasetUploadResponse(
        dataset_id=dataset.id,
        name=dataset.name,
        size_bytes=0,
        detected_format=fmt,
        status=DatasetStatus.PENDING,
    )


@router.post("/connect/huggingface", response_model=DatasetUploadResponse, status_code=status.HTTP_201_CREATED)
async def huggingface_import(
    payload: HuggingFaceImportRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> DatasetUploadResponse:
    """Import a dataset from HuggingFace Hub."""
    try:
        from pipeline.ingestion.connectors.huggingface import HuggingFaceConnector
        connector = HuggingFaceConnector(payload.hf_token)
        result = connector.download_dataset(
            payload.dataset_id,
            config=payload.config,
            split=payload.split or "train",
        )
    except Exception as exc:
        raise IngestionError("CONNECTOR_NOT_FOUND", f"HuggingFace import failed: {exc}")

    name = payload.dataset_name or payload.dataset_id.replace("/", "_")
    dataset = Dataset(
        user_id=current_user.id,
        name=name,
        source_type="huggingface",
        raw_file_path=result["minio_key"],
        size_bytes=result["size_bytes"],
        row_count=result["row_count"],
        column_count=len(result["columns"]),
        detected_format="parquet",
        status=DatasetStatus.PENDING,
    )
    db.add(dataset)
    await db.flush()
    await db.refresh(dataset)

    from pipeline.tasks.ingest import process_ingestion
    process_ingestion.delay(str(dataset.id))

    return DatasetUploadResponse(
        dataset_id=dataset.id,
        name=dataset.name,
        size_bytes=result["size_bytes"],
        detected_format="parquet",
        status=DatasetStatus.PENDING,
    )


@router.post("/connect/gdrive/auth", response_model=GDriveAuthResponse)
async def gdrive_auth(
    current_user: User = Depends(get_current_user),
) -> GDriveAuthResponse:
    """Get Google Drive OAuth2 authorization URL."""
    try:
        from pipeline.ingestion.connectors.google_drive import GoogleDriveConnector
        auth_url = GoogleDriveConnector.get_auth_url()
        return GDriveAuthResponse(auth_url=auth_url)
    except Exception as exc:
        raise IngestionError("CONNECTOR_AUTH_FAILED", f"Google Drive auth failed: {exc}")


@router.post("/connect/gdrive/import", response_model=DatasetUploadResponse, status_code=status.HTTP_201_CREATED)
async def gdrive_import(
    payload: GDriveImportRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> DatasetUploadResponse:
    """Import a file from Google Drive."""
    try:
        from pipeline.ingestion.connectors.google_drive import GoogleDriveConnector
        result = GoogleDriveConnector.download_file(payload.auth_code, payload.file_id)
    except Exception as exc:
        raise IngestionError("CONNECTOR_AUTH_FAILED", f"Google Drive import failed: {exc}")

    ext = os.path.splitext(result["filename"])[1].lower().lstrip(".")

    dataset = Dataset(
        user_id=current_user.id,
        name=payload.dataset_name,
        source_type="google_drive",
        raw_file_path=result["minio_key"],
        size_bytes=result["size_bytes"],
        detected_format=ext or "csv",
        status=DatasetStatus.PENDING,
    )
    db.add(dataset)
    await db.flush()
    await db.refresh(dataset)

    from pipeline.tasks.ingest import process_ingestion
    process_ingestion.delay(str(dataset.id))

    return DatasetUploadResponse(
        dataset_id=dataset.id,
        name=dataset.name,
        size_bytes=result["size_bytes"],
        detected_format=ext or "csv",
        status=DatasetStatus.PENDING,
    )
