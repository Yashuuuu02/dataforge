"""MinIO S3-compatible storage client and helper functions."""

import logging
from io import BytesIO
from typing import BinaryIO

from minio import Minio
from minio.error import S3Error

from app.core.config import settings

logger = logging.getLogger(__name__)

BUCKETS = ["dataforge-raw", "dataforge-processed"]


def get_minio_client() -> Minio:
    """Create and return a MinIO client instance."""
    return Minio(
        endpoint=settings.MINIO_ENDPOINT,
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_SECRET_KEY,
        secure=settings.MINIO_SECURE,
    )


def init_minio_buckets() -> None:
    """Create required MinIO buckets if they don't exist."""
    client = get_minio_client()
    for bucket in BUCKETS:
        try:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                logger.info("Created MinIO bucket: %s", bucket)
            else:
                logger.info("MinIO bucket already exists: %s", bucket)
        except S3Error as exc:
            logger.error("Failed to create bucket %s: %s", bucket, exc)
            raise


def upload_file(bucket: str, key: str, file: BinaryIO, length: int = -1, content_type: str = "application/octet-stream") -> str:
    """Upload a file to a MinIO bucket. Returns the object key."""
    client = get_minio_client()
    client.put_object(bucket, key, file, length=length, content_type=content_type, part_size=10 * 1024 * 1024)
    return key


def download_file(bucket: str, key: str) -> BytesIO:
    """Download a file from MinIO and return it as a BytesIO."""
    client = get_minio_client()
    response = client.get_object(bucket, key)
    data = BytesIO(response.read())
    response.close()
    response.release_conn()
    return data


def get_presigned_url(bucket: str, key: str, expires: int = 3600) -> str:
    """Generate a presigned URL for downloading an object."""
    from datetime import timedelta

    client = get_minio_client()
    return client.presigned_get_object(bucket, key, expires=timedelta(seconds=expires))
