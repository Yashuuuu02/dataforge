"""S3 connector — list and download objects from AWS S3 to MinIO."""

import logging
from typing import Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from app.core.minio_client import get_minio_client, upload_file as minio_upload
from io import BytesIO

logger = logging.getLogger(__name__)


class S3Connector:
    """Connect to AWS S3, list objects, and download to MinIO."""

    def __init__(self, access_key: str, secret_key: str, region: str = "us-east-1"):
        self.client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )

    def list_objects(self, bucket: str, prefix: str = "") -> list[dict]:
        """List objects in an S3 bucket at the given prefix."""
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            files: list[dict] = []
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    files.append({
                        "key": obj["Key"],
                        "size": obj["Size"],
                        "last_modified": obj["LastModified"].isoformat() if obj.get("LastModified") else None,
                    })
            return files
        except ClientError as exc:
            logger.error("S3 list failed: %s", exc)
            raise
        except NoCredentialsError:
            raise ValueError("Invalid S3 credentials")

    def download_to_minio(self, s3_bucket: str, s3_key: str, minio_bucket: str = "dataforge-raw") -> str:
        """Download an object from S3 and upload it to MinIO.

        Returns the MinIO object key.
        """
        try:
            response = self.client.get_object(Bucket=s3_bucket, Key=s3_key)
            body = response["Body"].read()
            size = len(body)
            content_type = response.get("ContentType", "application/octet-stream")

            # Use the S3 key basename as the MinIO key
            filename = s3_key.split("/")[-1]
            minio_key = f"s3-import/{filename}"

            data = BytesIO(body)
            minio_upload(minio_bucket, minio_key, data, length=size, content_type=content_type)

            logger.info("Downloaded s3://%s/%s -> MinIO %s/%s (%d bytes)", s3_bucket, s3_key, minio_bucket, minio_key, size)
            return minio_key
        except ClientError as exc:
            logger.error("S3 download failed: %s", exc)
            raise
