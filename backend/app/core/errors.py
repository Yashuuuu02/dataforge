"""Structured error handling for the ingestion layer."""

from typing import Optional

from fastapi import HTTPException, status


SUPPORTED_FORMATS = ["csv", "tsv", "json", "jsonl", "parquet", "xlsx", "txt", "md", "pdf", "html", "docx"]

# Error code constants
UNSUPPORTED_FORMAT = "UNSUPPORTED_FORMAT"
FILE_TOO_LARGE = "FILE_TOO_LARGE"
PARSE_ERROR = "PARSE_ERROR"
CONNECTOR_AUTH_FAILED = "CONNECTOR_AUTH_FAILED"
CONNECTOR_NOT_FOUND = "CONNECTOR_NOT_FOUND"
STORAGE_ERROR = "STORAGE_ERROR"
DATASET_NOT_FOUND = "DATASET_NOT_FOUND"
CHUNK_ERROR = "CHUNK_ERROR"


class IngestionError(HTTPException):
    """Structured ingestion error with error code and optional metadata."""

    def __init__(
        self,
        error_code: str,
        message: str,
        status_code: int = status.HTTP_400_BAD_REQUEST,
        extra: Optional[dict] = None,
    ):
        detail = {"error": error_code, "message": message}
        if extra:
            detail.update(extra)
        if error_code == UNSUPPORTED_FORMAT:
            detail["supported_formats"] = SUPPORTED_FORMATS
        super().__init__(status_code=status_code, detail=detail)


def raise_unsupported_format(ext: str) -> None:
    raise IngestionError(UNSUPPORTED_FORMAT, f"File format .{ext} is not supported")


def raise_parse_error(message: str) -> None:
    raise IngestionError(PARSE_ERROR, message, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)


def raise_storage_error(message: str) -> None:
    raise IngestionError(STORAGE_ERROR, message, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


def raise_connector_auth_failed(connector: str, message: str) -> None:
    raise IngestionError(CONNECTOR_AUTH_FAILED, f"{connector}: {message}", status_code=status.HTTP_401_UNAUTHORIZED)


def raise_dataset_not_found() -> None:
    raise IngestionError(DATASET_NOT_FOUND, "Dataset not found", status_code=status.HTTP_404_NOT_FOUND)
