"""File format auto-detection and validation."""

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

EXTENSION_MAP: dict[str, str] = {
    ".csv": "csv",
    ".tsv": "tsv",
    ".json": "json",
    ".jsonl": "jsonl",
    ".ndjson": "jsonl",
    ".parquet": "parquet",
    ".pq": "parquet",
    ".xlsx": "xlsx",
    ".xls": "xlsx",
    ".txt": "txt",
    ".md": "md",
    ".markdown": "md",
    ".pdf": "pdf",
    ".html": "html",
    ".htm": "html",
    ".docx": "docx",
}

MAGIC_SIGNATURES: dict[bytes, str] = {
    b"%PDF": "pdf",
    b"PK\x03\x04": "xlsx",  # Also docx — differentiated by extension
    b"PAR1": "parquet",
}

SUPPORTED_FORMATS = list(set(EXTENSION_MAP.values()))


def detect_format(file_path: str, filename: str) -> str:
    """Detect file format from extension, magic bytes, then content probe.

    Returns one of: csv, tsv, json, jsonl, parquet, xlsx, txt, md, pdf, html, docx
    """
    # 1. Extension check
    ext = os.path.splitext(filename)[1].lower()
    if ext in EXTENSION_MAP:
        fmt = EXTENSION_MAP[ext]
        # Disambiguate ZIP-based formats
        if fmt == "xlsx" and ext == ".docx":
            fmt = "docx"
        logger.info("Format detected by extension: %s -> %s", ext, fmt)
        return fmt

    # 2. Magic bytes
    try:
        with open(file_path, "rb") as f:
            header = f.read(16)
        for sig, fmt in MAGIC_SIGNATURES.items():
            if header.startswith(sig):
                # Disambiguate PK header
                if sig == b"PK\x03\x04" and ext == ".docx":
                    return "docx"
                logger.info("Format detected by magic bytes: %s", fmt)
                return fmt
    except Exception as exc:
        logger.warning("Magic byte detection failed: %s", exc)

    # 3. Content probe — try to read first 1KB and infer
    try:
        with open(file_path, "rb") as f:
            sample = f.read(1024)
        text_sample = sample.decode("utf-8", errors="ignore").strip()

        if not text_sample:
            return "txt"

        # JSON array or object
        if text_sample.startswith("[") or text_sample.startswith("{"):
            # Check if JSONL (multiple {} on separate lines)
            lines = text_sample.split("\n")
            json_lines = [l.strip() for l in lines if l.strip().startswith("{")]
            if len(json_lines) > 1:
                return "jsonl"
            return "json"

        # HTML detection
        lower = text_sample.lower()
        if "<html" in lower or "<!doctype html" in lower:
            return "html"

        # CSV/TSV detection — count delimiters
        tab_count = text_sample.count("\t")
        comma_count = text_sample.count(",")
        if tab_count > comma_count and tab_count > 3:
            return "tsv"
        if comma_count > 3:
            return "csv"

        return "txt"

    except Exception as exc:
        logger.warning("Content probe failed: %s", exc)
        return "txt"


def validate_file(file_path: str, filename: str) -> dict:
    """Validate that a file is parseable. Returns {format, is_valid, error_message}."""
    try:
        fmt = detect_format(file_path, filename)
    except Exception as exc:
        return {"format": "unknown", "is_valid": False, "error_message": str(exc)}

    if fmt not in SUPPORTED_FORMATS:
        return {
            "format": fmt,
            "is_valid": False,
            "error_message": f"Format '{fmt}' is not supported",
        }

    # Quick parse check for common formats
    try:
        size = os.path.getsize(file_path)
        if size == 0:
            return {"format": fmt, "is_valid": False, "error_message": "File is empty"}
    except OSError as exc:
        return {"format": fmt, "is_valid": False, "error_message": str(exc)}

    return {"format": fmt, "is_valid": True, "error_message": None}


def get_allowed_extensions() -> list[str]:
    """Return list of allowed file extensions."""
    return sorted(EXTENSION_MAP.keys())
