"""URL connector — download files and scrape web pages."""

import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from io import BytesIO
from typing import Optional
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from app.core.minio_client import upload_file as minio_upload

logger = logging.getLogger(__name__)

# Common file extensions for direct download
FILE_EXTENSIONS = {".csv", ".json", ".jsonl", ".parquet", ".xlsx", ".xls", ".pdf", ".docx", ".txt", ".md", ".zip", ".gz", ".tar"}


def is_direct_file(url: str, content_type: Optional[str] = None) -> bool:
    """Detect whether a URL points to a downloadable file."""
    parsed = urlparse(url)
    ext = os.path.splitext(parsed.path)[1].lower()
    if ext in FILE_EXTENSIONS:
        return True
    if content_type and not content_type.startswith("text/html"):
        return True
    return False


async def download_file_from_url(url: str, minio_bucket: str = "dataforge-raw") -> dict:
    """Download a file from a URL and upload to MinIO.

    Returns: {minio_key, filename, size, content_type}
    """
    async with httpx.AsyncClient(follow_redirects=True, timeout=120.0) as client:
        response = await client.get(url)
        response.raise_for_status()

        content_type = response.headers.get("content-type", "application/octet-stream")
        parsed = urlparse(url)
        filename = os.path.basename(parsed.path) or "downloaded_file"

        data = BytesIO(response.content)
        size = len(response.content)
        minio_key = f"url-import/{filename}"

        minio_upload(minio_bucket, minio_key, data, length=size, content_type=content_type)

        return {
            "minio_key": minio_key,
            "filename": filename,
            "size": size,
            "content_type": content_type,
        }


async def scrape_url(url: str) -> dict:
    """Scrape a web page and extract main content.

    Returns: {url, title, content, scraped_at}
    """
    async with httpx.AsyncClient(follow_redirects=True, timeout=60.0) as client:
        response = await client.get(url)
        response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")

    # Remove non-content elements
    for tag in soup(["script", "style", "nav", "footer", "header", "aside", "iframe"]):
        tag.decompose()

    title = soup.title.string.strip() if soup.title and soup.title.string else ""

    # Try to find main content
    main = soup.find("main") or soup.find("article") or soup.find(role="main") or soup.body
    content = main.get_text(separator="\n", strip=True) if main else soup.get_text(separator="\n", strip=True)

    return {
        "url": url,
        "title": title,
        "content": content,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


async def scrape_urls(
    urls: list[str],
    scrape_mode: str = "auto",
    minio_bucket: str = "dataforge-raw",
) -> dict:
    """Scrape or download multiple URLs and save as JSONL in MinIO.

    Args:
        urls: List of URLs to process.
        scrape_mode: "download" (direct file), "scrape" (extract text), "auto" (detect).
        minio_bucket: Target MinIO bucket.

    Returns: {minio_key, total_urls, scraped_count, downloaded_count}
    """
    records: list[dict] = []
    downloaded_keys: list[str] = []
    errors: list[str] = []

    for url in urls:
        try:
            if scrape_mode == "download":
                result = await download_file_from_url(url, minio_bucket)
                downloaded_keys.append(result["minio_key"])
            elif scrape_mode == "scrape":
                record = await scrape_url(url)
                records.append(record)
            else:
                # Auto-detect
                async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
                    head_resp = await client.head(url)
                    ct = head_resp.headers.get("content-type", "")

                if is_direct_file(url, ct):
                    result = await download_file_from_url(url, minio_bucket)
                    downloaded_keys.append(result["minio_key"])
                else:
                    record = await scrape_url(url)
                    records.append(record)
        except Exception as exc:
            logger.error("Failed to process URL %s: %s", url, exc)
            errors.append(f"{url}: {exc}")

    # Save scraped records as JSONL
    minio_key = None
    if records:
        jsonl = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
        data = BytesIO(jsonl.encode("utf-8"))
        minio_key = f"url-import/scraped_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.jsonl"
        minio_upload(minio_bucket, minio_key, data, length=len(jsonl.encode("utf-8")), content_type="application/jsonl")

    return {
        "minio_key": minio_key or (downloaded_keys[0] if downloaded_keys else None),
        "total_urls": len(urls),
        "scraped_count": len(records),
        "downloaded_count": len(downloaded_keys),
        "errors": errors,
    }
