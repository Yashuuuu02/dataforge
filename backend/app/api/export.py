"""Export API routes."""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status

from app.core.auth import get_current_user
from app.models.user import User

router = APIRouter(prefix="/export", tags=["Export"])


@router.get("/formats")
async def list_export_formats(
    current_user: User = Depends(get_current_user),
) -> dict:
    """List available export formats."""
    return {
        "formats": [
            {"id": "jsonl", "name": "JSONL", "description": "JSON Lines format for fine-tuning"},
            {"id": "parquet", "name": "Parquet", "description": "Apache Parquet columnar format"},
            {"id": "csv", "name": "CSV", "description": "Comma-separated values"},
            {"id": "huggingface", "name": "HuggingFace Dataset", "description": "Push to HuggingFace Hub"},
        ]
    }


@router.post("/{job_id}")
async def export_dataset(
    job_id: UUID,
    format: str = "jsonl",
    current_user: User = Depends(get_current_user),
) -> dict:
    """Export a processed dataset. Placeholder — full implementation in Phase 2."""
    return {
        "message": "Export endpoint ready",
        "job_id": str(job_id),
        "format": format,
        "status": "placeholder",
    }
