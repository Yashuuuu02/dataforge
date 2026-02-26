"""Settings API — LLM Providers and API Keys."""

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.auth import get_current_user
from app.core.database import get_db
from app.core.security import encrypt_key, decrypt_key
from app.models.user import User
from ai.litellm_client import LiteLLMClient

router = APIRouter(tags=["Settings"])

class LLMSettingsRequest(BaseModel):
    provider: str
    api_key: str
    model: str
    base_url: str | None = None

class LLMSettingsResponse(BaseModel):
    provider: str
    model: str
    base_url: str | None = None
    has_key: bool

class TestResponse(BaseModel):
    valid: bool
    error: str | None = None

@router.get("/llm", response_model=LLMSettingsResponse)
async def get_llm_settings(
    current_user: User = Depends(get_current_user),
):
    """Get current LLM config without exposing the raw API key."""
    keys = current_user.llm_provider_keys or {}
    return LLMSettingsResponse(
        provider=keys.get("provider", ""),
        model=keys.get("model", ""),
        base_url=keys.get("base_url"),
        has_key=bool(keys.get("api_key"))
    )

@router.put("/llm", response_model=TestResponse)
async def update_llm_settings(
    payload: LLMSettingsRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Update LLM config after validating it."""
    client = LiteLLMClient(
        provider=payload.provider,
        api_key=payload.api_key,
        model=payload.model,
        base_url=payload.base_url
    )
    
    # Check if credential is valid
    is_valid = client.test_connection()
    if not is_valid and payload.provider != "ollama":
        raise HTTPException(status_code=400, detail="Invalid API Key or connection failed.")

    # Encrypt and save
    encrypted_key = encrypt_key(payload.api_key)
    
    current_user.llm_provider_keys = {
        "provider": payload.provider,
        "api_key": encrypted_key,
        "model": payload.model,
        "base_url": payload.base_url
    }
    
    db.add(current_user)
    await db.commit()
    
    return TestResponse(valid=is_valid)

@router.post("/llm/test", response_model=TestResponse)
async def test_llm_connection(
    current_user: User = Depends(get_current_user),
):
    """Test saved LLM connection."""
    keys = current_user.llm_provider_keys or {}
    enc_key = keys.get("api_key")
    if not enc_key:
        return TestResponse(valid=False, error="No API key configured")
        
    client = LiteLLMClient(
        provider=keys.get("provider", "openai"),
        api_key=decrypt_key(enc_key),
        model=keys.get("model", "gpt-4o-mini"),
        base_url=keys.get("base_url")
    )
    
    is_valid = client.test_connection()
    return TestResponse(valid=is_valid, error=None if is_valid else "Connection failed")
