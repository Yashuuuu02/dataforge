"""LiteLLM client for AI agent interactions."""

import logging
from typing import Optional

from app.core.config import settings

logger = logging.getLogger(__name__)


class LiteLLMClient:
    """Wrapper around LiteLLM for multi-provider LLM access.

    Full implementation in Phase 2. This stub provides the interface
    that the agent module will use.
    """

    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        self.api_key = api_key or settings.LITELLM_API_KEY
        self.model = model or settings.LITELLM_MODEL

    async def chat(self, messages: list[dict], temperature: float = 0.7) -> dict:
        """Send a chat completion request.

        Args:
            messages: List of message dicts with 'role' and 'content'.
            temperature: Sampling temperature.

        Returns:
            Response dict with 'content' and 'usage' keys.
        """
        logger.info("LiteLLM chat called with model=%s (stub)", self.model)
        return {
            "content": "LLM integration coming in Phase 2.",
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
            "model": self.model,
        }

    async def classify(self, text: str, labels: list[str]) -> dict:
        """Classify text into one of the given labels. Stub."""
        return {"label": labels[0] if labels else "unknown", "confidence": 0.0}

    async def generate_schema(self, sample_data: str) -> dict:
        """Generate a data schema from sample data. Stub."""
        return {"schema": {}, "status": "placeholder"}
