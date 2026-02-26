"""Agent API routes — AI chat interface."""

from fastapi import APIRouter, Depends

from app.core.auth import get_current_user
from app.models.user import User

router = APIRouter(prefix="/agent", tags=["Agent"])


@router.post("/chat")
async def chat(
    message: dict,
    current_user: User = Depends(get_current_user),
) -> dict:
    """Send a message to the AI agent. Placeholder — full LLM integration in Phase 2."""
    return {
        "response": "Agent endpoint ready. LLM integration coming in Phase 2.",
        "user_message": message.get("message", ""),
        "status": "placeholder",
    }


@router.get("/history")
async def get_history(
    current_user: User = Depends(get_current_user),
) -> dict:
    """Get chat history with the AI agent. Placeholder."""
    return {"messages": [], "total": 0}
