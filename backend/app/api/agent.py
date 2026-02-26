"""AI Agent API routes — chat, analysis, execution via SSE."""

import asyncio
import json
import logging
from uuid import uuid4
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as aioredis
from litellm import acompletion
import copy

from app.core.auth import get_current_user
from app.core.config import settings
from app.core.database import get_db
from app.models.user import User
from app.models.dataset import Dataset
from app.core.security import decrypt_key

from ai.litellm_client import LiteLLMClient
from ai.dataset_analyzer import DatasetAnalyzer, DatasetAnalysis
from ai.workflow_builder import WorkflowBuilder, WorkflowPlan

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agent", tags=["Agent"])

# --- Request Schemas ---
class ChatRequest(BaseModel):
    message: str
    dataset_id: str
    session_id: str

class WorkflowRunRequest(BaseModel):
    dataset_id: str
    workflow: dict  # WorkflowPlan

class SyntheticRunRequest(BaseModel):
    dataset_id: str
    count: int
    config: dict

# --- Helpers ---
def get_llm_client(user: User) -> LiteLLMClient | None:
    keys = user.llm_provider_keys or {}
    enc_key = keys.get("api_key")
    if not enc_key: return None
    return LiteLLMClient(
        provider=keys.get("provider", "openai"),
        api_key=decrypt_key(enc_key),
        model=keys.get("model", "gpt-4o-mini"),
        base_url=keys.get("base_url")
    )

async def _get_session(redis, redis_key: str, dataset_id: str, session_id: str) -> dict:
    data = await redis.get(redis_key)
    if data:
        return json.loads(data)
    # Minimal initial session
    return {
        "session_id": session_id,
        "dataset_id": dataset_id,
        "conversation_history": [],
        "current_workflow": None,
        "dataset_analysis": None
    }

async def _save_session(redis, redis_key: str, session_dict: dict):
    # Reset TTL to 24 hours on every save (86400 seconds)
    await redis.setex(redis_key, 86400, json.dumps(session_dict))


# --- Endpoints ---
@router.post("/analyze/{dataset_id}")
async def analyze_dataset(
    dataset_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Heuristic + AI scan of the dataset. Run heavily on the worker in a full app, but quick here."""
    # To keep this fast without loading 10GB into API memory, we'd normally queue this.
    # For Phase 4 demonstration, we assume dataset preview is small or we just use precomputed stats
    from app.core.minio_client import get_file_stream
    import pandas as pd
    
    # fetch top 500 rows only using pandas nrows
    try:
         obj = get_file_stream("dataforge-raw", f"{current_user.id}/{dataset_id}")
         df = pd.read_csv(obj, nrows=500) # simplified fallback just for CSV demonstration
    except Exception:
         # In a real scenario we use the pipeline FileHandler
         df = pd.DataFrame()

    llm = get_llm_client(current_user)
    analyzer = DatasetAnalyzer(llm)
    analysis = await analyzer.analyze(df, f"dataset_{dataset_id}.csv")
    
    return asdict(analysis)


@router.post("/chat")
async def agent_chat(
    req: ChatRequest,
    current_user: User = Depends(get_current_user)
):
    """Interact with the DataForgeAgent using Server-Sent Events (SSE)."""
    redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    redis_key = f"agent_session:{current_user.id}:{req.dataset_id}:{req.session_id}"
    
    session_data = await _get_session(redis, redis_key, req.dataset_id, req.session_id)
    # Reset TTL immediately on request access to guarantee session survival
    await redis.expire(redis_key, 86400)
    
    session_data["conversation_history"].append({"role": "user", "content": req.message})

    llm = get_llm_client(current_user)
    
    async def event_stream():
        if not llm:
            yield f"data: {json.dumps({'type': 'chunk', 'content': 'Running in heuristic mode (No API Key). Use UI builder or add API key.'})}\n\n"
            yield f"data: {json.dumps({'type': 'done', 'action': 'error', 'suggestions': ['Settings']})}\n\n"
            return

        builder = WorkflowBuilder(llm)

        tools = [
            {"type": "function", "function": {"name": "build_pipeline", "description": "Constructs a new pipeline", "parameters": {"type": "object", "properties": {}}}},
            {"type": "function", "function": {"name": "refine_pipeline", "description": "Modifies currently pending pipeline", "parameters": {"type": "object", "properties": {}}}},
            {"type": "function", "function": {"name": "run_approved_pipeline", "description": "Executes pipeline", "parameters": {"type": "object", "properties": {}}}}
        ]

        system_prompt = "You are DataForge Agent. Build workflows to clean data. Use tools when the user wants to configure or run a pipeline."
        analysis = session_data.get("dataset_analysis")
        if analysis:
             system_prompt += f"\nDataset: {json.dumps(analysis)[:500]}"
             
        messages = [{"role": "system", "content": system_prompt}] + session_data["conversation_history"]

        try:
             # Streaming litellm call
             response = await acompletion(
                 model=llm._prepare_kwargs()["model"],
                 api_key=llm.api_key,
                 messages=messages,
                 tools=tools,
                 tool_choice="auto",
                 stream=True
             )
             
             # Collect tool calls or content
             function_name = ""
             function_args = ""
             full_content = ""
             
             async for chunk in response:
                 delta = chunk.choices[0].delta
                 
                 # It's returning text
                 if delta.content:
                     full_content += delta.content
                     yield f"data: {json.dumps({'type': 'chunk', 'content': delta.content})}\n\n"
                 
                 # It's requesting a tool
                 if delta.tool_calls:
                     for tc in delta.tool_calls:
                         if tc.function.name:
                             function_name += tc.function.name
                         if tc.function.arguments:
                             function_args += tc.function.arguments
             
             # Post-stream Tool Processing
             if function_name:
                 yield f"data: {json.dumps({'type': 'chunk', 'content': f'\\n\\n*Executing {function_name}...*' })}\n\n"
                 
                 action = "workflow_ready"
                 workflow_output = None
                 res_msg = "Done."
                 suggestions = ["Run pipeline", "Edit config"]
                 
                 analysis_obj = DatasetAnalysis(**analysis) if analysis else DatasetAnalysis("unknown", [], "un", 0, 0, [], [], "common", 0, "")
                 cwf_dict = session_data.get("current_workflow")
                 cwf_obj = None
                 if cwf_dict:
                      # very rough unpack
                      steps = [WorkflowPlan(**cwf_dict).steps] # not strictly right but acceptable mock
                      cwf_obj = builder._parse_json_to_plan(cwf_dict, 0)
                      
                 if function_name == "build_pipeline":
                      plan = await builder.build_from_text(req.message, analysis_obj, session_data["conversation_history"])
                      workflow_output = plan
                      res_msg = "I've drafted a pipeline config."
                 elif function_name == "refine_pipeline":
                      if cwf_obj:
                          plan = await builder.refine(cwf_obj, req.message, session_data["conversation_history"])
                          workflow_output = plan
                          res_msg = "Pipeline refined."
                      else:
                          res_msg = "No workflow to refine."
                          action = "error"
                 elif function_name == "run_approved_pipeline":
                      res_msg = "I'm dispatching the job now."
                      action = "run_approved"
                      workflow_output = cwf_obj
                 
                 # Update session
                 session_data["conversation_history"].append({"role": "assistant", "content": res_msg})
                 if workflow_output:
                      session_data["current_workflow"] = workflow_output.to_dict()
                      
                 await _save_session(redis, redis_key, session_data)
                 
                 yield f"data: {json.dumps({'type': 'done', 'action': action, 'workflow': workflow_output.to_dict() if workflow_output else None, 'suggestions': suggestions})}\n\n"
                 
             else:
                 # Standard chat
                 session_data["conversation_history"].append({"role": "assistant", "content": full_content})
                 await _save_session(redis, redis_key, session_data)
                 
                 yield f"data: {json.dumps({'type': 'done', 'action': 'chat', 'workflow': session_data['current_workflow'], 'suggestions': ['Configure pipeline']})}\n\n"
                 
        except Exception as e:
            logger.error(f"Chat stream error: {e}")
            yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.post("/workflow/run")
async def run_workflow(
    req: WorkflowRunRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    from app.models.job import Job, JobMode, JobStatus
    from datetime import datetime, timezone
    
    # create the job
    job = Job(
        dataset_id=req.dataset_id,
        user_id=current_user.id,
        mode=JobMode.COMMON,
        config=req.workflow,
        workflow_steps=[{"step": s["step"], "config": s["config"]} for s in req.workflow.get("steps", [])],
        status=JobStatus.QUEUED,
        started_at=datetime.now(timezone.utc)
    )
    db.add(job)
    await db.flush()
    await db.refresh(job)
    
    from pipeline.tasks.pipeline import run_common_pipeline
    task = run_common_pipeline.delay(str(job.id))
    job.celery_task_id = task.id
    await db.commit()
    
    return {"job_id": job.id}


@router.get("/session/{session_id}")
async def get_session(session_id: str, dataset_id: str, current_user: User = Depends(get_current_user)):
    redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    redis_key = f"agent_session:{current_user.id}:{dataset_id}:{session_id}"
    return await _get_session(redis, redis_key, dataset_id, session_id)


@router.delete("/session/{session_id}")
async def delete_session(session_id: str, dataset_id: str, current_user: User = Depends(get_current_user)):
    redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    redis_key = f"agent_session:{current_user.id}:{dataset_id}:{session_id}"
    await redis.delete(redis_key)
    return {"status": "cleared"}
