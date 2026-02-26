"""FastAPI application entry point."""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan: startup and shutdown events."""
    # --- Startup ---
    logger.info("Starting DataForge API v0.1.0...")

    # Test database connection
    try:
        from app.core.database import engine
        from sqlalchemy import text

        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        logger.info("Database connection OK")
    except Exception as exc:
        logger.error("Database connection failed: %s", exc)
        raise

    # Run Alembic migrations
    try:
        import subprocess
        result = subprocess.run(
            ["alembic", "upgrade", "head"],
            capture_output=True,
            text=True,
            cwd="/app/backend",
        )
        if result.returncode == 0:
            logger.info("Alembic migrations applied successfully")
        else:
            logger.warning("Alembic migration output: %s", result.stderr)
    except Exception as exc:
        logger.warning("Alembic migration skipped: %s", exc)

    # Initialize MinIO buckets
    try:
        from app.core.minio_client import init_minio_buckets

        init_minio_buckets()
        logger.info("MinIO buckets initialized")
    except Exception as exc:
        logger.warning("MinIO setup deferred: %s", exc)

    logger.info("DataForge API started successfully")
    yield

    # --- Shutdown ---
    from app.core.database import engine

    await engine.dispose()
    logger.info("DataForge API shut down")


app = FastAPI(
    title="DataForge API",
    description="AI-agent-driven data preparation platform for fine-tuning LLMs, RAG pipelines, and ML model training.",
    version="0.1.0",
    docs_url="/api/docs",
    openapi_url="/api/openapi.json",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.FRONTEND_URL, "http://localhost:3000", "http://localhost"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
from app.api.auth import router as auth_router
from app.api.datasets import router as datasets_router
from app.api.jobs import router as jobs_router
from app.api.ingestion import router as ingestion_router
from app.api.agent import router as agent_router
from app.api.workflows import router as workflows_router
from app.api.export import router as export_router

app.include_router(auth_router, prefix="/api")
app.include_router(datasets_router, prefix="/api")
app.include_router(jobs_router, prefix="/api")
app.include_router(ingestion_router, prefix="/api")
app.include_router(agent_router, prefix="/api")
app.include_router(workflows_router, prefix="/api")
app.include_router(export_router, prefix="/api")


@app.get("/api/health", tags=["Health"])
async def health_check() -> dict:
    """Health check endpoint."""
    return {"status": "ok", "version": "0.1.0"}


# ────────────────────────────────────────────────────────
# WebSocket — Real-time Ingestion Progress
# ────────────────────────────────────────────────────────

@app.websocket("/api/ws/ingestion/{dataset_id}")
async def ws_ingestion_progress(websocket: WebSocket, dataset_id: str):
    """WebSocket endpoint for real-time ingestion progress via Redis pub/sub."""
    await websocket.accept()
    logger.info("WebSocket connected for dataset: %s", dataset_id)

    redis_client = aioredis.from_url(settings.REDIS_URL)
    pubsub = redis_client.pubsub()
    channel = f"ingestion:{dataset_id}"

    try:
        await pubsub.subscribe(channel)

        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message and message["type"] == "message":
                data = message["data"]
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                await websocket.send_text(data)

                # Check if done
                try:
                    parsed = json.loads(data)
                    if parsed.get("status") in ("ready", "failed"):
                        break
                except json.JSONDecodeError:
                    pass

            # Small sleep to prevent tight loop
            await asyncio.sleep(0.1)

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected for dataset: %s", dataset_id)
    except Exception as exc:
        logger.error("WebSocket error for dataset %s: %s", dataset_id, exc)
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.close()
        await redis_client.close()


@app.websocket("/api/ws/job/{job_id}")
async def ws_job_progress(websocket: WebSocket, job_id: str):
    """WebSocket endpoint for real-time job/pipeline progress via Redis pub/sub."""
    await websocket.accept()
    logger.info("WebSocket connected for job: %s", job_id)

    redis_client = aioredis.from_url(settings.REDIS_URL)
    pubsub = redis_client.pubsub()
    channel = f"job:{job_id}"

    try:
        await pubsub.subscribe(channel)

        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message and message["type"] == "message":
                data = message["data"]
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                await websocket.send_text(data)

                try:
                    parsed = json.loads(data)
                    if parsed.get("status") in ("completed", "failed"):
                        break
                except json.JSONDecodeError:
                    pass

            await asyncio.sleep(0.1)

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected for job: %s", job_id)
    except Exception as exc:
        logger.error("WebSocket error for job %s: %s", job_id, exc)
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.close()
        await redis_client.close()

