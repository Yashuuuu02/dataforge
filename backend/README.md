# DataForge Backend

FastAPI-based backend for the DataForge data preparation platform.

## Services

- **API Server**: FastAPI with async SQLAlchemy, JWT auth, MinIO integration
- **Celery Worker**: Background job processing with Redis broker
- **Alembic**: Database migrations

## Running Standalone

### Prerequisites
- Python 3.11+
- PostgreSQL 15+
- Redis 7+
- MinIO

### Setup

```bash
cd backend
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r requirements.txt

# Copy and configure environment
cp ../.env.example .env

# Run migrations
alembic upgrade head

# Start API server
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# Start Celery worker (in a separate terminal)
celery -A pipeline.workers.celery_app worker --loglevel=info
```

## API Documentation

Once running, visit:
- Swagger UI: http://localhost:8000/api/docs
- OpenAPI JSON: http://localhost:8000/api/openapi.json
