# DataForge

**AI-agent-driven data preparation platform** for fine-tuning LLMs, RAG pipelines, and ML model training.

## Quick Start

```bash
# Clone and start all services
cd dataforge
cp .env.example .env
docker compose up --build
```

## Services

| Service | URL | Description |
|---|---|---|
| Frontend | http://localhost | Next.js 14 web app |
| API | http://localhost/api/health | FastAPI backend |
| API Docs | http://localhost/api/docs | Swagger UI |
| Flower | http://localhost:5555 | Celery job monitor |
| MinIO Console | http://localhost:9001 | Object storage (login: `dataforge` / `dataforge123`) |
| PostgreSQL | localhost:5432 | Database (user: `dataforge`) |
| Redis | localhost:6379 | Cache & message broker |

## Development Mode

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build
```

Enables hot reload for both frontend and backend with live volume mounts.

## Architecture

```
┌─────────┐     ┌──────────┐     ┌──────────┐
│  Nginx  │────▶│ Frontend │     │  MinIO   │
│  :80    │     │  :3000   │     │  :9000   │
│         │────▶│          │     │  :9001   │
│         │     └──────────┘     └──────────┘
│         │     ┌──────────┐     ┌──────────┐
│         │────▶│ Backend  │────▶│ Postgres │
│         │     │  :8000   │     │  :5432   │
└─────────┘     └────┬─────┘     └──────────┘
                     │
                ┌────▼─────┐     ┌──────────┐
                │  Redis   │◀───▶│  Worker  │
                │  :6379   │     │ (Celery) │
                └──────────┘     └──────────┘
                                 ┌──────────┐
                                 │  Flower  │
                                 │  :5555   │
                                 └──────────┘
```

## Tech Stack

- **Frontend**: Next.js 14, Tailwind CSS, shadcn/ui, TanStack Query, Zustand
- **Backend**: FastAPI, SQLAlchemy (async), Alembic, Pydantic v2
- **Queue**: Celery + Redis
- **Database**: PostgreSQL 15
- **Storage**: MinIO (S3-compatible)
- **Monitoring**: Flower
- **Proxy**: Nginx

## License

Open Source — MIT License
