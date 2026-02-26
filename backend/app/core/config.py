"""Application configuration loaded from environment variables."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Global application settings via Pydantic BaseSettings."""

    # Database
    DATABASE_URL: str = "postgresql+asyncpg://dataforge:dataforge@postgres:5432/dataforge"
    DATABASE_URL_SYNC: str = "postgresql://dataforge:dataforge@postgres:5432/dataforge"

    # Redis
    REDIS_URL: str = "redis://redis:6379/0"

    # MinIO
    MINIO_ENDPOINT: str = "minio:9000"
    MINIO_ACCESS_KEY: str = "dataforge"
    MINIO_SECRET_KEY: str = "dataforge123"
    MINIO_SECURE: bool = False

    # Auth
    JWT_SECRET_KEY: str = "your-secret-key-change-in-production"
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7

    # LLM
    LITELLM_API_KEY: str = ""
    LITELLM_MODEL: str = "gpt-4o-mini"

    # App
    APP_ENV: str = "development"
    FRONTEND_URL: str = "http://localhost:3000"

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
