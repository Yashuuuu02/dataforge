"""Initial migration - create all tables

Revision ID: 0001
Revises:
Create Date: 2025-01-01 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Users table
    op.create_table(
        "users",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("email", sa.String(255), unique=True, nullable=False, index=True),
        sa.Column("password_hash", sa.Text(), nullable=False),
        sa.Column("api_key", sa.String(255), unique=True, nullable=True, index=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("plan", sa.String(50), server_default="free"),
        sa.Column("settings", postgresql.JSONB(), server_default="{}"),
        sa.Column("llm_provider_keys", postgresql.JSONB(), server_default="{}"),
    )

    # Dataset status enum
    dataset_status = postgresql.ENUM("pending", "processing", "ready", "failed", name="dataset_status", create_type=True)
    dataset_status.create(op.get_bind(), checkfirst=True)

    # Datasets table
    op.create_table(
        "datasets",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False, index=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("source_type", sa.String(100), nullable=True),
        sa.Column("raw_file_path", sa.Text(), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("row_count", sa.Integer(), nullable=True),
        sa.Column("column_count", sa.Integer(), nullable=True),
        sa.Column("detected_format", sa.String(50), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("status", dataset_status, server_default="pending"),
    )

    # Job enums
    job_mode = postgresql.ENUM("finetune", "rag", "ml", "agent", name="job_mode", create_type=True)
    job_mode.create(op.get_bind(), checkfirst=True)

    job_status = postgresql.ENUM("queued", "running", "completed", "failed", name="job_status", create_type=True)
    job_status.create(op.get_bind(), checkfirst=True)

    # Jobs table
    op.create_table(
        "jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("dataset_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("datasets.id"), nullable=False, index=True),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False, index=True),
        sa.Column("mode", job_mode, nullable=False),
        sa.Column("config", postgresql.JSONB(), server_default="{}"),
        sa.Column("workflow_steps", postgresql.JSONB(), server_default="[]"),
        sa.Column("status", job_status, server_default="queued"),
        sa.Column("progress", sa.Integer(), server_default="0"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("celery_task_id", sa.String(255), nullable=True),
    )

    # Processed datasets table
    op.create_table(
        "processed_datasets",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("jobs.id"), nullable=False, index=True),
        sa.Column("output_path", sa.Text(), nullable=True),
        sa.Column("output_format", sa.String(50), nullable=True),
        sa.Column("row_count", sa.Integer(), nullable=True),
        sa.Column("quality_score_avg", sa.Float(), nullable=True),
        sa.Column("stats", postgresql.JSONB(), server_default="{}"),
        sa.Column("version", sa.Integer(), server_default="1"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Workflows table
    op.create_table(
        "workflows",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False, index=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("steps", postgresql.JSONB(), server_default="[]"),
        sa.Column("is_public", sa.Boolean(), server_default="false"),
        sa.Column("use_count", sa.Integer(), server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Versions table
    op.create_table(
        "versions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("dataset_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("datasets.id"), nullable=False, index=True),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("processed_dataset_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("processed_datasets.id"), nullable=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("jobs.id"), nullable=True),
        sa.Column("changelog", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("versions")
    op.drop_table("workflows")
    op.drop_table("processed_datasets")
    op.drop_table("jobs")
    op.drop_table("datasets")
    op.drop_table("users")

    # Drop enums
    op.execute("DROP TYPE IF EXISTS dataset_status")
    op.execute("DROP TYPE IF EXISTS job_mode")
    op.execute("DROP TYPE IF EXISTS job_status")
