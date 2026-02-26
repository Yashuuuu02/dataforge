"""Add stats and error_message to datasets

Revision ID: 0002
Revises: 0001
Create Date: 2025-02-01 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("datasets", sa.Column("error_message", sa.Text(), nullable=True))
    op.add_column("datasets", sa.Column("stats", postgresql.JSONB(), nullable=True))


def downgrade() -> None:
    op.drop_column("datasets", "stats")
    op.drop_column("datasets", "error_message")
