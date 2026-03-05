"""add task_share_links table

Revision ID: 0009a1b2c3dc
Revises: 0008a1b2c3db
Create Date: 2026-03-05 12:00:00.000000

Adds the task_share_links table for generating expiring, revocable
read-only share links for task results.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

# revision identifiers, used by Alembic.
revision: str = "0009a1b2c3dc"
down_revision: Union[str, None] = "0008a1b2c3db"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "task_share_links",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "task_id",
            UUID(as_uuid=True),
            sa.ForeignKey("analysis_tasks.id"),
            nullable=False,
            index=True,
        ),
        sa.Column("token", sa.String(128), unique=True, nullable=False, index=True),
        sa.Column(
            "created_by",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "is_active",
            sa.Boolean,
            nullable=False,
            server_default=sa.text("true"),
        ),
        sa.Column(
            "access_count",
            sa.Integer,
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("last_accessed_at", sa.DateTime(timezone=True)),
    )


def downgrade() -> None:
    op.drop_table("task_share_links")
