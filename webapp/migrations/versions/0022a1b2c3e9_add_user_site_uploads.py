"""add user_site_uploads table

Revision ID: 0022a1b2c3e9
Revises: 0021a1b2c3e8
Create Date: 2026-06-01 00:00:00.000000

Tracks asynchronous site-upload imports dispatched through Celery so the
admin UI can show queued/running/completed import status alongside the
resulting reusable site sets.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0022a1b2c3e9"
down_revision: Union[str, None] = "0021a1b2c3e8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    site_upload_status = postgresql.ENUM(
        "pending",
        "running",
        "completed",
        "failed",
        name="site_upload_status",
        create_type=False,
    )
    op.execute(
        "CREATE TYPE site_upload_status AS ENUM "
        "('pending', 'running', 'completed', 'failed')"
    )

    op.create_table(
        "user_site_uploads",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id"),
            nullable=False,
        ),
        sa.Column("original_filename", sa.String(length=500), nullable=False),
        sa.Column("celery_task_id", sa.String(length=255), nullable=True),
        sa.Column("site_set_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("site_set_name", sa.String(length=255), nullable=True),
        sa.Column("n_features", sa.Integer(), nullable=True),
        sa.Column("n_sites_imported", sa.Integer(), nullable=True),
        sa.Column(
            "status",
            site_upload_status,
            nullable=False,
            server_default="pending",
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("metadata", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.create_index(
        "ix_user_site_uploads_user_id", "user_site_uploads", ["user_id"], unique=False
    )
    op.create_index(
        "ix_user_site_uploads_site_set_id",
        "user_site_uploads",
        ["site_set_id"],
        unique=False,
    )
    op.create_index(
        "ix_user_site_uploads_celery_task_id",
        "user_site_uploads",
        ["celery_task_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_user_site_uploads_celery_task_id", table_name="user_site_uploads")
    op.drop_index("ix_user_site_uploads_site_set_id", table_name="user_site_uploads")
    op.drop_index("ix_user_site_uploads_user_id", table_name="user_site_uploads")
    op.drop_table("user_site_uploads")
    op.execute("DROP TYPE IF EXISTS site_upload_status")
