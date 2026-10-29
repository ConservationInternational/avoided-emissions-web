"""Add durable site-upload claims, progress, and publication identity.

Revision ID: 0032a1b2c3f3
Revises: 0031a1b2c3f2
Create Date: 2026-10-29 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0032a1b2c3f3"
down_revision: str | None = "0031a1b2c3f2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "user_site_uploads",
        sa.Column(
            "phase", sa.String(length=32), nullable=False, server_default="queued"
        ),
    )
    op.add_column(
        "user_site_uploads",
        sa.Column("rows_scanned", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "user_site_uploads",
        sa.Column("rows_accepted", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "user_site_uploads",
        sa.Column("rows_skipped", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "user_site_uploads",
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "user_site_uploads",
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "user_site_sets",
        sa.Column(
            "source_upload_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("user_site_uploads.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.create_unique_constraint(
        "uq_user_site_sets_source_upload_id",
        "user_site_sets",
        ["source_upload_id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_user_site_sets_source_upload_id", "user_site_sets", type_="unique"
    )
    op.drop_column("user_site_sets", "source_upload_id")
    op.drop_column("user_site_uploads", "heartbeat_at")
    op.drop_column("user_site_uploads", "attempt_count")
    op.drop_column("user_site_uploads", "rows_skipped")
    op.drop_column("user_site_uploads", "rows_accepted")
    op.drop_column("user_site_uploads", "rows_scanned")
    op.drop_column("user_site_uploads", "phase")
