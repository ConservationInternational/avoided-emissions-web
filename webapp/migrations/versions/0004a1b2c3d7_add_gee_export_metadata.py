"""add gee_export_metadata table

Revision ID: 0004a1b2c3d7
Revises: 0003a1b2c3d6
Create Date: 2026-03-03 12:00:00.000000

Tracks per-covariate GCS tile snapshots (ETags, sizes, md5 hashes) and
links them to the resulting merged COG on S3.  Allows the system to
detect whether tiles have changed since the last merge and skip
redundant work.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0004a1b2c3d7"
down_revision: Union[str, None] = "0003a1b2c3d6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    gee_export_meta_status = postgresql.ENUM(
        "detected",
        "pending_merge",
        "merging",
        "merged",
        "skipped_existing",
        "failed",
        name="gee_export_meta_status",
        create_type=False,
    )
    op.execute(
        "CREATE TYPE gee_export_meta_status AS ENUM "
        "('detected', 'pending_merge', 'merging', 'merged', "
        "'skipped_existing', 'failed')"
    )

    op.create_table(
        "gee_export_metadata",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column(
            "covariate_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("covariates.id"),
            nullable=True,
        ),
        sa.Column("covariate_name", sa.String(100), nullable=False, index=True),
        # GCS tile snapshot
        sa.Column("gcs_bucket", sa.String(255), nullable=True),
        sa.Column("gcs_prefix", sa.String(500), nullable=True),
        sa.Column("tile_count", sa.Integer(), nullable=True),
        sa.Column("tile_total_bytes", sa.BigInteger(), nullable=True),
        sa.Column(
            "tile_details",
            postgresql.JSON(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("tile_etag_hash", sa.String(64), nullable=True, index=True),
        sa.Column("tiles_detected_at", sa.DateTime(timezone=True), nullable=True),
        # GEE export info
        sa.Column("gee_task_id", sa.String(255), nullable=True),
        sa.Column("gee_completed_at", sa.DateTime(timezone=True), nullable=True),
        # Merge lifecycle
        sa.Column("merge_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("merge_completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("merge_duration_seconds", sa.Float(), nullable=True),
        # Merged COG on S3
        sa.Column("merged_cog_key", sa.String(1000), nullable=True),
        sa.Column("merged_cog_url", sa.String(1000), nullable=True),
        sa.Column("merged_cog_bytes", sa.BigInteger(), nullable=True),
        sa.Column("merged_cog_etag", sa.String(255), nullable=True),
        # Status
        sa.Column(
            "status",
            gee_export_meta_status,
            nullable=False,
            server_default="detected",
        ),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_table("gee_export_metadata")
    op.execute("DROP TYPE IF EXISTS gee_export_meta_status")
