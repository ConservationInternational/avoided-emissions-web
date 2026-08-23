"""add reference_layer_exports table

Revision ID: 0027a1b2c3ee
Revises: 0026a1b2c3ed
Create Date: 2026-06-02 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0027a1b2c3ee"
down_revision: str | None = "0026a1b2c3ed"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "reference_layer_exports",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("layer_name", sa.String(100), nullable=False),
        sa.Column("s3_uri", sa.String(500), nullable=False),
        sa.Column("feature_count", sa.Integer, nullable=True),
        sa.Column(
            "schema_version",
            sa.Integer,
            nullable=False,
            server_default="1",
        ),
        sa.Column("exported_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_reference_layer_exports_layer_name",
        "reference_layer_exports",
        ["layer_name"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_reference_layer_exports_layer_name", table_name="reference_layer_exports"
    )
    op.drop_table("reference_layer_exports")
