"""Add subdivided reference-region pieces for fast site splitting.

Revision ID: 0033a1b2c3f4
Revises: 0032a1b2c3f3
Create Date: 2026-11-12 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from geoalchemy2 import Geometry

revision: str = "0033a1b2c3f4"
down_revision: str | None = "0032a1b2c3f3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "reference_region_pieces",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("layer_name", sa.String(100), nullable=False),
        sa.Column("export_uri", sa.String(500), nullable=False),
        sa.Column("region_id", sa.Text(), nullable=False),
        sa.Column(
            "geom",
            Geometry("GEOMETRY", srid=4326, spatial_index=False),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_reference_region_pieces_layer_export",
        "reference_region_pieces",
        ["layer_name", "export_uri"],
    )
    op.create_index(
        "idx_reference_region_pieces_geom",
        "reference_region_pieces",
        ["geom"],
        postgresql_using="gist",
    )


def downgrade() -> None:
    op.drop_table("reference_region_pieces")
