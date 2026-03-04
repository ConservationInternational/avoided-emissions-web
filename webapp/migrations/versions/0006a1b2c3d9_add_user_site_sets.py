"""add user-uploaded reusable site sets

Revision ID: 0006a1b2c3d9
Revises: 0005a1b2c3d8
Create Date: 2026-03-04 12:00:00.000000

Adds persistent PostGIS-backed user site sets and links tasks to a selected set.
"""

from typing import Sequence, Union

import geoalchemy2
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0006a1b2c3d9"
down_revision: Union[str, None] = "0005a1b2c3d8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "user_site_sets",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("original_filename", sa.String(500), nullable=False),
        sa.Column("file_size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("file_format", sa.String(20), nullable=False),
        sa.Column(
            "uploaded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.Column("n_sites", sa.Integer(), nullable=False),
        sa.Column("bounds", postgresql.JSONB()),
        sa.Column(
            "metadata", postgresql.JSONB(), server_default=sa.text("'{}'::jsonb")
        ),
    )
    op.create_index("idx_user_site_sets_user_id", "user_site_sets", ["user_id"])
    op.create_index("idx_user_site_sets_uploaded_at", "user_site_sets", ["uploaded_at"])

    op.create_table(
        "user_site_features",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("uuid_generate_v4()"),
        ),
        sa.Column(
            "site_set_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("user_site_sets.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("site_id", sa.String(100), nullable=False),
        sa.Column("site_name", sa.String(255), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date()),
        sa.Column("area_ha", sa.Float()),
        sa.Column(
            "geom",
            geoalchemy2.types.Geometry(
                geometry_type="MULTIPOLYGON",
                srid=4326,
                from_text="ST_GeomFromEWKT",
                name="geometry",
            ),
            nullable=False,
        ),
    )
    op.create_index(
        "idx_user_site_features_site_set", "user_site_features", ["site_set_id"]
    )
    op.create_index("idx_user_site_features_site_id", "user_site_features", ["site_id"])
    op.create_index(
        "ix_user_site_features_geom",
        "user_site_features",
        ["geom"],
        postgresql_using="gist",
    )

    op.add_column(
        "analysis_tasks",
        sa.Column("site_set_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_analysis_tasks_site_set_id",
        "analysis_tasks",
        "user_site_sets",
        ["site_set_id"],
        ["id"],
    )
    op.create_index("idx_analysis_tasks_site_set_id", "analysis_tasks", ["site_set_id"])


def downgrade() -> None:
    op.drop_index("idx_analysis_tasks_site_set_id", table_name="analysis_tasks")
    op.drop_constraint(
        "fk_analysis_tasks_site_set_id", "analysis_tasks", type_="foreignkey"
    )
    op.drop_column("analysis_tasks", "site_set_id")

    op.drop_index("ix_user_site_features_geom", table_name="user_site_features")
    op.drop_index("idx_user_site_features_site_id", table_name="user_site_features")
    op.drop_index("idx_user_site_features_site_set", table_name="user_site_features")
    op.drop_table("user_site_features")

    op.drop_index("idx_user_site_sets_uploaded_at", table_name="user_site_sets")
    op.drop_index("idx_user_site_sets_user_id", table_name="user_site_sets")
    op.drop_table("user_site_sets")
