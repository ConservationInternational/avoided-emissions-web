"""add vector reference tables (geoboundaries, ecoregions, wdpa)

Revision ID: b3d5f7a1c9e2
Revises: a7c3e9f1b2d8
Create Date: 2026-03-15 00:00:00.000000
"""

from typing import Sequence, Union

import geoalchemy2
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b3d5f7a1c9e2"
down_revision: Union[str, None] = "a7c3e9f1b2d8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Ensure PostGIS extension is available
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")

    # -- GeoBoundaries ADM0 --
    op.create_table(
        "geoboundaries_adm0",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("shape_group", sa.String(length=10), nullable=False),
        sa.Column("shape_name", sa.String(length=255), nullable=True),
        sa.Column("shape_type", sa.String(length=20), nullable=True),
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
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_geoboundaries_adm0_shape_group", "geoboundaries_adm0", ["shape_group"])

    # -- GeoBoundaries ADM1 --
    op.create_table(
        "geoboundaries_adm1",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("shape_group", sa.String(length=10), nullable=False),
        sa.Column("shape_name", sa.String(length=255), nullable=True),
        sa.Column("shape_id", sa.String(length=100), nullable=True),
        sa.Column("shape_type", sa.String(length=20), nullable=True),
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
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_geoboundaries_adm1_shape_group", "geoboundaries_adm1", ["shape_group"])

    # -- GeoBoundaries ADM2 --
    op.create_table(
        "geoboundaries_adm2",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("shape_group", sa.String(length=10), nullable=False),
        sa.Column("shape_name", sa.String(length=255), nullable=True),
        sa.Column("shape_id", sa.String(length=100), nullable=True),
        sa.Column("shape_type", sa.String(length=20), nullable=True),
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
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_geoboundaries_adm2_shape_group", "geoboundaries_adm2", ["shape_group"])

    # -- RESOLVE Ecoregions --
    op.create_table(
        "ecoregions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("eco_id", sa.Integer(), nullable=False),
        sa.Column("eco_name", sa.String(length=255), nullable=True),
        sa.Column("biome_num", sa.Integer(), nullable=True),
        sa.Column("biome_name", sa.String(length=255), nullable=True),
        sa.Column("realm", sa.String(length=100), nullable=True),
        sa.Column("nnh", sa.Float(), nullable=True),
        sa.Column("color", sa.String(length=10), nullable=True),
        sa.Column("color_bio", sa.String(length=10), nullable=True),
        sa.Column("color_nnh", sa.String(length=10), nullable=True),
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
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_ecoregions_eco_id", "ecoregions", ["eco_id"])

    # -- WDPA Protected Areas --
    op.create_table(
        "wdpa",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("wdpaid", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=500), nullable=True),
        sa.Column("orig_name", sa.String(length=500), nullable=True),
        sa.Column("desig", sa.String(length=500), nullable=True),
        sa.Column("desig_type", sa.String(length=100), nullable=True),
        sa.Column("iucn_cat", sa.String(length=20), nullable=True),
        sa.Column("int_crit", sa.String(length=100), nullable=True),
        sa.Column("marine", sa.String(length=10), nullable=True),
        sa.Column("rep_m_area", sa.Float(), nullable=True),
        sa.Column("gis_m_area", sa.Float(), nullable=True),
        sa.Column("rep_area", sa.Float(), nullable=True),
        sa.Column("gis_area", sa.Float(), nullable=True),
        sa.Column("no_take", sa.String(length=50), nullable=True),
        sa.Column("no_tk_area", sa.Float(), nullable=True),
        sa.Column("status", sa.String(length=100), nullable=True),
        sa.Column("status_yr", sa.Integer(), nullable=True),
        sa.Column("gov_type", sa.String(length=255), nullable=True),
        sa.Column("own_type", sa.String(length=100), nullable=True),
        sa.Column("mang_auth", sa.String(length=500), nullable=True),
        sa.Column("mang_plan", sa.String(length=500), nullable=True),
        sa.Column("verif", sa.String(length=100), nullable=True),
        sa.Column("iso3", sa.String(length=10), nullable=True),
        sa.Column("parent_iso3", sa.String(length=10), nullable=True),
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
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_wdpa_wdpaid", "wdpa", ["wdpaid"])
    op.create_index("ix_wdpa_iso3", "wdpa", ["iso3"])


def downgrade() -> None:
    op.drop_table("wdpa")
    op.drop_table("ecoregions")
    op.drop_table("geoboundaries_adm2")
    op.drop_table("geoboundaries_adm1")
    op.drop_table("geoboundaries_adm0")
