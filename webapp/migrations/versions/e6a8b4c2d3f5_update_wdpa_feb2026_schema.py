"""update wdpa table to match Feb 2026 GDB schema

Revision ID: e6a8b4c2d3f5
Revises: d5f7a9b3e1c4
Create Date: 2026-03-02 13:00:00.000000

The WDPA Feb 2026 GDB release renamed several columns and added new
ones.  Since the wdpa table must be reimported from the new GDB anyway,
the simplest approach is to drop and recreate it with the correct schema.

Notable renames:
  WDPAID → SITE_ID, ORIG_NAME → NAME_ENG, MARINE → REALM,
  PARENT_ISO3 → PRNT_ISO3

New fields:
  SITE_PID, SITE_TYPE, DESIG_ENG, GOVSUBTYPE, OWNSUBTYPE,
  METADATAID, SUPP_INFO, CONS_OBJ, INLND_WTRS, OECM_ASMT
"""

from typing import Sequence, Union

import geoalchemy2
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e6a8b4c2d3f5"
down_revision: Union[str, None] = "d5f7a9b3e1c4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop old wdpa table and its indexes
    op.drop_index("ix_wdpa_iso3", table_name="wdpa")
    op.drop_index("ix_wdpa_wdpaid", table_name="wdpa")
    op.drop_table("wdpa")

    # Recreate with schema matching WDPA Feb 2026 GDB
    op.create_table(
        "wdpa",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("site_id", sa.Integer(), nullable=False),
        sa.Column("site_pid", sa.String(length=100), nullable=True),
        sa.Column("site_type", sa.String(length=50), nullable=True),
        sa.Column("name_eng", sa.String(length=500), nullable=True),
        sa.Column("name", sa.String(length=500), nullable=True),
        sa.Column("desig", sa.String(length=500), nullable=True),
        sa.Column("desig_eng", sa.String(length=500), nullable=True),
        sa.Column("desig_type", sa.String(length=100), nullable=True),
        sa.Column("iucn_cat", sa.String(length=20), nullable=True),
        sa.Column("int_crit", sa.String(length=100), nullable=True),
        sa.Column("realm", sa.String(length=50), nullable=True),
        sa.Column("rep_m_area", sa.Float(), nullable=True),
        sa.Column("gis_m_area", sa.Float(), nullable=True),
        sa.Column("rep_area", sa.Float(), nullable=True),
        sa.Column("gis_area", sa.Float(), nullable=True),
        sa.Column("no_take", sa.String(length=50), nullable=True),
        sa.Column("no_tk_area", sa.Float(), nullable=True),
        sa.Column("status", sa.String(length=100), nullable=True),
        sa.Column("status_yr", sa.Integer(), nullable=True),
        sa.Column("gov_type", sa.String(length=255), nullable=True),
        sa.Column("govsubtype", sa.String(length=255), nullable=True),
        sa.Column("own_type", sa.String(length=100), nullable=True),
        sa.Column("ownsubtype", sa.String(length=255), nullable=True),
        sa.Column("mang_auth", sa.String(length=500), nullable=True),
        sa.Column("mang_plan", sa.String(length=500), nullable=True),
        sa.Column("verif", sa.String(length=100), nullable=True),
        sa.Column("metadataid", sa.Integer(), nullable=True),
        sa.Column("prnt_iso3", sa.String(length=100), nullable=True),
        sa.Column("iso3", sa.String(length=100), nullable=True),
        sa.Column("supp_info", sa.Text(), nullable=True),
        sa.Column("cons_obj", sa.Text(), nullable=True),
        sa.Column("inlnd_wtrs", sa.String(length=50), nullable=True),
        sa.Column("oecm_asmt", sa.String(length=50), nullable=True),
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
    op.create_index("ix_wdpa_site_id", "wdpa", ["site_id"])
    op.create_index("ix_wdpa_iso3", "wdpa", ["iso3"])


def downgrade() -> None:
    # Drop new table
    op.drop_index("ix_wdpa_iso3", table_name="wdpa")
    op.drop_index("ix_wdpa_site_id", table_name="wdpa")
    op.drop_table("wdpa")

    # Recreate old schema
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
