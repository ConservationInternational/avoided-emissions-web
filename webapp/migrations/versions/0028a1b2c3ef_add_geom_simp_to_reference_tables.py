"""add pre-simplified geometry columns to reference tables

Adds a ``geom_simp`` column to each PostGIS reference table used for
site-splitting (ecoregions, geoboundaries_adm0/1/2), pre-populated with
``ST_SimplifyPreserveTopology(geom, 0.005)`` (~500 m at the equator).

This lets the splitting query read the simplified geometry directly instead of
computing the simplification on every request, which eliminates the dominant
per-query CPU cost for complex tables (e.g. ecoregions took 128 s per job at
run-time simplification with 0.005° tolerance).  The GIST index on ``geom_simp``
also speeds up the ``ST_Intersects`` spatial filter.

Revision ID: 0028a1b2c3ef
Revises: 0027a1b2c3ee
Create Date: 2026-06-22 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0028a1b2c3ef"
down_revision: Union[str, None] = "0027a1b2c3ee"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Tables that need a pre-simplified geometry column (must match _EXTENT_TABLE_MAP
# in webapp/services/reference_layers.py).
_TABLES = [
    "ecoregions",
    "geoboundaries_adm0",
    "geoboundaries_adm1",
    "geoboundaries_adm2",
]

# ~500 m at the equator (1° ≈ 111 320 m).
_TOL = 0.005


def upgrade() -> None:
    for table in _TABLES:
        # Add column (no-op if already present from a partial migration).
        op.execute(
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS geom_simp geometry"  # noqa: S608
        )
        # Populate from the full-resolution geometry.  This is intentionally
        # done outside CONCURRENTLY so it runs inside the migration transaction
        # and rolls back cleanly if anything else fails.
        op.execute(
            f"UPDATE {table} "  # noqa: S608
            f"SET geom_simp = ST_SimplifyPreserveTopology(geom, {_TOL}) "
            f"WHERE geom IS NOT NULL"
        )
        # Spatial index for fast ST_Intersects filtering on the simplified column.
        op.execute(
            f"CREATE INDEX IF NOT EXISTS {table}_geom_simp_idx "  # noqa: S608
            f"ON {table} USING GIST (geom_simp)"
        )


def downgrade() -> None:
    for table in reversed(_TABLES):
        op.execute(f"DROP INDEX IF EXISTS {table}_geom_simp_idx")  # noqa: S608
        op.execute(
            f"ALTER TABLE {table} DROP COLUMN IF EXISTS geom_simp"  # noqa: S608
        )
