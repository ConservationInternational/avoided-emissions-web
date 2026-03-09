"""add n_treatment_pixels and PostGIS grid pixel counting function

Revision ID: 0014a1b2c3e1
Revises: 0013a1b2c3e0
Create Date: 2026-03-09 20:00:00.000000

Stores the total number of treatment pixels per site so that the webapp
can compute an accurate % matched without approximating pixel area.

Also creates a PostGIS function ``count_grid_pixels(geometry)`` that
counts how many 30-arc-second grid cell centroids fall within a given
polygon.  The grid is aligned with the GEE-exported COG covariates
(origin 0°E/0°N, pixel size 1/120°).
"""

import sqlalchemy as sa
from alembic import op

revision = "0014a1b2c3e1"
down_revision = "0013a1b2c3e0"
branch_labels = None
depends_on = None

# PostGIS function that counts how many pixel centroids of the global
# 30-arc-second grid fall within a given geometry.  Uses
# generate_series to create centroids on the fly — no stored raster
# needed.  The grid matches gee-export/config.py exactly:
#   CRS = EPSG:4326, pixel_size = 1/120°, origin = (0, 0).
_CREATE_FUNCTION = """
CREATE OR REPLACE FUNCTION count_grid_pixels(
    site_geom geometry,
    pixel_size_deg double precision DEFAULT (1.0 / 120)
)
RETURNS integer
LANGUAGE sql
IMMUTABLE STRICT
AS $$
    WITH bbox AS (
        SELECT
            -- Snap bbox edges outward to the nearest pixel-edge multiple
            floor(ST_XMin(site_geom) / pixel_size_deg)
                * pixel_size_deg + pixel_size_deg / 2 AS x0,
            floor(ST_YMin(site_geom) / pixel_size_deg)
                * pixel_size_deg + pixel_size_deg / 2 AS y0,
            ceil(ST_XMax(site_geom) / pixel_size_deg)
                * pixel_size_deg - pixel_size_deg / 2  AS x1,
            ceil(ST_YMax(site_geom) / pixel_size_deg)
                * pixel_size_deg - pixel_size_deg / 2  AS y1
    )
    SELECT count(*)::integer
    FROM bbox,
         generate_series(bbox.x0::numeric, bbox.x1::numeric, pixel_size_deg::numeric) AS gx,
         generate_series(bbox.y0::numeric, bbox.y1::numeric, pixel_size_deg::numeric) AS gy
    WHERE ST_Contains(
        site_geom,
        ST_SetSRID(ST_MakePoint(gx, gy), 4326)
    );
$$;
"""

_DROP_FUNCTION = (
    "DROP FUNCTION IF EXISTS count_grid_pixels(geometry, double precision);"
)


def upgrade():
    op.add_column(
        "task_results_total",
        sa.Column("n_treatment_pixels", sa.Integer(), nullable=True),
    )
    op.execute(_CREATE_FUNCTION)


def downgrade():
    op.execute(_DROP_FUNCTION)
    op.drop_column("task_results_total", "n_treatment_pixels")
