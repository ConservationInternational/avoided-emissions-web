"""Add indices for large-dataset zoom sampling performance

Revision ID: 0025a1b2c3ec
Revises: 0024a1b2c3eb
Create Date: 2026-06-02 00:00:00.000000

Adds indices to speed up zoom-aware sampling queries that filter by site_set_id
and perform spatial intersection (ST_Intersects). These queries are called
frequently when users interact with the map preview on the submit page,
especially with large datasets (100k+ polygons).

Adds:

* ``user_site_features (site_set_id, site_id)`` — composite B-tree index
  speeds queries that filter by site_set_id and look up specific sites.

* ``user_site_features (geom)`` using BRIN — Block Range Index is more
  memory-efficient than GIST for very large spatial datasets and can be
  faster for spatial scans. Kept alongside existing GIST index to give
  query planner options. BRIN trades some accuracy for memory efficiency,
  which is fine for map rendering at varying zoom levels.
"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0025a1b2c3ec"
down_revision: Union[str, None] = "0024a1b2c3eb"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Composite index on (site_set_id, site_id) for site-set-specific lookups
    op.create_index(
        "ix_user_site_features_set_id",
        "user_site_features",
        ["site_set_id", "site_id"],
    )

    # BRIN spatial index on geom for large spatial scans.
    # BRIN is more memory-efficient than GIST for large tables
    # and performs well for map rendering queries at various zoom levels.
    op.execute(
        "CREATE INDEX ix_user_site_features_geom_brin "
        "ON user_site_features USING BRIN (geom) "
        "WITH (pages_per_range=128)"
    )


def downgrade() -> None:
    op.drop_index("ix_user_site_features_geom_brin", table_name="user_site_features")
    op.drop_index("ix_user_site_features_set_id", table_name="user_site_features")
