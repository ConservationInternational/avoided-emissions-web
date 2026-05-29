"""Add sub-site support to task_sites table

Revision ID: 0019a1b2c3e6
Revises: 0018a1b2c3e5
Create Date: 2026-05-29 12:00:00.000000

Adds columns to support sites that span multiple exact-match groups.
When group_by_exact_matches is enabled, sites crossing exact-match
boundaries (e.g., admin regions) are split into sub-polygons.
Each sub-polygon gets a separate TaskSite record with sub_site_index.
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0019a1b2c3e6"
down_revision = "0018a1b2c3e5"
branch_labels = None
depends_on = None


def upgrade():
    # Add sub-site tracking columns
    op.add_column(
        "task_sites",
        sa.Column(
            "sub_site_index",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "task_sites",
        sa.Column(
            "is_sub_site",
            sa.Boolean(),
            nullable=False,
            server_default="false",
        ),
    )
    op.add_column(
        "task_sites",
        sa.Column(
            "original_area_ha",
            sa.Float(),
            nullable=True,
        ),
    )


def downgrade():
    op.drop_column("task_sites", "original_area_ha")
    op.drop_column("task_sites", "is_sub_site")
    op.drop_column("task_sites", "sub_site_index")
