"""Add sub_site_index to task_results and update unique constraint.

Revision ID: 0021a1b2c3e8
Revises: 0020a1b2c3e7
Create Date: 2025-01-01 00:00:00.000000

When cross-site grouping is active the R analysis can produce multiple
rows per (site_id, year) — one per sub-site within a cross-site group.
The old unique constraint on (task_id, site_id, year) would reject these
duplicate-looking rows.  This migration:

  1. Adds ``sub_site_index`` (INTEGER NOT NULL DEFAULT 0) to task_results.
     0 = ordinary site with no sub-site splitting;
     1+ = a sub-site fragment within a cross-site grouping group.
  2. Drops the old three-column unique constraint.
  3. Adds a new four-column unique constraint that includes sub_site_index.
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0021a1b2c3e8"
down_revision = "0020a1b2c3e7"
branch_labels = None
depends_on = None


def upgrade():
    # 1. Add the new column with a server-side default so that existing rows
    #    get sub_site_index = 0 (i.e. no sub-site splitting).
    op.add_column(
        "task_results",
        sa.Column(
            "sub_site_index",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )

    # 2. Drop the old unique constraint.
    op.drop_constraint(
        "task_results_task_id_site_id_year_key",
        "task_results",
        type_="unique",
    )

    # 3. Create the new four-column unique constraint.
    op.create_unique_constraint(
        "task_results_task_id_site_id_sub_site_index_year_key",
        "task_results",
        ["task_id", "site_id", "sub_site_index", "year"],
    )


def downgrade():
    op.drop_constraint(
        "task_results_task_id_site_id_sub_site_index_year_key",
        "task_results",
        type_="unique",
    )
    op.create_unique_constraint(
        "task_results_task_id_site_id_year_key",
        "task_results",
        ["task_id", "site_id", "year"],
    )
    op.drop_column("task_results", "sub_site_index")
