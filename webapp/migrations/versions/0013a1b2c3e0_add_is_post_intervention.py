"""add is_post_intervention to task_results

Revision ID: 0013a1b2c3e0
Revises: 0012a1b2c3df
Create Date: 2026-03-09 18:00:00.000000

Adds an ``is_post_intervention`` boolean column to ``task_results`` so
that years after a site's end date can be flagged for comparison plots.
"""

import sqlalchemy as sa
from alembic import op

revision = "0013a1b2c3e0"
down_revision = "0012a1b2c3df"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "task_results",
        sa.Column(
            "is_post_intervention",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
    )


def downgrade():
    op.drop_column("task_results", "is_post_intervention")
