"""Separate immutable analysis identity from user-facing site labels."""

import sqlalchemy as sa
from alembic import op

revision = "0031a1b2c3f2"
down_revision = "0030a1b2c3f1"
branch_labels = None
depends_on = None


def upgrade():
    for table in ("task_sites", "task_results", "task_results_total"):
        op.add_column(table, sa.Column("display_site_id", sa.String(100)))
    op.execute("DELETE FROM reference_layer_exports")


def downgrade():
    for table in ("task_results_total", "task_results", "task_sites"):
        op.drop_column(table, "display_site_id")
