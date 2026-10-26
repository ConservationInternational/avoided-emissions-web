"""Simplify result schema for the corrected statistical pipeline

Results are now aggregated to whole sites (split-site fragments are summed in
the R summary step), so ``task_results.sub_site_index`` is dropped and the
``(task_id, site_id, year)`` unique key is restored.  Per-site totals gain
``n_eligible_pixels`` (pixels eligible for matching).

Existing result rows were produced by the previous pipeline, whose estimates
are not comparable, so they are deleted; affected tasks must be rerun.

Revision ID: 0029a1b2c3f0
Revises: 0028a1b2c3ef
Create Date: 2026-10-26 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0029a1b2c3f0"
down_revision: str | None = "0028a1b2c3ef"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("DELETE FROM task_results")
    op.execute("DELETE FROM task_results_total")
    op.drop_constraint(
        "task_results_task_id_site_id_sub_site_index_year_key",
        "task_results",
        type_="unique",
    )
    op.drop_column("task_results", "sub_site_index")
    op.create_unique_constraint(
        "task_results_task_id_site_id_year_key",
        "task_results",
        ["task_id", "site_id", "year"],
    )
    op.add_column(
        "task_results_total",
        sa.Column("n_eligible_pixels", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("task_results_total", "n_eligible_pixels")
    op.drop_constraint(
        "task_results_task_id_site_id_year_key", "task_results", type_="unique"
    )
    op.add_column(
        "task_results",
        sa.Column("sub_site_index", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_unique_constraint(
        "task_results_task_id_site_id_sub_site_index_year_key",
        "task_results",
        ["task_id", "site_id", "sub_site_index", "year"],
    )
