"""Store per-site cumulative deforestation replicate ranges.

Revision ID: 0036a1b2c3f7
Revises: 0035a1b2c3f6
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0036a1b2c3f7"
down_revision: str | None = "0035a1b2c3f6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "task_results",
        sa.Column("extrapolated_cumulative_treatment_defor_ha", sa.Float()),
    )
    op.add_column(
        "task_results",
        sa.Column("extrapolated_cumulative_control_defor_ha", sa.Float()),
    )
    for metric in ("treatment", "control"):
        for bound in ("lower", "upper"):
            op.add_column(
                "task_results",
                sa.Column(
                    f"extrapolated_cumulative_{metric}_defor_ha_ci_{bound}",
                    sa.Float(),
                ),
            )


def downgrade() -> None:
    for metric in ("treatment", "control"):
        for bound in ("lower", "upper"):
            op.drop_column(
                "task_results",
                f"extrapolated_cumulative_{metric}_defor_ha_ci_{bound}",
            )
    op.drop_column("task_results", "extrapolated_cumulative_control_defor_ha")
    op.drop_column("task_results", "extrapolated_cumulative_treatment_defor_ha")
