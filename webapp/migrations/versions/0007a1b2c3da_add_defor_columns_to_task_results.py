"""add treatment/control deforestation columns to task_results

Revision ID: 0007a1b2c3da
Revises: 0006a1b2c3d9
Create Date: 2026-03-04 18:00:00.000000

Adds per-year treatment and control deforestation and emissions columns
so the webapp can plot site-vs-control forest loss trajectories.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0007a1b2c3da"
down_revision: Union[str, None] = "0006a1b2c3d9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "task_results",
        sa.Column("treatment_defor_ha", sa.Float(), nullable=True),
    )
    op.add_column(
        "task_results",
        sa.Column("control_defor_ha", sa.Float(), nullable=True),
    )
    op.add_column(
        "task_results",
        sa.Column("treatment_emissions_mgco2e", sa.Float(), nullable=True),
    )
    op.add_column(
        "task_results",
        sa.Column("control_emissions_mgco2e", sa.Float(), nullable=True),
    )
    op.add_column(
        "task_results",
        sa.Column(
            "is_pre_intervention", sa.Boolean(), nullable=True, server_default="false"
        ),
    )


def downgrade() -> None:
    op.drop_column("task_results", "is_pre_intervention")
    op.drop_column("task_results", "control_emissions_mgco2e")
    op.drop_column("task_results", "treatment_emissions_mgco2e")
    op.drop_column("task_results", "control_defor_ha")
    op.drop_column("task_results", "treatment_defor_ha")
