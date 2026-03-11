"""add confidence interval columns to task_results and task_results_total

Revision ID: 0016a1b2c3e3
Revises: 0015a1b2c3e2
Create Date: 2026-03-11 10:00:00.000000

Adds CI lower/upper bound columns to TaskResult and TaskResultTotal
for repeated matching confidence intervals.  All columns are nullable
(NULL when n_replicates == 1).
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0016a1b2c3e3"
down_revision = "0015a1b2c3e2"
branch_labels = None
depends_on = None


def upgrade():
    # TaskResult CI columns
    ci_cols_result = [
        "treatment_defor_ha_ci_lower",
        "treatment_defor_ha_ci_upper",
        "control_defor_ha_ci_lower",
        "control_defor_ha_ci_upper",
        "forest_loss_avoided_ha_ci_lower",
        "forest_loss_avoided_ha_ci_upper",
        "treatment_emissions_mgco2e_ci_lower",
        "treatment_emissions_mgco2e_ci_upper",
        "control_emissions_mgco2e_ci_lower",
        "control_emissions_mgco2e_ci_upper",
        "emissions_avoided_mgco2e_ci_lower",
        "emissions_avoided_mgco2e_ci_upper",
    ]
    for col in ci_cols_result:
        op.add_column("task_results", sa.Column(col, sa.Float(), nullable=True))

    # TaskResultTotal CI columns
    ci_cols_total = [
        "forest_loss_avoided_ha_ci_lower",
        "forest_loss_avoided_ha_ci_upper",
        "emissions_avoided_mgco2e_ci_lower",
        "emissions_avoided_mgco2e_ci_upper",
    ]
    for col in ci_cols_total:
        op.add_column("task_results_total", sa.Column(col, sa.Float(), nullable=True))


def downgrade():
    ci_cols_total = [
        "emissions_avoided_mgco2e_ci_upper",
        "emissions_avoided_mgco2e_ci_lower",
        "forest_loss_avoided_ha_ci_upper",
        "forest_loss_avoided_ha_ci_lower",
    ]
    for col in ci_cols_total:
        op.drop_column("task_results_total", col)

    ci_cols_result = [
        "emissions_avoided_mgco2e_ci_upper",
        "emissions_avoided_mgco2e_ci_lower",
        "control_emissions_mgco2e_ci_upper",
        "control_emissions_mgco2e_ci_lower",
        "treatment_emissions_mgco2e_ci_upper",
        "treatment_emissions_mgco2e_ci_lower",
        "forest_loss_avoided_ha_ci_upper",
        "forest_loss_avoided_ha_ci_lower",
        "control_defor_ha_ci_upper",
        "control_defor_ha_ci_lower",
        "treatment_defor_ha_ci_upper",
        "treatment_defor_ha_ci_lower",
    ]
    for col in ci_cols_result:
        op.drop_column("task_results", col)
