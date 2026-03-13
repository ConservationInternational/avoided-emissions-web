"""Rename columns to extrapolated_* and sample_* prefixes

Revision ID: 0017a1b2c3e4
Revises: 0016a1b2c3e3
Create Date: 2026-03-13 12:00:00.000000

Renames result columns to clarify which values are extrapolated
(scaled to full site using sampling weights) vs sample values
(directly measured from sampled pixels only).
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0017a1b2c3e4"
down_revision = "0016a1b2c3e3"
branch_labels = None
depends_on = None


def upgrade():
    # Rename columns in task_results (by-year)
    op.alter_column(
        "task_results",
        "forest_loss_avoided_ha",
        new_column_name="extrapolated_forest_loss_avoided_ha",
    )
    op.alter_column(
        "task_results",
        "emissions_avoided_mgco2e",
        new_column_name="extrapolated_emissions_avoided_mgco2e",
    )
    op.alter_column(
        "task_results",
        "treatment_defor_ha",
        new_column_name="extrapolated_treatment_defor_ha",
    )
    op.alter_column(
        "task_results",
        "control_defor_ha",
        new_column_name="extrapolated_control_defor_ha",
    )
    op.alter_column(
        "task_results",
        "treatment_emissions_mgco2e",
        new_column_name="extrapolated_treatment_emissions_mgco2e",
    )
    op.alter_column(
        "task_results",
        "control_emissions_mgco2e",
        new_column_name="extrapolated_control_emissions_mgco2e",
    )
    op.alter_column(
        "task_results", "n_matched_pixels", new_column_name="n_sample_pixels"
    )
    # CI columns
    op.alter_column(
        "task_results",
        "treatment_defor_ha_ci_lower",
        new_column_name="extrapolated_treatment_defor_ha_ci_lower",
    )
    op.alter_column(
        "task_results",
        "treatment_defor_ha_ci_upper",
        new_column_name="extrapolated_treatment_defor_ha_ci_upper",
    )
    op.alter_column(
        "task_results",
        "control_defor_ha_ci_lower",
        new_column_name="extrapolated_control_defor_ha_ci_lower",
    )
    op.alter_column(
        "task_results",
        "control_defor_ha_ci_upper",
        new_column_name="extrapolated_control_defor_ha_ci_upper",
    )
    op.alter_column(
        "task_results",
        "forest_loss_avoided_ha_ci_lower",
        new_column_name="extrapolated_forest_loss_avoided_ha_ci_lower",
    )
    op.alter_column(
        "task_results",
        "forest_loss_avoided_ha_ci_upper",
        new_column_name="extrapolated_forest_loss_avoided_ha_ci_upper",
    )
    op.alter_column(
        "task_results",
        "treatment_emissions_mgco2e_ci_lower",
        new_column_name="extrapolated_treatment_emissions_mgco2e_ci_lower",
    )
    op.alter_column(
        "task_results",
        "treatment_emissions_mgco2e_ci_upper",
        new_column_name="extrapolated_treatment_emissions_mgco2e_ci_upper",
    )
    op.alter_column(
        "task_results",
        "control_emissions_mgco2e_ci_lower",
        new_column_name="extrapolated_control_emissions_mgco2e_ci_lower",
    )
    op.alter_column(
        "task_results",
        "control_emissions_mgco2e_ci_upper",
        new_column_name="extrapolated_control_emissions_mgco2e_ci_upper",
    )
    op.alter_column(
        "task_results",
        "emissions_avoided_mgco2e_ci_lower",
        new_column_name="extrapolated_emissions_avoided_mgco2e_ci_lower",
    )
    op.alter_column(
        "task_results",
        "emissions_avoided_mgco2e_ci_upper",
        new_column_name="extrapolated_emissions_avoided_mgco2e_ci_upper",
    )

    # Rename columns in task_results_total
    op.alter_column(
        "task_results_total",
        "forest_loss_avoided_ha",
        new_column_name="extrapolated_forest_loss_avoided_ha",
    )
    op.alter_column(
        "task_results_total",
        "emissions_avoided_mgco2e",
        new_column_name="extrapolated_emissions_avoided_mgco2e",
    )
    op.alter_column(
        "task_results_total", "n_matched_pixels", new_column_name="n_sample_pixels"
    )
    # CI columns
    op.alter_column(
        "task_results_total",
        "forest_loss_avoided_ha_ci_lower",
        new_column_name="extrapolated_forest_loss_avoided_ha_ci_lower",
    )
    op.alter_column(
        "task_results_total",
        "forest_loss_avoided_ha_ci_upper",
        new_column_name="extrapolated_forest_loss_avoided_ha_ci_upper",
    )
    op.alter_column(
        "task_results_total",
        "emissions_avoided_mgco2e_ci_lower",
        new_column_name="extrapolated_emissions_avoided_mgco2e_ci_lower",
    )
    op.alter_column(
        "task_results_total",
        "emissions_avoided_mgco2e_ci_upper",
        new_column_name="extrapolated_emissions_avoided_mgco2e_ci_upper",
    )


def downgrade():
    # Reverse renames in task_results_total
    op.alter_column(
        "task_results_total",
        "extrapolated_emissions_avoided_mgco2e_ci_upper",
        new_column_name="emissions_avoided_mgco2e_ci_upper",
    )
    op.alter_column(
        "task_results_total",
        "extrapolated_emissions_avoided_mgco2e_ci_lower",
        new_column_name="emissions_avoided_mgco2e_ci_lower",
    )
    op.alter_column(
        "task_results_total",
        "extrapolated_forest_loss_avoided_ha_ci_upper",
        new_column_name="forest_loss_avoided_ha_ci_upper",
    )
    op.alter_column(
        "task_results_total",
        "extrapolated_forest_loss_avoided_ha_ci_lower",
        new_column_name="forest_loss_avoided_ha_ci_lower",
    )
    op.alter_column(
        "task_results_total", "n_sample_pixels", new_column_name="n_matched_pixels"
    )
    op.alter_column(
        "task_results_total",
        "extrapolated_emissions_avoided_mgco2e",
        new_column_name="emissions_avoided_mgco2e",
    )
    op.alter_column(
        "task_results_total",
        "extrapolated_forest_loss_avoided_ha",
        new_column_name="forest_loss_avoided_ha",
    )

    # Reverse renames in task_results
    op.alter_column(
        "task_results",
        "extrapolated_emissions_avoided_mgco2e_ci_upper",
        new_column_name="emissions_avoided_mgco2e_ci_upper",
    )
    op.alter_column(
        "task_results",
        "extrapolated_emissions_avoided_mgco2e_ci_lower",
        new_column_name="emissions_avoided_mgco2e_ci_lower",
    )
    op.alter_column(
        "task_results",
        "extrapolated_control_emissions_mgco2e_ci_upper",
        new_column_name="control_emissions_mgco2e_ci_upper",
    )
    op.alter_column(
        "task_results",
        "extrapolated_control_emissions_mgco2e_ci_lower",
        new_column_name="control_emissions_mgco2e_ci_lower",
    )
    op.alter_column(
        "task_results",
        "extrapolated_treatment_emissions_mgco2e_ci_upper",
        new_column_name="treatment_emissions_mgco2e_ci_upper",
    )
    op.alter_column(
        "task_results",
        "extrapolated_treatment_emissions_mgco2e_ci_lower",
        new_column_name="treatment_emissions_mgco2e_ci_lower",
    )
    op.alter_column(
        "task_results",
        "extrapolated_forest_loss_avoided_ha_ci_upper",
        new_column_name="forest_loss_avoided_ha_ci_upper",
    )
    op.alter_column(
        "task_results",
        "extrapolated_forest_loss_avoided_ha_ci_lower",
        new_column_name="forest_loss_avoided_ha_ci_lower",
    )
    op.alter_column(
        "task_results",
        "extrapolated_control_defor_ha_ci_upper",
        new_column_name="control_defor_ha_ci_upper",
    )
    op.alter_column(
        "task_results",
        "extrapolated_control_defor_ha_ci_lower",
        new_column_name="control_defor_ha_ci_lower",
    )
    op.alter_column(
        "task_results",
        "extrapolated_treatment_defor_ha_ci_upper",
        new_column_name="treatment_defor_ha_ci_upper",
    )
    op.alter_column(
        "task_results",
        "extrapolated_treatment_defor_ha_ci_lower",
        new_column_name="treatment_defor_ha_ci_lower",
    )
    op.alter_column(
        "task_results", "n_sample_pixels", new_column_name="n_matched_pixels"
    )
    op.alter_column(
        "task_results",
        "extrapolated_control_emissions_mgco2e",
        new_column_name="control_emissions_mgco2e",
    )
    op.alter_column(
        "task_results",
        "extrapolated_treatment_emissions_mgco2e",
        new_column_name="treatment_emissions_mgco2e",
    )
    op.alter_column(
        "task_results",
        "extrapolated_control_defor_ha",
        new_column_name="control_defor_ha",
    )
    op.alter_column(
        "task_results",
        "extrapolated_treatment_defor_ha",
        new_column_name="treatment_defor_ha",
    )
    op.alter_column(
        "task_results",
        "extrapolated_emissions_avoided_mgco2e",
        new_column_name="emissions_avoided_mgco2e",
    )
    op.alter_column(
        "task_results",
        "extrapolated_forest_loss_avoided_ha",
        new_column_name="forest_loss_avoided_ha",
    )
