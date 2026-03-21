"""Add resolution_m column to covariates table

Revision ID: 0018a1b2c3e5
Revises: 0017a1b2c3e4
Create Date: 2026-03-20 12:00:00.000000

Tracks the resolution (in metres) of each covariate export so the
system can manage 1 km and 250 m COGs independently.
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0018a1b2c3e5"
down_revision = "0017a1b2c3e4"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "covariates",
        sa.Column(
            "resolution_m",
            sa.Integer(),
            nullable=False,
            server_default="1000",
        ),
    )


def downgrade():
    op.drop_column("covariates", "resolution_m")
