"""add exact_match_vars to covariate_presets

Revision ID: 0002a1b2c3d5
Revises: 0001a1b2c3d4
Create Date: 2026-03-02 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0002a1b2c3d5"
down_revision: Union[str, None] = "0001a1b2c3d4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "covariate_presets",
        sa.Column("exact_match_vars", postgresql.ARRAY(sa.Text()), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("covariate_presets", "exact_match_vars")
