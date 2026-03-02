"""add rasterizing to covariate_status enum

Revision ID: d5f7a9b3e1c4
Revises: c4e6f8a2d1b3
Create Date: 2026-03-02 12:00:00.000000

Adds 'rasterizing' to the covariate_status PostgreSQL enum so that
the rasterize_vectors_task can track in-progress rasterization of
PostGIS vector layers.
"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d5f7a9b3e1c4"
down_revision: Union[str, None] = "c4e6f8a2d1b3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TYPE covariate_status ADD VALUE IF NOT EXISTS 'rasterizing'")


def downgrade() -> None:
    # PostgreSQL does not support removing values from an enum without
    # recreating the type.  The value is harmless if left in place.
    pass
