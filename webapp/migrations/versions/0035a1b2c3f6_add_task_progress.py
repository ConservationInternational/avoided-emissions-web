"""Cache API execution progress for the task list.

Revision ID: 0035a1b2c3f6
Revises: 0034a1b2c3f5
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0035a1b2c3f6"
down_revision: str | None = "0034a1b2c3f5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("analysis_tasks", sa.Column("progress", sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column("analysis_tasks", "progress")
