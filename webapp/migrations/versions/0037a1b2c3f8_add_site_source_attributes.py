"""Retain source attributes for site filtering.

Revision ID: 0037a1b2c3f8
Revises: 0036a1b2c3f7
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSON

revision: str = "0037a1b2c3f8"
down_revision: str | None = "0036a1b2c3f7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "user_site_features",
        sa.Column("source_attributes", JSON(), nullable=False, server_default="{}"),
    )


def downgrade() -> None:
    op.drop_column("user_site_features", "source_attributes")
