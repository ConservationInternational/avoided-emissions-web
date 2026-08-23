"""add cancelled to site_upload_status

Revision ID: 0023a1b2c3ea
Revises: 0022a1b2c3e9
Create Date: 2026-06-01 00:00:00.000000

Allow background site uploads to be marked as cancelled when a user
revokes an import before completion.
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0023a1b2c3ea"
down_revision: str | None = "0022a1b2c3e9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TYPE site_upload_status ADD VALUE IF NOT EXISTS 'cancelled'")


def downgrade() -> None:
    pass
