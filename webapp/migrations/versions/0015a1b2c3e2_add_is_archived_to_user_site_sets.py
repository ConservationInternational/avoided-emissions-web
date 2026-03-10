"""add is_archived to user_site_sets

Revision ID: 0015a1b2c3e2
Revises: 0014a1b2c3e1
Create Date: 2026-03-10 14:00:00.000000

Allows users to hide site sets from the dropdown without deleting them,
even when the site set is still referenced by submitted tasks.
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0015a1b2c3e2"
down_revision = "0014a1b2c3e1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "user_site_sets",
        sa.Column(
            "is_archived",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )


def downgrade() -> None:
    op.drop_column("user_site_sets", "is_archived")
