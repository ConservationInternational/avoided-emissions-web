"""add te_user_id to trendsearth_credentials

Revision ID: 0008a1b2c3db
Revises: 0007a1b2c3da
Create Date: 2026-03-04 23:00:00.000000

Stores the trends.earth API user UUID alongside the OAuth2 client
credentials so adopted tasks can be matched to the local user who
originally submitted them.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0008a1b2c3db"
down_revision: Union[str, None] = "0007a1b2c3da"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "trendsearth_credentials",
        sa.Column("te_user_id", sa.String(128), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("trendsearth_credentials", "te_user_id")
