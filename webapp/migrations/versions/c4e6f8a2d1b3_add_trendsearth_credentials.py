"""add trendsearth_credentials table

Revision ID: c4e6f8a2d1b3
Revises: b3d5f7a1c9e2
Create Date: 2026-02-28 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "c4e6f8a2d1b3"
down_revision: Union[str, None] = "b3d5f7a1c9e2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "trendsearth_credentials",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("uuid_generate_v4()"),
            primary_key=True,
        ),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id"),
            nullable=False,
            unique=True,
        ),
        sa.Column("te_email", sa.String(255), nullable=False),
        sa.Column("client_id", sa.String(128), nullable=False),
        sa.Column("client_secret_encrypted", sa.Text(), nullable=False),
        sa.Column(
            "client_name",
            sa.String(255),
            nullable=False,
            server_default="avoided-emissions-web",
        ),
        sa.Column("api_client_db_id", sa.String(128), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
        ),
    )
    op.create_index(
        "idx_trendsearth_credentials_user_id",
        "trendsearth_credentials",
        ["user_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index(
        "idx_trendsearth_credentials_user_id",
        table_name="trendsearth_credentials",
    )
    op.drop_table("trendsearth_credentials")
