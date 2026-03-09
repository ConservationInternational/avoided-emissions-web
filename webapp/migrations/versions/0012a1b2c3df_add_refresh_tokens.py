"""add refresh_tokens table

Revision ID: 0012a1b2c3df
Revises: 0011a1b2c3de
Create Date: 2026-03-09 12:00:00.000000

Adds the ``refresh_tokens`` table for persistent login with activity-
based expiry.  Each row stores a SHA-256 hash of the plaintext token
(sent to the client as an HTTP-only cookie), an absolute expiry date,
and a ``last_activity`` timestamp that is updated on authenticated
requests.  Users are logged out after 4 hours of inactivity.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision = "0012a1b2c3df"
down_revision = "0011a1b2c3de"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "refresh_tokens",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id", UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False
        ),
        sa.Column("token_hash", sa.String(255), unique=True, nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "last_activity", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
        sa.Column("revoked_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_refresh_tokens_user_id", "refresh_tokens", ["user_id"])
    op.create_index(
        "ix_refresh_tokens_token_hash", "refresh_tokens", ["token_hash"], unique=True
    )


def downgrade():
    op.drop_index("ix_refresh_tokens_token_hash", table_name="refresh_tokens")
    op.drop_index("ix_refresh_tokens_user_id", table_name="refresh_tokens")
    op.drop_table("refresh_tokens")
