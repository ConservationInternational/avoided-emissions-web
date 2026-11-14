"""Unify covariate and matching settings presets, discarding old presets.

Revision ID: 0034a1b2c3f5
Revises: 0033a1b2c3f4
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0034a1b2c3f5"
down_revision: str | None = "0033a1b2c3f4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "matching_presets",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("covariates", postgresql.ARRAY(sa.Text), nullable=False),
        sa.Column("exact_match_vars", postgresql.ARRAY(sa.Text), nullable=True),
        sa.Column("settings", postgresql.JSON, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True)),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_matching_presets_user_id", "matching_presets", ["user_id"])
    op.create_index(
        "ix_matching_presets_user_name",
        "matching_presets",
        ["user_id", "name"],
        unique=True,
    )
    op.drop_table("covariate_presets")
    op.drop_table("matching_settings_presets")


def downgrade() -> None:
    for table, columns in (
        (
            "covariate_presets",
            [
                sa.Column("covariates", postgresql.ARRAY(sa.Text), nullable=False),
                sa.Column("exact_match_vars", postgresql.ARRAY(sa.Text)),
            ],
        ),
        (
            "matching_settings_presets",
            [sa.Column("settings", postgresql.JSON, nullable=False)],
        ),
    ):
        op.create_table(
            table,
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("name", sa.String(255), nullable=False),
            *columns,
            sa.Column("created_at", sa.DateTime(timezone=True)),
            sa.Column("updated_at", sa.DateTime(timezone=True)),
        )
        index_name = (
            "idx_covariate_presets_user_id"
            if table == "covariate_presets"
            else "ix_matching_settings_presets_user_id"
        )
        op.create_index(index_name, table, ["user_id"])
    op.create_index(
        "ix_presets_user_name",
        "covariate_presets",
        ["user_id", "name"],
        unique=True,
    )
    op.drop_table("matching_presets")
