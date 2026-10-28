"""Use Trends.Earth OIDC identities instead of local user accounts

Users now sign in with their Trends.Earth account, and the user's id in this
app is the Trends.Earth ``sub`` (a UUID).  The local ``users`` table and the
password, refresh-token and stored-credential tables are dropped together
with every foreign key that referenced ``users.id``; the user-id columns are
kept as plain UUIDs holding the Trends.Earth subject.

``analysis_tasks`` gains a snapshot of the submitter's name and email (for
display without a user table) and the id of the server-side session used to
submit the analysis on the user's behalf.

This migration is not reversible: local accounts cannot be reconstructed.

Revision ID: 0030a1b2c3f1
Revises: 0029a1b2c3f0
Create Date: 2026-11-02 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0030a1b2c3f1"
down_revision: str | None = "0029a1b2c3f0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("DROP TABLE IF EXISTS refresh_tokens")
    op.execute("DROP TABLE IF EXISTS password_reset_tokens")
    op.execute("DROP TABLE IF EXISTS trendsearth_credentials")
    # CASCADE removes the foreign keys on analysis_tasks.submitted_by,
    # covariates.started_by, user_site_sets.user_id, user_site_uploads.user_id,
    # covariate_presets.user_id, matching_settings_presets.user_id and
    # task_share_links.created_by; the columns themselves are kept.
    op.execute("DROP TABLE IF EXISTS users CASCADE")
    op.execute("DROP TYPE IF EXISTS user_role")

    op.add_column(
        "analysis_tasks", sa.Column("submitter_email", sa.String(255), nullable=True)
    )
    op.add_column(
        "analysis_tasks", sa.Column("submitter_name", sa.String(255), nullable=True)
    )
    op.add_column(
        "analysis_tasks",
        sa.Column("submit_session_id", sa.String(128), nullable=True),
    )


def downgrade() -> None:
    raise NotImplementedError(
        "Local user accounts cannot be restored; restore from a backup instead."
    )
