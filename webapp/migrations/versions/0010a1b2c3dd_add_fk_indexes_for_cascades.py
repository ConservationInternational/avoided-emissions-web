"""add indexes on FK columns used by cascade deletes

Revision ID: 0010a1b2c3dd
Revises: 0009a1b2c3dc
Create Date: 2026-03-06 12:00:00.000000

Adds indexes on foreign-key columns that are queried or checked during
user/task/site-set cascade deletes. PostgreSQL does not auto-index FK
columns, so without these the delete_user path requires sequential scans
on every referencing table.
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0010a1b2c3dd"
down_revision = "0009a1b2c3dc"
branch_labels = None
depends_on = None

# (table, column) pairs that need an index.
_FK_COLUMNS = [
    ("analysis_tasks", "submitted_by"),
    ("analysis_tasks", "site_set_id"),
    ("covariates", "started_by"),
    ("task_sites", "task_id"),
    ("task_results", "task_id"),
    ("task_results_total", "task_id"),
    ("user_site_sets", "user_id"),
    ("user_site_features", "site_set_id"),
    ("task_share_links", "created_by"),
    ("covariate_presets", "user_id"),
]


def _ix_name(table, column):
    return f"ix_{table}_{column}"


def upgrade():
    for table, column in _FK_COLUMNS:
        op.create_index(_ix_name(table, column), table, [column])


def downgrade():
    for table, column in reversed(_FK_COLUMNS):
        op.drop_index(_ix_name(table, column), table_name=table)
