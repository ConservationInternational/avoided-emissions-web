"""add query-performance indexes

Revision ID: 0011a1b2c3de
Revises: 0010a1b2c3dd
Create Date: 2026-03-06 18:00:00.000000

Adds indexes on columns that are filtered or ordered in hot query paths
but currently lack coverage.

* ``gee_export_metadata (covariate_id, status, created_at DESC)`` —
  composite that covers all six lookup patterns in the merge and
  polling tasks (filter by covariate_id + status, order by created_at
  DESC).  Without this every Celery Beat tick seq-scans the table.

* ``trendsearth_credentials (te_user_id)`` — B-tree used by
  ``adopt_api_execution`` in the 30-second polling loop to match API
  executions to local users.

* ``analysis_tasks (extract_job_id)`` — partial index (WHERE NOT NULL)
  used by ``poll_batch_tasks`` every 30 seconds to build the set of
  known API execution IDs.

* ``task_results (task_id, site_id, year)`` — composite covering the
  task-detail page query which filters on ``task_id`` and orders by
  ``site_id, year``.  Eliminates a filesort on every page load.

* ``covariate_presets (user_id, name)`` — unique composite that both
  enforces the business rule (preset names unique per user) and speeds
  the upsert check in ``save_covariate_preset``.
"""

from alembic import op

revision = "0011a1b2c3de"
down_revision = "0010a1b2c3dd"
branch_labels = None
depends_on = None


def upgrade():
    # 1. gee_export_metadata: covers merge/polling lookups
    #    (covariate_id + status filter, created_at DESC ordering)
    op.execute(
        "CREATE INDEX ix_gee_export_meta_cov_status_created "
        "ON gee_export_metadata (covariate_id, status, created_at DESC)"
    )

    # 2. trendsearth_credentials: reverse-lookup by TE user id
    op.create_index(
        "ix_te_credentials_te_user_id",
        "trendsearth_credentials",
        ["te_user_id"],
    )

    # 3. analysis_tasks: partial index on extract_job_id for polling
    op.execute(
        "CREATE INDEX ix_tasks_extract_job_id "
        "ON analysis_tasks (extract_job_id) "
        "WHERE extract_job_id IS NOT NULL"
    )

    # 4. task_results: composite covering detail-page ORDER BY
    op.create_index(
        "ix_results_task_site_year",
        "task_results",
        ["task_id", "site_id", "year"],
    )

    # 5. covariate_presets: unique per user+name
    op.create_index(
        "ix_presets_user_name",
        "covariate_presets",
        ["user_id", "name"],
        unique=True,
    )


def downgrade():
    op.drop_index("ix_presets_user_name", table_name="covariate_presets")
    op.drop_index("ix_results_task_site_year", table_name="task_results")
    op.execute("DROP INDEX IF EXISTS ix_tasks_extract_job_id")
    op.drop_index("ix_te_credentials_te_user_id", table_name="trendsearth_credentials")
    op.drop_index(
        "ix_gee_export_meta_cov_status_created",
        table_name="gee_export_metadata",
    )
