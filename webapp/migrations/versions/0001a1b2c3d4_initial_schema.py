"""initial schema with unified covariates table

Revision ID: 0001a1b2c3d4
Revises: None
Create Date: 2025-01-01 00:00:00.000000

This is the baseline migration representing the schema created by
database/init.sql.  On a fresh database that was initialised with
init.sql, run:

    alembic stamp 0001a1b2c3d4

to mark the database at this revision without re-running the DDL.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0001a1b2c3d4"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Extensions
    op.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
    op.execute('CREATE EXTENSION IF NOT EXISTS "postgis"')

    # Enum types
    user_role = postgresql.ENUM("admin", "user", name="user_role", create_type=False)
    task_status = postgresql.ENUM(
        "pending", "submitted", "running", "succeeded", "failed", "cancelled",
        name="task_status", create_type=False,
    )
    covariate_status = postgresql.ENUM(
        "pending_export", "exporting", "exported",
        "pending_merge", "merging", "merged",
        "failed", "cancelled",
        name="covariate_status", create_type=False,
    )

    op.execute("CREATE TYPE user_role AS ENUM ('admin', 'user')")
    op.execute(
        "CREATE TYPE task_status AS ENUM "
        "('pending', 'submitted', 'running', 'succeeded', 'failed', 'cancelled')"
    )
    op.execute(
        "CREATE TYPE covariate_status AS ENUM "
        "('pending_export', 'exporting', 'exported', "
        "'pending_merge', 'merging', 'merged', 'failed', 'cancelled')"
    )

    # Users
    op.create_table(
        "users",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("uuid_generate_v4()"), primary_key=True),
        sa.Column("email", sa.String(255), unique=True, nullable=False),
        sa.Column("password_hash", sa.String(255), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("role", user_role, nullable=False, server_default="user"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("last_login", sa.DateTime(timezone=True)),
        sa.Column("is_active", sa.Boolean(), server_default="true"),
        sa.Column("is_approved", sa.Boolean(), server_default="false"),
    )
    op.create_index("idx_users_email", "users", ["email"])

    # Covariates (unified export + merge tracking)
    op.create_table(
        "covariates",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("uuid_generate_v4()"), primary_key=True),
        sa.Column("covariate_name", sa.String(100), nullable=False),
        sa.Column("gee_task_id", sa.String(255)),
        sa.Column("gcs_bucket", sa.String(255)),
        sa.Column("gcs_prefix", sa.String(500)),
        sa.Column("output_bucket", sa.String(255)),
        sa.Column("output_prefix", sa.String(500)),
        sa.Column("n_tiles", sa.Integer()),
        sa.Column("merged_url", sa.String(1000)),
        sa.Column("size_bytes", sa.BigInteger()),
        sa.Column("status", covariate_status, nullable=False, server_default="pending_export"),
        sa.Column("started_by", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id")),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("error_message", sa.Text()),
        sa.Column("metadata", postgresql.JSONB(), server_default="{}"),
    )
    op.create_index("idx_covariates_status", "covariates", ["status"])
    op.create_index("idx_covariates_name", "covariates", ["covariate_name"])

    # Analysis tasks
    op.create_table(
        "analysis_tasks",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("uuid_generate_v4()"), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text()),
        sa.Column("submitted_by", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("status", task_status, nullable=False, server_default="pending"),
        sa.Column("extract_job_id", sa.String(255)),
        sa.Column("match_job_id", sa.String(255)),
        sa.Column("summarize_job_id", sa.String(255)),
        sa.Column("config", postgresql.JSONB(), nullable=False, server_default="{}"),
        sa.Column("covariates", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column("n_sites", sa.Integer()),
        sa.Column("sites_s3_uri", sa.String(500)),
        sa.Column("config_s3_uri", sa.String(500)),
        sa.Column("results_s3_uri", sa.String(500)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("submitted_at", sa.DateTime(timezone=True)),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("error_message", sa.Text()),
        sa.Column("metadata", postgresql.JSONB(), server_default="{}"),
    )
    op.create_index("idx_tasks_status", "analysis_tasks", ["status"])
    op.create_index("idx_tasks_user", "analysis_tasks", ["submitted_by"])
    op.create_index("idx_tasks_created", "analysis_tasks", ["created_at"], postgresql_ops={"created_at": "DESC"})

    # Task sites
    op.create_table(
        "task_sites",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("uuid_generate_v4()"), primary_key=True),
        sa.Column("task_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("analysis_tasks.id", ondelete="CASCADE"), nullable=False),
        sa.Column("site_id", sa.String(100), nullable=False),
        sa.Column("site_name", sa.String(255)),
        sa.Column("start_date", sa.Date()),
        sa.Column("end_date", sa.Date()),
        sa.Column("area_ha", sa.Float()),
        sa.UniqueConstraint("task_id", "site_id"),
    )
    op.create_index("idx_task_sites_task", "task_sites", ["task_id"])
    # PostGIS geometry column and spatial index are handled by init.sql
    # and cannot be expressed via pure SQLAlchemy.
    op.execute(
        "SELECT AddGeometryColumn('task_sites', 'geometry', 4326, 'MULTIPOLYGON', 2)"
    )
    op.execute(
        "CREATE INDEX idx_task_sites_geom ON task_sites USING GIST (geometry)"
    )

    # Task results (per-site per-year)
    op.create_table(
        "task_results",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("uuid_generate_v4()"), primary_key=True),
        sa.Column("task_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("analysis_tasks.id", ondelete="CASCADE"), nullable=False),
        sa.Column("site_id", sa.String(100), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("forest_loss_avoided_ha", sa.Float()),
        sa.Column("emissions_avoided_mgco2e", sa.Float()),
        sa.Column("n_matched_pixels", sa.Integer()),
        sa.Column("sampled_fraction", sa.Float()),
        sa.UniqueConstraint("task_id", "site_id", "year"),
    )
    op.create_index("idx_results_task", "task_results", ["task_id"])
    op.create_index("idx_results_site", "task_results", ["site_id"])

    # Task results total (per-site aggregate)
    op.create_table(
        "task_results_total",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("uuid_generate_v4()"), primary_key=True),
        sa.Column("task_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("analysis_tasks.id", ondelete="CASCADE"), nullable=False),
        sa.Column("site_id", sa.String(100), nullable=False),
        sa.Column("site_name", sa.String(255)),
        sa.Column("forest_loss_avoided_ha", sa.Float()),
        sa.Column("emissions_avoided_mgco2e", sa.Float()),
        sa.Column("area_ha", sa.Float()),
        sa.Column("n_matched_pixels", sa.Integer()),
        sa.Column("sampled_fraction", sa.Float()),
        sa.Column("first_year", sa.Integer()),
        sa.Column("last_year", sa.Integer()),
        sa.Column("n_years", sa.Integer()),
        sa.UniqueConstraint("task_id", "site_id"),
    )
    op.create_index("idx_results_total_task", "task_results_total", ["task_id"])


def downgrade() -> None:
    op.drop_table("task_results_total")
    op.drop_table("task_results")
    op.drop_table("task_sites")
    op.drop_table("analysis_tasks")
    op.drop_table("covariates")
    op.drop_table("users")
    op.execute("DROP TYPE IF EXISTS covariate_status")
    op.execute("DROP TYPE IF EXISTS task_status")
    op.execute("DROP TYPE IF EXISTS user_role")
