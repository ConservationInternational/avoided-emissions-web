"""initial schema

Revision ID: 0001a1b2c3d4
Revises: None
Create Date: 2025-01-01 00:00:00.000000

Baseline migration that creates the full schema from scratch.
"""

from typing import Sequence, Union

import geoalchemy2
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
        "pending",
        "submitted",
        "running",
        "succeeded",
        "failed",
        "cancelled",
        name="task_status",
        create_type=False,
    )
    covariate_status = postgresql.ENUM(
        "pending_export",
        "exporting",
        "exported",
        "pending_merge",
        "merging",
        "merged",
        "rasterizing",
        "failed",
        "cancelled",
        name="covariate_status",
        create_type=False,
    )

    op.execute("CREATE TYPE user_role AS ENUM ('admin', 'user')")
    op.execute(
        "CREATE TYPE task_status AS ENUM "
        "('pending', 'submitted', 'running', 'succeeded', 'failed', 'cancelled')"
    )
    op.execute(
        "CREATE TYPE covariate_status AS ENUM "
        "('pending_export', 'exporting', 'exported', "
        "'pending_merge', 'merging', 'merged', 'rasterizing', "
        "'failed', 'cancelled')"
    )

    # ── Users ──
    op.create_table(
        "users",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("uuid_generate_v4()"),
            primary_key=True,
        ),
        sa.Column("email", sa.String(255), unique=True, nullable=False),
        sa.Column("password_hash", sa.String(255), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("role", user_role, nullable=False, server_default="user"),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")
        ),
        sa.Column("last_login", sa.DateTime(timezone=True)),
        sa.Column("is_active", sa.Boolean(), server_default="true"),
        sa.Column("is_approved", sa.Boolean(), server_default="false"),
    )
    op.create_index("idx_users_email", "users", ["email"])

    # ── Covariates  ──
    op.create_table(
        "covariates",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("uuid_generate_v4()"),
            primary_key=True,
        ),
        sa.Column("covariate_name", sa.String(100), nullable=False),
        sa.Column("gee_task_id", sa.String(255)),
        sa.Column("gcs_bucket", sa.String(255)),
        sa.Column("gcs_prefix", sa.String(500)),
        sa.Column("output_bucket", sa.String(255)),
        sa.Column("output_prefix", sa.String(500)),
        sa.Column("n_tiles", sa.Integer()),
        sa.Column("merged_url", sa.String(1000)),
        sa.Column("size_bytes", sa.BigInteger()),
        sa.Column(
            "status", covariate_status, nullable=False, server_default="pending_export"
        ),
        sa.Column(
            "started_by", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id")
        ),
        sa.Column(
            "started_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("error_message", sa.Text()),
        sa.Column("metadata", postgresql.JSONB(), server_default="{}"),
    )
    op.create_index("idx_covariates_status", "covariates", ["status"])
    op.create_index("idx_covariates_name", "covariates", ["covariate_name"])

    # ── Analysis tasks ──
    op.create_table(
        "analysis_tasks",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("uuid_generate_v4()"),
            primary_key=True,
        ),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text()),
        sa.Column(
            "submitted_by",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id"),
            nullable=False,
        ),
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
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")
        ),
        sa.Column("submitted_at", sa.DateTime(timezone=True)),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("error_message", sa.Text()),
        sa.Column("metadata", postgresql.JSONB(), server_default="{}"),
    )
    op.create_index("idx_tasks_status", "analysis_tasks", ["status"])
    op.create_index("idx_tasks_user", "analysis_tasks", ["submitted_by"])
    op.create_index(
        "idx_tasks_created",
        "analysis_tasks",
        ["created_at"],
        postgresql_ops={"created_at": "DESC"},
    )

    # ── Task sites ──
    op.create_table(
        "task_sites",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("uuid_generate_v4()"),
            primary_key=True,
        ),
        sa.Column(
            "task_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("analysis_tasks.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("site_id", sa.String(100), nullable=False),
        sa.Column("site_name", sa.String(255)),
        sa.Column("start_date", sa.Date()),
        sa.Column("end_date", sa.Date()),
        sa.Column("area_ha", sa.Float()),
        sa.UniqueConstraint("task_id", "site_id"),
    )
    op.create_index("idx_task_sites_task", "task_sites", ["task_id"])
    op.execute(
        "SELECT AddGeometryColumn('task_sites', 'geometry', 4326, 'MULTIPOLYGON', 2)"
    )
    op.execute("CREATE INDEX idx_task_sites_geom ON task_sites USING GIST (geometry)")

    # ── Task results (per-site per-year) ──
    op.create_table(
        "task_results",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("uuid_generate_v4()"),
            primary_key=True,
        ),
        sa.Column(
            "task_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("analysis_tasks.id", ondelete="CASCADE"),
            nullable=False,
        ),
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

    # ── Task results total (per-site aggregate) ──
    op.create_table(
        "task_results_total",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("uuid_generate_v4()"),
            primary_key=True,
        ),
        sa.Column(
            "task_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("analysis_tasks.id", ondelete="CASCADE"),
            nullable=False,
        ),
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

    # ── Covariate presets ──
    op.create_table(
        "covariate_presets",
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
        ),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("covariates", postgresql.ARRAY(sa.Text()), nullable=False),
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
    op.create_index("idx_covariate_presets_user_id", "covariate_presets", ["user_id"])

    # ── Trends.Earth credentials ──
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

    # ── GeoBoundaries ADM0 ──
    op.create_table(
        "geoboundaries_adm0",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("shape_group", sa.String(length=10), nullable=False),
        sa.Column("shape_name", sa.String(length=255), nullable=True),
        sa.Column("shape_type", sa.String(length=20), nullable=True),
        sa.Column(
            "geom",
            geoalchemy2.types.Geometry(
                geometry_type="MULTIPOLYGON",
                srid=4326,
                from_text="ST_GeomFromEWKT",
                name="geometry",
            ),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_geoboundaries_adm0_shape_group", "geoboundaries_adm0", ["shape_group"]
    )
    op.execute(
        "CREATE INDEX ix_geoboundaries_adm0_geom "
        "ON geoboundaries_adm0 USING GIST (geom)"
    )

    # ── GeoBoundaries ADM1 ──
    op.create_table(
        "geoboundaries_adm1",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("shape_group", sa.String(length=10), nullable=False),
        sa.Column("shape_name", sa.String(length=255), nullable=True),
        sa.Column("shape_id", sa.String(length=100), nullable=True),
        sa.Column("shape_type", sa.String(length=20), nullable=True),
        sa.Column(
            "geom",
            geoalchemy2.types.Geometry(
                geometry_type="MULTIPOLYGON",
                srid=4326,
                from_text="ST_GeomFromEWKT",
                name="geometry",
            ),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_geoboundaries_adm1_shape_group", "geoboundaries_adm1", ["shape_group"]
    )
    op.execute(
        "CREATE INDEX ix_geoboundaries_adm1_geom "
        "ON geoboundaries_adm1 USING GIST (geom)"
    )

    # ── GeoBoundaries ADM2 ──
    op.create_table(
        "geoboundaries_adm2",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("shape_group", sa.String(length=10), nullable=False),
        sa.Column("shape_name", sa.String(length=255), nullable=True),
        sa.Column("shape_id", sa.String(length=100), nullable=True),
        sa.Column("shape_type", sa.String(length=20), nullable=True),
        sa.Column(
            "geom",
            geoalchemy2.types.Geometry(
                geometry_type="MULTIPOLYGON",
                srid=4326,
                from_text="ST_GeomFromEWKT",
                name="geometry",
            ),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_geoboundaries_adm2_shape_group", "geoboundaries_adm2", ["shape_group"]
    )
    op.execute(
        "CREATE INDEX ix_geoboundaries_adm2_geom "
        "ON geoboundaries_adm2 USING GIST (geom)"
    )

    # ── RESOLVE Ecoregions ──
    op.create_table(
        "ecoregions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("eco_id", sa.Integer(), nullable=False),
        sa.Column("eco_name", sa.String(length=255), nullable=True),
        sa.Column("biome_num", sa.Integer(), nullable=True),
        sa.Column("biome_name", sa.String(length=255), nullable=True),
        sa.Column("realm", sa.String(length=100), nullable=True),
        sa.Column("nnh", sa.Float(), nullable=True),
        sa.Column("color", sa.String(length=10), nullable=True),
        sa.Column("color_bio", sa.String(length=10), nullable=True),
        sa.Column("color_nnh", sa.String(length=10), nullable=True),
        sa.Column(
            "geom",
            geoalchemy2.types.Geometry(
                geometry_type="MULTIPOLYGON",
                srid=4326,
                from_text="ST_GeomFromEWKT",
                name="geometry",
            ),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_ecoregions_eco_id", "ecoregions", ["eco_id"])
    op.execute("CREATE INDEX ix_ecoregions_geom ON ecoregions USING GIST (geom)")

    # ── WDPA Protected Areas (Feb 2026 GDB schema) ──
    op.create_table(
        "wdpa",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("site_id", sa.Integer(), nullable=False),
        sa.Column("site_pid", sa.String(length=100), nullable=True),
        sa.Column("site_type", sa.String(length=50), nullable=True),
        sa.Column("name_eng", sa.String(length=500), nullable=True),
        sa.Column("name", sa.String(length=500), nullable=True),
        sa.Column("desig", sa.String(length=500), nullable=True),
        sa.Column("desig_eng", sa.String(length=500), nullable=True),
        sa.Column("desig_type", sa.String(length=100), nullable=True),
        sa.Column("iucn_cat", sa.String(length=20), nullable=True),
        sa.Column("int_crit", sa.String(length=100), nullable=True),
        sa.Column("realm", sa.String(length=50), nullable=True),
        sa.Column("rep_m_area", sa.Float(), nullable=True),
        sa.Column("gis_m_area", sa.Float(), nullable=True),
        sa.Column("rep_area", sa.Float(), nullable=True),
        sa.Column("gis_area", sa.Float(), nullable=True),
        sa.Column("no_take", sa.String(length=50), nullable=True),
        sa.Column("no_tk_area", sa.Float(), nullable=True),
        sa.Column("status", sa.String(length=100), nullable=True),
        sa.Column("status_yr", sa.Integer(), nullable=True),
        sa.Column("gov_type", sa.String(length=255), nullable=True),
        sa.Column("govsubtype", sa.String(length=255), nullable=True),
        sa.Column("own_type", sa.String(length=100), nullable=True),
        sa.Column("ownsubtype", sa.String(length=255), nullable=True),
        sa.Column("mang_auth", sa.String(length=500), nullable=True),
        sa.Column("mang_plan", sa.String(length=500), nullable=True),
        sa.Column("verif", sa.String(length=100), nullable=True),
        sa.Column("metadataid", sa.Integer(), nullable=True),
        sa.Column("prnt_iso3", sa.String(length=100), nullable=True),
        sa.Column("iso3", sa.String(length=100), nullable=True),
        sa.Column("supp_info", sa.Text(), nullable=True),
        sa.Column("cons_obj", sa.Text(), nullable=True),
        sa.Column("inlnd_wtrs", sa.String(length=50), nullable=True),
        sa.Column("oecm_asmt", sa.String(length=50), nullable=True),
        sa.Column(
            "geom",
            geoalchemy2.types.Geometry(
                geometry_type="MULTIPOLYGON",
                srid=4326,
                from_text="ST_GeomFromEWKT",
                name="geometry",
            ),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_wdpa_site_id", "wdpa", ["site_id"])
    op.create_index("ix_wdpa_iso3", "wdpa", ["iso3"])
    op.create_index("ix_wdpa_iucn_cat", "wdpa", ["iucn_cat"])
    op.execute("CREATE INDEX ix_wdpa_geom ON wdpa USING GIST (geom)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_wdpa_geom")
    op.drop_index("ix_wdpa_iucn_cat", table_name="wdpa")
    op.drop_index("ix_wdpa_iso3", table_name="wdpa")
    op.drop_index("ix_wdpa_site_id", table_name="wdpa")
    op.drop_table("wdpa")
    op.execute("DROP INDEX IF EXISTS ix_ecoregions_geom")
    op.drop_index("ix_ecoregions_eco_id", table_name="ecoregions")
    op.drop_table("ecoregions")
    op.execute("DROP INDEX IF EXISTS ix_geoboundaries_adm2_geom")
    op.drop_index("ix_geoboundaries_adm2_shape_group", table_name="geoboundaries_adm2")
    op.drop_table("geoboundaries_adm2")
    op.execute("DROP INDEX IF EXISTS ix_geoboundaries_adm1_geom")
    op.drop_index("ix_geoboundaries_adm1_shape_group", table_name="geoboundaries_adm1")
    op.drop_table("geoboundaries_adm1")
    op.execute("DROP INDEX IF EXISTS ix_geoboundaries_adm0_geom")
    op.drop_index("ix_geoboundaries_adm0_shape_group", table_name="geoboundaries_adm0")
    op.drop_table("geoboundaries_adm0")
    op.drop_index(
        "idx_trendsearth_credentials_user_id",
        table_name="trendsearth_credentials",
    )
    op.drop_table("trendsearth_credentials")
    op.drop_index("idx_covariate_presets_user_id", table_name="covariate_presets")
    op.drop_table("covariate_presets")
    op.drop_index("idx_results_total_task", table_name="task_results_total")
    op.drop_table("task_results_total")
    op.drop_index("idx_results_site", table_name="task_results")
    op.drop_index("idx_results_task", table_name="task_results")
    op.drop_table("task_results")
    op.execute("DROP INDEX IF EXISTS idx_task_sites_geom")
    op.drop_index("idx_task_sites_task", table_name="task_sites")
    op.drop_table("task_sites")
    op.drop_index("idx_tasks_created", table_name="analysis_tasks")
    op.drop_index("idx_tasks_user", table_name="analysis_tasks")
    op.drop_index("idx_tasks_status", table_name="analysis_tasks")
    op.drop_table("analysis_tasks")
    op.drop_index("idx_covariates_name", table_name="covariates")
    op.drop_index("idx_covariates_status", table_name="covariates")
    op.drop_table("covariates")
    op.drop_index("idx_users_email", table_name="users")
    op.drop_table("users")
    op.execute("DROP TYPE IF EXISTS covariate_status")
    op.execute("DROP TYPE IF EXISTS task_status")
    op.execute("DROP TYPE IF EXISTS user_role")
