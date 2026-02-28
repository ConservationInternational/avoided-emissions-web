-- Database initialization for the avoided emissions web application.
-- Creates tables for users, GEE export tasks, analysis tasks, and results.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "postgis";

-- User roles
CREATE TYPE user_role AS ENUM ('admin', 'user');

-- Task status values
CREATE TYPE task_status AS ENUM (
    'pending',
    'submitted',
    'running',
    'succeeded',
    'failed',
    'cancelled'
);

-- Covariate lifecycle status (export from GEE → merge tiles → COG on S3)
CREATE TYPE covariate_status AS ENUM (
    'pending_export',
    'exporting',
    'exported',
    'pending_merge',
    'merging',
    'merged',
    'failed',
    'cancelled'
);


-- Users table
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    role user_role NOT NULL DEFAULT 'user',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_login TIMESTAMP WITH TIME ZONE,
    is_active BOOLEAN DEFAULT TRUE,
    is_approved BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_users_email ON users(email);


-- Covariates table: tracks the full lifecycle of each covariate layer
-- from GEE export through to merged Cloud-Optimized GeoTIFF on S3.
CREATE TABLE covariates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    covariate_name VARCHAR(100) NOT NULL,

    -- GEE export fields
    gee_task_id VARCHAR(255),
    gcs_bucket VARCHAR(255),
    gcs_prefix VARCHAR(500),

    -- COG merge / output fields
    output_bucket VARCHAR(255),
    output_prefix VARCHAR(500),
    n_tiles INTEGER,
    merged_url VARCHAR(1000),
    size_bytes BIGINT,

    -- Lifecycle
    status covariate_status NOT NULL DEFAULT 'pending_export',
    started_by UUID REFERENCES users(id),
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    metadata JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX idx_covariates_status ON covariates(status);
CREATE INDEX idx_covariates_name ON covariates(covariate_name);


-- Analysis tasks (submitted to AWS Batch)
CREATE TABLE analysis_tasks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    submitted_by UUID NOT NULL REFERENCES users(id),
    status task_status NOT NULL DEFAULT 'pending',

    -- AWS Batch job IDs
    extract_job_id VARCHAR(255),
    match_job_id VARCHAR(255),
    summarize_job_id VARCHAR(255),

    -- Configuration
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    covariates TEXT[] NOT NULL,
    n_sites INTEGER,

    -- S3 locations
    sites_s3_uri VARCHAR(500),
    config_s3_uri VARCHAR(500),
    results_s3_uri VARCHAR(500),

    -- Timing
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    submitted_at TIMESTAMP WITH TIME ZONE,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,

    -- Error tracking
    error_message TEXT,
    metadata JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX idx_tasks_status ON analysis_tasks(status);
CREATE INDEX idx_tasks_user ON analysis_tasks(submitted_by);
CREATE INDEX idx_tasks_created ON analysis_tasks(created_at DESC);


-- Sites within each analysis task
CREATE TABLE task_sites (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    task_id UUID NOT NULL REFERENCES analysis_tasks(id) ON DELETE CASCADE,
    site_id VARCHAR(100) NOT NULL,
    site_name VARCHAR(255),
    start_date DATE,
    end_date DATE,
    area_ha DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 4326),
    UNIQUE(task_id, site_id)
);

CREATE INDEX idx_task_sites_task ON task_sites(task_id);
CREATE INDEX idx_task_sites_geom ON task_sites USING GIST (geometry);


-- Per-site per-year results
CREATE TABLE task_results (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    task_id UUID NOT NULL REFERENCES analysis_tasks(id) ON DELETE CASCADE,
    site_id VARCHAR(100) NOT NULL,
    year INTEGER NOT NULL,
    forest_loss_avoided_ha DOUBLE PRECISION,
    emissions_avoided_mgco2e DOUBLE PRECISION,
    n_matched_pixels INTEGER,
    sampled_fraction DOUBLE PRECISION,
    UNIQUE(task_id, site_id, year)
);

CREATE INDEX idx_results_task ON task_results(task_id);
CREATE INDEX idx_results_site ON task_results(site_id);


-- Per-site total results
CREATE TABLE task_results_total (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    task_id UUID NOT NULL REFERENCES analysis_tasks(id) ON DELETE CASCADE,
    site_id VARCHAR(100) NOT NULL,
    site_name VARCHAR(255),
    forest_loss_avoided_ha DOUBLE PRECISION,
    emissions_avoided_mgco2e DOUBLE PRECISION,
    area_ha DOUBLE PRECISION,
    n_matched_pixels INTEGER,
    sampled_fraction DOUBLE PRECISION,
    first_year INTEGER,
    last_year INTEGER,
    n_years INTEGER,
    UNIQUE(task_id, site_id)
);

CREATE INDEX idx_results_total_task ON task_results_total(task_id);


-- NOTE: No default admin user is seeded here.  Create one via:
--   docker compose exec webapp python -c "
--     from auth import hash_password; from models import User, get_db;
--     db = get_db();
--     db.add(User(email='admin@example.org',
--                 password_hash=hash_password('CHANGE_ME'),
--                 name='Administrator', role='admin',
--                 is_approved=True));
--     db.commit(); db.close()
--   "
-- Be sure to use a strong, unique password.
