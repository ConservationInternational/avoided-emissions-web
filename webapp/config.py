"""Application configuration loaded from environment variables."""

import os


def _build_database_url() -> str:
    """Construct DATABASE_URL from individual POSTGRES_* vars if not set."""
    explicit = os.environ.get("DATABASE_URL")
    if explicit:
        return explicit
    user = os.environ.get("POSTGRES_USER", "ae_user")
    password = os.environ.get("POSTGRES_PASSWORD", "ae_password")
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "avoided_emissions")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY", "change-me-in-production")
    DATABASE_URL = _build_database_url()
    AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
    AWS_BATCH_JOB_QUEUE = os.environ.get("AWS_BATCH_JOB_QUEUE", "")
    AWS_BATCH_JOB_DEFINITION = os.environ.get("AWS_BATCH_JOB_DEFINITION", "")
    S3_BUCKET = os.environ.get("S3_BUCKET", "")
    S3_PREFIX = os.environ.get("S3_PREFIX", "avoided-emissions")
    GCS_BUCKET = os.environ.get("GCS_BUCKET", "")
    GCS_PREFIX = os.environ.get("GCS_PREFIX", "avoided-emissions/covariates")
    GEE_PROJECT_ID = os.environ.get("GOOGLE_PROJECT_ID", "")
    GEE_ENDPOINT = os.environ.get("GEE_ENDPOINT", "")
    DEBUG = os.environ.get("DEBUG", "false").lower() == "true"
    R_ANALYSIS_IMAGE_TAG = os.environ.get("R_ANALYSIS_IMAGE_TAG", "latest")
    ROLLBAR_ACCESS_TOKEN = os.environ.get("ROLLBAR_ACCESS_TOKEN", "")
    ROLLBAR_ENVIRONMENT = os.environ.get("ROLLBAR_ENVIRONMENT", os.environ.get("ENVIRONMENT", "development"))
