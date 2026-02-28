"""Application configuration loaded from environment variables."""

import logging
import os
import sys

_logger = logging.getLogger(__name__)


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
    GIT_REVISION = os.environ.get("GIT_REVISION", "")
    CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL", "redis://redis:6379/0")
    CELERY_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", "redis://redis:6379/0")

    # trends.earth API integration
    TRENDSEARTH_API_URL = os.environ.get(
        "TRENDSEARTH_API_URL", "https://api.trends.earth/api/v1"
    )
    TRENDSEARTH_API_KEY = os.environ.get("TRENDSEARTH_API_KEY", "")
    TRENDSEARTH_API_EMAIL = os.environ.get("TRENDSEARTH_API_EMAIL", "")
    TRENDSEARTH_API_PASSWORD = os.environ.get("TRENDSEARTH_API_PASSWORD", "")
    TRENDSEARTH_SCRIPT_ID = os.environ.get("TRENDSEARTH_SCRIPT_ID", "")
    # Set to True to route analysis tasks through the trends.earth API
    # instead of direct AWS Batch submission.
    USE_TRENDSEARTH_API = (
        os.environ.get("USE_TRENDSEARTH_API", "false").lower() == "true"
    )


def report_exception(**extra):
    """Report the current exception to Rollbar (if configured).

    Call from an ``except`` block to send the active exception to Rollbar.
    Silently does nothing when ``ROLLBAR_ACCESS_TOKEN`` is not set or when
    Rollbar has not been initialised yet.

    Parameters
    ----------
    **extra
        Arbitrary key/value pairs attached to the Rollbar item as
        ``extra_data``.
    """
    if not Config.ROLLBAR_ACCESS_TOKEN:
        return
    try:
        import rollbar

        rollbar.report_exc_info(sys.exc_info(), extra_data=extra or None)
    except Exception:
        _logger.debug("Failed to report exception to Rollbar", exc_info=True)
