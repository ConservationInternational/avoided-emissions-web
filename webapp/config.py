"""Application configuration loaded from environment variables."""

import logging
import os
import subprocess
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


def _get_git_revision() -> str:
    """Get git revision from environment or auto-detect from .git directory.

    Tries in order:
    1. GIT_REVISION environment variable (set by CI/CD)
    2. Running `git rev-parse HEAD` (works if git is installed)
    3. Reading .git/HEAD file directly (fallback for containers without git)

    Returns empty string if git revision cannot be determined.
    """
    # 1. Check environment variable first (production/CI)
    env_revision = os.environ.get("GIT_REVISION", "").strip()
    if env_revision:
        return env_revision

    # 2. Try running git command (development with git installed)
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=os.path.dirname(__file__) or ".",
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        pass

    # 3. Fallback: read .git/HEAD directly (Docker volume mount without git)
    try:
        # Look for .git in parent directories
        git_dir = None
        check_dir = os.path.dirname(os.path.abspath(__file__))
        for _ in range(5):  # Check up to 5 levels
            candidate = os.path.join(check_dir, ".git")
            if os.path.isdir(candidate):
                git_dir = candidate
                break
            parent = os.path.dirname(check_dir)
            if parent == check_dir:
                break
            check_dir = parent

        if not git_dir:
            return ""

        head_file = os.path.join(git_dir, "HEAD")
        if not os.path.isfile(head_file):
            return ""

        with open(head_file) as f:
            head_content = f.read().strip()

        # If HEAD is a ref (e.g., "ref: refs/heads/main"), follow it
        if head_content.startswith("ref: "):
            ref_path = head_content[5:]  # Remove "ref: " prefix
            ref_file = os.path.join(git_dir, ref_path)
            if os.path.isfile(ref_file):
                with open(ref_file) as f:
                    return f.read().strip()
            # Try packed-refs as fallback
            packed_refs = os.path.join(git_dir, "packed-refs")
            if os.path.isfile(packed_refs):
                with open(packed_refs) as f:
                    for line in f:
                        if line.startswith("#"):
                            continue
                        parts = line.strip().split()
                        if len(parts) >= 2 and parts[1] == ref_path:
                            return parts[0]
            return ""

        # HEAD contains a commit SHA directly (detached HEAD)
        if len(head_content) == 40 and all(
            c in "0123456789abcdef" for c in head_content
        ):
            return head_content

    except (OSError, IOError):
        pass

    return ""


class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY", "change-me-in-production")
    ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY", "")
    DATABASE_URL = _build_database_url()
    ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")
    AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
    S3_BUCKET = os.environ.get("S3_BUCKET", "avoided-emissions-data")
    S3_PREFIX = os.environ.get("S3_PREFIX", "avoided-emissions")
    GCS_BUCKET = os.environ.get("GCS_BUCKET", "")
    GCS_PREFIX = os.environ.get("GCS_PREFIX", "avoided-emissions/covariates_1km")
    GEE_PROJECT_ID = os.environ.get("GOOGLE_PROJECT_ID", "")
    GEE_ENDPOINT = os.environ.get("GEE_ENDPOINT", "")
    DEBUG = os.environ.get("DEBUG", "false").lower() == "true"
    # Auto-adopt untracked API executions.  Enabled by default only in
    # development; set ENABLE_TASK_ADOPTION=true in production to opt in.
    ENABLE_TASK_ADOPTION = (
        os.environ.get(
            "ENABLE_TASK_ADOPTION", str(ENVIRONMENT == "development")
        ).lower()
        == "true"
    )
    R_ANALYSIS_IMAGE_TAG = os.environ.get("R_ANALYSIS_IMAGE_TAG", "latest")
    ROLLBAR_ACCESS_TOKEN = os.environ.get("ROLLBAR_ACCESS_TOKEN", "")
    ROLLBAR_ENVIRONMENT = os.environ.get(
        "ROLLBAR_ENVIRONMENT", os.environ.get("ENVIRONMENT", "development")
    )
    GIT_REVISION = _get_git_revision()
    CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL", "redis://redis:6379/0")
    CELERY_RESULT_BACKEND = os.environ.get(
        "CELERY_RESULT_BACKEND", "redis://redis:6379/0"
    )

    # trends.earth API integration
    TRENDSEARTH_API_URL = os.environ.get(
        "TRENDSEARTH_API_URL", "https://api.trends.earth/api/v1"
    )
    TRENDSEARTH_CLIENT_ID = os.environ.get("TRENDSEARTH_CLIENT_ID", "")
    TRENDSEARTH_CLIENT_SECRET = os.environ.get("TRENDSEARTH_CLIENT_SECRET", "")
    TRENDSEARTH_SCRIPT_ID = os.environ.get("TRENDSEARTH_SCRIPT_ID", "")

    # AWS Batch overrides sent in params["batch"] when creating an execution.
    # Leave blank to use the defaults configured on the API / Script model.
    BATCH_JOB_QUEUE = os.environ.get("BATCH_JOB_QUEUE", "")
    BATCH_JOB_DEFINITION = os.environ.get("BATCH_JOB_DEFINITION", "")
    # Total timeout for the Batch job in seconds.  When using pipeline mode
    # (separate extract → match → summarize jobs), each step has its own
    # timeout in the pipeline descriptor.  This value is used as the
    # default fallback for any step that doesn't specify its own timeout.
    # Default: 50 400 s = 14 h.
    BATCH_TIMEOUT_SECONDS = int(os.environ.get("BATCH_TIMEOUT_SECONDS", "50400"))
    # Default memory (MiB) and vCPU count for Batch containers.
    # The extract step loads full COG grids into memory, so the default
    # must be large enough to avoid OOM kills.  Per-step overrides in
    # the pipeline descriptor can refine this for lighter steps.
    BATCH_MEMORY_MIB = int(os.environ.get("BATCH_MEMORY_MIB", "61440"))  # 60 GB
    BATCH_VCPUS = int(os.environ.get("BATCH_VCPUS", "4"))

    # SparkPost email configuration
    SPARKPOST_API_KEY = os.environ.get("SPARKPOST_API_KEY", "")
    SPARKPOST_FROM_EMAIL = os.environ.get(
        "SPARKPOST_FROM_EMAIL", "noreply@avoided-emissions.org"
    )
    # Public URL used to build password-reset links in emails
    APP_URL = os.environ.get("APP_URL", "http://localhost:8050")


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


def report_message(message, level="error", **extra):
    """Send an ad-hoc message to Rollbar (if configured).

    Unlike :func:`report_exception` this does not require an active
    exception context — use it to flag configuration errors or other
    noteworthy conditions that aren't Python exceptions.

    Parameters
    ----------
    message : str
        Human-readable description of the problem.
    level : str
        Rollbar severity level (``"critical"``, ``"error"``,
        ``"warning"``, ``"info"``, or ``"debug"``).
    **extra
        Arbitrary key/value pairs attached to the Rollbar item.
    """
    if not Config.ROLLBAR_ACCESS_TOKEN:
        return
    try:
        import rollbar

        rollbar.report_message(message, level=level, extra_data=extra or None)
    except Exception:
        _logger.debug("Failed to report message to Rollbar", exc_info=True)
