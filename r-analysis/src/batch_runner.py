"""Lightweight Batch runner for trends.earth script containers.

Downloads execution parameters from S3, delegates to ``main.run()``,
and uploads the results dict back to S3.

Status management (RUNNING → FINISHED / FAILED) is handled entirely by
the API's ``monitor_batch_executions`` periodic task — this runner only
needs S3 access (provided by the Batch job's IAM role), not API
credentials.

Usage
-----
Typically invoked from the container's entrypoint when ``EXECUTION_ID``
is set::

    python batch_runner.py           # step determined by params["step"]
    python batch_runner.py extract   # override step
"""

import gzip
import json
import logging
import os
import sys
import tempfile
import uuid
from pathlib import Path

import boto3


def _ensure_scripts_dir_on_path():
    """Add r-analysis/scripts to sys.path for shared Python helpers."""
    current_dir = Path(__file__).resolve().parent
    candidate_dirs = (current_dir / "scripts", current_dir.parent / "scripts")
    for scripts_dir in candidate_dirs:
        scripts_dir_str = str(scripts_dir)
        if scripts_dir.is_dir() and scripts_dir_str not in sys.path:
            sys.path.insert(0, scripts_dir_str)
            break


_ensure_scripts_dir_on_path()

from logging_utils import configure_third_party_logging, parse_log_level  # noqa: E402


def _configure_logging():
    """Configure logging with reduced third-party noise by default."""
    app_log_level = parse_log_level(
        os.getenv("BATCH_RUNNER_LOG_LEVEL", "INFO"),
        default=logging.INFO,
    )

    logging.basicConfig(
        level=app_log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        stream=sys.stderr,
    )

    configure_third_party_logging()

    batch_logger = logging.getLogger("batch_runner")
    batch_logger.setLevel(app_log_level)
    return batch_logger


logger = _configure_logging()

# ---------------------------------------------------------------------------
# S3 helpers (self-contained — no gefcore dependency)
# ---------------------------------------------------------------------------

EXECUTION_ID = os.getenv("EXECUTION_ID", "")
PARAMS_S3_BUCKET = os.getenv("PARAMS_S3_BUCKET", "")
PARAMS_S3_PREFIX = os.getenv("PARAMS_S3_PREFIX", "")


class _UUIDEncoder(json.JSONEncoder):
    """JSON encoder that converts UUID objects to strings."""

    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            return str(obj)
        return super().default(obj)


def _get_params():
    """Download and decompress execution params from S3."""
    key = f"{PARAMS_S3_PREFIX}/{EXECUTION_ID}.json.gz"
    s3 = boto3.client("s3")
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / f"{EXECUTION_ID}.json.gz"
        logger.info("Downloading s3://%s/%s", PARAMS_S3_BUCKET, key)
        s3.download_file(PARAMS_S3_BUCKET, key, str(local))
        with gzip.open(local, "r") as f:
            return json.loads(f.read().decode("utf-8"))


def _put_results(results_dict):
    """Compress and upload results dict to S3."""
    key = f"{PARAMS_S3_PREFIX}/{EXECUTION_ID}_results.json.gz"
    s3 = boto3.client("s3")
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / f"{EXECUTION_ID}_results.json.gz"
        data = json.dumps(results_dict, cls=_UUIDEncoder).encode("utf-8")
        with gzip.open(local, "wb") as f:
            f.write(data)
        s3.upload_file(str(local), PARAMS_S3_BUCKET, key)
        logger.info("Results uploaded to s3://%s/%s", PARAMS_S3_BUCKET, key)


def main(step_override=None):
    """Download params → run script → upload results."""
    execution_id = os.getenv("EXECUTION_ID", "")
    if not execution_id or not PARAMS_S3_BUCKET:
        logger.error(
            "EXECUTION_ID and PARAMS_S3_BUCKET must be set. "
            "Got EXECUTION_ID=%r, PARAMS_S3_BUCKET=%r",
            execution_id,
            PARAMS_S3_BUCKET,
        )
        sys.exit(1)

    logger.info(
        "batch_runner: starting execution %s "
        "(PARAMS_S3_BUCKET=%s, PARAMS_S3_PREFIX=%s)",
        execution_id,
        PARAMS_S3_BUCKET,
        PARAMS_S3_PREFIX,
    )

    # ---- download params from S3 ----
    logger.info("Downloading params from S3...")
    params = _get_params()
    if params is None:
        logger.error("Failed to download params from S3")
        sys.exit(1)

    logger.info(
        "Params downloaded: task_id=%s, step=%s, n_covariates=%d, n_sites_uri=%s",
        params.get("task_id", "?"),
        params.get("step", "?"),
        len(params.get("covariates", [])),
        params.get("sites_s3_uri", "?"),
    )
    params["EXECUTION_ID"] = execution_id

    # Allow the entrypoint to override the step (e.g. for pipeline jobs
    # where each Batch job runs a different step).
    if step_override:
        params["step"] = step_override

    # ---- run script ----
    from main import run  # noqa: E402 — the script's own main.py

    logger.info(
        "Running analysis script (step=%s, task_id=%s)…",
        params.get("step", "all"),
        params.get("task_id", "?"),
    )
    result = run(params, logger)
    logger.info(
        "Script completed successfully (result_keys=%s)",
        list(result.keys()) if isinstance(result, dict) else type(result).__name__,
    )

    # ---- upload results to S3 ----
    if result is not None:
        _put_results(result)
    else:
        logger.warning("Script returned None — no results to upload")

    logger.info("batch_runner: execution %s complete", execution_id)


if __name__ == "__main__":
    step = sys.argv[1] if len(sys.argv) > 1 else None
    main(step_override=step)
