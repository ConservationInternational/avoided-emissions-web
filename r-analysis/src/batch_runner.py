"""Lightweight Batch runner for trends.earth script containers.

Downloads execution parameters from S3 via :func:`gefcore.api.get_params`,
delegates to ``main.run()``, and uploads the results dict back to S3 via
:func:`gefcore.api.put_results`.

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

import logging
import os
import sys

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("batch_runner")


def main(step_override=None):
    """Download params → run script → upload results."""
    execution_id = os.getenv("EXECUTION_ID", "")
    if not execution_id or not os.getenv("PARAMS_S3_BUCKET"):
        logger.error(
            "EXECUTION_ID and PARAMS_S3_BUCKET must be set. "
            "Got EXECUTION_ID=%r, PARAMS_S3_BUCKET=%r",
            execution_id,
            os.getenv("PARAMS_S3_BUCKET", ""),
        )
        sys.exit(1)

    logger.info("batch_runner: starting execution %s", execution_id)

    # ---- download params (reuses gefcore's S3 + retry logic) ----
    from gefcore.api import get_params, put_results

    params = get_params()
    if params is None:
        logger.error("Failed to download params from S3")
        sys.exit(1)

    params["EXECUTION_ID"] = execution_id

    # Allow the entrypoint to override the step (e.g. for pipeline jobs
    # where each Batch job runs a different step).
    if step_override:
        params["step"] = step_override

    # ---- run script ----
    from main import run  # noqa: E402 — the script's own main.py

    logger.info("Running analysis script…")
    result = run(params, logger)
    logger.info("Script completed successfully")

    # ---- upload results (reuses gefcore's S3 + retry logic) ----
    if result is not None:
        put_results(result)
        logger.info("Results uploaded to S3")
    else:
        logger.warning("Script returned None — no results to upload")

    logger.info("batch_runner: execution %s complete", execution_id)


if __name__ == "__main__":
    step = sys.argv[1] if len(sys.argv) > 1 else None
    main(step_override=step)
