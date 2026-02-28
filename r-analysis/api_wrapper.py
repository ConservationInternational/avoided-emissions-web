"""API client wrapper for the R analysis container.

This module is installed into the R container image alongside the R scripts.
It replaces the direct Batch-status polling model with API-driven lifecycle
management:

1. Fetch execution params from the trends.earth API.
2. Update execution status to RUNNING.
3. Orchestrate the R scripts (extract → match → summarise).
4. Post results back via PATCH /api/v1/execution/{id}.

Environment variables (set by the Batch container overrides):
    EXECUTION_ID        – API execution UUID
    API_URL             – Base URL of the trends.earth API (no trailing /)
    API_USER            – API login email  (or X_API_KEY for key auth)
    API_PASSWORD        – API login password
    X_API_KEY           – Optional API key (preferred over user/pass)
    CONFIG_S3_URI       – S3 URI to the compressed params JSON
    PARAMS_S3_BUCKET    – S3 bucket for params
    PARAMS_S3_PREFIX    – S3 key prefix for params
    R_SCRIPTS_DIR       – Path to R scripts (default: /app/scripts)
"""

import gzip
import json
import logging
import os
import subprocess
import sys
import tempfile
import traceback

import boto3
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("api_wrapper")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_URL = os.environ.get("API_URL", "")
EXECUTION_ID = os.environ.get("EXECUTION_ID", "")
API_USER = os.environ.get("API_USER", "")
API_PASSWORD = os.environ.get("API_PASSWORD", "")
API_KEY = os.environ.get("X_API_KEY", "")
PARAMS_S3_BUCKET = os.environ.get("PARAMS_S3_BUCKET", "")
PARAMS_S3_PREFIX = os.environ.get("PARAMS_S3_PREFIX", "execution_params")
R_SCRIPTS_DIR = os.environ.get("R_SCRIPTS_DIR", "/app/scripts")
R_STEP_TIMEOUT = int(os.environ.get("R_STEP_TIMEOUT", "14400"))

# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

_token = None


def _get_auth_headers():
    """Return authorization headers, preferring API key over JWT."""
    global _token
    if API_KEY:
        return {"X-API-Key": API_KEY}

    if _token:
        return {"Authorization": f"Bearer {_token}"}

    # Log in and cache the JWT
    resp = requests.post(
        f"{API_URL}/auth",
        json={"email": API_USER, "password": API_PASSWORD},
        timeout=30,
    )
    resp.raise_for_status()
    _token = resp.json().get("access_token")
    return {"Authorization": f"Bearer {_token}"}


# ---------------------------------------------------------------------------
# API communication
# ---------------------------------------------------------------------------


def patch_execution(payload):
    """PATCH the execution on the API with *payload*."""
    url = f"{API_URL}/api/v1/execution/{EXECUTION_ID}"
    headers = _get_auth_headers()
    headers["Content-Type"] = "application/json"
    resp = requests.patch(url, json=payload, headers=headers, timeout=60)
    if resp.status_code != 200:
        logger.error(
            "PATCH %s failed (%s): %s", url, resp.status_code, resp.text[:500]
        )
    return resp


def set_status(status):
    """Update execution status on the API."""
    logger.info("Setting execution status to %s", status)
    return patch_execution({"status": status})


def send_results(results):
    """Post results and set status to FINISHED."""
    logger.info("Sending results for execution %s", EXECUTION_ID)
    return patch_execution({"results": results, "status": "FINISHED"})


# ---------------------------------------------------------------------------
# Params retrieval
# ---------------------------------------------------------------------------


def get_params():
    """Download and decompress execution params from S3."""
    key = f"{PARAMS_S3_PREFIX}/{EXECUTION_ID}.json.gz"
    s3 = boto3.client("s3")
    with tempfile.NamedTemporaryFile(suffix=".json.gz") as tmp:
        logger.info("Downloading params from s3://%s/%s", PARAMS_S3_BUCKET, key)
        s3.download_file(PARAMS_S3_BUCKET, key, tmp.name)
        with gzip.open(tmp.name, "rt") as f:
            return json.load(f)


# ---------------------------------------------------------------------------
# R pipeline execution
# ---------------------------------------------------------------------------

STEP_SCRIPTS = {
    "extract": "01_extract_covariates.R",
    "match": "02_perform_matching.R",
    "summarize": "03_summarize_results.R",
}


def run_r_step(step, config_path, site_id=None):
    """Run a single R pipeline step as a subprocess."""
    script_path = os.path.join(R_SCRIPTS_DIR, STEP_SCRIPTS[step])
    cmd = ["Rscript", script_path, "--config", config_path]
    if site_id:
        cmd += ["--site-id", site_id]

    logger.info("Running R step '%s': %s", step, " ".join(cmd))
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=R_STEP_TIMEOUT
    )

    for line in (result.stdout or "").strip().splitlines():
        logger.info("[R] %s", line)
    for line in (result.stderr or "").strip().splitlines():
        logger.warning("[R stderr] %s", line)

    if result.returncode != 0:
        raise RuntimeError(
            f"R step '{step}' failed with exit code {result.returncode}"
        )


def run_pipeline(params):
    """Run the full extract → match → summarise pipeline."""
    data_dir = tempfile.mkdtemp(prefix="ae_")
    input_dir = os.path.join(data_dir, "input")
    output_dir = os.path.join(data_dir, "output")
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    # Download sites file
    sites_s3_uri = params["sites_s3_uri"]
    sites_local = os.path.join(input_dir, "sites.geojson")
    bucket, key = _parse_s3(sites_s3_uri)
    boto3.client("s3").download_file(bucket, key, sites_local)

    # Write config for R scripts
    config = {
        "task_id": params.get("task_id", EXECUTION_ID),
        "data_dir": data_dir,
        "sites_file": sites_local,
        "cog_bucket": params["cog_bucket"],
        "cog_prefix": params["cog_prefix"],
        "covariates": params.get("covariates", []),
        "exact_match_vars": params.get(
            "exact_match_vars", ["region", "ecoregion", "pa"]
        ),
        "fc_years": params.get("fc_years", list(range(2000, 2024))),
        "max_treatment_pixels": params.get("max_treatment_pixels", 1000),
        "control_multiplier": params.get("control_multiplier", 50),
    }
    config_path = os.path.join(data_dir, "config.json")
    with open(config_path, "w") as f:
        json.dump(config, f)

    step = params.get("step", "all")
    steps = ["extract", "match", "summarize"] if step == "all" else [step]

    for s in steps:
        run_r_step(s, config_path, params.get("site_id"))

    return _collect_results(output_dir, params.get("task_id", EXECUTION_ID))


def _parse_s3(uri):
    if uri.startswith("s3://"):
        uri = uri[5:]
    parts = uri.split("/", 1)
    return parts[0], parts[1] if len(parts) > 1 else ""


def _collect_results(output_dir, task_id):
    """Read the summary files written by step 3."""
    import csv

    summary_path = os.path.join(output_dir, "results_summary.json")
    by_year_path = os.path.join(output_dir, "results_by_site_year.csv")
    by_total_path = os.path.join(output_dir, "results_by_site_total.csv")

    results = {
        "type": "AvoidedEmissionsResults",
        "task_id": task_id,
        "n_sites": 0,
        "total_emissions_avoided_mgco2e": 0.0,
        "total_forest_loss_avoided_ha": 0.0,
        "total_area_ha": 0.0,
        "by_site_year": [],
        "by_site_total": [],
    }

    if os.path.isfile(summary_path):
        with open(summary_path) as f:
            summary = json.load(f)
        results.update(
            {
                k: summary[k]
                for k in (
                    "n_sites",
                    "total_emissions_avoided_mgco2e",
                    "total_forest_loss_avoided_ha",
                    "total_area_ha",
                )
                if k in summary
            }
        )

    if os.path.isfile(by_year_path):
        with open(by_year_path, newline="") as f:
            for row in csv.DictReader(f):
                results["by_site_year"].append(
                    {
                        "site_id": row["site_id"],
                        "year": int(row["year"]),
                        "forest_loss_avoided_ha": float(
                            row.get("forest_loss_avoided_ha", 0)
                        ),
                        "emissions_avoided_mgco2e": float(
                            row.get("emissions_avoided_mgco2e", 0)
                        ),
                    }
                )

    if os.path.isfile(by_total_path):
        with open(by_total_path, newline="") as f:
            for row in csv.DictReader(f):
                results["by_site_total"].append(
                    {
                        "site_id": row["site_id"],
                        "site_name": row.get("site_name", ""),
                        "forest_loss_avoided_ha": float(
                            row.get("forest_loss_avoided_ha", 0)
                        ),
                        "emissions_avoided_mgco2e": float(
                            row.get("emissions_avoided_mgco2e", 0)
                        ),
                        "area_ha": float(row.get("area_ha", 0)),
                    }
                )

    return results


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main():
    """Main entry point: get params → run pipeline → post results."""
    logger.info("API wrapper starting for execution %s", EXECUTION_ID)

    if not EXECUTION_ID:
        logger.error("EXECUTION_ID not set")
        sys.exit(1)

    try:
        set_status("RUNNING")
        params = get_params()
        logger.info("Retrieved params, starting pipeline...")
        results = run_pipeline(params)
        send_results(results)
        logger.info("Pipeline completed successfully")
    except Exception:
        tb = traceback.format_exc()
        logger.error("Pipeline failed:\n%s", tb)
        set_status("FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
