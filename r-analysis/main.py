"""Avoided-emissions R analysis pipeline.

This module is the script entry point for the avoided-emissions analysis.
When the trends.earth API creates an execution for this script, it places
this file at ``gefcore/script/main.py`` inside the Environment Docker image.
``gefcore.runner`` then calls ``main.run(params, logger)`` and handles all
lifecycle management (status updates, params retrieval, result posting).

The module exposes the two attributes that ``gefcore.runner`` expects:

    run(params: dict, logger) -> dict
    REQUIRES_GEE: bool

Environment variables read by this module:
    R_SCRIPTS_DIR   – Path to R scripts (default: /app/scripts)
    R_STEP_TIMEOUT  – Per-step timeout in seconds (default: 14400 = 4h)
"""

import csv
import json
import logging
import os
import subprocess
import tempfile

import boto3

from te_schemas.analysis import AnalysisRecord, AnalysisResults, AnalysisTimeStep

logger = logging.getLogger(__name__)

# The avoided-emissions pipeline is pure R — no Google Earth Engine needed.
REQUIRES_GEE = False

R_SCRIPTS_DIR = os.environ.get("R_SCRIPTS_DIR", "/app/scripts")
R_STEP_TIMEOUT = int(os.environ.get("R_STEP_TIMEOUT", "14400"))

STEP_SCRIPTS = {
    "extract": "01_extract_covariates.R",
    "match": "02_perform_matching.R",
    "summarize": "03_summarize_results.R",
}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run(params, log=None):
    """Execute the avoided-emissions R pipeline.

    Parameters
    ----------
    params : dict
        Execution parameters provided by ``gefcore.runner``.
        Required keys: ``sites_s3_uri``, ``cog_bucket``, ``cog_prefix``.
    log : logging.Logger, optional
        Logger instance (falls back to module-level logger).

    Returns
    -------
    dict
        Results payload (``AnalysisResults.dump()``) suitable for
        ``Execution.results``.
    """
    log = log or logger
    step = params.get("step", "all")
    task_id = params.get("task_id", params.get("EXECUTION_ID", "unknown"))

    log.info("avoided_emissions: starting task %s (step=%s)", task_id, step)

    # ----- prepare local working directory -----
    data_dir = params.get("data_dir") or tempfile.mkdtemp(prefix="ae_")
    input_dir = os.path.join(data_dir, "input")
    output_dir = os.path.join(data_dir, "output")
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    # ----- download sites file from S3 -----
    sites_s3_uri = params["sites_s3_uri"]
    sites_local = os.path.join(input_dir, "sites.geojson")
    _download_s3(sites_s3_uri, sites_local, log)

    # ----- write config JSON consumed by the R scripts -----
    config = {
        "task_id": task_id,
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
        "min_site_area_ha": params.get("min_site_area_ha", 100),
        "min_glm_treatment_pixels": params.get("min_glm_treatment_pixels", 15),
    }
    if params.get("site_id"):
        config["site_id"] = params["site_id"]

    config_path = os.path.join(data_dir, "config.json")
    with open(config_path, "w") as f:
        json.dump(config, f)
    log.info("Config written to %s", config_path)

    # ----- run R pipeline steps -----
    steps = _expand_steps(step)
    for s in steps:
        script_path = os.path.join(R_SCRIPTS_DIR, STEP_SCRIPTS[s])
        log.info("Running R step '%s': %s", s, script_path)
        _run_r_script(script_path, config_path, params.get("site_id"), log)

    # ----- collect results -----
    results = _collect_results(output_dir, task_id, log)

    # ----- upload results to S3 if configured -----
    results_s3_uri = params.get("results_s3_uri")
    if results_s3_uri:
        _upload_results(output_dir, results_s3_uri, log)
        results["results_s3_uri"] = results_s3_uri

    log.info("avoided_emissions: task %s complete", task_id)
    return results


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _expand_steps(step):
    """Map the *step* parameter to a list of R script names to run."""
    if step == "all":
        return ["extract", "match", "summarize"]
    if step in ("extract", "match", "summarize"):
        return [step]
    raise ValueError(f"Unknown step: {step!r}")


def _run_r_script(script_path, config_path, site_id, log):
    """Execute a single R script as a subprocess."""
    cmd = ["Rscript", script_path, "--config", config_path]
    if site_id:
        cmd += ["--site-id", site_id]

    log.info("$ %s", " ".join(cmd))
    result = subprocess.run(  # nosec B603
        cmd,
        capture_output=True,
        text=True,
        timeout=R_STEP_TIMEOUT,
    )

    if result.stdout:
        for line in result.stdout.strip().splitlines():
            log.info("[R] %s", line)
    if result.stderr:
        for line in result.stderr.strip().splitlines():
            log.warning("[R stderr] %s", line)

    if result.returncode != 0:
        raise RuntimeError(
            f"R script {os.path.basename(script_path)} failed "
            f"(exit code {result.returncode})"
        )


def _download_s3(s3_uri, local_path, log):
    """Download an S3 object to a local path."""
    bucket, key = _parse_s3_uri(s3_uri)
    log.info("Downloading s3://%s/%s → %s", bucket, key, local_path)
    s3 = boto3.client("s3")
    s3.download_file(bucket, key, local_path)


def _upload_results(output_dir, s3_uri, log):
    """Upload all files in *output_dir* to S3."""
    bucket, prefix = _parse_s3_uri(s3_uri)
    s3 = boto3.client("s3")
    for root, _dirs, files in os.walk(output_dir):
        for fname in files:
            local_path = os.path.join(root, fname)
            rel = os.path.relpath(local_path, output_dir)
            key = f"{prefix}/{rel}"
            log.info("Uploading %s → s3://%s/%s", local_path, bucket, key)
            s3.upload_file(local_path, bucket, key)


def _parse_s3_uri(uri):
    """Split ``s3://bucket/key`` into ``(bucket, key)``."""
    if uri.startswith("s3://"):
        uri = uri[5:]
    parts = uri.split("/", 1)
    return parts[0], parts[1] if len(parts) > 1 else ""


def _collect_results(output_dir, task_id, log):
    """Read the summary files written by step 3 and build an AnalysisResults dict.

    Returns a plain dict produced by ``AnalysisResults.dump()`` so it can be
    serialised to JSON and stored in ``Execution.results``.
    """
    summary_path = os.path.join(output_dir, "results_summary.json")
    by_year_path = os.path.join(output_dir, "results_by_site_year.csv")
    by_total_path = os.path.join(output_dir, "results_by_site_total.csv")

    # --- summary ---
    summary = {}
    if os.path.isfile(summary_path):
        with open(summary_path) as f:
            summary = json.load(f)

    year_range = summary.get("year_range", {})
    summary_dict = {
        "task_id": task_id,
        "n_sites": summary.get("n_sites", 0),
        "total_emissions_avoided_mgco2e": summary.get(
            "total_emissions_avoided_mgco2e", 0.0
        ),
        "total_forest_loss_avoided_ha": summary.get(
            "total_forest_loss_avoided_ha", 0.0
        ),
        "total_area_ha": summary.get("total_area_ha", 0.0),
        "year_range_min": year_range.get("min"),
        "year_range_max": year_range.get("max"),
    }

    # --- per-site-year time series ---
    time_series = []
    if os.path.isfile(by_year_path):
        with open(by_year_path, newline="") as f:
            for row in csv.DictReader(f):
                time_series.append(
                    AnalysisTimeStep(
                        entity_id=row["site_id"],
                        year=int(row["year"]),
                        values={
                            "forest_loss_avoided_ha": float(
                                row.get("forest_loss_avoided_ha", 0)
                            ),
                            "emissions_avoided_mgco2e": float(
                                row.get("emissions_avoided_mgco2e", 0)
                            ),
                        },
                        entity_name=row.get("site_name") or None,
                        metadata={
                            "n_matched_pixels": int(
                                row.get("n_matched_pixels", 0)
                            ),
                            "sampled_fraction": float(
                                row.get("sampled_fraction", 1)
                            ),
                        },
                    )
                )

    # --- per-site totals ---
    records = []
    if os.path.isfile(by_total_path):
        with open(by_total_path, newline="") as f:
            for row in csv.DictReader(f):
                records.append(
                    AnalysisRecord(
                        entity_id=row["site_id"],
                        values={
                            "forest_loss_avoided_ha": float(
                                row.get("forest_loss_avoided_ha", 0)
                            ),
                            "emissions_avoided_mgco2e": float(
                                row.get("emissions_avoided_mgco2e", 0)
                            ),
                            "area_ha": float(row.get("area_ha", 0)),
                        },
                        entity_name=row.get("site_name") or None,
                        period_start=(
                            int(row["first_year"])
                            if row.get("first_year")
                            else None
                        ),
                        period_end=(
                            int(row["last_year"])
                            if row.get("last_year")
                            else None
                        ),
                        metadata={
                            "n_matched_pixels": int(
                                row.get("n_matched_pixels", 0)
                            ),
                            "sampled_fraction": float(
                                row.get("sampled_fraction", 1)
                            ),
                            "n_years": int(row.get("n_years", 0)),
                        },
                    )
                )

    analysis = AnalysisResults(
        name="Avoided emissions",
        analysis_type="avoided_emissions",
        summary=summary_dict,
        records=records or None,
        time_series=time_series or None,
    )

    log.info(
        "Collected results: %d sites, %.1f MgCO2e avoided",
        summary_dict["n_sites"],
        summary_dict["total_emissions_avoided_mgco2e"],
    )
    return analysis.dump()


__all__ = ["run", "REQUIRES_GEE"]
