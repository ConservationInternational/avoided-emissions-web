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
import math
import os
import subprocess
import tempfile
import threading
from datetime import UTC, datetime

import boto3
from bootstrap import ensure_scripts_dir_on_path
from te_schemas.analysis import AnalysisRecord, AnalysisResults, AnalysisTimeStep

logger = logging.getLogger(__name__)


ensure_scripts_dir_on_path(__file__)

from logging_utils import configure_third_party_logging

configure_third_party_logging()

# The avoided-emissions pipeline is pure R — no Google Earth Engine needed.
REQUIRES_GEE = False

R_SCRIPTS_DIR = os.environ.get("R_SCRIPTS_DIR", "/app/scripts")
R_STEP_TIMEOUT = int(os.environ.get("R_STEP_TIMEOUT", "14400"))

STEP_SCRIPTS = {
    "extract": "01_extract_covariates.py",
    "match": "02_perform_matching.R",
    "summarize": "03_summarize_results.R",
}

# Step labels for user-visible log messages
STEP_LABELS = {
    "extract": "Extracting covariate values",
    "match": "Propensity score matching",
    "summarize": "Summarizing results",
}

# Canonical order of pipeline steps (used for progress calculation).
PIPELINE_STEP_ORDER = ["extract", "match", "summarize"]

# Files produced by the extract step that subsequent steps need.
# The combined treatments_and_controls.parquet is no longer written —
# the match step uses treatment_pixels.parquet + S3-streamed control_pixels/,
# and the summarize step only reads sites_processed.parquet and
# treatment_cell_key.parquet.
EXTRACT_OUTPUT_FILES = [
    "sites_processed.parquet",
    "treatment_cell_key.parquet",
    "treatment_pixels.parquet",
    "formula.json",
    "site_id_key.csv",
    "grid_metadata.json",
]

# Subset downloaded by match workers.  control_pixels/ is streamed directly
# from S3 via Arrow's native S3 filesystem (no local download).
# treatment_pixels.parquet is small (~20-50 MB) and is the only pixel-data
# file downloaded locally.
MATCH_STEP_DOWNLOAD_FILES = [
    "sites_processed.parquet",
    "treatment_pixels.parquet",
    "treatment_cell_key.parquet",
    "formula.json",
    "site_id_key.csv",
    "grid_metadata.json",
]


# ---------------------------------------------------------------------------
# Progress / log reporting helpers
# ---------------------------------------------------------------------------


def _report_progress(progress, log_text=None):
    """Best-effort progress update via the trends.earth API.

    Uses ``gefcore.api.patch_execution`` (requires ``EXECUTION_ID``,
    ``API_URL``, ``API_USER``, ``API_PASSWORD`` in the environment —
    all provided by the API's ``batch_run`` task).  Failures are logged
    but never prevent the pipeline from continuing.
    """
    try:
        from gefcore.api import patch_execution, save_log

        patch_execution(json={"progress": progress})
        if log_text:
            save_log(json={"text": log_text, "level": "INFO"})
    except ImportError:
        pass  # gefcore not installed — skip silently
    except Exception as exc:  # noqa: BLE001
        # Best-effort: never let a reporting failure stop the pipeline
        logging.getLogger(__name__).debug(
            "Progress report failed (progress=%s): %s", progress, exc
        )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run(params, log=None):
    """Execute the avoided-emissions R pipeline.

    Parameters
    ----------
    params : dict
        Execution parameters provided by ``gefcore.runner``.
        Required keys: ``cog_bucket``, ``cog_prefix``, and one of
        ``sites_parquet_s3_uri`` or ``sites_s3_uri``.
    log : logging.Logger, optional
        Logger instance (falls back to module-level logger).

    Returns
    -------
    dict or None
        For the final step (``"all"`` or ``"summarize"``), returns the
        results payload (``AnalysisResults.dump()``).  For intermediate
        pipeline steps (``"extract"``, ``"match"``), returns ``None`` so
        that ``batch_runner`` skips the results upload.
    """
    log = log or logger
    step = params.get("step", "all")
    task_id = params.get("task_id", params.get("EXECUTION_ID", "unknown"))

    # ---- detect pipeline mode ----
    # When the API dispatches a multi-step pipeline (extract → match →
    # summarize as separate Batch jobs), each container runs a single
    # step and uses S3 to pass intermediate data between steps.
    intermediate_uri = params.get("intermediate_s3_uri")
    pipeline_mode = step != "all" and intermediate_uri is not None

    log.info(
        "avoided_emissions: starting task %s (step=%s, pipeline=%s, "
        "cog_bucket=%s, cog_prefix=%s, resolution_m=%s, sites=%s, "
        "n_covariates=%d)",
        task_id,
        step,
        pipeline_mode,
        params.get("cog_bucket", "?"),
        params.get("cog_prefix", "?"),
        params.get("resolution_m", "?"),
        params.get("sites_parquet_s3_uri") or params.get("sites_s3_uri", "?"),
        len(params.get("covariates", [])),
    )

    # ---- pipeline-aware progress boundaries ----
    # When running a single step inside a pipeline, map progress to the
    # step's position in the overall pipeline (0-33-66-100 for 3 steps).
    _step_for_progress = "match" if step.startswith("match_chunk_") else step
    if pipeline_mode and _step_for_progress in PIPELINE_STEP_ORDER:
        p_idx = PIPELINE_STEP_ORDER.index(_step_for_progress)
        p_total = len(PIPELINE_STEP_ORDER)
        progress_start = int(p_idx / p_total * 100)
        progress_end = int((p_idx + 1) / p_total * 100)
    else:
        progress_start = 0
        progress_end = 100

    # ----- prepare local working directory -----
    data_dir = params.get("data_dir") or tempfile.mkdtemp(prefix="ae_")
    input_dir = os.path.join(data_dir, "input")
    output_dir = os.path.join(data_dir, "output")
    matches_dir = os.path.join(output_dir, "matches")
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(matches_dir, exist_ok=True)

    # ----- pipeline: download intermediate data from S3 -----
    if pipeline_mode and (
        step in ("match", "summarize") or step.startswith("match_chunk_")
    ):
        _is_match_step = step in ("match",) or step.startswith("match_chunk_")
        _download_list = (
            MATCH_STEP_DOWNLOAD_FILES if _is_match_step else EXTRACT_OUTPUT_FILES
        )
        log.info(
            "Downloading intermediate extract outputs from S3 (%s)",
            "match subset" if _is_match_step else "full set",
        )
        _download_s3_files(intermediate_uri, output_dir, _download_list, log)
        # Load spatial params written by the extract step so that
        # matching_extent and sites_exclusion_buffer are available in config.
        try:
            _download_s3_files(intermediate_uri, output_dir, ["prep_summary.json"], log)
            _prep_path = os.path.join(output_dir, "prep_summary.json")
            with open(_prep_path) as _fh:
                _prep = json.load(_fh)
            if _prep.get("matching_extent"):
                params = dict(params, matching_extent=_prep["matching_extent"])
                log.info("matching_extent loaded from prep_summary.json")
            if _prep.get("sites_exclusion_buffer"):
                params = dict(
                    params, sites_exclusion_buffer=_prep["sites_exclusion_buffer"]
                )
                log.info("sites_exclusion_buffer loaded from prep_summary.json")
        except Exception as _exc:  # noqa: BLE001
            log.warning("Could not load prep_summary.json: %s — continuing", _exc)
    if pipeline_mode and step == "summarize":
        log.info("Downloading match results from S3")
        _download_s3_prefix(intermediate_uri + "/matches", matches_dir, log)

    # ----- download sites file from S3 -----
    sites_artifact_uri = params.get("sites_parquet_s3_uri") or params.get(
        "sites_s3_uri"
    )
    if not sites_artifact_uri:
        raise KeyError(
            "Missing required parameter: sites_parquet_s3_uri or sites_s3_uri"
        )
    sites_extension = (
        ".parquet" if sites_artifact_uri.endswith(".parquet") else ".geojson"
    )
    sites_local = os.path.join(input_dir, f"sites{sites_extension}")
    _download_s3(sites_artifact_uri, sites_local, log)

    # ----- write config JSON consumed by the R scripts -----
    config = {
        "task_id": task_id,
        "data_dir": data_dir,
        "output_dir": output_dir,
        "sites_file": sites_local,
        # 00_prep.py runs as a subprocess and needs the S3 URI to download the
        # sites file itself (it doesn't share main.py's in-process state).
        "sites_parquet_s3_uri": sites_artifact_uri,
        "cog_bucket": params["cog_bucket"],
        "cog_prefix": params["cog_prefix"],
        "covariates": params["covariates"],
        "exact_match_vars": params["exact_match_vars"],
        "fc_years": params["fc_years"],
        "max_treatment_pixels": params["max_treatment_pixels"],
        "control_multiplier": params["control_multiplier"],
        "min_glm_treatment_pixels": params["min_glm_treatment_pixels"],
        "caliper_width": params["caliper_width"],
        "max_controls_per_treatment": params["max_controls_per_treatment"],
    }
    if params.get("random_seed") is not None:
        config["random_seed"] = int(params["random_seed"])
    if params.get("n_replicates") is not None:
        config["n_replicates"] = int(params["n_replicates"])
    if params.get("matching_extent"):
        config["matching_extent"] = params["matching_extent"]
    if params.get("sites_exclusion_buffer"):
        config["sites_exclusion_buffer"] = params["sites_exclusion_buffer"]
    if params.get("min_control_distance_km") is not None:
        config["min_control_distance_km"] = float(params["min_control_distance_km"])
    if params.get("match_batch_size") is not None:
        config["match_batch_size"] = int(params["match_batch_size"])
    if params.get("separation_fallback_mahalanobis") is not None:
        config["separation_fallback_mahalanobis"] = bool(
            params["separation_fallback_mahalanobis"]
        )
    group_mapping = params.get("exact_match_group_mapping")
    if group_mapping is not None and step.startswith("match_chunk_"):
        # Each match chunk processes only its own groups.
        chunk_idx = int(step.split("_")[-1])
        group_mapping = {
            gid: group_mapping[gid]
            for gid in map(str, params["match_chunks"][chunk_idx])
            if gid in group_mapping
        }
    if group_mapping is not None:
        config["exact_match_group_mapping"] = group_mapping
    if params.get("site_id"):
        config["site_id"] = params["site_id"]
    if params.get("resolution_m") is not None:
        config["resolution_m"] = int(params["resolution_m"])
    if params.get("reference_layer_uris"):
        config["reference_layer_uris"] = params["reference_layer_uris"]
    # Provide the S3 URI for the partitioned control-pixels dataset so match
    # workers can stream it directly from S3 (no local download).  Only set
    # in pipeline mode for match steps; summarize and all-in-one runs don't
    # need it because they never read control_pixels/ at all.
    _is_match_step = step in ("match",) or step.startswith("match_chunk_")
    if pipeline_mode and _is_match_step and intermediate_uri:
        config["control_pixels_s3_uri"] = intermediate_uri + "/control_pixels"

    config_path = os.path.join(data_dir, "config.json")
    with open(config_path, "w") as f:
        json.dump(config, f)
    log.info("Config written to %s", config_path)

    # ----- run R pipeline steps -----
    steps = _expand_steps(step)
    log.info(
        "Pipeline steps to execute: %s (total=%d)",
        " → ".join(steps),
        len(steps),
    )
    for step_idx, s in enumerate(steps, 1):
        script_path = os.path.join(
            R_SCRIPTS_DIR,
            STEP_SCRIPTS[s] if s in STEP_SCRIPTS else STEP_SCRIPTS["match"],
        )
        # Map local step progress [0, 1] onto the pipeline-aware range.
        local_frac_start = (step_idx - 1) / len(steps)
        local_frac_end = step_idx / len(steps)
        pct_start = progress_start + int(
            local_frac_start * (progress_end - progress_start)
        )
        pct_end = progress_start + int(local_frac_end * (progress_end - progress_start))

        label = STEP_LABELS.get(s, s)
        log.info(
            "Running step %d/%d '%s': %s",
            step_idx,
            len(steps),
            s,
            script_path,
        )
        _report_progress(
            pct_start,
            f"Step {step_idx}/{len(steps)}: {label} (starting)",
        )

        if pipeline_mode and (s == "match" or s.startswith("match_chunk_")):
            # In pipeline mode the match step runs as an AWS Batch array
            # job — one child per site.  If the R subprocess crashes
            # (e.g. OOM-killed, exit -9) we must NOT propagate the error
            # because that would mark *this* array child as FAILED,
            # which in turn marks the whole array job FAILED and blocks
            # the summarize step.  Instead, write a failure marker JSON
            # file so the summarize step can report the site as failed
            # while still producing results for all other sites.
            try:
                _run_r_script(
                    script_path,
                    config_path,
                    params.get("site_id"),
                    log,
                )
            except RuntimeError as exc:
                log.warning(
                    "Match step failed — writing failure marker: %s",
                    exc,
                )
                _write_match_failure_marker(matches_dir, str(exc), log)
        else:
            _run_r_script(
                script_path,
                config_path,
                params.get("site_id"),
                log,
            )

        log.info("Step %d/%d '%s' completed", step_idx, len(steps), s)
        _report_progress(
            pct_end,
            f"Step {step_idx}/{len(steps)}: {label} (completed)",
        )

    if pipeline_mode and step == "extract":
        log.info("Uploading intermediate extract outputs to S3")
        _upload_to_s3_prefix(output_dir, intermediate_uri, log)
        log.info("avoided_emissions: extract step complete (task %s)", task_id)
        return None  # no final results yet

    if pipeline_mode and (step == "match" or step.startswith("match_chunk_")):
        log.info("Uploading match results to S3")
        _upload_to_s3_prefix(matches_dir, intermediate_uri + "/matches", log)
        log.info("avoided_emissions: match step complete (task %s)", task_id)
        return None  # no final results yet

    # ----- collect results (for "all" or "summarize") -----
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
    """Map the *step* parameter to a list of script names to run."""
    if step == "all":
        # "all" runs the classic non-pipeline workflow: no prep step.
        # matching_extent and sites_exclusion_buffer come from params.
        return ["extract", "match", "summarize"]
    if step in ("prep", "extract", "match", "summarize"):
        return [step]
    if step.startswith("match_chunk_"):
        return [step]
    raise ValueError(f"Unknown step: {step!r}")


def _run_r_script(script_path, config_path, site_id, log):
    """Execute a single R or Python script as a subprocess.

    Output is streamed line-by-line so that long-running steps produce
    visible log output in real time rather than buffering everything in
    memory until completion.
    """
    if script_path.endswith(".py"):
        cmd = ["python", script_path, "--config", config_path]
    else:
        cmd = ["Rscript", script_path, "--config", config_path]
    if site_id:
        cmd += ["--site-id", site_id]

    log.info("$ %s", " ".join(cmd))

    proc = subprocess.Popen(  # nosec B603
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    def _stream(stream, prefix, level):
        """Read *stream* line-by-line and log with *prefix*."""
        try:
            for line in stream:
                line = line.rstrip("\n\r")
                if line:
                    log.log(level, "[%s] %s", prefix, line)
        except ValueError:
            pass  # stream closed

    # Read stdout and stderr concurrently so neither pipe blocks
    stdout_thread = threading.Thread(
        target=_stream,
        args=(proc.stdout, "R", logging.INFO),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=_stream,
        args=(proc.stderr, "R stderr", logging.WARNING),
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()

    try:
        proc.wait(timeout=R_STEP_TIMEOUT)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        raise RuntimeError(
            f"R script {os.path.basename(script_path)} timed out "
            f"after {R_STEP_TIMEOUT}s"
        ) from None
    finally:
        stdout_thread.join(timeout=5)
        stderr_thread.join(timeout=5)

    if proc.returncode != 0:
        raise RuntimeError(
            f"R script {os.path.basename(script_path)} failed "
            f"(exit code {proc.returncode})"
        )


def _write_match_failure_marker(matches_dir, error_msg, log):
    """Write a worker-level failure marker after the R subprocess crashed.

    One array element can process several site fragments, so the marker does
    not name a site.  The summarize step reports every fragment and
    replicate without a match file as failed.
    """
    array_index = os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX", "unknown")
    marker = {
        "array_index": array_index,
        "error": error_msg,
        "timestamp": datetime.now(tz=UTC).isoformat(),
    }
    path = os.path.join(matches_dir, f"failed_array_{array_index}.json")
    with open(path, "w") as f:
        json.dump(marker, f)
    log.info("Failure marker written to %s", path)


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
    uri = uri.removeprefix("s3://")
    parts = uri.split("/", 1)
    return parts[0], parts[1] if len(parts) > 1 else ""


# ---------------------------------------------------------------------------
# Pipeline intermediate S3 helpers
# ---------------------------------------------------------------------------


def _upload_to_s3_prefix(local_dir, s3_uri, log):
    """Upload all files under *local_dir* to the S3 prefix in *s3_uri*.

    Walks subdirectories and preserves the relative path structure.
    """
    bucket, prefix = _parse_s3_uri(s3_uri)
    s3 = boto3.client("s3")
    count = 0
    for root, _dirs, files in os.walk(local_dir):
        for fname in files:
            local_path = os.path.join(root, fname)
            rel = os.path.relpath(local_path, local_dir)
            key = f"{prefix}/{rel}".replace("\\", "/")
            log.info("Uploading %s → s3://%s/%s", local_path, bucket, key)
            s3.upload_file(local_path, bucket, key)
            count += 1
    log.info("Uploaded %d files to s3://%s/%s", count, bucket, prefix)


def _download_s3_prefix(s3_uri, local_dir, log):
    """Download all objects under an S3 prefix to *local_dir*.

    Preserves the key structure relative to the prefix as local sub-paths.
    """
    bucket, prefix = _parse_s3_uri(s3_uri)
    prefix = prefix.rstrip("/")
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix + "/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            rel = key[len(prefix) :].lstrip("/")
            if not rel:
                continue
            local_path = os.path.join(local_dir, rel)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            log.info("Downloading s3://%s/%s → %s", bucket, key, local_path)
            s3.download_file(bucket, key, local_path)
            count += 1
    log.info("Downloaded %d files from s3://%s/%s", count, bucket, prefix)


def _download_s3_files(s3_uri, local_dir, filenames, log):
    """Download specific *filenames* from an S3 prefix to *local_dir*."""
    bucket, prefix = _parse_s3_uri(s3_uri)
    s3 = boto3.client("s3")
    os.makedirs(local_dir, exist_ok=True)
    for fname in filenames:
        key = f"{prefix}/{fname}"
        local_path = os.path.join(local_dir, fname)
        log.info("Downloading s3://%s/%s → %s", bucket, key, local_path)
        s3.download_file(bucket, key, local_path)


YEAR_METRICS = [
    "extrapolated_forest_loss_avoided_ha",
    "extrapolated_emissions_avoided_mgco2e",
    "extrapolated_treatment_defor_ha",
    "extrapolated_control_defor_ha",
    "extrapolated_treatment_emissions_mgco2e",
    "extrapolated_control_emissions_mgco2e",
]
TOTAL_METRICS = [
    "extrapolated_forest_loss_avoided_ha",
    "extrapolated_emissions_avoided_mgco2e",
]


def _number(row, key):
    """Return a required finite number from a result CSV row."""
    try:
        value = float(row[key])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"Invalid result value {key}={row.get(key)!r}") from exc
    if not math.isfinite(value):
        raise ValueError(f"Invalid result value {key}={row[key]!r}")
    return value


def _metric_values(row, metrics, n_replicates):
    """Required metrics plus their replicate bounds when n_replicates > 1."""
    values = {key: _number(row, key) for key in metrics}
    if n_replicates > 1:
        for key in metrics:
            for suffix in ("_ci_lower", "_ci_upper"):
                values[key + suffix] = _number(row, key + suffix)
    return values


def _flag(row, key):
    value = row[key]
    if value not in ("TRUE", "FALSE"):
        raise ValueError(f"Invalid result flag {key}={value!r}")
    return value == "TRUE"


def _collect_results(output_dir, task_id, log):
    """Read the summary files written by step 3 and build an AnalysisResults dict.

    Returns a plain dict produced by ``AnalysisResults.dump()`` so it can be
    serialised to JSON and stored in ``Execution.results``.  Missing files or
    values raise, so an incomplete run is never reported as a result.
    """
    with open(os.path.join(output_dir, "results_summary.json")) as f:
        summary = json.load(f)
    n_replicates = int(summary["n_replicates"])

    summary_dict = {
        "task_id": task_id,
        "n_sites": summary["n_sites"],
        "n_failed_sites": summary["n_failed_sites"],
        "n_replicates": n_replicates,
        "bounds_label": summary["bounds_label"],
        "r_analysis_git_sha": summary.get("r_analysis_git_sha"),
        "total_emissions_avoided_mgco2e": summary[
            "extrapolated_total_emissions_avoided_mgco2e"
        ],
        "total_forest_loss_avoided_ha": summary[
            "extrapolated_total_forest_loss_avoided_ha"
        ],
        "sample_total_emissions_avoided_mgco2e": summary[
            "sample_total_emissions_avoided_mgco2e"
        ],
        "sample_total_forest_loss_avoided_ha": summary[
            "sample_total_forest_loss_avoided_ha"
        ],
        "total_area_ha": summary["total_area_ha"],
        "year_range_min": summary["year_range"]["min"],
        "year_range_max": summary["year_range"]["max"],
        "failed_sites": summary["failed_sites"],
        "worker_failures": summary["worker_failures"],
        "subsampled_sites": summary["subsampled_sites"],
        "methodology_warnings": summary["methodology_warnings"],
        "portfolio": summary["portfolio"],
    }

    time_series = []
    with open(os.path.join(output_dir, "results_by_site_year.csv"), newline="") as f:
        for row in csv.DictReader(f):
            time_series.append(
                AnalysisTimeStep(
                    entity_id=row["site_id"],
                    year=int(row["year"]),
                    values=_metric_values(row, YEAR_METRICS, n_replicates),
                    entity_name=row["site_name"] or None,
                    metadata={
                        "n_sample_pixels": int(_number(row, "n_sample_pixels")),
                        "sampled_fraction": _number(row, "sampled_fraction"),
                        "is_pre_intervention": _flag(row, "is_pre_intervention"),
                        "is_post_intervention": _flag(row, "is_post_intervention"),
                    },
                )
            )

    records = []
    with open(os.path.join(output_dir, "results_by_site_total.csv"), newline="") as f:
        for row in csv.DictReader(f):
            values = _metric_values(row, TOTAL_METRICS, n_replicates)
            values["area_ha"] = _number(row, "area_ha")
            records.append(
                AnalysisRecord(
                    entity_id=row["site_id"],
                    values=values,
                    entity_name=row["site_name"] or None,
                    period_start=int(row["first_year"]),
                    period_end=int(row["last_year"]),
                    metadata={
                        "n_sample_pixels": int(_number(row, "n_sample_pixels")),
                        "n_eligible_pixels": int(_number(row, "n_eligible_pixels")),
                        "n_treatment_pixels": int(_number(row, "n_treatment_pixels")),
                        "sampled_fraction": _number(row, "sampled_fraction"),
                        "coverage_fraction": _number(row, "coverage_fraction"),
                        "match_rate": _number(row, "match_rate"),
                        "n_years": int(row["n_years"]),
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
        "Collected results: %d sites (%d failed), %.1f MgCO2e avoided",
        summary_dict["n_sites"],
        summary_dict["n_failed_sites"],
        summary_dict["total_emissions_avoided_mgco2e"],
    )
    return analysis.dump()


__all__ = ["REQUIRES_GEE", "run"]
