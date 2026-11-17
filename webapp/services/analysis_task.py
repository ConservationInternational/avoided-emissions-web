"""Analysis task submission, status tracking, results retrieval, and download."""

import json
import logging
import tempfile
import uuid
from datetime import UTC, datetime

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import tasks as webapp_tasks
from config import Config
from models import (
    AnalysisTask,
    TaskResult,
    TaskResultTotal,
    TaskSite,
    get_db,
)
from sqlalchemy import case, func

from gee_export import gee_config
from services.covariate import get_ready_covariate_names
from services.polygon_partition import equal_area_ha as _equal_area_ha
from services.polygon_partition import polygonal
from services.reference_layers import (
    compute_exact_match_groups_with_splitting,
    compute_matching_extent,
    compute_sites_exclusion_buffer,
    get_reference_layer_uris,
)
from services.s3 import get_s3_client
from services.site_set import (
    _get_site_set_min_start_year,
    _stream_site_set_to_parquet_buf,
    get_user_site_set_centroids_geojson,
    get_user_site_set_geojson,
    upload_sites_parquet_to_s3,
    upload_sites_to_geojson,
    upload_sites_to_s3,
)

logger = logging.getLogger(__name__)

FC_YEAR_MIN = gee_config.FC_YEAR_MIN
FC_YEAR_MAX = gee_config.FC_YEAR_MAX

ALLOWED_MATCHING_JOB_QUEUES = {
    "ae-spot-gp3",
    "ae-ondemand-gp3",
}

DEFAULT_MATCHING_JOB_QUEUE = "ae-spot-gp3"

# Warn when more than this share of the site area has no exact-match region
# (e.g. marine sites) and so cannot be matched.
UNCOVERED_AREA_WARNING_FRACTION = 0.2

# -- Analysis task default settings ------------------------------------------
# Single source of truth for matching parameter defaults.  Imported by
# layouts.py (UI form pre-fill) and callbacks.py (server-side fallbacks).
# Forest cover year boundaries are imported from gee-export config at top.

ANALYSIS_DEFAULTS = {
    "max_treatment_pixels": 1000,
    "control_multiplier": 50,
    "min_site_area_ha": 100,
    "min_glm_treatment_pixels": 15,
    "caliper_width": 0.2,
    "max_controls_per_treatment": 1,
    "min_control_distance_km": 10,
    "separation_fallback_mahalanobis": False,
    "matching_method": "optimal",
    "n_replicates": 1,
    "match_batch_size": 25,
    # Replicates matched by one Nextflow worker with its pixels loaded once.
    "replicate_block_size": 10,
    "match_memory_gb": 30,
    "match_memory_mib": 30 * 1024,  # 30 GB in MiB
    "fc_year_start": FC_YEAR_MIN,
    "fc_year_end": FC_YEAR_MAX + 1,  # exclusive upper bound for range()
    "resolution_m": 1000,  # nominal resolution: 1000 m (1 km) or 250 m
}


def queue_analysis_task(
    task_name,
    description,
    user_id,
    site_set_id,
    covariates,
    exact_match_vars,
    max_treatment_pixels=ANALYSIS_DEFAULTS["max_treatment_pixels"],
    control_multiplier=ANALYSIS_DEFAULTS["control_multiplier"],
    min_site_area_ha=ANALYSIS_DEFAULTS["min_site_area_ha"],
    min_glm_treatment_pixels=ANALYSIS_DEFAULTS["min_glm_treatment_pixels"],
    caliper_width=ANALYSIS_DEFAULTS["caliper_width"],
    max_controls_per_treatment=ANALYSIS_DEFAULTS["max_controls_per_treatment"],
    min_control_distance_km=ANALYSIS_DEFAULTS["min_control_distance_km"],
    separation_fallback_mahalanobis=ANALYSIS_DEFAULTS[
        "separation_fallback_mahalanobis"
    ],
    matching_method=ANALYSIS_DEFAULTS["matching_method"],
    n_replicates=ANALYSIS_DEFAULTS["n_replicates"],
    match_batch_size=ANALYSIS_DEFAULTS["match_batch_size"],
    random_seed=None,
    match_memory_mib=ANALYSIS_DEFAULTS["match_memory_mib"],
    matching_job_queue=DEFAULT_MATCHING_JOB_QUEUE,
    resolution_m=ANALYSIS_DEFAULTS["resolution_m"],
    source_sites_s3_uri=None,
    source_sites_parquet_s3_uri=None,
    session_id=None,
    submitter_email=None,
    submitter_name=None,
):
    """Create an analysis task record and queue it for async submission.

    Performs fast validation, creates a local DB record with
    ``status='submitting'``, and dispatches
    :func:`tasks.submit_analysis_task_worker` to handle the slow parts:
    PostGIS geometry computations, S3 site upload, and the trends.earth
    API call.  Returns the task_id immediately so the UI can redirect
    to the task detail page without waiting.

    ``site_set_id`` must be provided when sites are stored in the local
    PostGIS database (the normal path).  ``source_sites_s3_uri`` is an
    S3 fallback used when resubmitting adopted tasks that have no local
    site set. ``source_sites_parquet_s3_uri`` is the preferred fallback
    for GeoParquet site artifacts.

    Raises ``ValueError`` for validation failures so the caller can
    surface them to the user before any DB work is done.
    """
    if not exact_match_vars:
        raise ValueError(
            "At least one exact match variable must be selected "
            "(admin0, admin1, admin2, ecoregion, or pa)."
        )

    overlap = set(covariates or []) & set(exact_match_vars)
    if overlap:
        raise ValueError(
            "The following variables are selected as both covariates and "
            "exact matches. Each variable must be used as one or the "
            "other, not both: " + ", ".join(sorted(overlap))
        )

    ready_covariates = set(get_ready_covariate_names())
    requested_covariates = set(covariates or [])
    unavailable_covariates = sorted(requested_covariates - ready_covariates)
    if unavailable_covariates:
        raise ValueError(
            "The following covariates are not fully processed and ready: "
            + ", ".join(unavailable_covariates)
        )

    if max_treatment_pixels < 1:
        raise ValueError("max_treatment_pixels must be at least 1")
    if control_multiplier < 1:
        raise ValueError("control_multiplier must be at least 1")
    if min_site_area_ha < 0:
        raise ValueError("min_site_area_ha must be zero or greater")
    if min_glm_treatment_pixels < 1:
        raise ValueError("min_glm_treatment_pixels must be at least 1")
    if caliper_width < 0:
        raise ValueError("caliper_width must be zero (disabled) or positive")
    if max_controls_per_treatment < 0:
        raise ValueError("max_controls_per_treatment must be 0 (no limit) or positive")
    if matching_method == "nearest" and max_controls_per_treatment == 0:
        raise ValueError(
            "Full matching (no limit on controls per treatment) requires the "
            "optimal matching method."
        )
    if min_control_distance_km < 0:
        raise ValueError("min_control_distance_km must be 0 (disabled) or positive")
    if random_seed is not None and (random_seed < 1 or random_seed > 2_147_483_647):
        raise ValueError("random_seed must be between 1 and 2147483647")
    if n_replicates < 1 or n_replicates > 1000:
        raise ValueError("n_replicates must be between 1 and 1000")
    if not isinstance(match_batch_size, int) or match_batch_size < 1:
        raise ValueError("match_batch_size must be a positive integer")
    if type(match_memory_mib) is not int or not 1 <= match_memory_mib <= 122880:
        raise ValueError("Matching memory must be between 1 and 122880 MiB (120 GB)")
    if matching_job_queue not in ALLOWED_MATCHING_JOB_QUEUES:
        raise ValueError(
            "matching_job_queue must be one of: "
            + ", ".join(sorted(ALLOWED_MATCHING_JOB_QUEUES))
        )

    # The worker submits the analysis to trends.earth with the user's own
    # access token, so check the session up front rather than leaving a
    # stuck "submitting" task.
    import session_store

    if not session_id or session_store.get(session_id) is None:
        raise ValueError(
            "Your Trends.Earth session has ended. Sign in again to submit "
            "analysis tasks."
        )
    if not Config.TRENDSEARTH_SCRIPT_ID:
        raise ValueError(
            "TRENDSEARTH_SCRIPT_ID is not configured. Set this "
            "environment variable to the UUID of the avoided-emissions "
            "script registered on the trends.earth API."
        )

    db = get_db()
    try:
        task_id = str(uuid.uuid4())
        task = AnalysisTask(
            id=task_id,
            name=task_name,
            description=description,
            submitted_by=user_id,
            submitter_email=submitter_email,
            submitter_name=submitter_name,
            submit_session_id=session_id,
            site_set_id=site_set_id,
            status="submitting",
            config={
                "exact_match_vars": list(exact_match_vars),
                "max_treatment_pixels": max_treatment_pixels,
                "control_multiplier": control_multiplier,
                "min_site_area_ha": min_site_area_ha,
                "min_glm_treatment_pixels": min_glm_treatment_pixels,
                "caliper_width": caliper_width,
                "max_controls_per_treatment": max_controls_per_treatment,
                "min_control_distance_km": min_control_distance_km,
                "separation_fallback_mahalanobis": bool(
                    separation_fallback_mahalanobis
                ),
                "matching_method": matching_method,
                "n_replicates": n_replicates,
                "match_batch_size": match_batch_size,
                "resolution_m": resolution_m,
                **({"random_seed": random_seed} if random_seed is not None else {}),
                "match_memory_mib": match_memory_mib,
                "matching_job_queue": matching_job_queue,
                **(
                    {"code_git_sha": Config.GIT_REVISION} if Config.GIT_REVISION else {}
                ),
                **(
                    {"source_sites_s3_uri": source_sites_s3_uri}
                    if source_sites_s3_uri
                    else {}
                ),
                **(
                    {"source_sites_parquet_s3_uri": source_sites_parquet_s3_uri}
                    if source_sites_parquet_s3_uri
                    else {}
                ),
            },
            covariates=covariates,
        )
        db.add(task)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    # Dispatch the slow parts to a Celery worker.
    webapp_tasks.submit_analysis_task_worker.delay(task_id, str(user_id))
    logger.info(
        "[QUEUE] Analysis task %s queued for async submission (user=%s)",
        task_id,
        user_id,
    )
    return task_id


def _complete_analysis_task_submission(task_id, user_id):
    """Execute the slow parts of an analysis task submission.

    Called by the :func:`tasks.submit_analysis_task_worker` Celery task
    after :func:`queue_analysis_task` has created the DB record.

    1. Loads the task and its configuration from the database.
    2. Recovers the sites GeoDataFrame from the linked site set or S3.
    3. Runs PostGIS computations (matching extent, exclusion buffer,
       optional site splitting).
    4. Creates ``TaskSite`` rows and uploads sites to S3.
    5. Submits the execution to the trends.earth API.
    6. Updates the task record to ``status='submitted'``.

    On any error the task is marked ``status='failed'`` with the error
    message stored, and the exception is re-raised so the Celery task
    can report it to Rollbar.
    """
    import time as _time

    import session_store
    from trendsearth_client import TrendsEarthClient

    _t0 = _time.perf_counter()

    db = get_db()
    try:
        task = db.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
        if not task:
            raise ValueError(f"Task {task_id} not found in database")

        if task.status != "submitting":
            logger.warning(
                "[SUBMIT-WORKER] Task %s has unexpected status %r — expected "
                "'submitting'. Aborting.",
                task_id,
                task.status,
            )
            return

        # ── Heartbeat ─────────────────────────────────────────────────────────
        # Record that this worker has started so expire_stale_submitting_tasks
        # can distinguish "message lost before pickup" (no heartbeat after 5 min)
        # from "worker started but stalled" (heartbeat present but >40 min old).
        config = task.config or {}
        task.config = {
            **config,
            "worker_started_at": datetime.now(UTC).isoformat(),
        }
        db.commit()
        # ─────────────────────────────────────────────────────────────────────
        exact_match_vars = config.get("exact_match_vars", [])
        max_treatment_pixels = config.get(
            "max_treatment_pixels", ANALYSIS_DEFAULTS["max_treatment_pixels"]
        )
        control_multiplier = config.get(
            "control_multiplier", ANALYSIS_DEFAULTS["control_multiplier"]
        )
        min_site_area_ha = config.get(
            "min_site_area_ha", ANALYSIS_DEFAULTS["min_site_area_ha"]
        )
        min_glm_treatment_pixels = config.get(
            "min_glm_treatment_pixels", ANALYSIS_DEFAULTS["min_glm_treatment_pixels"]
        )
        caliper_width = config.get("caliper_width", ANALYSIS_DEFAULTS["caliper_width"])
        max_controls_per_treatment = config.get(
            "max_controls_per_treatment",
            ANALYSIS_DEFAULTS["max_controls_per_treatment"],
        )
        min_control_distance_km = config.get(
            "min_control_distance_km", ANALYSIS_DEFAULTS["min_control_distance_km"]
        )
        separation_fallback_mahalanobis = config.get(
            "separation_fallback_mahalanobis", False
        )
        matching_method = config.get(
            "matching_method", ANALYSIS_DEFAULTS["matching_method"]
        )
        n_replicates = config.get("n_replicates", ANALYSIS_DEFAULTS["n_replicates"])
        match_batch_size = config.get(
            "match_batch_size", ANALYSIS_DEFAULTS["match_batch_size"]
        )
        random_seed = config.get("random_seed")
        match_memory_mib = config.get(
            "match_memory_mib", ANALYSIS_DEFAULTS["match_memory_mib"]
        )
        matching_job_queue = config.get(
            "matching_job_queue", DEFAULT_MATCHING_JOB_QUEUE
        )
        resolution_m = config.get("resolution_m", ANALYSIS_DEFAULTS["resolution_m"])

        _site_set_id = str(task.site_set_id) if task.site_set_id else None

        parquet_buf = None  # caller-owned temporary GeoParquet file
        gdf = None

        if _site_set_id:
            # Step 1 — forest-cover year range via scalar DB query.
            _db_meta = get_db()
            try:
                min_year = _get_site_set_min_start_year(_site_set_id, _db_meta)
            finally:
                _db_meta.close()
            if min_year is None:
                raise ValueError(
                    f"Task {task_id}: site set {_site_set_id} has no features."
                )

            # Step 2 — load reference layer S3 URIs for the Batch prep step.
            # matching_extent and sites_exclusion_buffer are now computed
            # by the dedicated Batch ``prep`` step from the exported
            # GeoParquets, keeping all heavy PostGIS work out of this worker.
            reference_layer_uris = get_reference_layer_uris(exact_match_vars)
            matching_extent = None
            sites_exclusion_buffer = None

            if not reference_layer_uris:
                # Reference layers not yet exported — fall back to PostGIS path
                # so that the task can still be submitted (e.g. during initial
                # deployment before the export task has run).
                logger.warning(
                    "[SUBMIT-WORKER] Task %s: reference layers not exported to S3 yet "
                    "— falling back to PostGIS matching-extent computation",
                    task_id,
                )
                _pe_t0 = _time.perf_counter()
                matching_extent = compute_matching_extent(
                    None, exact_match_vars, site_set_id=_site_set_id
                )
                logger.info(
                    "[SUBMIT-WORKER] Task %s: matching extent computed in %.2fs",
                    task_id,
                    _time.perf_counter() - _pe_t0,
                )
                _buf_t0 = _time.perf_counter()
                sites_exclusion_buffer = compute_sites_exclusion_buffer(
                    None, min_control_distance_km, site_set_id=_site_set_id
                )
                logger.info(
                    "[SUBMIT-WORKER] Task %s: exclusion buffer computed in %.2fs",
                    task_id,
                    _time.perf_counter() - _buf_t0,
                )

            # Step 3 — stream bounded row groups to a temporary file.
            _stream_t0 = _time.perf_counter()
            _db_stream = get_db()
            try:
                parquet_buf, n_sites_streamed = _stream_site_set_to_parquet_buf(
                    _site_set_id, _db_stream
                )
            finally:
                _db_stream.close()
            logger.info(
                "[SUBMIT-WORKER] Task %s: streamed %d sites to Parquet in %.2fs",
                task_id,
                n_sites_streamed,
                _time.perf_counter() - _stream_t0,
            )

            # Pre-intervention deforestation needs cover from six years
            # before the earliest start (see baseline_year() in R).
            fc_min = max(ANALYSIS_DEFAULTS["fc_year_start"], min_year - 6)
            fc_max = ANALYSIS_DEFAULTS["fc_year_end"]
            fc_years = list(range(fc_min, fc_max))

        else:
            # Legacy path — adopted task; load GDF from S3.
            source_parquet_uri = config.get("source_sites_parquet_s3_uri")
            if source_parquet_uri:
                gdf = _fetch_sites_parquet_from_s3(source_parquet_uri)
            if gdf is None or gdf.empty:
                source_uri = config.get("source_sites_s3_uri")
                if source_uri:
                    geojson_fc = _fetch_sites_geojson_from_s3(source_uri)
                    if geojson_fc and geojson_fc.get("features"):
                        gdf = gpd.GeoDataFrame.from_features(
                            geojson_fc["features"], crs="EPSG:4326"
                        )
            if gdf is None or gdf.empty:
                raise ValueError(
                    f"Task {task_id}: could not load site data from the linked "
                    "site set or S3 URI. Cannot complete submission."
                )

            start_dates = pd.to_datetime(gdf["start_date"])
            fc_min = max(
                ANALYSIS_DEFAULTS["fc_year_start"],
                int(start_dates.dt.year.min()) - 6,
            )
            fc_max = ANALYSIS_DEFAULTS["fc_year_end"]
            fc_years = list(range(fc_min, fc_max))

            _pe_t0 = _time.perf_counter()
            matching_extent = compute_matching_extent(gdf, exact_match_vars)
            logger.info(
                "[SUBMIT-WORKER] Task %s: matching extent computed in %.2fs",
                task_id,
                _time.perf_counter() - _pe_t0,
            )
            _buf_t0 = _time.perf_counter()
            sites_exclusion_buffer = compute_sites_exclusion_buffer(
                gdf, min_control_distance_km
            )
            logger.info(
                "[SUBMIT-WORKER] Task %s: exclusion buffer computed in %.2fs",
                task_id,
                _time.perf_counter() - _buf_t0,
            )
            # Legacy path: no reference layer GeoParquets — prep step is skipped.
            reference_layer_uris = {}

        # Step 5 — load GDF only now (from Parquet buffer or existing gdf).
        # For the DB path the GDF is read from the temporary GeoParquet;
        # for the S3 fallback path the GDF is already loaded above.
        if parquet_buf is not None and gdf is None:
            try:
                gdf = gpd.read_parquet(parquet_buf)
            finally:
                parquet_buf.close()
                parquet_buf = None

        if gdf is None or gdf.empty:
            raise ValueError(
                f"Task {task_id}: could not load site data. Cannot complete submission."
            )
        if gdf.crs is None or gdf.crs.to_epsg() != 4326:
            raise ValueError("Analysis inputs must declare EPSG:4326.")
        if gdf["site_id"].isna().any() or gdf["site_id"].duplicated().any():
            raise ValueError("Analysis inputs require unique immutable feature keys.")
        atom_values = _atom_metadata_values(gdf)
        if atom_values.notna().any():
            if atom_values.isna().any():
                raise ValueError("Atom-enabled inputs must declare ae_atoms for every parent site.")
            gdf = gdf.copy()
            gdf["ae_atoms"] = atom_values
        repaired_count = int((~shapely.is_valid(gdf.geometry.to_numpy())).sum())
        gdf = gdf.copy()
        gdf["geometry"] = [polygonal(geom) for geom in gdf.geometry]

        logger.info(
            "[SUBMIT-WORKER] Task %s: %d sites ready (%.2fs so far)",
            task_id,
            len(gdf),
            _time.perf_counter() - _t0,
        )

        # Apply the minimum area to whole sites before they are split, so a
        # large site keeps any small fragments created by region boundaries.
        gdf["area_ha"] = _equal_area_ha(gdf.geometry)
        input_count = len(gdf)
        input_area = float(gdf["area_ha"].sum())
        # The partition cache covers the whole site set so any minimum-area
        # setting reuses the same entry.
        all_sites = gdf[["site_id", "geometry"]]
        if "ae_atoms" in gdf.columns and gdf["ae_atoms"].notna().any():
            excluded_atoms = gdf.loc[
                gdf["area_ha"] < min_site_area_ha, "site_id"
            ].astype(str).tolist()
            if excluded_atoms:
                raise ValueError(
                    "Atom-enabled analyses cannot filter declared parent sites by "
                    "minimum site area. Increase the minimum site area or submit "
                    "all atom parents; excluded site IDs: "
                    + ", ".join(excluded_atoms[:10])
                )
        gdf = gdf[gdf["area_ha"] >= min_site_area_ha].reset_index(drop=True)
        preparation = {
            "version": "authoritative-conservative-v1",
            "area_method": "Mollweide; boundaries densified to 0.005 degrees",
            "input_sites": input_count,
            "retained_sites": len(gdf),
            "excluded_sites": input_count - len(gdf),
            "excluded_by_reason": {"minimum_site_area": input_count - len(gdf)},
            "geometry_repairs": repaired_count,
            "input_area_ha": input_area,
            "retained_area_ha": float(gdf["area_ha"].sum()),
            "min_site_area_ha": min_site_area_ha,
        }
        task.extra_metadata = {
            **(task.extra_metadata or {}),
            "preparation": preparation,
        }
        db.commit()
        if gdf.empty:
            raise ValueError(f"No sites are at least {min_site_area_ha} ha.")

        # Always split sites across exact-match boundaries when polygon-type
        # exact_match_vars are specified.  Splitting assigns each site piece
        # to a unique region combination so Batch can process one group at a
        # time.
        if exact_match_vars:
            _split_t0 = _time.perf_counter()
            split_gdf, group_mapping = compute_exact_match_groups_with_splitting(
                gdf,
                exact_match_vars,
                reference_layer_uris=reference_layer_uris,
                cache_sites=all_sites,
            )
            logger.info(
                "[SUBMIT-WORKER] Task %s: site splitting %d → %d in %.2fs",
                task_id,
                len(gdf),
                len(split_gdf),
                _time.perf_counter() - _split_t0,
            )
            gdf_for_db = split_gdf
            if "ae_atoms" in gdf.columns:
                atoms_by_site = gdf.set_index("site_id")["ae_atoms"]
                gdf_for_db["ae_atoms"] = gdf_for_db["site_id"].map(atoms_by_site)
        else:
            gdf_for_db = gdf.assign(
                sub_site_index=0, is_sub_site=False, original_area_ha=None
            )
            group_mapping = None
        if (
            reference_layer_uris
            and get_reference_layer_uris(exact_match_vars) != reference_layer_uris
        ):
            raise ValueError(
                "Reference layers changed during preparation. Retry the analysis."
            )

        # Store the same equal-area value in TaskSite (web UI) and the S3
        # Parquet (R analysis pipeline).
        _geoms_deg = np.asarray(gdf_for_db.geometry, dtype=object)
        _areas_ha_arr = _equal_area_ha(_geoms_deg)
        gdf_for_db = gdf_for_db.assign(area_ha=_areas_ha_arr)

        # Delete any TaskSite rows left by a previous interrupted attempt so
        # this step is idempotent on Celery retry (acks_late + reject_on_worker_lost
        # can re-queue the task after a partial commit was rolled back by Postgres,
        # but some rows may have been flushed before the connection was lost).
        existing_site_count = (
            db.query(TaskSite).filter(TaskSite.task_id == task_id).delete()
        )
        if existing_site_count:
            db.flush()
            logger.warning(
                "[SUBMIT-WORKER] Task %s: deleted %d TaskSite rows from a "
                "previous interrupted attempt before re-inserting",
                task_id,
                existing_site_count,
            )

        # Bulk-insert TaskSite rows using SQLAlchemy Core (bypasses ORM overhead).
        # For 200K rows, db.add() in a loop takes 20-70s; execute(insert(), mappings)
        # takes <2s because it avoids per-row Python object instantiation and
        # sends all rows in a single INSERT statement.
        task.n_sites = len(gdf)
        _empty_mask = shapely.is_empty(_geoms_deg) | shapely.is_missing(_geoms_deg)
        del _geoms_deg
        _sid_arr = gdf_for_db["site_id"].astype(str).tolist()
        _display_arr = (
            gdf_for_db["display_site_id"].astype(str).tolist()
            if "display_site_id" in gdf_for_db.columns
            else _sid_arr
        )
        _name_col = (
            gdf_for_db["site_name"]
            if "site_name" in gdf_for_db.columns
            else gdf_for_db["site_id"]
        )
        _name_arr = _name_col.astype(str).tolist()
        _start_py = pd.to_datetime(gdf_for_db["start_date"]).tolist()
        _end_dt = pd.to_datetime(
            gdf_for_db["end_date"]
            if "end_date" in gdf_for_db.columns
            else pd.Series(pd.NaT, index=gdf_for_db.index),
            errors="coerce",
        )
        _end_py = [None if pd.isna(v) else v for v in _end_dt]
        _sub_idx_arr = (
            gdf_for_db["sub_site_index"].to_numpy(dtype=int)
            if "sub_site_index" in gdf_for_db.columns
            else np.zeros(len(gdf_for_db), dtype=int)
        )
        _is_sub_arr = (
            gdf_for_db["is_sub_site"].to_numpy(dtype=bool)
            if "is_sub_site" in gdf_for_db.columns
            else np.zeros(len(gdf_for_db), dtype=bool)
        )
        _orig_raw = (
            gdf_for_db["original_area_ha"].tolist()
            if "original_area_ha" in gdf_for_db.columns
            else [None] * len(gdf_for_db)
        )
        _orig_py = [
            None if (v is None or (isinstance(v, float) and np.isnan(v))) else float(v)
            for v in _orig_raw
        ]
        _site_mappings = [
            {
                "task_id": task_id,
                "site_id": _sid_arr[i],
                "display_site_id": _display_arr[i],
                "site_name": _name_arr[i],
                "start_date": _start_py[i],
                "end_date": _end_py[i],
                "area_ha": (
                    None
                    if (_empty_mask[i] or np.isnan(_areas_ha_arr[i]))
                    else float(_areas_ha_arr[i])
                ),
                "sub_site_index": int(_sub_idx_arr[i]),
                "is_sub_site": bool(_is_sub_arr[i]) or int(_sub_idx_arr[i]) > 0,
                "original_area_ha": _orig_py[i],
            }
            for i in range(len(gdf_for_db))
        ]
        db.execute(TaskSite.__table__.insert(), _site_mappings)
        assigned_mask = np.ones(len(gdf_for_db), dtype=bool)
        if group_mapping and 0 in group_mapping:
            unassigned = set(group_mapping[0])
            assigned_mask = np.array(
                [
                    (sid, int(sub)) not in unassigned
                    for sid, sub in zip(_sid_arr, _sub_idx_arr, strict=True)
                ]
            )
        _fragment_area = float(_areas_ha_arr.sum())
        _uncovered_area = float(_areas_ha_arr[~assigned_mask].sum())
        _warnings = []
        if (
            _fragment_area > 0
            and _uncovered_area / _fragment_area > UNCOVERED_AREA_WARNING_FRACTION
        ):
            _warnings.append(
                {
                    "code": "large_uncovered_area",
                    "message": (
                        f"{_uncovered_area / _fragment_area:.0%} of the site area "
                        f"({_uncovered_area:,.0f} ha) has no exact-match region "
                        "(for example marine areas) and will not be matched. "
                        "Consider removing these sites before upload."
                    ),
                }
            )
            logger.warning(
                "[SUBMIT-WORKER] Task %s: %s", task_id, _warnings[-1]["message"]
            )
        task.extra_metadata = {
            **(task.extra_metadata or {}),
            "preparation": {
                **preparation,
                "fragments": len(gdf_for_db),
                "fragment_area_ha": _fragment_area,
                "covered_area_ha": float(_areas_ha_arr[assigned_mask].sum()),
                "uncovered_area_ha": _uncovered_area,
                "warnings": _warnings,
                "reassigned_area_ha": 0.0,
                "unmatched_fragments": int((~assigned_mask).sum()),
                "excluded_from_matching_sites": len(
                    set(np.asarray(_sid_arr)[~assigned_mask])
                    - set(np.asarray(_sid_arr)[assigned_mask])
                ),
                "reference_versions": gdf_for_db.attrs.get("reference_versions", {}),
                "reference_artifacts": reference_layer_uris,
            },
        }
        db.commit()

        logger.info(
            "[SUBMIT-WORKER] Task %s: %d site rows created, uploading to S3",
            task_id,
            len(gdf_for_db),
        )

        _s3_t0 = _time.perf_counter()
        # Always encode gdf_for_db to Parquet so the R analysis pipeline reads
        # the same equal-area area_ha values that were written to TaskSite.
        sites_parquet_uri = upload_sites_parquet_to_s3(gdf_for_db, task_id)
        task.config = {
            **(task.config or {}),
            "sites_parquet_s3_uri": sites_parquet_uri,
        }
        db.commit()
        sites_uri = None
        if not task.site_set_id:
            sites_uri = upload_sites_to_s3(gdf_for_db, task_id)
        logger.info(
            "[SUBMIT-WORKER] Task %s: site artifacts uploaded (parquet=%s, geojson=%s) (%.2fs)",
            task_id,
            sites_parquet_uri,
            sites_uri,
            _time.perf_counter() - _s3_t0,
        )

        # Group 0 holds sites with no exact-match region assignment (all polygon
        # exact_match_vars were NULL for that site). These sites are reported as
        # failed by the summarize step.
        if group_mapping and 0 in group_mapping:
            logger.warning(
                "[SUBMIT-WORKER] Task %s: %d site(s) had no exact-match region "
                "assignment (group 0) and will be excluded from matching",
                task_id,
                len(group_mapping[0]),
            )
            match_groups = {k: v for k, v in group_mapping.items() if k != 0}
        else:
            match_groups = group_mapping
        if group_mapping is not None and not match_groups:
            raise ValueError("No sites have exact-match region assignments.")
        # Build params matching AvoidedEmissionsParams schema.
        _cog_suffixes = {1000: "_1km", 250: "_250m"}
        _cog_suffix = _cog_suffixes.get(resolution_m, "_1km")
        cog_prefix = f"{Config.S3_PREFIX}/cog{_cog_suffix}"

        params = {
            "workflow_backend": "nextflow",
            "task_id": task_id,
            "task_name": task.name,
            "task_description": task.description,
            **({"sites_s3_uri": sites_uri} if sites_uri else {}),
            "sites_parquet_s3_uri": sites_parquet_uri,
            "cog_bucket": Config.S3_BUCKET,
            "cog_prefix": cog_prefix,
            "resolution_m": resolution_m,
            "covariates": list(task.covariates or []),
            "exact_match_vars": exact_match_vars,
            "matching_extent": matching_extent,
            "fc_years": fc_years,
            "max_treatment_pixels": max_treatment_pixels,
            "control_multiplier": control_multiplier,
            "min_glm_treatment_pixels": min_glm_treatment_pixels,
            "caliper_width": caliper_width,
            "max_controls_per_treatment": max_controls_per_treatment,
            "min_control_distance_km": min_control_distance_km,
            "separation_fallback_mahalanobis": bool(separation_fallback_mahalanobis),
            "matching_method": matching_method,
            "n_replicates": n_replicates,
            "replicate_block_size": min(
                n_replicates, ANALYSIS_DEFAULTS["replicate_block_size"]
            ),
            "match_batch_size": match_batch_size,
            **(
                {"sites_exclusion_buffer": sites_exclusion_buffer}
                if sites_exclusion_buffer
                else {}
            ),
            **(
                {"exact_match_group_mapping": match_groups}
                if match_groups is not None
                else {}
            ),
            **({"random_seed": random_seed} if random_seed is not None else {}),
            **(
                {"reference_layer_uris": reference_layer_uris}
                if reference_layer_uris
                else {}
            ),
            "results_s3_uri": (
                f"s3://{Config.S3_BUCKET}/{Config.S3_PREFIX}/tasks/{task_id}/output"
            ),
            "match_memory_mib": match_memory_mib,
            "matching_job_queue": matching_job_queue,
        }

        # Submit as the user, with a fresh access token from their session
        # (refreshed here if it is about to expire).
        script_id = Config.TRENDSEARTH_SCRIPT_ID
        _auth_t0 = _time.perf_counter()
        try:
            access_token = session_store.get_access_token(
                task.submit_session_id, margin=60
            )
        except session_store.SessionExpired as exc:
            raise ValueError(
                "Your Trends.Earth session ended before the analysis could be "
                "submitted. Sign in again and resubmit the task."
            ) from exc
        client = TrendsEarthClient(
            api_url=Config.TRENDSEARTH_API_URL, access_token=access_token
        )
        logger.info(
            "[SUBMIT-WORKER] Task %s: user token ready in %.2fs, calling API "
            "(script=%s)",
            task_id,
            _time.perf_counter() - _auth_t0,
            script_id,
        )
        _api_t0 = _time.perf_counter()
        execution = client.create_execution(script_id, params)
        logger.info(
            "[SUBMIT-WORKER] Task %s: API create_execution took %.2fs",
            task_id,
            _time.perf_counter() - _api_t0,
        )

        exec_data = execution.get("data", {})
        exec_id = exec_data.get("id", "")
        exec_status = exec_data.get("status", "unknown")
        logger.info(
            "[SUBMIT-WORKER] Task %s: API execution created — exec_id=%s, "
            "initial_status=%s",
            task_id,
            exec_id,
            exec_status,
        )

        task.sites_s3_uri = sites_uri
        task.config = {
            **config,
            "workflow_backend": "nextflow",
            **({"sites_s3_uri": sites_uri} if sites_uri else {}),
            "sites_parquet_s3_uri": sites_parquet_uri,
        }
        task.results_s3_uri = params["results_s3_uri"]
        task.status = "submitted"
        task.submitted_at = datetime.now(UTC)
        task.extract_job_id = f"api:{exec_id}"
        db.commit()
        logger.info(
            "[SUBMIT-WORKER] Task %s: status → submitted (api:%s) — total time %.2fs",
            task_id,
            exec_id,
            _time.perf_counter() - _t0,
        )

    except Exception as e:
        logger.exception(
            "[SUBMIT-WORKER] Task %s FAILED: %s",
            task_id,
            e,  # noqa: TRY401
        )
        db.rollback()
        try:
            task_obj = db.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
            if task_obj:
                task_obj.status = "failed"
                task_obj.error_message = str(e)
                db.commit()
        except Exception:  # noqa: BLE001
            db.rollback()
        raise
    finally:
        if "parquet_buf" in locals() and parquet_buf is not None:
            parquet_buf.close()
        db.close()


def _cancel_api_execution(task, user, api_exec_id):
    """Cancel an execution on trends.earth.

    The owner cancels with their own token.  When an app admin cancels
    someone else's task, the service client is used, since the admin's
    Trends.Earth account does not own the execution.
    """
    import session_store
    from trendsearth_client import TrendsEarthClient

    if str(task.submitted_by) == str(user.id):
        client = TrendsEarthClient(
            api_url=Config.TRENDSEARTH_API_URL,
            access_token=session_store.get_access_token(user.session_id),
        )
    else:
        client = TrendsEarthClient.from_oauth2_credentials(
            api_url=Config.TRENDSEARTH_API_URL,
            client_id=Config.TRENDSEARTH_CLIENT_ID,
            client_secret=Config.TRENDSEARTH_CLIENT_SECRET,
        )
    client.cancel_execution(api_exec_id)


def can_resume_task(task, user):
    """Whether a failed, API-backed Nextflow task can offer a resume action."""
    if not (
        user
        and (user.is_admin or str(task.submitted_by) == str(user.id))
        and task.status == "failed"
        and (task.extract_job_id or "").startswith("api:")
        and task.extract_job_id[4:]
    ):
        return False
    config = task.config or {}
    if "workflow_backend" in config:
        return config["workflow_backend"] == "nextflow"
    jobs = (task.extra_metadata or {}).get("batch_jobs") or {}
    return bool(jobs.get("nextflow"))


def resume_task(task_id, user):
    """Resume the same API execution, then reactivate local status polling."""
    import session_store
    from trendsearth_client import TrendsEarthClient

    db = get_db()
    try:
        task = (
            db.query(AnalysisTask)
            .filter(AnalysisTask.id == task_id)
            .with_for_update()
            .first()
        )
        if not task:
            raise ValueError("Task not found.")
        if not user or (not user.is_admin and str(task.submitted_by) != str(user.id)):
            raise PermissionError("You can only resume your own tasks.")
        if not can_resume_task(task, user):
            raise ValueError(
                "Only failed Nextflow tasks with an API execution can be resumed."
            )
        client = TrendsEarthClient(
            api_url=Config.TRENDSEARTH_API_URL,
            access_token=session_store.get_access_token(user.session_id),
        )
        client.resume_execution(task.extract_job_id[4:])
        task.status = "submitted"
        task.progress = 0
        task.completed_at = None
        task.error_message = None
        task.submit_session_id = user.session_id
        task.config = {**(task.config or {}), "workflow_backend": "nextflow"}
        metadata = dict(task.extra_metadata or {})
        metadata.pop("batch_jobs", None)
        task.extra_metadata = metadata
        db.commit()
        logger.info("[RESUME] Task %s: resuming %s", task_id, task.extract_job_id)
    finally:
        db.close()


def cancel_task(task_id, user):
    """Cancel a running task and its API execution.

    Sets the local task status to ``cancelled`` and calls the
    trends.earth API cancel endpoint if an execution ID is tracked.
    """
    db = get_db()
    try:
        task = db.query(AnalysisTask).get(task_id)
        if not task:
            raise ValueError("Task not found.")

        if not user.is_admin and str(task.submitted_by) != str(user.id):
            raise PermissionError("You can only cancel your own tasks.")

        if task.status in ("succeeded", "failed", "cancelled"):
            raise ValueError(f"Task is already {task.status}.")

        # Cancel on the API side if we have an execution ID
        exec_ref = task.extract_job_id or ""
        if exec_ref.startswith("api:"):
            api_exec_id = exec_ref[4:]
            try:
                _cancel_api_execution(task, user, api_exec_id)
                logger.info(
                    "[CANCEL] Task %s: API execution %s cancelled",
                    task_id,
                    api_exec_id,
                )
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "[CANCEL] Task %s: failed to cancel API execution %s: %s",
                    task_id,
                    api_exec_id,
                    e,
                )

        task.status = "cancelled"
        db.commit()
        logger.info("[CANCEL] Task %s: status → cancelled", task_id)
    finally:
        db.close()


def get_task_execution_logs(task_id, user):
    """Fetch user-visible execution logs using the acting user's credentials."""
    import session_store
    from trendsearth_client import TrendsEarthClient

    db = get_db()
    try:
        task = db.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
        if not task:
            raise ValueError("Task not found.")
        if not user or (not user.is_admin and str(task.submitted_by) != str(user.id)):
            raise PermissionError("You can only view logs for your own tasks.")
        execution = task.extract_job_id or ""
        if not execution.startswith("api:") or not execution[4:]:
            raise ValueError("No API execution logs are available for this task.")
        execution_id = execution[4:]
    finally:
        db.close()

    client = TrendsEarthClient(
        api_url=Config.TRENDSEARTH_API_URL,
        access_token=session_store.get_access_token(user.session_id),
    )
    return client.get_execution_logs(execution_id)


def get_task_list(user_id=None, limit=None):
    """Get analysis tasks, optionally filtered by user.

    Heavy metadata is loaded only for failed legacy tasks that need it to
    determine resume eligibility. Pass ``limit`` to cap results; by default
    the full history is available for client-side sort/filter.
    """
    from sqlalchemy.orm import load_only

    db = get_db()
    try:
        query = (
            db.query(AnalysisTask)
            .options(
                load_only(
                    AnalysisTask.id,
                    AnalysisTask.name,
                    AnalysisTask.status,
                    AnalysisTask.progress,
                    AnalysisTask.n_sites,
                    AnalysisTask.submitted_by,
                    AnalysisTask.submitter_name,
                    AnalysisTask.submitter_email,
                    AnalysisTask.config,
                    AnalysisTask.covariates,
                    AnalysisTask.created_at,
                    AnalysisTask.submitted_at,
                    AnalysisTask.completed_at,
                    AnalysisTask.error_message,
                    AnalysisTask.extract_job_id,
                ),
            )
            .order_by(AnalysisTask.created_at.desc())
        )
        if user_id:
            query = query.filter(AnalysisTask.submitted_by == user_id)
        if limit is not None:
            query = query.limit(limit)
        tasks = query.all()
        legacy_ids = [
            task.id
            for task in tasks
            if task.status == "failed"
            and (task.extract_job_id or "").startswith("api:")
            and task.extract_job_id[4:]
            and "workflow_backend" not in (task.config or {})
        ]
        if legacy_ids:
            (
                db.query(AnalysisTask)
                .options(load_only(AnalysisTask.id, AnalysisTask.extra_metadata))
                .filter(AnalysisTask.id.in_(legacy_ids))
                .all()
            )
        return tasks
    finally:
        db.close()


def _fetch_sites_geojson_from_s3(sites_s3_uri):
    """Download a sites GeoJSON FeatureCollection from S3.

    Parameters
    ----------
    sites_s3_uri : str
        S3 URI (``s3://bucket/key``) pointing to a GeoJSON file.

    Returns
    -------
    dict | None
        Parsed GeoJSON FeatureCollection, or ``None`` on error.
    """
    if not sites_s3_uri or not sites_s3_uri.startswith("s3://"):
        return None
    try:
        bucket, key = _split_s3_uri(sites_s3_uri)
        s3 = get_s3_client()
        response = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except Exception:
        logger.warning(
            "Failed to download sites GeoJSON from %s", sites_s3_uri, exc_info=True
        )
        return None


def _fetch_sites_parquet_from_s3(sites_s3_uri):
    """Download a sites GeoParquet file from S3 into a GeoDataFrame."""
    if not sites_s3_uri or not sites_s3_uri.startswith("s3://"):
        return None
    try:
        bucket, key = _split_s3_uri(sites_s3_uri)
        s3 = get_s3_client()
        response = s3.get_object(Bucket=bucket, Key=key)
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            tmp.write(response["Body"].read())
            tmp.flush()
            return gpd.read_parquet(tmp.name)
    except Exception:
        logger.warning(
            "Failed to download sites GeoParquet from %s",
            sites_s3_uri,
            exc_info=True,
        )
        return None


def _split_s3_uri(s3_uri):
    """Split an S3 URI into ``(bucket, key)``."""
    without_scheme = s3_uri[5:]
    bucket, _, key = without_scheme.partition("/")
    if not bucket or not key:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    return bucket, key


def update_task_info(task_id, name=None, description=None, user_id=None):
    """Update the name and/or description of an AnalysisTask.

    When *user_id* is provided the function verifies that the task
    belongs to that user (defense-in-depth).  Callers should always
    pass the authenticated user's ID.

    Returns the updated task dict ``{"name": ..., "description": ...}``
    on success, or *None* if the task does not exist or access is denied.
    """
    db = get_db()
    try:
        task = db.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
        if not task:
            return None
        if user_id is not None and str(task.submitted_by) != str(user_id):
            # Admins should pass user_id=None to bypass the ownership check.
            return None
        if name is not None:
            task.name = name.strip()[:255]
        if description is not None:
            task.description = description.strip() or None
        db.commit()
        return {"name": task.name, "description": task.description}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# Tasks with more sites than this threshold use aggregated DB queries for the
# results/plots view instead of loading every per-site-year row into Python.
# This prevents OOM and serialization timeouts in the Dash process.
LARGE_TASK_THRESHOLD = 200
SITE_RANKING_YEAR = 2025


def get_task_top_site_yearly(task_id, site_ids):
    """Load 2025 leaders and signed yearly remainders without loading all sites."""
    db = get_db()
    try:
        metrics = (
            TaskResult.extrapolated_emissions_avoided_mgco2e,
            TaskResult.extrapolated_forest_loss_avoided_ha,
        )
        filters = (
            TaskResult.task_id == task_id,
            TaskResult.site_id.in_(site_ids),
        )
        top_sites = []
        signed_yearly = {}
        for metric in metrics:
            ranking = (
                db.query(TaskResult.site_id)
                .filter(*filters, TaskResult.year == SITE_RANKING_YEAR)
                .group_by(TaskResult.site_id)
            )
            total = func.sum(metric)
            positive = (
                ranking.having(total > 0)
                .order_by(total.desc(), TaskResult.site_id)
                .limit(3)
                .subquery()
            )
            negative = (
                ranking.having(total < 0)
                .order_by(total.asc(), TaskResult.site_id)
                .limit(3)
                .subquery()
            )
            selected = db.query(positive.c.site_id).union(db.query(negative.c.site_id))
            top_sites.extend((positive, negative))
            remainder = (
                db.query(
                    TaskResult.year,
                    func.sum(case((metric > 0, metric), else_=0)).label("positive"),
                    func.sum(case((metric < 0, metric), else_=0)).label("negative"),
                )
                .filter(*filters, TaskResult.site_id.not_in(selected))
                .group_by(TaskResult.year)
                .order_by(TaskResult.year)
                .all()
            )
            signed_yearly[metric.key.removeprefix("extrapolated_")] = [
                {
                    "year": row.year,
                    "positive": row.positive,
                    "negative": row.negative,
                }
                for row in remainder
            ]
        selected = db.query(top_sites[0].c.site_id).union(
            *(db.query(sites.c.site_id) for sites in top_sites[1:])
        )
        rows = (
            db.query(TaskResult.site_id, TaskResult.year, *metrics)
            .filter(*filters, TaskResult.site_id.in_(selected))
            .order_by(TaskResult.site_id, TaskResult.year)
            .all()
        )
        return {
            "site_yearly": [
                {
                    "site_id": row.site_id,
                    "year": row.year,
                    "emissions_avoided_mgco2e": row.extrapolated_emissions_avoided_mgco2e,
                    "forest_loss_avoided_ha": row.extrapolated_forest_loss_avoided_ha,
                }
                for row in rows
            ],
            "signed_yearly": signed_yearly,
        }
    finally:
        db.close()


def get_task_site_results(task_id, site_id):
    """Return per-year TaskResult rows for a single site as a list of dicts.

    Used by the site drill-down callback on large tasks so the full result
    set does not need to be loaded into memory upfront.
    """
    db = get_db()
    try:
        rows = (
            db.query(TaskResult)
            .filter(TaskResult.task_id == task_id, TaskResult.site_id == site_id)
            .order_by(TaskResult.year)
            .all()
        )
        return [
            {
                "site_id": r.site_id,
                "year": r.year,
                "treatment_defor_ha": r.extrapolated_treatment_defor_ha or 0,
                "control_defor_ha": r.extrapolated_control_defor_ha or 0,
                "emissions_avoided_mgco2e": r.extrapolated_emissions_avoided_mgco2e
                or 0,
                "forest_loss_avoided_ha": r.extrapolated_forest_loss_avoided_ha or 0,
                "treatment_emissions_mgco2e": r.extrapolated_treatment_emissions_mgco2e
                or 0,
                "control_emissions_mgco2e": r.extrapolated_control_emissions_mgco2e
                or 0,
                "treatment_emissions_mgco2e_ci_lower": r.extrapolated_treatment_emissions_mgco2e_ci_lower,
                "treatment_emissions_mgco2e_ci_upper": r.extrapolated_treatment_emissions_mgco2e_ci_upper,
                "control_emissions_mgco2e_ci_lower": r.extrapolated_control_emissions_mgco2e_ci_lower,
                "control_emissions_mgco2e_ci_upper": r.extrapolated_control_emissions_mgco2e_ci_upper,
                "cumulative_treatment_defor_ha": r.extrapolated_cumulative_treatment_defor_ha,
                "cumulative_control_defor_ha": r.extrapolated_cumulative_control_defor_ha,
                "cumulative_treatment_defor_ha_ci_lower": r.extrapolated_cumulative_treatment_defor_ha_ci_lower,
                "cumulative_treatment_defor_ha_ci_upper": r.extrapolated_cumulative_treatment_defor_ha_ci_upper,
                "cumulative_control_defor_ha_ci_lower": r.extrapolated_cumulative_control_defor_ha_ci_lower,
                "cumulative_control_defor_ha_ci_upper": r.extrapolated_cumulative_control_defor_ha_ci_upper,
                "is_pre_intervention": bool(r.is_pre_intervention),
                "is_post_intervention": bool(r.is_post_intervention),
                "treatment_defor_ha_ci_lower": r.extrapolated_treatment_defor_ha_ci_lower,
                "treatment_defor_ha_ci_upper": r.extrapolated_treatment_defor_ha_ci_upper,
                "control_defor_ha_ci_lower": r.extrapolated_control_defor_ha_ci_lower,
                "control_defor_ha_ci_upper": r.extrapolated_control_defor_ha_ci_upper,
                "emissions_avoided_mgco2e_ci_lower": r.extrapolated_emissions_avoided_mgco2e_ci_lower,
                "emissions_avoided_mgco2e_ci_upper": r.extrapolated_emissions_avoided_mgco2e_ci_upper,
                "forest_loss_avoided_ha_ci_lower": r.extrapolated_forest_loss_avoided_ha_ci_lower,
                "forest_loss_avoided_ha_ci_upper": r.extrapolated_forest_loss_avoided_ha_ci_upper,
            }
            for r in rows
        ]
    finally:
        db.close()


def get_task_detail(task_id):
    """Get full task details including sites and results.

    For tasks with more than ``LARGE_TASK_THRESHOLD`` sites the per-site-year
    ``TaskResult`` rows and ``TaskSite`` rows are **not** loaded into Python,
    so the Dash process never has to hold tens of thousands of ORM objects in
    memory.  The ``is_large`` flag tells callers which path was taken.
    ``agg_yearly`` is the all-site yearly series (with replicate ranges)
    computed by the R summary step.
    """
    db = get_db()
    try:
        task = db.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
        if not task:
            return None

        n_sites = task.n_sites or 0
        is_large = n_sites > LARGE_TASK_THRESHOLD

        # Always load per-site totals — one row per site, manageable even at
        # thousands of sites, and needed for the overview summary cards.
        totals = (
            db.query(TaskResultTotal).filter(TaskResultTotal.task_id == task_id).all()
        )

        # Portfolio (all-site) yearly series computed by the R summary step.
        # Bounds must come from here: summing per-site bounds is invalid.
        agg_yearly = (task.extra_metadata or {}).get("portfolio", {}).get("yearly", [])

        if is_large:
            # Avoid loading tens of thousands of per-site-year rows.
            sites = []
            results = None
            # Load centroid GeoJSON for the results map so that all sites are
            # shown as emission-coloured circles regardless of dataset size.
            sites_geojson = None
            if task.site_set_id:
                try:
                    sites_geojson = get_user_site_set_centroids_geojson(
                        task.site_set_id
                    )
                except Exception:
                    logger.warning(
                        "get_task_detail: could not load centroids for site set %s",
                        task.site_set_id,
                        exc_info=True,
                    )
            logger.info(
                "get_task_detail: task %s has %d sites (> threshold %d) — "
                "using aggregated path",
                task_id,
                n_sites,
                LARGE_TASK_THRESHOLD,
            )
        else:
            sites = db.query(TaskSite).filter(TaskSite.task_id == task_id).all()
            results = (
                db.query(TaskResult)
                .filter(TaskResult.task_id == task_id)
                .order_by(TaskResult.site_id, TaskResult.year)
                .all()
            )
            sites_geojson = None
            if task.site_set_id:
                sites_geojson = get_user_site_set_geojson(task.site_set_id)
            if not sites_geojson or not sites_geojson.get("features"):
                # Adopted tasks have no local site set — fall back to S3
                s3_uri = task.sites_s3_uri
                if not s3_uri:
                    s3_uri = (task.config or {}).get("sites_s3_uri")
                if s3_uri:
                    sites_geojson = _fetch_sites_geojson_from_s3(s3_uri)
                else:
                    parquet_uri = (task.config or {}).get("sites_parquet_s3_uri")
                    if parquet_uri:
                        parquet_gdf = _fetch_sites_parquet_from_s3(parquet_uri)
                        if parquet_gdf is not None and not parquet_gdf.empty:
                            sites_geojson = json.loads(
                                upload_sites_to_geojson(parquet_gdf)
                            )

        return {
            "task": task,
            "sites": sites,
            "results": results,
            "totals": totals,
            "sites_geojson": sites_geojson,
            "is_large": is_large,
            "agg_yearly": agg_yearly,
        }
    finally:
        db.close()


_YEAR_METRICS = (
    "extrapolated_forest_loss_avoided_ha",
    "extrapolated_emissions_avoided_mgco2e",
    "extrapolated_treatment_defor_ha",
    "extrapolated_control_defor_ha",
    "extrapolated_treatment_emissions_mgco2e",
    "extrapolated_control_emissions_mgco2e",
)
_CUMULATIVE_YEAR_METRICS = (
    "extrapolated_cumulative_treatment_defor_ha",
    "extrapolated_cumulative_control_defor_ha",
)
_TOTAL_METRICS = (
    "extrapolated_forest_loss_avoided_ha",
    "extrapolated_emissions_avoided_mgco2e",
)
# Summary fields stored on AnalysisTask.extra_metadata for the UI.
_SUMMARY_METADATA = (
    "n_sites",
    "n_failed_sites",
    "n_replicates",
    "biomass_reference_year",
    "bounds_label",
    "failed_sites",
    "worker_failures",
    "subsampled_sites",
    "methodology_warnings",
    "portfolio",
)


class ResultImportError(ValueError):
    """The results payload is missing values or is internally inconsistent."""


def _metric_columns(values, metrics, n_replicates):
    """Metric values plus their replicate bounds when n_replicates > 1."""
    columns = {key: values[key] for key in metrics}
    if n_replicates > 1:
        for key in metrics:
            for suffix in ("_ci_lower", "_ci_upper"):
                columns[key + suffix] = values[key + suffix]
    return columns


def _parse_results_payload(task_id, payload):
    """Return ``(summary, year_rows, total_rows)`` or raise ResultImportError."""
    try:
        summary = payload["summary"]
        n_replicates = int(summary["n_replicates"])
        year_rows = []
        for ts in payload.get("time_series") or []:
            meta = ts["metadata"]
            values = ts["values"]
            cumulative_metrics = (
                _CUMULATIVE_YEAR_METRICS
                if _CUMULATIVE_YEAR_METRICS[0] in values
                else ()
            )
            year_rows.append(
                {
                    "task_id": task_id,
                    "site_id": ts["entity_id"],
                    "year": int(ts["year"]),
                    **_metric_columns(ts["values"], _YEAR_METRICS, n_replicates),
                    **_metric_columns(values, cumulative_metrics, n_replicates),
                    "is_pre_intervention": bool(meta["is_pre_intervention"]),
                    "is_post_intervention": bool(meta["is_post_intervention"]),
                    "n_sample_pixels": meta["n_sample_pixels"],
                    "sampled_fraction": meta["sampled_fraction"],
                }
            )
        total_rows = []
        for rec in payload.get("records") or []:
            meta = rec["metadata"]
            total_rows.append(
                {
                    "task_id": task_id,
                    "site_id": rec["entity_id"],
                    "site_name": rec.get("entity_name"),
                    **_metric_columns(rec["values"], _TOTAL_METRICS, n_replicates),
                    "area_ha": rec["values"]["area_ha"],
                    "n_sample_pixels": meta["n_sample_pixels"],
                    "n_eligible_pixels": meta["n_eligible_pixels"],
                    "n_treatment_pixels": meta["n_treatment_pixels"],
                    "sampled_fraction": meta["sampled_fraction"],
                    "first_year": rec["period_start"],
                    "last_year": rec["period_end"],
                    "n_years": meta["n_years"],
                }
            )
        metadata = {key: summary[key] for key in _SUMMARY_METADATA}
    except (KeyError, TypeError, ValueError) as exc:
        raise ResultImportError(f"Invalid results payload: {exc!r}") from exc

    year_keys = [(r["site_id"], r["year"]) for r in year_rows]
    site_keys = [r["site_id"] for r in total_rows]
    if len(set(year_keys)) != len(year_keys) or len(set(site_keys)) != len(site_keys):
        raise ResultImportError("Results payload contains duplicate site rows")
    if summary.get("r_analysis_git_sha"):
        metadata["r_analysis_git_sha"] = summary["r_analysis_git_sha"]
    return metadata, year_rows, total_rows


def import_execution_results(task_id, results_payload, db=None):
    """Validate an AnalysisResults payload and save it to TaskResult / TaskResultTotal.

    Called by the polling task when an API execution finishes.  The
    *results_payload* is the dict returned by
    ``TrendsEarthClient.get_execution_results()`` — a serialised
    ``AnalysisResults`` object with ``records`` (per-site totals) and
    ``time_series`` (per-site-year observations).

    Raises :class:`ResultImportError` when the payload is incomplete or
    inconsistent.  Existing result rows for the task are replaced, so the
    import can be retried.

    Parameters
    ----------
    task_id : str
        UUID of the local ``AnalysisTask``.
    results_payload : dict
        Serialised ``AnalysisResults`` from the API.
    db : Session, optional
        Existing DB session.  If *None*, a new session is created and
        committed/closed within this function.
    """
    metadata, year_rows, total_rows = _parse_results_payload(task_id, results_payload)
    own_session = db is None
    if own_session:
        db = get_db()
    try:
        task_obj = db.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
        if task_obj:
            task_obj.extra_metadata = {**(task_obj.extra_metadata or {}), **metadata}
        labels = {
            row.site_id: row.display_site_id
            for row in db.query(TaskSite).filter(TaskSite.task_id == task_id).all()
        }
        for row in year_rows + total_rows:
            row["display_site_id"] = labels.get(row["site_id"], row["site_id"])
        db.query(TaskResult).filter(TaskResult.task_id == task_id).delete()
        db.query(TaskResultTotal).filter(TaskResultTotal.task_id == task_id).delete()
        if year_rows:
            db.execute(TaskResult.__table__.insert(), year_rows)
        if total_rows:
            db.execute(TaskResultTotal.__table__.insert(), total_rows)
        if own_session:
            db.commit()
        logger.info(
            "import_execution_results(%s): imported %d site-year rows, %d site rows",
            task_id,
            len(year_rows),
            len(total_rows),
        )
    except Exception:
        if own_session:
            db.rollback()
        raise
    finally:
        if own_session:
            db.close()


def adopt_api_execution(exec_data, db):
    """Create a local AnalysisTask from an API execution not yet tracked.

    Called by the polling task when it discovers an avoided-emissions
    execution on the trends.earth API that has no corresponding local
    ``AnalysisTask`` record.  A stub task is created with as much
    metadata as can be extracted from the execution's ``params``.

    Parameters
    ----------
    exec_data : dict
        Serialised execution record from the API (the ``data`` dict),
        containing at minimum ``id``, ``status``, ``params``,
        ``start_date``.
    db : Session
        Open DB session (caller manages commit/close).

    Returns
    -------
    AnalysisTask
        The newly created task object (already added to the session).
    """
    exec_id = exec_data["id"]
    params = exec_data.get("params") or {}
    api_status = (exec_data.get("status") or "PENDING").upper()

    # Map API status to local status
    status_map = {
        "FINISHED": "succeeded",
        "FAILED": "failed",
        "CANCELLED": "cancelled",
        "RUNNING": "running",
        "READY": "running",
    }
    local_status = status_map.get(api_status, "submitted")

    # Users are identified by their trends.earth user id (the OIDC
    # subject), so the execution's owner is the task's owner.
    try:
        owner_id = uuid.UUID(str(exec_data.get("user_id")))
    except (TypeError, ValueError):
        logger.warning("[ADOPT] Execution %s has no usable user_id; skipping", exec_id)
        return None

    # Reconstruct n_sites from the pipeline array_size if available,
    # and extract match_memory_mib from the "match" pipeline step.
    n_sites = 1
    pipeline = params.get("pipeline") or []
    for step in pipeline:
        if isinstance(step, dict):
            if step.get("array_size"):
                n_sites = step["array_size"]
            if step.get("name") == "match" and step.get("memory_mib"):
                params.setdefault("match_memory_mib", step["memory_mib"])

    # Extract matching_job_queue and memory from batch overrides.
    # For non-pipeline (single-site) tasks the pipeline loop above
    # won't find a "match" step, so fall back to batch.memory_mib.
    batch = params.get("batch") or {}
    if batch.get("job_queue"):
        params.setdefault("matching_job_queue", batch["job_queue"])
    if batch.get("memory_mib"):
        params.setdefault("match_memory_mib", batch["memory_mib"])

    task = AnalysisTask(
        id=uuid.uuid4(),
        name=params.get("task_name")
        or params.get("task_id", f"Discovered: {exec_id[:8]}"),
        description=params.get("task_description")
        or f"Auto-discovered from trends.earth API execution {exec_id}",
        submitted_by=owner_id,
        status=local_status,
        progress=exec_data.get("progress"),
        extract_job_id=f"api:{exec_id}",
        config=params,
        covariates=params.get("covariates", []),
        n_sites=n_sites,
        sites_s3_uri=params.get("sites_s3_uri"),
        results_s3_uri=params.get("results_s3_uri"),
        submitted_at=_parse_iso_datetime(exec_data.get("start_date")),
        started_at=_parse_iso_datetime(exec_data.get("start_date")),
        completed_at=_parse_iso_datetime(exec_data.get("end_date")),
        extra_metadata={
            "discovered_from_api": True,
            "api_exec_id": exec_id,
            **(
                {"batch_jobs": (exec_data.get("results") or {}).get("batch_jobs")}
                if isinstance((exec_data.get("results") or {}).get("batch_jobs"), dict)
                else {}
            ),
        },
    )

    if local_status in ("failed",):
        results = exec_data.get("results") or {}
        task.error_message = results.get("error", "Execution failed on API")

    db.add(task)
    logger.info(
        "adopt_api_execution: created local task %s for API exec %s (status=%s)",
        task.id,
        exec_id,
        local_status,
    )
    return task


def _parse_iso_datetime(value):
    """Parse an ISO-8601 datetime string, returning None on failure."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (ValueError, AttributeError):
        return None


def _atom_metadata_values(gdf):
    """Expose the preserved ae_atoms source property on submitted sites."""
    if "ae_atoms" in gdf.columns:
        return gdf["ae_atoms"]
    if "source_attributes" not in gdf.columns:
        return pd.Series([None] * len(gdf), index=gdf.index)

    def get_value(value):
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError as exc:
                raise ValueError("Invalid source_attributes JSON.") from exc
        return value.get("ae_atoms") if isinstance(value, dict) else None

    return gdf["source_attributes"].map(get_value)


def download_results_csv(task_id, result_type="by_site_year", results_s3_uri=None):
    """Download result CSV from S3 for a completed task.

    Args:
        task_id: The task UUID.
        result_type: One of 'by_site_year', 'by_site_total', 'pixel_level',
            'match_covariates', 'balance', 'propensity_scores',
            'match_quality_summary', 'matched_pixels'.
        results_s3_uri: Optional ``s3://bucket/prefix`` URI pointing to the
            output directory.  When provided the bucket and prefix are
            extracted from this URI instead of being derived from
            ``Config.S3_PREFIX`` and *task_id*.  This is important for
            tasks adopted from the trends.earth API whose local UUID
            differs from the original task_id embedded in the S3 path.
            If *None*, falls back to looking up the task's stored URI
            in the database before constructing a default path.

    Returns:
        CSV content as string, or None if not found.
    """
    filename_map = {
        "by_site_year": "results_by_site_year.csv",
        "by_site_total": "results_by_site_total.csv",
        "pixel_level": "results_pixel_year_emissions.csv",
        "match_covariates": "results_pixel_covariates.csv",
        "balance": "results_covariate_balance.csv",
        "propensity_scores": "results_propensity_scores.csv",
        "match_quality_summary": "results_match_quality_summary.json",
        "matched_pixels": "results_pixel_locations.csv",
        "summary": "results_summary.json",
        "site_identity": "site_identity.csv",
        "atom_replicates": "results_by_atom_replicate_year.csv",
        "atom_status": "results_atom_status.csv",
    }
    filename = filename_map.get(result_type)
    if not filename:
        return None

    # Resolve the S3 location of the output directory.  Priority:
    #   1. Explicit results_s3_uri argument
    #   2. AnalysisTask.results_s3_uri from the database
    #   3. Constructed default from Config.S3_PREFIX + task_id
    if not results_s3_uri:
        from models import AnalysisTask, get_db

        db = get_db()
        try:
            task = (
                db.query(AnalysisTask.results_s3_uri)
                .filter(AnalysisTask.id == task_id)
                .first()
            )
            if task and task.results_s3_uri:
                results_s3_uri = task.results_s3_uri
        finally:
            db.close()

    s3 = get_s3_client()

    if results_s3_uri:
        # Parse s3://bucket/prefix
        uri = results_s3_uri
        uri = uri.removeprefix("s3://")
        parts = uri.split("/", 1)
        bucket = parts[0]
        prefix = parts[1].rstrip("/") if len(parts) > 1 else ""
        key = f"{prefix}/{filename}"
    else:
        bucket = Config.S3_BUCKET
        key = f"{Config.S3_PREFIX}/tasks/{task_id}/output/{filename}"

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return response["Body"].read().decode("utf-8")
    except s3.exceptions.NoSuchKey:
        return None


def get_filtered_portfolio_yearly(task_id, site_ids, n_replicates, results_s3_uri=None):
    """Aggregate site-year replicate outcomes before computing group ranges."""
    bucket, prefix = _resolve_results_s3(task_id, results_s3_uri)
    key = f"{prefix}/results_by_site_replicate_year.csv"
    body = get_s3_client().get_object(Bucket=bucket, Key=key)["Body"]
    names = {
        "extrapolated_treatment_defor_ha": "treatment_defor_ha",
        "extrapolated_control_defor_ha": "control_defor_ha",
        "extrapolated_treatment_emissions_mgco2e": "treatment_emissions_mgco2e",
        "extrapolated_control_emissions_mgco2e": "control_emissions_mgco2e",
        "extrapolated_forest_loss_avoided_ha": "forest_loss_avoided_ha",
        "extrapolated_emissions_avoided_mgco2e": "emissions_avoided_mgco2e",
    }
    columns = ["site_id", "year", "replicate", *names]
    parts = []
    seen_sites = set()
    pending = None

    def consume(rows):
        if rows.empty:
            return
        sites = set(rows["site_id"])
        if seen_sites.intersection(sites):
            raise ResultImportError("Replicate outcomes are not ordered by site.")
        seen_sites.update(sites)
        rows = rows.copy()
        for column in columns[1:]:
            rows[column] = pd.to_numeric(rows[column], errors="raise")
        coverage = rows.groupby(["site_id", "year"])["replicate"].agg(
            ["size", "nunique", "min", "max"]
        )
        if (
            rows[columns].isna().any().any()
            or not np.isfinite(rows[columns[1:]].to_numpy()).all()
            or not coverage["size"].eq(n_replicates).all()
            or not coverage["nunique"].eq(n_replicates).all()
            or not coverage["min"].eq(1).all()
            or not coverage["max"].eq(n_replicates).all()
        ):
            raise ResultImportError(
                "The selected sites have incomplete or invalid replicate outcomes."
            )
        parts.append(
            rows.groupby(["replicate", "year"], as_index=False)[list(names)].sum()
        )

    try:
        for chunk in pd.read_csv(
            body,
            usecols=columns,
            dtype={"site_id": str},
            keep_default_na=False,
            chunksize=100_000,
        ):
            part = chunk[chunk["site_id"].isin(site_ids)]
            if pending is not None:
                part = pd.concat([pending, part], ignore_index=True)
            if part.empty:
                continue
            last_site = part["site_id"].iloc[-1]
            pending = part[part["site_id"] == last_site]
            consume(part[part["site_id"] != last_site])
        if pending is not None:
            consume(pending)
    finally:
        body.close()
    if not parts:
        raise ResultImportError(
            "No replicate outcomes were found for the selected sites."
        )
    if seen_sites != set(site_ids):
        raise ResultImportError(
            "Replicate outcomes are missing for some selected sites."
        )
    yearly = pd.concat(parts).groupby(["replicate", "year"], as_index=False)[
        list(names)
    ].sum()
    yearly = yearly.rename(columns=names).sort_values(["replicate", "year"])
    for kind in ("treatment", "control"):
        yearly[f"cum_{kind}_defor_ha"] = yearly.groupby("replicate")[
            f"{kind}_defor_ha"
        ].cumsum()
    metrics = [*names.values(), "cum_treatment_defor_ha", "cum_control_defor_ha"]
    grouped = yearly.groupby("year")[metrics]
    result = grouped.mean()
    if n_replicates > 1:
        for bound, quantile in (("lower", 0.025), ("upper", 0.975)):
            bounds = grouped.quantile(quantile).add_suffix(f"_ci_{bound}")
            result = result.join(bounds)
    return result.reset_index().to_dict("records")


def get_filtered_portfolio_total(task_id, site_ids, n_replicates, results_s3_uri=None):
    """Aggregate per-site replicate totals before computing portfolio ranges."""
    site_ids = {str(site_id) for site_id in site_ids}
    if not site_ids:
        raise ValueError("At least one site is required.")
    if n_replicates < 1:
        raise ValueError("n_replicates must be positive.")

    bucket, prefix = _resolve_results_s3(task_id, results_s3_uri)
    key = f"{prefix}/results_by_site_replicate_total.csv"
    body = get_s3_client().get_object(Bucket=bucket, Key=key)["Body"]
    metrics = [
        "extrapolated_emissions_avoided_mgco2e",
        "extrapolated_forest_loss_avoided_ha",
    ]
    columns = ["site_id", "replicate", *metrics]
    parts = []
    try:
        for chunk in pd.read_csv(
            body,
            usecols=columns,
            dtype={"site_id": str},
            keep_default_na=False,
            chunksize=100_000,
        ):
            rows = chunk[chunk["site_id"].isin(site_ids)]
            if rows.empty:
                continue
            rows = rows.copy()
            rows["replicate"] = pd.to_numeric(rows["replicate"], errors="raise")
            for metric in metrics:
                rows[metric] = pd.to_numeric(rows[metric], errors="raise")
            parts.append(rows[columns])
    finally:
        body.close()

    if not parts:
        raise ResultImportError("No replicate totals were found for the selected sites.")
    rows = pd.concat(parts, ignore_index=True)
    replicate_ids = set(range(1, n_replicates + 1))
    if (
        rows[columns[1:]].isna().any().any()
        or not np.isfinite(rows[columns[1:]].to_numpy()).all()
        or not rows["replicate"].eq(np.floor(rows["replicate"])).all()
        or rows.duplicated(["site_id", "replicate"]).any()
        or set(rows["site_id"]) != site_ids
        or any(
            set(group["replicate"]) != replicate_ids
            for _, group in rows.groupby("site_id")
        )
    ):
        raise ResultImportError(
            "The selected sites have incomplete or invalid replicate totals."
        )

    by_replicate = rows.groupby("replicate")[metrics].sum()
    summary = {}
    for metric in metrics:
        values = by_replicate[metric]
        summary[metric.removeprefix("extrapolated_")] = float(values.mean())
        if n_replicates > 1:
            summary[f"{metric.removeprefix('extrapolated_')}_ci_lower"] = float(
                values.quantile(0.025)
            )
            summary[f"{metric.removeprefix('extrapolated_')}_ci_upper"] = float(
                values.quantile(0.975)
            )
    return summary


def get_filtered_match_quality_pixels(task_id, site_ids, results_s3_uri=None):
    """Load selected sites' matched pixel covariates without buffering the full CSV."""
    site_ids = {str(site_id) for site_id in site_ids}
    if not site_ids:
        raise ValueError("At least one site is required.")

    bucket, prefix = _resolve_results_s3(task_id, results_s3_uri)
    s3 = get_s3_client()

    def read_selected_rows(filename):
        body = s3.get_object(Bucket=bucket, Key=f"{prefix}/{filename}")["Body"]
        parts = []
        try:
            for chunk in pd.read_csv(body, dtype={"site_id": str}, chunksize=100_000):
                if "site_id" not in chunk:
                    raise ResultImportError(
                        f"{filename} does not contain a site_id column."
                    )
                rows = chunk[chunk["site_id"].isin(site_ids)]
                if not rows.empty:
                    parts.append(rows)
        finally:
            body.close()
        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    pixels = read_selected_rows("results_pixel_covariates.csv")
    if pixels.empty:
        raise ResultImportError(
            "No matched pixel covariates were found for the selected sites."
        )
    join_columns = ["cell", "site_id", "treatment", "match_group", "match_weight"]
    required = set(join_columns)
    if missing := required - set(pixels.columns):
        raise ResultImportError(
            "The pixel covariate output is missing required columns: "
            + ", ".join(sorted(missing))
        )

    if "pscore" not in pixels:
        scores = read_selected_rows("results_propensity_scores.csv")
        score_columns = required | {"pscore"}
        if missing := score_columns - set(scores.columns):
            raise ResultImportError(
                "The propensity score output is missing required columns: "
                + ", ".join(sorted(missing))
            )
        if scores.duplicated(join_columns).any():
            raise ResultImportError(
                "The propensity score output contains duplicate matched pixels."
            )
        pixels = pixels.merge(
            scores[[*join_columns, "pscore"]],
            on=join_columns,
            how="left",
            validate="one_to_one",
        )
        if pixels["pscore"].isna().any():
            raise ResultImportError(
                "Propensity scores are missing for some selected matched pixels."
            )

    return pixels


def list_task_s3_files(task_id, results_s3_uri=None):
    """List all S3 files under a task's output directory.

    Returns a list of dicts with ``key``, ``filename``, ``size_bytes``,
    and ``last_modified`` for each object found.
    """
    bucket, prefix = _resolve_results_s3(task_id, results_s3_uri)
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix + "/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            rel = key[len(prefix) :].lstrip("/")
            if not rel:
                continue
            files.append(
                {
                    "key": key,
                    "filename": rel,
                    "size_bytes": obj.get("Size", 0),
                    "last_modified": obj.get("LastModified", "").isoformat()
                    if hasattr(obj.get("LastModified", ""), "isoformat")
                    else str(obj.get("LastModified", "")),
                    "download_url": s3.generate_presigned_url(
                        "get_object",
                        Params={"Bucket": bucket, "Key": key},
                        ExpiresIn=3600,
                    ),
                }
            )
    return files


def _resolve_results_s3(task_id, results_s3_uri=None):
    """Return ``(bucket, prefix)`` for a task's result directory on S3.

    Resolution priority: explicit *results_s3_uri*, then the value stored
    on the ``AnalysisTask`` row, then ``Config.S3_PREFIX`` + *task_id*.
    """
    if not results_s3_uri:
        from models import AnalysisTask, get_db

        db = get_db()
        try:
            row = (
                db.query(AnalysisTask.results_s3_uri)
                .filter(AnalysisTask.id == task_id)
                .first()
            )
            if row and row.results_s3_uri:
                results_s3_uri = row.results_s3_uri
        finally:
            db.close()

    if results_s3_uri:
        uri = results_s3_uri
        uri = uri.removeprefix("s3://")
        parts = uri.split("/", 1)
        bucket = parts[0]
        prefix = parts[1].rstrip("/") if len(parts) > 1 else ""
    else:
        bucket = Config.S3_BUCKET
        prefix = f"{Config.S3_PREFIX}/tasks/{task_id}/output"

    return bucket, prefix


# ---------------------------------------------------------------------------
# Covariate inventory & COG merge functions
# ---------------------------------------------------------------------------
