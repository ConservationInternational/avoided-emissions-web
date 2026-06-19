"""Analysis task submission, status tracking, results retrieval, and download."""

import json
import logging
import tempfile
import uuid
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
from sqlalchemy import text

from config import Config
from models import (
    AnalysisTask,
    TaskResult,
    TaskResultTotal,
    TaskSite,
    TrendsEarthCredential,
    User,
    get_db,
)
import tasks as webapp_tasks
from gee_export import gee_config

from services.covariate import get_ready_covariate_names
from services.reference_layers import (
    compute_exact_match_groups_with_splitting,
    compute_matching_extent,
    compute_sites_exclusion_buffer,
    get_reference_layer_uris,
)
from services.s3 import S3_COST_TAGGING, get_s3_client
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

# Maximum number of elements in an AWS Batch array job.
# Submitting more than this causes a ClientException at SubmitJob time.
BATCH_MAX_ARRAY_SIZE = 10_000

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
    "group_by_exact_matches": False,
    "matching_method": "optimal",
    "n_replicates": 1,
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
    group_by_exact_matches=ANALYSIS_DEFAULTS["group_by_exact_matches"],
    matching_method=ANALYSIS_DEFAULTS["matching_method"],
    n_replicates=ANALYSIS_DEFAULTS["n_replicates"],
    random_seed=None,
    match_memory_mib=ANALYSIS_DEFAULTS["match_memory_mib"],
    matching_job_queue=DEFAULT_MATCHING_JOB_QUEUE,
    resolution_m=ANALYSIS_DEFAULTS["resolution_m"],
    source_sites_s3_uri=None,
    source_sites_parquet_s3_uri=None,
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
    if min_control_distance_km < 0:
        raise ValueError("min_control_distance_km must be 0 (disabled) or positive")
    if random_seed is not None and (random_seed < 1 or random_seed > 2_147_483_647):
        raise ValueError("random_seed must be between 1 and 2147483647")
    if n_replicates < 1 or n_replicates > 1000:
        raise ValueError("n_replicates must be between 1 and 1000")
    if matching_job_queue not in ALLOWED_MATCHING_JOB_QUEUES:
        raise ValueError(
            "matching_job_queue must be one of: "
            + ", ".join(sorted(ALLOWED_MATCHING_JOB_QUEUES))
        )

    # Verify trends.earth credentials before any DB work so the user
    # gets an immediate error rather than a stuck "submitting" task.
    from credential_store import get_decrypted_secret

    user_creds = get_decrypted_secret(user_id)
    if not user_creds:
        raise ValueError(
            "You must link your trends.earth account before "
            "submitting analysis tasks.  Go to Profile \u2192 "
            "trends.earth API Integration to connect your account."
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
                "group_by_exact_matches": bool(group_by_exact_matches),
                "matching_method": matching_method,
                "n_replicates": n_replicates,
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

    from credential_store import get_decrypted_secret
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
            "worker_started_at": datetime.now(timezone.utc).isoformat(),
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
        group_by_exact_matches = config.get("group_by_exact_matches", False)
        matching_method = config.get(
            "matching_method", ANALYSIS_DEFAULTS["matching_method"]
        )
        n_replicates = config.get("n_replicates", ANALYSIS_DEFAULTS["n_replicates"])
        random_seed = config.get("random_seed")
        match_memory_mib = config.get(
            "match_memory_mib", ANALYSIS_DEFAULTS["match_memory_mib"]
        )
        matching_job_queue = config.get(
            "matching_job_queue", DEFAULT_MATCHING_JOB_QUEUE
        )
        resolution_m = config.get("resolution_m", ANALYSIS_DEFAULTS["resolution_m"])

        _site_set_id = str(task.site_set_id) if task.site_set_id else None

        # ── OOM-safe path for DB-linked site sets ────────────────────────────
        # When the task is linked to a site_set_id we can derive everything we
        # need from PostGIS *before* loading any geometry into Python.  This
        # means the worker never holds the full GDF + the matching-extent query
        # result + the Parquet buffer simultaneously.
        #
        # 1. fc_years  — SELECT MIN(start_date) from user_site_features (scalar)
        # 2. matching_extent  — PostGIS aggregate via ST_Collect subquery
        # 3. exclusion_buffer — PostGIS aggregate via ST_Collect subquery
        # 4. sites Parquet    — streamed in batches; GDF batches are released
        #                       after each write; peak = O(batch) + O(parquet)
        # 5. GDF for splitting / TaskSite — read back from in-memory Parquet
        #                       buffer only when needed; Parquet is ~4–8× smaller
        #                       than the equivalent GeoDataFrame.
        #
        # For adopted tasks loaded from S3 (no site_set_id) we fall through to
        # the legacy path that fetches the GDF from S3 directly.
        # ─────────────────────────────────────────────────────────────────────

        parquet_buf = None  # holds the in-memory GeoParquet buffer
        gdf = None  # populated lazily from parquet_buf when needed

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
            reference_layer_uris = get_reference_layer_uris()
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

            # Step 3 — stream site set to in-memory Parquet (batch-at-a-time).
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

            fc_min = max(ANALYSIS_DEFAULTS["fc_year_start"], min_year - 5)
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
                int(start_dates.dt.year.min()) - 5,
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
        # For the DB path the GDF is read from the compact in-memory Parquet;
        # for the S3 fallback path the GDF is already loaded above.
        if parquet_buf is not None and gdf is None:
            gdf = gpd.read_parquet(parquet_buf)
            parquet_buf.seek(0)  # rewind so we can upload the same buffer to S3

        if gdf is None or gdf.empty:
            raise ValueError(
                f"Task {task_id}: could not load site data. Cannot complete submission."
            )

        logger.info(
            "[SUBMIT-WORKER] Task %s: %d sites ready (%.2fs so far)",
            task_id,
            len(gdf),
            _time.perf_counter() - _t0,
        )

        # Always split sites across exact-match boundaries when exact_match_vars
        # are specified. Group-based batching (batch_group_sites) is always
        # applied for efficiency; the matching methodology (joint vs per-site)
        # is controlled separately by group_by_exact_matches.
        if exact_match_vars:
            _split_t0 = _time.perf_counter()
            split_gdf, group_mapping = compute_exact_match_groups_with_splitting(
                gdf, exact_match_vars
            )
            logger.info(
                "[SUBMIT-WORKER] Task %s: site splitting %d → %d in %.2fs",
                task_id,
                len(gdf),
                len(split_gdf),
                _time.perf_counter() - _split_t0,
            )
            gdf_for_db = split_gdf
        else:
            gdf_for_db = gdf
            group_mapping = None

        # Vectorize CRS reprojection: project the whole GDF once instead of
        # creating a new single-row GeoDataFrame per site (O(n) allocations).
        _gdf_cea = gdf_for_db.to_crs("ESRI:54009")
        _areas_ha = _gdf_cea.geometry.area / 10_000.0

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

        # Create TaskSite rows and update n_sites on the task.
        # sub_site_index is derived from a per-site_id counter rather than
        # reading a column so that duplicate site_ids in the source data
        # (multiple features sharing the same site_id) each get a unique
        # sequential index instead of all defaulting to 0 and violating
        # the task_sites_task_id_site_id_sub_site_index_key constraint.
        task.n_sites = len(gdf)
        _sub_site_counters: dict[str, int] = {}
        for i, (_, row) in enumerate(gdf_for_db.iterrows()):
            geom = row.geometry
            area_ha = (
                float(_areas_ha.iloc[i])
                if (geom is not None and not geom.is_empty)
                else None
            )

            sid = str(row["site_id"])
            sub_site_index = _sub_site_counters.get(sid, 0)
            _sub_site_counters[sid] = sub_site_index + 1

            site = TaskSite(
                task_id=task_id,
                site_id=sid,
                site_name=str(row.get("site_name", "")),
                start_date=pd.to_datetime(row["start_date"]),
                end_date=pd.to_datetime(row["end_date"])
                if pd.notna(row.get("end_date"))
                else None,
                area_ha=area_ha,
                sub_site_index=sub_site_index,
                is_sub_site=sub_site_index > 0 or bool(row.get("is_sub_site", False)),
                original_area_ha=row.get("original_area_ha"),
            )
            db.add(site)
        db.commit()

        logger.info(
            "[SUBMIT-WORKER] Task %s: %d site rows created, uploading to S3",
            task_id,
            len(gdf_for_db),
        )

        # Upload sites to S3.
        # When parquet_buf is available (DB-linked path) the buffer was produced
        # by _stream_site_set_to_parquet_buf and is already seeked to 0; upload
        # it directly to avoid re-encoding the GDF a second time.
        _s3_t0 = _time.perf_counter()
        if parquet_buf is not None and group_mapping is None:
            # Fast path: upload the streaming buffer directly.
            s3_client = get_s3_client()
            parquet_key = f"{Config.S3_PREFIX}/tasks/{task_id}/sites.parquet"
            s3_client.put_object(
                Bucket=Config.S3_BUCKET,
                Key=parquet_key,
                Body=parquet_buf,
                ContentType="application/vnd.apache.parquet",
                Tagging=S3_COST_TAGGING,
            )
            sites_parquet_uri = f"s3://{Config.S3_BUCKET}/{parquet_key}"
        else:
            # gdf_for_db may differ from original (splitting), or no buffer.
            sites_parquet_uri = upload_sites_parquet_to_s3(gdf_for_db, task_id)
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

        # Determine batch-grouping strategy and pre-compute match pipeline steps.
        # batch_group_sites: True when exact_match_vars are present — each Batch
        # array element processes one (group × replicate) with data loaded once
        # per group.  This is independent of group_by_exact_matches, which
        # controls whether sites within a group are matched jointly or independently.
        _batch_group_sites = bool(group_mapping)
        _n_match_units = len(group_mapping) if _batch_group_sites else len(gdf_for_db)
        _raw_array_size = _n_match_units * n_replicates
        _match_step_base = {
            "timeout_seconds": 14400,  # 4 h per element
            "memory_mib": match_memory_mib,
            "vcpus": 2,
            "retry_attempts": 5,
        }
        if _raw_array_size <= BATCH_MAX_ARRAY_SIZE:
            _match_steps = [
                {
                    "name": "match",
                    "command": ["match"],
                    "array_size": _raw_array_size,
                    **_match_step_base,
                }
            ]
            _match_chunks = None
        else:
            _groups_per_chunk = max(1, BATCH_MAX_ARRAY_SIZE // max(1, n_replicates))
            _unit_ids = (
                list(group_mapping.keys())
                if _batch_group_sites
                else list(range(_n_match_units))
            )
            _chunks = [
                _unit_ids[i : i + _groups_per_chunk]
                for i in range(0, len(_unit_ids), _groups_per_chunk)
            ]
            _match_steps = [
                {
                    "name": f"match_chunk_{i}",
                    "command": [f"match_chunk_{i}"],
                    "array_size": len(chunk) * n_replicates,
                    **_match_step_base,
                }
                for i, chunk in enumerate(_chunks)
            ]
            _match_chunks = _chunks
            logger.warning(
                "[SUBMIT-WORKER] Task %s: array size %d exceeds limit %d; "
                "splitting match into %d chunks of \u2264%d units",
                task_id,
                _raw_array_size,
                BATCH_MAX_ARRAY_SIZE,
                len(_chunks),
                _groups_per_chunk,
            )

        # Build params matching AvoidedEmissionsParams schema.
        _cog_suffixes = {1000: "_1km", 250: "_250m"}
        _cog_suffix = _cog_suffixes.get(resolution_m, "_1km")
        cog_prefix = f"{Config.S3_PREFIX}/cog{_cog_suffix}"

        params = {
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
            "min_site_area_ha": min_site_area_ha,
            "min_glm_treatment_pixels": min_glm_treatment_pixels,
            "caliper_width": caliper_width,
            "max_controls_per_treatment": max_controls_per_treatment,
            "min_control_distance_km": min_control_distance_km,
            "separation_fallback_mahalanobis": bool(separation_fallback_mahalanobis),
            "group_by_exact_matches": bool(group_by_exact_matches),
            "matching_method": matching_method,
            "n_replicates": n_replicates,
            **(
                {"sites_exclusion_buffer": sites_exclusion_buffer}
                if sites_exclusion_buffer
                else {}
            ),
            "batch_group_sites": _batch_group_sites,
            **({"exact_match_group_mapping": group_mapping} if group_mapping else {}),
            **({"match_chunks": _match_chunks} if _match_chunks is not None else {}),
            **({"random_seed": random_seed} if random_seed is not None else {}),
            **(
                {"reference_layer_uris": reference_layer_uris}
                if reference_layer_uris
                else {}
            ),
            "results_s3_uri": (
                f"s3://{Config.S3_BUCKET}/{Config.S3_PREFIX}/tasks/{task_id}/output"
            ),
            "intermediate_s3_uri": (
                f"s3://{Config.S3_BUCKET}/{Config.S3_PREFIX}"
                f"/tasks/{task_id}/intermediate"
            ),
            **(
                {
                    "pipeline": [
                        # Prep step: compute matching_extent + exclusion_buffer
                        # from exported reference GeoParquets on S3.
                        # Only included when reference layers have been exported.
                        *(
                            [
                                {
                                    "name": "prep",
                                    "command": ["prep"],
                                    "timeout_seconds": 3600,  # 1 h
                                    "memory_mib": 8192,
                                    "vcpus": 2,
                                    "retry_attempts": 2,
                                }
                            ]
                            if reference_layer_uris
                            else []
                        ),
                        {
                            "name": "extract",
                            "command": ["extract"],
                            "timeout_seconds": 14400,  # 4 h
                            "memory_mib": max(
                                61440, match_memory_mib
                            ),  # at least 60 GB
                            "vcpus": 4,
                            "retry_attempts": 3,
                        },
                        *_match_steps,
                        {
                            "name": "summarize",
                            "command": ["summarize"],
                            "timeout_seconds": 7200,  # 2 h
                            "memory_mib": max(16384, match_memory_mib // 2),
                            "vcpus": 2,
                            "retry_attempts": 3,
                        },
                    ],
                }
                if _raw_array_size > 1
                else {}
            ),
        }

        # Attach AWS Batch overrides.
        batch_overrides = {
            "timeout_seconds": Config.BATCH_TIMEOUT_SECONDS,
            "memory_mib": max(Config.BATCH_MEMORY_MIB, match_memory_mib),
            "vcpus": Config.BATCH_VCPUS,
        }
        if matching_job_queue:
            batch_overrides["job_queue"] = matching_job_queue
        elif Config.BATCH_JOB_QUEUE:
            batch_overrides["job_queue"] = Config.BATCH_JOB_QUEUE
        if Config.BATCH_JOB_DEFINITION:
            batch_overrides["job_definition"] = Config.BATCH_JOB_DEFINITION
        params["batch"] = batch_overrides

        # Re-validate credentials (may have changed since queuing).
        user_creds = get_decrypted_secret(user_id)
        if not user_creds:
            raise ValueError(
                f"trends.earth credentials are no longer available for user "
                f"{user_id}. Cannot complete submission."
            )
        script_id = Config.TRENDSEARTH_SCRIPT_ID
        client_id, client_secret = user_creds

        _auth_t0 = _time.perf_counter()
        client = TrendsEarthClient.from_oauth2_credentials(
            api_url=Config.TRENDSEARTH_API_URL,
            client_id=client_id,
            client_secret=client_secret,
        )
        logger.info(
            "[SUBMIT-WORKER] Task %s: OAuth2 auth in %.2fs, calling API (script=%s)",
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
            **({"sites_s3_uri": sites_uri} if sites_uri else {}),
            "sites_parquet_s3_uri": sites_parquet_uri,
        }
        task.results_s3_uri = params["results_s3_uri"]
        task.status = "submitted"
        task.submitted_at = datetime.now(timezone.utc)
        task.extract_job_id = f"api:{exec_id}"
        db.commit()
        logger.info(
            "[SUBMIT-WORKER] Task %s: status → submitted (api:%s) — total time %.2fs",
            task_id,
            exec_id,
            _time.perf_counter() - _t0,
        )

    except Exception as e:
        logger.error(
            "[SUBMIT-WORKER] Task %s FAILED: %s",
            task_id,
            e,
            exc_info=True,
        )
        db.rollback()
        try:
            task_obj = db.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
            if task_obj:
                task_obj.status = "failed"
                task_obj.error_message = str(e)
                db.commit()
        except Exception:
            db.rollback()
        raise
    finally:
        db.close()


def submit_analysis_task(
    task_name,
    description,
    user_id,
    gdf,
    covariates,
    exact_match_vars,
    fc_years=None,
    site_set_id=None,
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
    group_by_exact_matches=ANALYSIS_DEFAULTS["group_by_exact_matches"],
    matching_method=ANALYSIS_DEFAULTS["matching_method"],
    n_replicates=ANALYSIS_DEFAULTS["n_replicates"],
    random_seed=None,
    match_memory_mib=ANALYSIS_DEFAULTS["match_memory_mib"],
    matching_job_queue=DEFAULT_MATCHING_JOB_QUEUE,
    resolution_m=ANALYSIS_DEFAULTS["resolution_m"],
):
    """Create and submit a full analysis task via the trends.earth API.

    Creates an Execution on the API which handles AWS Batch dispatch,
    status tracking, and result collection.

    ``exact_match_vars`` must contain at least one variable name from
    ``["admin0", "admin1", "admin2", "ecoregion", "pa"]``.  A
    ``ValueError`` is raised if the list is empty.

    Before submission the function queries PostGIS to compute the
    *matching extent* — the intersection of all polygon-type exact-match
    layers that overlap the uploaded sites.  This extent is passed to
    the analysis pipeline so control pixels are only drawn from areas
    where they can potentially share exact-match values with treatment
    sites.

    Requires the submitting user to have linked their trends.earth account
    (i.e. stored OAuth2 client credentials via the Settings page).  Raises
    ``ValueError`` if the user has not linked their account.

    1. Validates exact match selection
    2. Computes matching extent via PostGIS
    3. Creates the local database record
    4. Uploads sites and config to S3
    5. Submits to the trends.earth API
    6. Updates the database with tracking IDs

    Returns the task ID.
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
    if min_control_distance_km < 0:
        raise ValueError("min_control_distance_km must be 0 (disabled) or positive")
    if random_seed is not None and (random_seed < 1 or random_seed > 2_147_483_647):
        raise ValueError("random_seed must be between 1 and 2147483647")
    if n_replicates < 1 or n_replicates > 1000:
        raise ValueError("n_replicates must be between 1 and 1000")
    if matching_job_queue not in ALLOWED_MATCHING_JOB_QUEUES:
        raise ValueError(
            "matching_job_queue must be one of: "
            + ", ".join(sorted(ALLOWED_MATCHING_JOB_QUEUES))
        )

    # Verify trends.earth integration *before* any DB or S3 work so the
    # user gets an immediate error on the submission form instead of a
    # half-created failed task.
    from credential_store import get_decrypted_secret
    from trendsearth_client import TrendsEarthClient

    user_creds = get_decrypted_secret(user_id)
    if not user_creds:
        raise ValueError(
            "You must link your trends.earth account before "
            "submitting analysis tasks.  Go to Profile \u2192 "
            "trends.earth API Integration to connect your account."
        )
    script_id = Config.TRENDSEARTH_SCRIPT_ID
    if not script_id:
        raise ValueError(
            "TRENDSEARTH_SCRIPT_ID is not configured. Set this "
            "environment variable to the UUID of the avoided-emissions "
            "script registered on the trends.earth API."
        )

    import time as _time

    _submit_t0 = _time.perf_counter()

    # Compute the matching extent polygon from PostGIS.
    # Pass site_set_id so the function can query site geometries directly
    # from user_site_features, avoiding a Python-side unary_union.
    _site_set_id = str(site_set_id) if site_set_id else None
    matching_extent = compute_matching_extent(
        gdf, exact_match_vars, site_set_id=_site_set_id
    )
    logger.info(
        "[SUBMIT] matching extent computed in %.2fs",
        _time.perf_counter() - _submit_t0,
    )

    # Pre-compute the exclusion buffer around all sites in PostGIS
    # (geography-based, so distance is correct on the sphere).
    _buf_t0 = _time.perf_counter()
    sites_exclusion_buffer = compute_sites_exclusion_buffer(
        gdf, min_control_distance_km, site_set_id=_site_set_id
    )
    logger.info(
        "[SUBMIT] sites exclusion buffer computed in %.2fs",
        _time.perf_counter() - _buf_t0,
    )

    # Always split sites across exact-match boundaries when exact_match_vars
    # are specified. Group-based batching (batch_group_sites) is always
    # applied for efficiency; the matching methodology (joint vs per-site)
    # is controlled separately by group_by_exact_matches.
    if exact_match_vars:
        _split_t0 = _time.perf_counter()
        split_gdf, group_mapping = compute_exact_match_groups_with_splitting(
            gdf, exact_match_vars
        )
        logger.info(
            "[SUBMIT] Site splitting: %d → %d pieces in %.2fs",
            len(gdf),
            len(split_gdf),
            _time.perf_counter() - _split_t0,
        )
        # Use split sites for DB storage and S3 upload
        gdf_for_db = split_gdf
    else:
        # No splitting — use original gdf, no group mapping
        gdf_for_db = gdf
        group_mapping = None

    if fc_years is None:
        fc_years = list(
            range(ANALYSIS_DEFAULTS["fc_year_start"], ANALYSIS_DEFAULTS["fc_year_end"])
        )

    db = get_db()
    try:
        task_id = str(uuid.uuid4())
        logger.info(
            "[SUBMIT] Creating analysis task %s: name=%r, user=%s, "
            "n_sites=%d, covariates=%d, exact_match=%s",
            task_id,
            task_name,
            user_id,
            len(gdf),
            len(covariates),
            exact_match_vars,
        )

        task = AnalysisTask(
            id=task_id,
            name=task_name,
            description=description,
            submitted_by=user_id,
            site_set_id=site_set_id,
            status="pending",
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
                "group_by_exact_matches": bool(group_by_exact_matches),
                "matching_method": matching_method,
                "n_replicates": n_replicates,
                "resolution_m": resolution_m,
                **({"random_seed": random_seed} if random_seed is not None else {}),
                "match_memory_mib": match_memory_mib,
                "matching_job_queue": matching_job_queue,
                **(
                    {"code_git_sha": Config.GIT_REVISION} if Config.GIT_REVISION else {}
                ),
            },
            covariates=covariates,
            n_sites=len(gdf),
        )
        db.add(task)

        _sub_site_counters: dict[str, int] = {}
        for _, row in gdf_for_db.iterrows():
            # Compute area in hectares from the polygon geometry using
            # an equal-area projection (Mollweide).
            geom = row.geometry
            if geom is not None and not geom.is_empty:
                area_gdf = gpd.GeoDataFrame(geometry=[geom], crs="EPSG:4326").to_crs(
                    "ESRI:54009"
                )
                area_ha = area_gdf.geometry.iloc[0].area / 10_000.0
            else:
                area_ha = None

            sid = str(row["site_id"])
            sub_site_index = _sub_site_counters.get(sid, 0)
            _sub_site_counters[sid] = sub_site_index + 1

            site = TaskSite(
                task_id=task_id,
                site_id=sid,
                site_name=str(row.get("site_name", "")),
                start_date=pd.to_datetime(row["start_date"]),
                end_date=pd.to_datetime(row["end_date"])
                if pd.notna(row.get("end_date"))
                else None,
                area_ha=area_ha,
                sub_site_index=sub_site_index,
                is_sub_site=sub_site_index > 0 or bool(row.get("is_sub_site", False)),
                original_area_ha=row.get("original_area_ha"),
            )
            db.add(site)
        db.commit()

        logger.info(
            "[SUBMIT] Task %s: DB record created, uploading sites to S3", task_id
        )

        # Upload sites to S3.
        # When cross-site grouping is enabled, the split GeoDataFrame must be
        # uploaded so Batch uses the same site/sub-site geometry used to build
        # exact_match_group_mapping.
        _s3_t0 = _time.perf_counter()
        sites_parquet_uri = upload_sites_parquet_to_s3(gdf_for_db, task_id)
        sites_uri = None
        if not site_set_id:
            sites_uri = upload_sites_to_s3(gdf_for_db, task_id)
        logger.info(
            "[SUBMIT] Task %s: site artifacts uploaded (parquet=%s, geojson=%s) (%.2fs)",
            task_id,
            sites_parquet_uri,
            sites_uri,
            _time.perf_counter() - _s3_t0,
        )

        # Determine batch-grouping strategy and pre-compute match pipeline steps.
        # batch_group_sites: True when exact_match_vars are present — each Batch
        # array element processes one (group × replicate) with data loaded once
        # per group.  This is independent of group_by_exact_matches, which
        # controls whether sites within a group are matched jointly or independently.
        _batch_group_sites = bool(group_mapping)
        _n_match_units = len(group_mapping) if _batch_group_sites else len(gdf_for_db)
        _raw_array_size = _n_match_units * n_replicates
        _match_step_base = {
            "timeout_seconds": 14400,  # 4 h per element
            "memory_mib": match_memory_mib,
            "vcpus": 2,
            "retry_attempts": 5,
        }
        if _raw_array_size <= BATCH_MAX_ARRAY_SIZE:
            _match_steps = [
                {
                    "name": "match",
                    "command": ["match"],
                    "array_size": _raw_array_size,
                    **_match_step_base,
                }
            ]
            _match_chunks = None
        else:
            _groups_per_chunk = max(1, BATCH_MAX_ARRAY_SIZE // max(1, n_replicates))
            _unit_ids = (
                list(group_mapping.keys())
                if _batch_group_sites
                else list(range(_n_match_units))
            )
            _chunks = [
                _unit_ids[i : i + _groups_per_chunk]
                for i in range(0, len(_unit_ids), _groups_per_chunk)
            ]
            _match_steps = [
                {
                    "name": f"match_chunk_{i}",
                    "command": [f"match_chunk_{i}"],
                    "array_size": len(chunk) * n_replicates,
                    **_match_step_base,
                }
                for i, chunk in enumerate(_chunks)
            ]
            _match_chunks = _chunks
            logger.warning(
                "[SUBMIT] Task %s: array size %d exceeds limit %d; "
                "splitting match into %d chunks of \u2264%d units",
                task_id,
                _raw_array_size,
                BATCH_MAX_ARRAY_SIZE,
                len(_chunks),
                _groups_per_chunk,
            )

        # Build params matching AvoidedEmissionsParams schema
        # Resolve the COG prefix for the chosen resolution.
        # Both resolutions have an explicit suffix (_1km / _250m).
        _cog_suffixes = {1000: "_1km", 250: "_250m"}
        _cog_suffix = _cog_suffixes.get(resolution_m, "_1km")
        cog_prefix = f"{Config.S3_PREFIX}/cog{_cog_suffix}"

        params = {
            "task_id": task_id,
            "task_name": task_name,
            "task_description": description,
            **({"sites_s3_uri": sites_uri} if sites_uri else {}),
            "sites_parquet_s3_uri": sites_parquet_uri,
            "cog_bucket": Config.S3_BUCKET,
            "cog_prefix": cog_prefix,
            "resolution_m": resolution_m,
            "covariates": covariates,
            "exact_match_vars": exact_match_vars,
            "matching_extent": matching_extent,
            "fc_years": fc_years,
            "max_treatment_pixels": max_treatment_pixels,
            "control_multiplier": control_multiplier,
            "min_site_area_ha": min_site_area_ha,
            "min_glm_treatment_pixels": min_glm_treatment_pixels,
            "caliper_width": caliper_width,
            "max_controls_per_treatment": max_controls_per_treatment,
            "min_control_distance_km": min_control_distance_km,
            "separation_fallback_mahalanobis": bool(separation_fallback_mahalanobis),
            "group_by_exact_matches": bool(group_by_exact_matches),
            "matching_method": matching_method,
            "n_replicates": n_replicates,
            **(
                {"sites_exclusion_buffer": sites_exclusion_buffer}
                if sites_exclusion_buffer
                else {}
            ),
            "batch_group_sites": _batch_group_sites,
            **({"exact_match_group_mapping": group_mapping} if group_mapping else {}),
            **({"match_chunks": _match_chunks} if _match_chunks is not None else {}),
            **({"random_seed": random_seed} if random_seed is not None else {}),
            "results_s3_uri": (
                f"s3://{Config.S3_BUCKET}/{Config.S3_PREFIX}/tasks/{task_id}/output"
            ),
            "intermediate_s3_uri": (
                f"s3://{Config.S3_BUCKET}/{Config.S3_PREFIX}"
                f"/tasks/{task_id}/intermediate"
            ),
            # For multi-site jobs, use a pipeline of chained AWS Batch
            # jobs (extract → match array → summarize) so each site
            # can run its matching step in parallel as an array child.
            # For single-site, single-replicate jobs, skip the pipeline
            # entirely and run all steps in one container (step="all")
            # — this avoids the overhead of S3 intermediate data
            # transfer and extra job scheduling.  The R analysis
            # container handles both modes via the ``step`` parameter.
            **(
                {
                    "pipeline": [
                        {
                            "name": "extract",
                            "command": ["extract"],
                            "timeout_seconds": 14400,  # 4 h
                            "memory_mib": max(
                                61440, match_memory_mib
                            ),  # at least 60 GB
                            "vcpus": 4,
                            "retry_attempts": 3,
                        },
                        *_match_steps,
                        {
                            "name": "summarize",
                            "command": ["summarize"],
                            "timeout_seconds": 7200,  # 2 h
                            "memory_mib": max(16384, match_memory_mib // 2),
                            "vcpus": 2,
                            "retry_attempts": 3,
                        },
                    ],
                }
                if _raw_array_size > 1
                else {}
            ),
        }

        # Attach AWS Batch overrides so the API routes this execution to
        # the correct job queue / job definition (if configured).
        # Always include timeout_seconds — the pipeline runs three
        # sequential steps so the Batch job timeout must be large enough
        # to cover all of them (default: 14 h, see Config).
        batch_overrides = {
            "timeout_seconds": Config.BATCH_TIMEOUT_SECONDS,
            "memory_mib": max(Config.BATCH_MEMORY_MIB, match_memory_mib),
            "vcpus": Config.BATCH_VCPUS,
        }
        if matching_job_queue:
            batch_overrides["job_queue"] = matching_job_queue
        elif Config.BATCH_JOB_QUEUE:
            batch_overrides["job_queue"] = Config.BATCH_JOB_QUEUE
        if Config.BATCH_JOB_DEFINITION:
            batch_overrides["job_definition"] = Config.BATCH_JOB_DEFINITION
        params["batch"] = batch_overrides

        # Submit via trends.earth API using the user's own OAuth2 creds
        # (credentials and script_id already validated above)
        _auth_t0 = _time.perf_counter()
        client_id, client_secret = user_creds
        client = TrendsEarthClient.from_oauth2_credentials(
            api_url=Config.TRENDSEARTH_API_URL,
            client_id=client_id,
            client_secret=client_secret,
        )
        logger.info(
            "[SUBMIT] Task %s: OAuth2 auth in %.2fs, calling trends.earth "
            "API (script=%s, api_url=%s, batch_overrides=%s)",
            task_id,
            _time.perf_counter() - _auth_t0,
            script_id,
            Config.TRENDSEARTH_API_URL,
            batch_overrides if batch_overrides else "none",
        )
        _api_t0 = _time.perf_counter()
        execution = client.create_execution(script_id, params)
        logger.info(
            "[SUBMIT] Task %s: API create_execution took %.2fs",
            task_id,
            _time.perf_counter() - _api_t0,
        )

        # Store the API execution ID for polling
        exec_data = execution.get("data", {})
        exec_id = exec_data.get("id", "")
        exec_status = exec_data.get("status", "unknown")
        logger.info(
            "[SUBMIT] Task %s: API execution created — exec_id=%s, "
            "initial_status=%s, full_response_keys=%s",
            task_id,
            exec_id,
            exec_status,
            list(exec_data.keys()),
        )
        task.sites_s3_uri = sites_uri
        task.config = {
            **(task.config or {}),
            **({"sites_s3_uri": sites_uri} if sites_uri else {}),
            "sites_parquet_s3_uri": sites_parquet_uri,
        }
        task.results_s3_uri = params["results_s3_uri"]
        task.status = "submitted"
        task.submitted_at = datetime.now(timezone.utc)
        # Store the API execution ID in a new-ish field; reuse
        # extract_job_id since we no longer need the Batch job IDs.
        task.extract_job_id = f"api:{exec_id}"
        db.commit()
        logger.info(
            "[SUBMIT] Task %s: status → submitted (tracking as api:%s) "
            "— total submit time %.2fs",
            task_id,
            exec_id,
            _time.perf_counter() - _submit_t0,
        )

        return task_id

    except Exception as e:
        logger.error(
            "[SUBMIT] Task %s FAILED during submission: %s",
            task_id if "task_id" in dir() else "(pre-creation)",
            e,
            exc_info=True,
        )
        db.rollback()
        if "task_id" in dir():
            task = db.query(AnalysisTask).get(task_id)
            if task:
                task.status = "failed"
                task.error_message = str(e)
                db.commit()
        raise
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
                from credential_store import get_decrypted_secret
                from trendsearth_client import TrendsEarthClient

                user_creds = get_decrypted_secret(user.id)
                if user_creds:
                    client = TrendsEarthClient.from_oauth2_credentials(
                        api_url=Config.TRENDSEARTH_API_URL,
                        client_id=user_creds[0],
                        client_secret=user_creds[1],
                    )
                    client.cancel_execution(api_exec_id)
                    logger.info(
                        "[CANCEL] Task %s: API execution %s cancelled",
                        task_id,
                        api_exec_id,
                    )
            except Exception as e:
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


def get_task_list(user_id=None, limit=None):
    """Get analysis tasks, optionally filtered by user.

    Uses ``load_only`` to skip the heavy ``extra_metadata`` JSON column
    that the task list view never reads.  Pass ``limit`` to cap results;
    defaults to no limit so the full history is available for
    client-side sort/filter in the dashboard.
    """
    from sqlalchemy.orm import joinedload, load_only

    db = get_db()
    try:
        query = (
            db.query(AnalysisTask)
            .options(
                load_only(
                    AnalysisTask.id,
                    AnalysisTask.name,
                    AnalysisTask.status,
                    AnalysisTask.n_sites,
                    AnalysisTask.submitted_by,
                    AnalysisTask.config,
                    AnalysisTask.covariates,
                    AnalysisTask.created_at,
                    AnalysisTask.submitted_at,
                    AnalysisTask.completed_at,
                    AnalysisTask.error_message,
                ),
                joinedload(AnalysisTask.user).load_only(User.id, User.name),
            )
            .order_by(AnalysisTask.created_at.desc())
        )
        if user_id:
            query = query.filter(AnalysisTask.submitted_by == user_id)
        if limit is not None:
            query = query.limit(limit)
        return query.all()
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


def _get_task_results_aggregated(task_id, db):
    """Return per-year aggregate of task_results rows as a list of plain dicts.

    Performs a single SQL GROUP BY query rather than loading every row, so it
    is safe to call for tasks with thousands of sites.
    """
    sql = text(
        """
        SELECT
            year,
            SUM(extrapolated_treatment_defor_ha)               AS treatment_defor_ha,
            SUM(extrapolated_control_defor_ha)                 AS control_defor_ha,
            SUM(extrapolated_emissions_avoided_mgco2e)         AS emissions_avoided_mgco2e,
            SUM(extrapolated_forest_loss_avoided_ha)           AS forest_loss_avoided_ha,
            SUM(extrapolated_treatment_emissions_mgco2e)       AS treatment_emissions_mgco2e,
            SUM(extrapolated_control_emissions_mgco2e)         AS control_emissions_mgco2e,
            SUM(extrapolated_treatment_defor_ha_ci_lower)      AS treatment_defor_ha_ci_lower,
            SUM(extrapolated_treatment_defor_ha_ci_upper)      AS treatment_defor_ha_ci_upper,
            SUM(extrapolated_control_defor_ha_ci_lower)        AS control_defor_ha_ci_lower,
            SUM(extrapolated_control_defor_ha_ci_upper)        AS control_defor_ha_ci_upper,
            SUM(extrapolated_emissions_avoided_mgco2e_ci_lower)  AS emissions_avoided_mgco2e_ci_lower,
            SUM(extrapolated_emissions_avoided_mgco2e_ci_upper)  AS emissions_avoided_mgco2e_ci_upper,
            SUM(extrapolated_forest_loss_avoided_ha_ci_lower)    AS forest_loss_avoided_ha_ci_lower,
            SUM(extrapolated_forest_loss_avoided_ha_ci_upper)    AS forest_loss_avoided_ha_ci_upper,
            COUNT(DISTINCT site_id)                            AS n_sites
        FROM task_results
        WHERE task_id = :task_id
        GROUP BY year
        ORDER BY year
        """
    )
    result = db.execute(sql, {"task_id": str(task_id)})
    return [dict(r._mapping) for r in result.fetchall()]


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
                "is_pre_intervention": bool(r.is_pre_intervention),
                "is_post_intervention": bool(getattr(r, "is_post_intervention", False)),
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
    ``TaskResult`` rows and ``TaskSite`` rows are **not** loaded into Python.
    Instead a pre-aggregated yearly summary is returned in ``agg_yearly`` so
    the Dash process never has to hold tens of thousands of ORM objects in
    memory.  The ``is_large`` flag tells callers which path was taken.
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

        # Aggregated yearly summary — always computed; used by plots for
        # large tasks and as a cheap cross-check for small ones.
        agg_yearly = _get_task_results_aggregated(task_id, db)

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


def import_execution_results(task_id, results_payload, db=None):
    """Parse an AnalysisResults payload and save rows to TaskResult / TaskResultTotal.

    Called by the polling task when an API execution finishes.  The
    *results_payload* is the dict returned by
    ``TrendsEarthClient.get_execution_results()`` — a serialised
    ``AnalysisResults`` object with ``records`` (per-site totals) and
    ``time_series`` (per-site-year observations).

    This function is idempotent: existing result rows for the task are
    deleted before new ones are inserted.

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
    own_session = db is None
    if own_session:
        db = get_db()

    try:
        if not results_payload:
            logger.warning(
                "import_execution_results(%s): empty results payload", task_id
            )
            return

        # Store summary-level info (failed sites, subsampled sites) on the
        # AnalysisTask so the UI can display diagnostics.
        summary = results_payload.get("summary") or {}
        task_obj = db.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
        if task_obj:
            meta = dict(task_obj.extra_metadata or {})
            meta["failed_sites"] = summary.get("failed_sites", [])
            meta["n_failed_sites"] = summary.get("n_failed_sites", 0)
            meta["subsampled_sites"] = summary.get("subsampled_sites", [])
            meta["n_sites"] = summary.get("n_sites", 0)
            meta["n_replicates"] = summary.get("n_replicates", 1)
            if summary.get("r_analysis_git_sha"):
                meta["r_analysis_git_sha"] = summary["r_analysis_git_sha"]
            task_obj.extra_metadata = meta

        # Delete any existing results (idempotent re-import)
        db.query(TaskResult).filter(TaskResult.task_id == task_id).delete()
        db.query(TaskResultTotal).filter(TaskResultTotal.task_id == task_id).delete()

        # --- per-site-year time series → TaskResult ---
        # Each entry may carry a sub_site_index in its metadata (0 when the
        # site was not split into sub-sites; 1+ for cross-site grouping
        # fragments).  The unique constraint is now on
        # (task_id, site_id, sub_site_index, year) so sub-site rows are
        # stored and distinguished correctly.
        time_series = results_payload.get("time_series") or []

        # Deduplicate by (entity_id, sub_site_index, year) — the API
        # payload can occasionally contain duplicate rows for the same key
        # (e.g. when the R script emits two replicates for the same site/year).
        # Keep the last occurrence so we get a deterministic single row per key.
        _seen_ts: dict = {}
        for ts in time_series:
            meta = ts.get("metadata", {})
            key = (
                ts.get("entity_id"),
                int(meta.get("sub_site_index", 0)),
                ts.get("year"),
            )
            _seen_ts[key] = ts
        if len(_seen_ts) < len(time_series):
            logger.warning(
                "import_execution_results(%s): dropped %d duplicate time-series "
                "row(s) from results payload",
                task_id,
                len(time_series) - len(_seen_ts),
            )
        time_series = list(_seen_ts.values())

        for ts in time_series:
            values = ts.get("values", {})
            metadata = ts.get("metadata", {})
            db.add(
                TaskResult(
                    task_id=task_id,
                    site_id=ts["entity_id"],
                    sub_site_index=int(metadata.get("sub_site_index", 0)),
                    year=ts["year"],
                    extrapolated_forest_loss_avoided_ha=values.get(
                        "extrapolated_forest_loss_avoided_ha"
                    ),
                    extrapolated_emissions_avoided_mgco2e=values.get(
                        "extrapolated_emissions_avoided_mgco2e"
                    ),
                    extrapolated_treatment_defor_ha=values.get(
                        "extrapolated_treatment_defor_ha"
                    ),
                    extrapolated_control_defor_ha=values.get(
                        "extrapolated_control_defor_ha"
                    ),
                    extrapolated_treatment_emissions_mgco2e=values.get(
                        "extrapolated_treatment_emissions_mgco2e"
                    ),
                    extrapolated_control_emissions_mgco2e=values.get(
                        "extrapolated_control_emissions_mgco2e"
                    ),
                    is_pre_intervention=bool(
                        metadata.get("is_pre_intervention", False)
                    ),
                    is_post_intervention=bool(
                        metadata.get("is_post_intervention", False)
                    ),
                    n_sample_pixels=metadata.get("n_sample_pixels"),
                    sampled_fraction=metadata.get("sampled_fraction"),
                    extrapolated_treatment_defor_ha_ci_lower=values.get(
                        "extrapolated_treatment_defor_ha_ci_lower"
                    ),
                    extrapolated_treatment_defor_ha_ci_upper=values.get(
                        "extrapolated_treatment_defor_ha_ci_upper"
                    ),
                    extrapolated_control_defor_ha_ci_lower=values.get(
                        "extrapolated_control_defor_ha_ci_lower"
                    ),
                    extrapolated_control_defor_ha_ci_upper=values.get(
                        "extrapolated_control_defor_ha_ci_upper"
                    ),
                    extrapolated_forest_loss_avoided_ha_ci_lower=values.get(
                        "extrapolated_forest_loss_avoided_ha_ci_lower"
                    ),
                    extrapolated_forest_loss_avoided_ha_ci_upper=values.get(
                        "extrapolated_forest_loss_avoided_ha_ci_upper"
                    ),
                    extrapolated_treatment_emissions_mgco2e_ci_lower=values.get(
                        "extrapolated_treatment_emissions_mgco2e_ci_lower"
                    ),
                    extrapolated_treatment_emissions_mgco2e_ci_upper=values.get(
                        "extrapolated_treatment_emissions_mgco2e_ci_upper"
                    ),
                    extrapolated_control_emissions_mgco2e_ci_lower=values.get(
                        "extrapolated_control_emissions_mgco2e_ci_lower"
                    ),
                    extrapolated_control_emissions_mgco2e_ci_upper=values.get(
                        "extrapolated_control_emissions_mgco2e_ci_upper"
                    ),
                    extrapolated_emissions_avoided_mgco2e_ci_lower=values.get(
                        "extrapolated_emissions_avoided_mgco2e_ci_lower"
                    ),
                    extrapolated_emissions_avoided_mgco2e_ci_upper=values.get(
                        "extrapolated_emissions_avoided_mgco2e_ci_upper"
                    ),
                )
            )

        # --- per-site totals → TaskResultTotal ---
        records = results_payload.get("records") or []
        for rec in records:
            values = rec.get("values", {})
            metadata = rec.get("metadata", {})
            db.add(
                TaskResultTotal(
                    task_id=task_id,
                    site_id=rec["entity_id"],
                    site_name=rec.get("entity_name"),
                    extrapolated_forest_loss_avoided_ha=values.get(
                        "extrapolated_forest_loss_avoided_ha"
                    ),
                    extrapolated_emissions_avoided_mgco2e=values.get(
                        "extrapolated_emissions_avoided_mgco2e"
                    ),
                    area_ha=values.get("area_ha"),
                    n_sample_pixels=metadata.get("n_sample_pixels"),
                    n_treatment_pixels=metadata.get("n_treatment_pixels"),
                    sampled_fraction=metadata.get("sampled_fraction"),
                    first_year=rec.get("period_start"),
                    last_year=rec.get("period_end"),
                    n_years=metadata.get("n_years"),
                    extrapolated_forest_loss_avoided_ha_ci_lower=values.get(
                        "extrapolated_forest_loss_avoided_ha_ci_lower"
                    ),
                    extrapolated_forest_loss_avoided_ha_ci_upper=values.get(
                        "extrapolated_forest_loss_avoided_ha_ci_upper"
                    ),
                    extrapolated_emissions_avoided_mgco2e_ci_lower=values.get(
                        "extrapolated_emissions_avoided_mgco2e_ci_lower"
                    ),
                    extrapolated_emissions_avoided_mgco2e_ci_upper=values.get(
                        "extrapolated_emissions_avoided_mgco2e_ci_upper"
                    ),
                )
            )

        if own_session:
            db.commit()

        logger.info(
            "import_execution_results(%s): imported %d time-series rows, %d total rows",
            task_id,
            len(time_series),
            len(records),
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

    # Try to match the execution to the local user who submitted it,
    # via the trends.earth user ID stored in TrendsEarthCredential.
    from models import User

    owner = None
    api_user_id = exec_data.get("user_id")
    if api_user_id:
        cred = (
            db.query(TrendsEarthCredential)
            .filter(TrendsEarthCredential.te_user_id == str(api_user_id))
            .first()
        )
        if cred:
            owner = db.query(User).filter(User.id == cred.user_id).first()

    # Fall back to the first admin, then any user
    if not owner:
        owner = db.query(User).filter(User.role == "admin").first()
    if not owner:
        owner = db.query(User).first()
    if not owner:
        # Caller should check for this and log once, not per-execution
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
        submitted_by=owner.id,
        status=local_status,
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
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


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
        if uri.startswith("s3://"):
            uri = uri[5:]
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
        if uri.startswith("s3://"):
            uri = uri[5:]
        parts = uri.split("/", 1)
        bucket = parts[0]
        prefix = parts[1].rstrip("/") if len(parts) > 1 else ""
    else:
        bucket = Config.S3_BUCKET
        prefix = f"{Config.S3_PREFIX}/tasks/{task_id}/output"

    return bucket, prefix


def generate_match_quality_summary(task_id, results_s3_uri=None):
    """Build ``results_match_quality_summary.json`` from existing raw CSVs.

    This is the *backfill* path: for tasks that completed before the R
    summarize script started producing this file.  It downloads the raw
    CSVs to temporary files and processes them with chunked reads to keep
    memory usage low.  The resulting JSON is uploaded back to S3 alongside
    the other result artefacts.

    Returns the parsed summary dict, or ``None`` on failure.
    """
    import json
    import tempfile

    import numpy as np

    N_BINS = 40
    N_QQ = 500
    CHUNK = 50_000

    bucket, prefix = _resolve_results_s3(task_id, results_s3_uri)
    s3 = get_s3_client()

    summary = {
        "summary_stats": {},
        "histograms": {},
        "qq_quantiles": {},
        "covariate_cols": [],
    }

    # ---- Histograms from results_pixel_covariates.csv --------------------
    cov_key = f"{prefix}/results_pixel_covariates.csv"
    try:
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            s3.download_fileobj(Bucket=bucket, Key=cov_key, Fileobj=tmp)
            cov_path = tmp.name
    except Exception:
        logger.info(
            "generate_match_quality_summary(%s): "
            "match_covariates CSV not found, skipping histograms",
            task_id,
        )
        cov_path = None

    id_cols = {"cell", "site_id", "treatment", "match_group", "match_weight"}
    covariate_cols = []
    site_stats: dict = {}
    cov_ranges: dict = {}

    if cov_path:
        import os

        try:
            # Pass 1 — compute ranges and counts
            for chunk in pd.read_csv(cov_path, chunksize=CHUNK):
                if not covariate_cols:
                    covariate_cols = [c for c in chunk.columns if c not in id_cols]
                for sid in chunk["site_id"].unique():
                    s = str(sid)
                    mask = chunk["site_id"] == sid
                    sub = chunk[mask]
                    if s not in site_stats:
                        site_stats[s] = {"n_treatment": 0, "n_control": 0}
                    site_stats[s]["n_treatment"] += int(sub["treatment"].sum())
                    site_stats[s]["n_control"] += int((~sub["treatment"]).sum())
                for cov in covariate_cols:
                    vals = chunk[cov].dropna()
                    if vals.empty:
                        continue
                    lo, hi = float(vals.min()), float(vals.max())
                    if cov not in cov_ranges:
                        cov_ranges[cov] = {"min": lo, "max": hi}
                    else:
                        cov_ranges[cov]["min"] = min(cov_ranges[cov]["min"], lo)
                        cov_ranges[cov]["max"] = max(cov_ranges[cov]["max"], hi)

            summary["covariate_cols"] = covariate_cols

            # --- Read sampling-by-site for total treatment counts ----------
            sampling_by_site = {}
            sampling_key = f"{prefix}/results_sampling_by_site.csv"
            try:
                import io as _io

                obj = s3.get_object(Bucket=bucket, Key=sampling_key)
                sbs_df = pd.read_csv(_io.BytesIO(obj["Body"].read()))
                for _, row in sbs_df.iterrows():
                    sid_str = str(row.get("site_id", ""))
                    frac = row.get("sampled_fraction", 1.0)
                    if sid_str and frac and frac > 0:
                        sampling_by_site[sid_str] = float(frac)
            except Exception:
                pass  # Not available for older tasks

            # Compute total treatment / control pool counts
            for sid_str, stats in site_stats.items():
                frac = sampling_by_site.get(sid_str, 1.0)
                if frac > 0:
                    stats["n_treatment_total"] = round(stats["n_treatment"] / frac)

            # Summary stats
            n_treatment_total_all = sum(
                s.get("n_treatment_total", s["n_treatment"])
                for s in site_stats.values()
            )
            summary["summary_stats"]["__all__"] = {
                "n_treatment": sum(s["n_treatment"] for s in site_stats.values()),
                "n_control": sum(s["n_control"] for s in site_stats.values()),
                "n_sites": len(site_stats),
                "n_treatment_total": n_treatment_total_all,
            }
            for sid_str, stats in site_stats.items():
                summary["summary_stats"][sid_str] = stats

            # Compute bin edges per covariate
            bin_edges: dict = {}
            for cov in covariate_cols:
                if cov in cov_ranges:
                    r = cov_ranges[cov]
                    if r["max"] > r["min"]:
                        bin_edges[cov] = np.linspace(
                            r["min"], r["max"], N_BINS + 1
                        ).tolist()

            # Pass 2 — accumulate bin counts
            hist_counts: dict = {}
            for chunk in pd.read_csv(cov_path, chunksize=CHUNK):
                scope_masks = [("__all__", slice(None))]
                for sid in chunk["site_id"].unique():
                    scope_masks.append((str(sid), chunk["site_id"] == sid))
                for scope, mask in scope_masks:
                    sub = chunk if scope == "__all__" else chunk[mask]
                    if scope not in hist_counts:
                        hist_counts[scope] = {}
                    for cov in covariate_cols:
                        if cov not in bin_edges:
                            continue
                        if cov not in hist_counts[scope]:
                            hist_counts[scope][cov] = {
                                "treatment": np.zeros(N_BINS, dtype=np.int64),
                                "control": np.zeros(N_BINS, dtype=np.int64),
                            }
                        edges = bin_edges[cov]
                        t_v = sub.loc[sub["treatment"], cov].dropna()
                        c_v = sub.loc[~sub["treatment"], cov].dropna()
                        if len(t_v) > 0:
                            h, _ = np.histogram(t_v, bins=edges)
                            hist_counts[scope][cov]["treatment"] += h
                        if len(c_v) > 0:
                            h, _ = np.histogram(c_v, bins=edges)
                            hist_counts[scope][cov]["control"] += h

            # Convert counts → percentages
            for scope, covs in hist_counts.items():
                scope_hists = {}
                for cov, counts in covs.items():
                    tt = counts["treatment"].sum()
                    ct = counts["control"].sum()
                    scope_hists[cov] = {
                        "bin_edges": bin_edges[cov],
                        "treatment_pct": (
                            (counts["treatment"] / tt * 100).tolist()
                            if tt > 0
                            else [0.0] * N_BINS
                        ),
                        "control_pct": (
                            (counts["control"] / ct * 100).tolist()
                            if ct > 0
                            else [0.0] * N_BINS
                        ),
                    }
                summary["histograms"][scope] = scope_hists
        finally:
            os.unlink(cov_path)

    # ---- QQ quantiles from results_propensity_scores.csv -----------------
    ps_key = f"{prefix}/results_propensity_scores.csv"
    try:
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            s3.download_fileobj(Bucket=bucket, Key=ps_key, Fileobj=tmp)
            ps_path = tmp.name
    except Exception:
        ps_path = None

    if ps_path:
        import os as _os

        try:
            # Collect scores per scope — pscores have only 6 columns so
            # the per-row memory is small even for large pixel counts.
            pscores: dict = {}
            for chunk in pd.read_csv(
                ps_path,
                chunksize=CHUNK,
                usecols=["site_id", "treatment", "pscore"],
            ):
                for scope, mask in [("__all__", slice(None))] + [
                    (str(sid), chunk["site_id"] == sid)
                    for sid in chunk["site_id"].unique()
                ]:
                    sub = chunk if scope == "__all__" else chunk[mask]
                    if scope not in pscores:
                        pscores[scope] = {"treatment": [], "control": []}
                    t_ps = sub.loc[sub["treatment"], "pscore"].dropna()
                    c_ps = sub.loc[~sub["treatment"], "pscore"].dropna()
                    pscores[scope]["treatment"].extend(t_ps.tolist())
                    pscores[scope]["control"].extend(c_ps.tolist())

            for scope, data in pscores.items():
                t_s = np.sort(data["treatment"])
                c_s = np.sort(data["control"])
                if len(t_s) >= 2 and len(c_s) >= 2:
                    n_pts = min(N_QQ, max(len(t_s), len(c_s)))
                    probs = np.linspace(0, 1, n_pts)
                    summary["qq_quantiles"][scope] = {
                        "quantiles": probs.tolist(),
                        "treatment_values": np.quantile(t_s, probs).tolist(),
                        "control_values": np.quantile(c_s, probs).tolist(),
                    }
        finally:
            _os.unlink(ps_path)

    # ---- Upload summary to S3 --------------------------------------------
    summary_json = json.dumps(summary)
    summary_key = f"{prefix}/results_match_quality_summary.json"
    s3.put_object(Bucket=bucket, Key=summary_key, Body=summary_json.encode("utf-8"))

    logger.info(
        "generate_match_quality_summary(%s): uploaded to s3://%s/%s",
        task_id,
        bucket,
        summary_key,
    )
    return summary


# ---------------------------------------------------------------------------
# Covariate inventory & COG merge functions
# ---------------------------------------------------------------------------
