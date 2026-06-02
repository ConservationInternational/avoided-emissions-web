"""GEE covariate export orchestration and inventory management."""

import json
import logging
import os


from config import Config
from models import (
    Covariate,
    get_db,
)
from gee_export import gee_config
from gee_export import gee_tasks
import tasks as webapp_tasks

logger = logging.getLogger(__name__)

FC_YEAR_MIN = gee_config.FC_YEAR_MIN
FC_YEAR_MAX = gee_config.FC_YEAR_MAX


def _cleanup_covariate_downstream(covariate_name, db, *, resolution_m=None):
    """Delete downstream artefacts for a covariate before re-export.

    Removes the S3 COG, GCS tiles, and existing DB records so that a
    fresh GEE export starts from a clean slate.  Called from both
    :func:`start_gee_export` and :func:`force_reexport`.

    Parameters
    ----------
    covariate_name : str
        Covariate key from config.COVARIATES.
    db : sqlalchemy.orm.Session
        An open database session (caller manages commit/close).
    resolution_m : int | None
        If provided, only clean up artefacts at this resolution.
        If ``None``, clean up all resolutions (legacy behaviour).
    """
    from cog_merge import delete_gcs_tiles, delete_s3_cog

    # 1. Delete S3 COG (if exists) - use resolution-specific path
    if Config.S3_BUCKET and resolution_m is not None:
        cog_suffix = "_1km" if resolution_m == 1000 else "_250m"
        cog_prefix = f"{Config.S3_PREFIX}/cog{cog_suffix}"
        try:
            delete_s3_cog(
                Config.S3_BUCKET,
                cog_prefix,
                covariate_name,
                region=Config.AWS_REGION,
            )
        except Exception:
            logger.warning(
                "Failed to delete S3 COG for %s at %sm resolution",
                covariate_name,
                resolution_m,
            )

    # 2. Delete GCS tiles (if exists)
    if Config.GCS_BUCKET:
        try:
            delete_gcs_tiles(
                Config.GCS_BUCKET,
                Config.GCS_PREFIX,
                covariate_name,
            )
        except Exception:
            logger.warning("Failed to delete GCS tiles for %s", covariate_name)

    # 3. Remove old DB records for this covariate (at the target resolution
    #    only, or all resolutions when resolution_m is None).
    #    Flush so that a concurrent merge worker sees the deletion
    #    immediately and can bail out.
    q = db.query(Covariate).filter(Covariate.covariate_name == covariate_name)
    if resolution_m is not None:
        q = q.filter(Covariate.resolution_m == resolution_m)
    old_records = q.all()
    for rec in old_records:
        db.delete(rec)
    db.flush()


def start_gee_export(covariate_names, user_id, *, resolution_m=1000):
    """Start GEE export tasks for the specified covariates.

    Any existing downstream artefacts (GCS tiles, S3 COGs, DB records)
    are cleaned up before starting the new export so that re-exports
    always produce a consistent fresh state.

    Creates database records and starts GEE batch tasks. Returns a list
    of export record IDs.
    """
    import ee

    project = Config.GEE_PROJECT_ID or None
    opt_url = Config.GEE_ENDPOINT or None

    # Authenticate with a service account if credentials are provided
    ee_sa_json = os.environ.get("EE_SERVICE_ACCOUNT_JSON", "")
    if ee_sa_json:
        import base64

        try:
            key_data = base64.b64decode(ee_sa_json).decode("utf-8")
        except Exception:
            # Assume it's already plain JSON, not base64-encoded
            key_data = ee_sa_json
        sa_info = json.loads(key_data)
        credentials = ee.ServiceAccountCredentials(
            sa_info["client_email"], key_data=json.dumps(sa_info)
        )
        ee.Initialize(credentials=credentials, project=project, opt_url=opt_url)
    else:
        ee.Initialize(project=project, opt_url=opt_url)

    db = get_db()
    export_ids = []

    # Resolution-aware GCS prefix (e.g. covariates_1km / covariates_250m)
    gcs_prefix = gee_config.get_gcs_prefix(Config.GCS_PREFIX, resolution_m)
    # Resolution-aware S3 output prefix for the eventual merge
    _cog_suffixes = {1000: "_1km", 250: "_250m"}
    _cog_suffix = _cog_suffixes.get(resolution_m, "_1km")
    s3_output_prefix = f"{Config.S3_PREFIX}/cog{_cog_suffix}"

    try:
        for name in covariate_names:
            # Clean up any existing downstream artefacts before re-export
            _cleanup_covariate_downstream(name, db, resolution_m=resolution_m)

            task = gee_tasks.start_export_task(
                covariate_name=name,
                bucket=Config.GCS_BUCKET,
                prefix=gcs_prefix,
                resolution_m=resolution_m,
            )

            export = Covariate(
                covariate_name=name,
                resolution_m=resolution_m,
                gee_task_id=task.id,
                gcs_bucket=Config.GCS_BUCKET,
                gcs_prefix=gcs_prefix,
                output_bucket=Config.S3_BUCKET,
                output_prefix=s3_output_prefix,
                status="exporting",
                started_by=user_id,
            )
            db.add(export)
            export_ids.append(str(export.id))

        db.commit()
        return export_ids
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def list_export_tiles(bucket, prefix, covariate_name):
    """List exported tile URLs from GCS for a covariate.

    Uses the public GCS JSON API to list objects matching the export
    prefix.  Returns a list of public ``https://storage.googleapis.com/…``
    URLs, or an empty list if listing fails.
    """
    import requests

    obj_prefix = f"{prefix}/{covariate_name}".strip("/")
    api_url = (
        f"https://storage.googleapis.com/storage/v1/b/{bucket}/o"
        f"?prefix={obj_prefix}&maxResults=1000"
    )
    try:
        resp = requests.get(api_url, timeout=15)
        resp.raise_for_status()
        items = resp.json().get("items", [])
        urls = [
            f"https://storage.googleapis.com/{bucket}/{item['name']}"
            for item in items
            if item["name"].endswith(".tif")
        ]
        return sorted(urls)
    except Exception as exc:
        logger.warning(
            "Failed to list GCS tiles for %s/%s: %s",
            bucket,
            covariate_name,
            exc,
        )
        return []


def force_reexport(covariate_name, user_id, *, resolution_m=1000):
    """Force re-export a covariate from GEE.

    Delegates to :func:`start_gee_export`, which cleans up any existing
    downstream artefacts (S3 COG, GCS tiles, DB records) before starting
    a fresh GEE export.

    Parameters
    ----------
    covariate_name : str
        Covariate key from config.COVARIATES.
    user_id : uuid.UUID
        Admin user who triggered the action.
    resolution_m : int
        Target resolution in metres (default 1000).

    Returns
    -------
    dict
        ``{"status": "ok", "export_id": …}`` on success.
    """
    export_ids = start_gee_export([covariate_name], user_id, resolution_m=resolution_m)
    return {"status": "ok", "export_id": export_ids[0] if export_ids else None}


def force_remerge(covariate_name, user_id, *, resolution_m=1000):
    """Force re-merge GCS tiles to a new S3 COG.

    Deletes the existing S3 COG (if any), resets the DB record to
    ``pending_merge``, and dispatches a Celery merge task.

    Parameters
    ----------
    covariate_name : str
        Covariate key from config.COVARIATES.
    user_id : uuid.UUID
        Admin user who triggered the action.
    resolution_m : int
        Target resolution in metres (default 1000).

    Returns
    -------
    dict
        ``{"status": "ok", "layer_id": …}`` on success.
    """
    from cog_merge import delete_s3_cog

    # 1. Delete existing S3 COG - use resolution-specific path
    cog_suffix = "_1km" if resolution_m == 1000 else "_250m"
    if Config.S3_BUCKET:
        cog_prefix = f"{Config.S3_PREFIX}/cog{cog_suffix}"
        try:
            delete_s3_cog(
                Config.S3_BUCKET,
                cog_prefix,
                covariate_name,
                region=Config.AWS_REGION,
            )
        except Exception:
            logger.warning(
                "Failed to delete S3 COG for %s at %sm resolution",
                covariate_name,
                resolution_m,
            )

    # 2. Update or create DB record
    db = get_db()
    layer_id = None
    try:
        existing = (
            db.query(Covariate)
            .filter(
                Covariate.covariate_name == covariate_name,
                Covariate.resolution_m == resolution_m,
            )
            .order_by(Covariate.started_at.desc())
            .first()
        )
        if existing:
            existing.status = "pending_merge"
            existing.merged_url = None
            existing.size_bytes = None
            existing.error_message = None
            existing.completed_at = None
            existing.output_bucket = Config.S3_BUCKET
            existing.output_prefix = f"{Config.S3_PREFIX}/cog{cog_suffix}"
            layer_id = str(existing.id)
        else:
            layer = Covariate(
                covariate_name=covariate_name,
                resolution_m=resolution_m,
                status="pending_merge",
                gcs_bucket=Config.GCS_BUCKET,
                gcs_prefix=Config.GCS_PREFIX,
                output_bucket=Config.S3_BUCKET,
                output_prefix=f"{Config.S3_PREFIX}/cog{cog_suffix}",
                started_by=user_id,
            )
            db.add(layer)
            db.flush()
            layer_id = str(layer.id)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    # 3. Dispatch Celery merge task
    webapp_tasks.run_cog_merge.delay(layer_id)
    return {"status": "ok", "layer_id": layer_id}


def get_covariate_inventory():
    """Build a comprehensive inventory of all covariates with GCS/S3 status.

    Scans GCS for exported tiles, S3 for merged COGs, and the database
    for export/merge status.  Returns one row per (covariate, resolution)
    pair that has at least a DB record or tiles on GCS/S3.  Covariates
    with no data at any resolution get a single placeholder row at 1 km.

    Returns
    -------
    list[dict]
        Each dict has keys: covariate_name, category, description,
        resolution, gcs_tiles, on_s3, s3_url, status, gee_task_id,
        size_mb, merged_url, started_at, completed_at, error_message.
    """
    from cog_merge import list_all_gcs_tiles, list_s3_cog_objects

    # Load covariate definitions from GEE export config
    # gee_config already imported at module level
    covariates = gee_config.COVARIATES
    cov_resolutions = gee_config.RESOLUTIONS  # {1000: {...}, 250: {...}}

    cat_labels = {
        "climate": "Climate",
        "terrain": "Terrain",
        "accessibility": "Accessibility",
        "demographics": "Demographics",
        "biomass": "Biomass",
        "soil": "Soil",
        "land_cover": "Land Cover",
        "forest_cover": "Forest Cover",
        "ecological": "Ecological",
        "administrative": "Administrative",
        "sdg": "SDG",
        "cropland": "Cropland",
    }

    # Non-GEE covariates: layers produced by vector rasterization or SDG
    # ingestion.  They live on S3 and in the DB but aren't in the GEE
    # COVARIATES dict.  We define category/description here so they show
    # up in the admin inventory.
    from ingest_sdg_cog import SDG_LAYERS
    from rasterize_vectors import VECTOR_LAYERS

    _non_gee_covariates: dict[str, dict] = {}
    for _vl in VECTOR_LAYERS:
        _non_gee_covariates[_vl["output_name"]] = {
            "category": "administrative",
            "description": _vl["description"],
        }
    for _sdg_name, _sdg_info in SDG_LAYERS.items():
        _non_gee_covariates[_sdg_name] = {
            "category": "sdg",
            "description": _sdg_info["description"],
        }

    # 1. Scan GCS for tiles per resolution
    # Keyed by (covariate_name, resolution_m)
    gcs_counts: dict[tuple[str, int], int] = {}
    cov_names = list(covariates.keys())
    try:
        if Config.GCS_BUCKET:
            for res_m in cov_resolutions:
                gcs_prefix = gee_config.get_gcs_prefix(Config.GCS_PREFIX, res_m)
                for name, count in list_all_gcs_tiles(
                    Config.GCS_BUCKET, gcs_prefix, cov_names
                ).items():
                    gcs_counts[(name, res_m)] = count
    except Exception:
        logger.exception("Failed to scan GCS for tiles")

    # 2. Scan S3 for merged COGs per resolution
    _cog_suffixes = {1000: "_1km", 250: "_250m"}
    s3_cogs: dict[tuple[str, int], dict] = {}
    try:
        if Config.S3_BUCKET:
            for res_m, cog_suffix in _cog_suffixes.items():
                cog_prefix = f"{Config.S3_PREFIX}/cog{cog_suffix}"
                for obj in list_s3_cog_objects(
                    Config.S3_BUCKET, cog_prefix, Config.AWS_REGION
                ):
                    s3_cogs[(obj["covariate"], res_m)] = obj
    except Exception:
        logger.exception("Failed to scan S3 for COGs")

    # 3. Get most recent DB record per (covariate, resolution)
    db_records: dict[tuple[str, int], Covariate] = {}
    db = get_db()
    try:
        for rec in db.query(Covariate).all():
            key = (rec.covariate_name, rec.resolution_m)
            existing = db_records.get(key)
            if existing is None or (
                rec.started_at
                and (
                    existing.started_at is None or rec.started_at > existing.started_at
                )
            ):
                db_records[key] = rec
    finally:
        db.close()

    # 4. Build inventory rows
    def _fmt(dt):
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt else ""

    res_labels = {r: cfg["label"] for r, cfg in cov_resolutions.items()}

    rows = []
    for name, cfg in covariates.items():
        raw_cat = cfg.get("category", "other")

        # Show a row for every configured resolution so that
        # covariates are always visible even before their first export.
        seen_resolutions = set(cov_resolutions.keys())

        for res_m in sorted(seen_resolutions):
            key = (name, res_m)
            gcs_tile_count = gcs_counts.get(key, 0)
            s3_obj = s3_cogs.get(key)
            db_rec = db_records.get(key)

            row = {
                "covariate_name": name,
                "category": cat_labels.get(raw_cat, raw_cat),
                "description": cfg.get("description", ""),
                "resolution": res_labels.get(res_m, f"{res_m} m"),
                "resolution_m": res_m,
                "gcs_tiles": gcs_tile_count,
                "on_s3": bool(s3_obj),
                "status": db_rec.status if db_rec else "",
                "gee_task_id": (
                    db_rec.gee_task_id if db_rec and db_rec.gee_task_id else ""
                ),
                "size_mb": (
                    round(db_rec.size_bytes / (1024 * 1024), 1)
                    if db_rec and db_rec.size_bytes
                    else (round(s3_obj["size"] / (1024 * 1024), 1) if s3_obj else None)
                ),
                "merged_url": (
                    db_rec.merged_url
                    if db_rec and db_rec.merged_url
                    else (s3_obj["url"] if s3_obj else "")
                ),
                "started_at": _fmt(db_rec.started_at) if db_rec else "",
                "completed_at": _fmt(db_rec.completed_at) if db_rec else "",
                "error_message": (
                    db_rec.error_message if db_rec and db_rec.error_message else ""
                ),
            }
            rows.append(row)

    # 5. Include non-GEE covariates (vector/SDG layers) that have DB
    #    records or S3 COGs but aren't in the GEE COVARIATES dict.
    gee_names = set(covariates.keys())
    extra_keys = {k for k in db_records if k[0] not in gee_names} | {
        k for k in s3_cogs if k[0] not in gee_names
    }
    for name, res_m in sorted(extra_keys):
        key = (name, res_m)
        gcs_tile_count = gcs_counts.get(key, 0)
        s3_obj = s3_cogs.get(key)
        db_rec = db_records.get(key)
        non_gee_cfg = _non_gee_covariates.get(name, {})
        raw_cat = non_gee_cfg.get("category", "other")

        row = {
            "covariate_name": name,
            "category": cat_labels.get(raw_cat, raw_cat),
            "description": non_gee_cfg.get("description", ""),
            "resolution": res_labels.get(res_m, f"{res_m} m"),
            "resolution_m": res_m,
            "gcs_tiles": gcs_tile_count,
            "on_s3": bool(s3_obj),
            "status": db_rec.status if db_rec else "",
            "gee_task_id": (
                db_rec.gee_task_id if db_rec and db_rec.gee_task_id else ""
            ),
            "size_mb": (
                round(db_rec.size_bytes / (1024 * 1024), 1)
                if db_rec and db_rec.size_bytes
                else (round(s3_obj["size"] / (1024 * 1024), 1) if s3_obj else None)
            ),
            "merged_url": (
                db_rec.merged_url
                if db_rec and db_rec.merged_url
                else (s3_obj["url"] if s3_obj else "")
            ),
            "started_at": _fmt(db_rec.started_at) if db_rec else "",
            "completed_at": _fmt(db_rec.completed_at) if db_rec else "",
            "error_message": (
                db_rec.error_message if db_rec and db_rec.error_message else ""
            ),
        }
        rows.append(row)

    return rows


def get_ready_covariate_names(resolution_m=1000):
    """Return covariate names that are fully merged at *resolution_m*.

    A covariate is considered ready when it has a ``Covariate`` record
    with ``status='merged'``, a non-empty ``merged_url``, **and**
    ``resolution_m`` matching the requested resolution.

    Forest-cover year layers (``fc_*``) are excluded because they are
    handled automatically by the analysis pipeline via ``fc_years``.
    The returned order follows the GEE export config definition.
    """
    # gee_config already imported at module level
    covariate_order = list(gee_config.COVARIATES.keys())

    # Build a set of covariate names that are merged at the desired resolution.
    merged_at_res: set[str] = set()
    db = get_db()
    try:
        for rec in (
            db.query(Covariate)
            .filter(
                Covariate.resolution_m == resolution_m,
                Covariate.status == "merged",
                Covariate.merged_url.isnot(None),
            )
            .all()
        ):
            merged_at_res.add(rec.covariate_name)
    finally:
        db.close()

    ready_names = []
    for covariate_name in covariate_order:
        if covariate_name.startswith("fc_"):
            continue
        if covariate_name in merged_at_res:
            ready_names.append(covariate_name)

    # Dual-purpose variables (ecoregion, pa) are rasterized from vector
    # data and uploaded to S3 on startup.  Include them only if they
    # actually have a merged COG at this resolution.
    from layouts import DUAL_PURPOSE_VARS

    for var_name in DUAL_PURPOSE_VARS:
        if var_name not in ready_names and var_name in merged_at_res:
            ready_names.append(var_name)

    return ready_names


def get_ready_exact_match_names(resolution_m=1000):
    """Return exact-match variable names available at *resolution_m*.

    Exact-match layers (admin0, admin1, admin2, ecoregion, pa) are
    produced by the rasterize-vectors task.  A layer is considered
    available when it has a ``Covariate`` record with ``status='merged'``
    and ``resolution_m`` matching the requested resolution.
    """
    from layouts import EXACT_MATCH_OPTIONS

    all_names = [opt["value"] for opt in EXACT_MATCH_OPTIONS]

    merged_at_res: set[str] = set()
    db = get_db()
    try:
        for rec in (
            db.query(Covariate.covariate_name)
            .filter(
                Covariate.covariate_name.in_(all_names),
                Covariate.resolution_m == resolution_m,
                Covariate.status == "merged",
                Covariate.merged_url.isnot(None),
            )
            .all()
        ):
            merged_at_res.add(rec.covariate_name)
    finally:
        db.close()

    return [n for n in all_names if n in merged_at_res]


# -- Covariate presets -------------------------------------------------------
