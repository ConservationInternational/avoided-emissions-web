"""Service layer for interacting with AWS Batch and GEE.

Provides functions for submitting analysis tasks, checking job status,
uploading site files, and managing GEE covariate exports. Used by the
Dash callbacks to keep business logic out of the UI layer.
"""

import io
import json
import logging
import os
import tempfile
import uuid
from datetime import datetime, timezone

import boto3
import geopandas as gpd
import pandas as pd

from config import Config
from models import (
    AnalysisTask,
    Covariate,
    CovariatePreset,
    TaskResult,
    TaskResultTotal,
    TaskSite,
    get_db,
)

logger = logging.getLogger(__name__)

# Lazy-import batch_jobs to avoid requiring boto3 at module level in tests
_batch_module = None


def _get_batch_module():
    global _batch_module
    if _batch_module is None:
        import importlib
        import sys
        # r-analysis is mounted at /app/r-analysis inside the container
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "r-analysis"))
        _batch_module = importlib.import_module("batch_jobs")
    return _batch_module


def get_s3_client():
    return boto3.client("s3", region_name=Config.AWS_REGION)


def parse_sites_file(file_content, filename):
    """Parse an uploaded GeoJSON or GeoPackage file into a GeoDataFrame.

    Validates required columns and geometry types. Returns the GeoDataFrame
    and a list of validation errors (empty if valid).
    """
    errors = []
    gdf = None

    try:
        ext = os.path.splitext(filename)[1].lower()
        if ext in (".geojson", ".json"):
            gdf = gpd.read_file(io.BytesIO(file_content), driver="GeoJSON")
        elif ext == ".gpkg":
            with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as f:
                f.write(file_content)
                tmp_path = f.name
            gdf = gpd.read_file(tmp_path)
            os.unlink(tmp_path)
        else:
            errors.append(f"Unsupported file format: {ext}")
            return None, errors
    except Exception as e:
        errors.append(f"Failed to read file: {str(e)}")
        return None, errors

    # Validate required columns
    required = {"site_id", "site_name", "start_date"}
    missing = required - set(gdf.columns)
    if missing:
        errors.append(f"Missing required columns: {', '.join(missing)}")

    # Validate geometries
    if gdf is not None and not gdf.empty:
        invalid_geom = gdf[~gdf.geometry.is_valid]
        if len(invalid_geom) > 0:
            details = []
            for idx, row in invalid_geom.iterrows():
                site_id = row.get("site_id", "N/A")
                site_name = row.get("site_name", "N/A")
                details.append(
                    f"  Feature {idx}: site_id={site_id}, "
                    f"site_name={site_name}"
                )
            detail_str = "\n".join(details)
            errors.append(
                f"{len(invalid_geom)} invalid geometries found:\n"
                f"{detail_str}\n"
                "Please fix geometry errors before uploading."
            )
        # Ensure EPSG:4326
        if gdf.crs and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)

    return gdf, errors


def upload_sites_to_s3(gdf, task_id):
    """Upload a GeoDataFrame as GeoJSON to S3.

    Returns the S3 URI of the uploaded file.
    """
    s3 = get_s3_client()
    key = f"{Config.S3_PREFIX}/tasks/{task_id}/sites.geojson"
    # Convert any Timestamp columns to strings to avoid JSON serialization errors
    for col in gdf.columns:
        if pd.api.types.is_datetime64_any_dtype(gdf[col]):
            gdf[col] = gdf[col].dt.strftime("%Y-%m-%d")
        elif gdf[col].apply(lambda v: isinstance(v, pd.Timestamp)).any():
            gdf[col] = gdf[col].apply(
                lambda v: v.strftime("%Y-%m-%d") if isinstance(v, pd.Timestamp) else v
            )
    body = gdf.to_json()
    s3.put_object(
        Bucket=Config.S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    return f"s3://{Config.S3_BUCKET}/{key}"


def upload_config_to_s3(config_dict, task_id):
    """Upload a task configuration JSON to S3.

    Returns the S3 URI.
    """
    s3 = get_s3_client()
    key = f"{Config.S3_PREFIX}/tasks/{task_id}/config.json"
    body = json.dumps(config_dict, indent=2)
    s3.put_object(
        Bucket=Config.S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    return f"s3://{Config.S3_BUCKET}/{key}"


def submit_analysis_task(task_name, description, user_id, gdf,
                         covariates, fc_years=None):
    """Create and submit a full analysis task.

    Routes to the trends.earth API when ``Config.USE_TRENDSEARTH_API`` is
    True, otherwise falls back to direct AWS Batch submission.

    1. Creates the local database record
    2. Uploads sites and config to S3
    3. Submits to the appropriate backend
    4. Updates the database with tracking IDs

    Returns the task ID.
    """
    if Config.USE_TRENDSEARTH_API:
        return _submit_via_api(
            task_name, description, user_id, gdf, covariates, fc_years
        )
    return _submit_via_batch(
        task_name, description, user_id, gdf, covariates, fc_years
    )


def _submit_via_api(task_name, description, user_id, gdf,
                     covariates, fc_years=None):
    """Submit the analysis task through the trends.earth API.

    Creates an Execution on the API which handles AWS Batch dispatch,
    status tracking, and result collection.

    Uses the submitting user's stored OAuth2 client credentials when
    available, falling back to the global API key / email+password from
    environment variables.
    """
    from credential_store import get_decrypted_secret
    from trendsearth_client import TrendsEarthClient

    if fc_years is None:
        fc_years = list(range(2000, 2024))

    db = get_db()
    try:
        task_id = str(uuid.uuid4())

        task = AnalysisTask(
            id=task_id,
            name=task_name,
            description=description,
            submitted_by=user_id,
            status="pending",
            covariates=covariates,
            n_sites=len(gdf),
        )
        db.add(task)

        for _, row in gdf.iterrows():
            site = TaskSite(
                task_id=task_id,
                site_id=str(row["site_id"]),
                site_name=str(row.get("site_name", "")),
                start_date=pd.to_datetime(row["start_date"]),
                end_date=pd.to_datetime(row["end_date"])
                if pd.notna(row.get("end_date")) else None,
            )
            db.add(site)
        db.commit()

        # Upload sites to S3
        sites_uri = upload_sites_to_s3(gdf, task_id)

        # Build params matching AvoidedEmissionsParams schema
        params = {
            "task_id": task_id,
            "sites_s3_uri": sites_uri,
            "cog_bucket": Config.S3_BUCKET,
            "cog_prefix": f"{Config.S3_PREFIX}/cog",
            "covariates": covariates,
            "exact_match_vars": ["region", "ecoregion", "pa"],
            "fc_years": fc_years,
            "max_treatment_pixels": 1000,
            "control_multiplier": 50,
            "min_site_area_ha": 100,
            "min_glm_treatment_pixels": 15,
            "step": "all",
            "results_s3_uri": (
                f"s3://{Config.S3_BUCKET}/{Config.S3_PREFIX}"
                f"/tasks/{task_id}/output"
            ),
        }

        # Submit via trends.earth API — prefer user's stored OAuth2 creds
        user_creds = get_decrypted_secret(user_id)
        if user_creds:
            client_id, client_secret = user_creds
            client = TrendsEarthClient.from_oauth2_credentials(
                api_url=Config.TRENDSEARTH_API_URL,
                client_id=client_id,
                client_secret=client_secret,
            )
        else:
            client = TrendsEarthClient(
                api_url=Config.TRENDSEARTH_API_URL,
                api_key=Config.TRENDSEARTH_API_KEY,
                email=Config.TRENDSEARTH_API_EMAIL,
                password=Config.TRENDSEARTH_API_PASSWORD,
            )
        script_id = Config.TRENDSEARTH_SCRIPT_ID
        execution = client.create_execution(script_id, params)

        # Store the API execution ID for polling
        exec_data = execution.get("data", {})
        exec_id = exec_data.get("id", "")
        task.sites_s3_uri = sites_uri
        task.results_s3_uri = params["results_s3_uri"]
        task.status = "submitted"
        task.submitted_at = datetime.now(timezone.utc)
        # Store the API execution ID in a new-ish field; reuse
        # extract_job_id since we no longer need the Batch job IDs.
        task.extract_job_id = f"api:{exec_id}"
        db.commit()

        return task_id

    except Exception as e:
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


def _submit_via_batch(task_name, description, user_id, gdf,
                      covariates, fc_years=None):
    """Original direct-to-Batch submission (legacy path)."""
    if fc_years is None:
        fc_years = list(range(2000, 2024))

    db = get_db()
    try:
        task_id = str(uuid.uuid4())

        # Create task record
        task = AnalysisTask(
            id=task_id,
            name=task_name,
            description=description,
            submitted_by=user_id,
            status="pending",
            covariates=covariates,
            n_sites=len(gdf),
        )
        db.add(task)

        # Store site metadata
        for _, row in gdf.iterrows():
            site = TaskSite(
                task_id=task_id,
                site_id=str(row["site_id"]),
                site_name=str(row.get("site_name", "")),
                start_date=pd.to_datetime(row["start_date"]),
                end_date=pd.to_datetime(row["end_date"])
                if pd.notna(row.get("end_date")) else None,
            )
            db.add(site)

        db.commit()

        # Upload to S3
        sites_uri = upload_sites_to_s3(gdf, task_id)
        config_dict = {
            "task_id": task_id,
            "data_dir": "/data",
            "cog_bucket": Config.S3_BUCKET,
            "cog_prefix": f"{Config.S3_PREFIX}/cog",
            "sites_file": "/data/input/sites.geojson",
            "covariates": covariates,
            "exact_match_vars": ["region", "ecoregion", "pa"],
            "fc_years": fc_years,
            "max_treatment_pixels": 1000,
            "control_multiplier": 50,
            "min_site_area_ha": 100,
            "min_glm_treatment_pixels": 15,
        }
        config_uri = upload_config_to_s3(config_dict, task_id)

        # Submit to AWS Batch
        data_s3_uri = f"s3://{Config.S3_BUCKET}/{Config.S3_PREFIX}/tasks/{task_id}"
        batch = _get_batch_module()
        job_ids = batch.submit_full_pipeline(
            job_queue=Config.AWS_BATCH_JOB_QUEUE,
            job_definition=Config.AWS_BATCH_JOB_DEFINITION,
            n_sites=len(gdf),
            config_s3_uri=config_uri,
            data_s3_uri=data_s3_uri,
        )

        # Update task with job IDs
        task.extract_job_id = job_ids["extract_job_id"]
        task.match_job_id = job_ids["match_job_id"]
        task.summarize_job_id = job_ids["summarize_job_id"]
        task.sites_s3_uri = sites_uri
        task.config_s3_uri = config_uri
        task.results_s3_uri = f"{data_s3_uri}/output"
        task.status = "submitted"
        task.submitted_at = datetime.now(timezone.utc)
        db.commit()

        return task_id

    except Exception as e:
        db.rollback()
        # If task was created, mark it as failed
        if "task_id" in dir():
            task = db.query(AnalysisTask).get(task_id)
            if task:
                task.status = "failed"
                task.error_message = str(e)
                db.commit()
        raise
    finally:
        db.close()


def get_task_list(user_id=None, limit=50):
    """Get recent analysis tasks, optionally filtered by user."""
    db = get_db()
    try:
        query = db.query(AnalysisTask).order_by(
            AnalysisTask.created_at.desc()
        )
        if user_id:
            query = query.filter(AnalysisTask.submitted_by == user_id)
        return query.limit(limit).all()
    finally:
        db.close()


def get_task_detail(task_id):
    """Get full task details including sites and results."""
    db = get_db()
    try:
        task = db.query(AnalysisTask).filter(
            AnalysisTask.id == task_id
        ).first()
        if not task:
            return None

        sites = db.query(TaskSite).filter(
            TaskSite.task_id == task_id
        ).all()

        results = db.query(TaskResult).filter(
            TaskResult.task_id == task_id
        ).order_by(TaskResult.site_id, TaskResult.year).all()

        totals = db.query(TaskResultTotal).filter(
            TaskResultTotal.task_id == task_id
        ).all()

        return {
            "task": task,
            "sites": sites,
            "results": results,
            "totals": totals,
        }
    finally:
        db.close()


def start_gee_export(covariate_names, user_id):
    """Start GEE export tasks for the specified covariates.

    Creates database records and starts GEE batch tasks. Returns a list
    of export record IDs.
    """
    import ee
    import importlib.util
    import sys

    gee_dir = os.path.join(os.path.dirname(__file__), "gee-export")

    # Load gee-export/config.py as its own module, then temporarily
    # inject it into sys.modules["config"] so that gee-export/tasks.py
    # (which does "from config import COVARIATES") picks it up instead
    # of the webapp's config.py.
    gee_cfg_spec = importlib.util.spec_from_file_location(
        "gee_export_config", os.path.join(gee_dir, "config.py")
    )
    gee_cfg = importlib.util.module_from_spec(gee_cfg_spec)
    gee_cfg_spec.loader.exec_module(gee_cfg)

    original_config = sys.modules.get("config")
    sys.modules["config"] = gee_cfg
    # Also add gee-export dir to sys.path so tasks.py can find
    # sibling modules like derived_layers
    path_inserted = gee_dir not in sys.path
    if path_inserted:
        sys.path.insert(0, gee_dir)
    try:
        gee_tasks_spec = importlib.util.spec_from_file_location(
            "gee_export_tasks", os.path.join(gee_dir, "tasks.py")
        )
        gee_tasks = importlib.util.module_from_spec(gee_tasks_spec)
        gee_tasks_spec.loader.exec_module(gee_tasks)
        start_export_task = gee_tasks.start_export_task
    finally:
        # Restore the webapp config module
        if original_config is not None:
            sys.modules["config"] = original_config
        else:
            sys.modules.pop("config", None)
        if path_inserted:
            sys.path.remove(gee_dir)

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
    try:
        for name in covariate_names:
            task = start_export_task(
                covariate_name=name,
                bucket=Config.GCS_BUCKET,
                prefix=Config.GCS_PREFIX,
            )

            export = Covariate(
                covariate_name=name,
                gee_task_id=task.id,
                gcs_bucket=Config.GCS_BUCKET,
                gcs_prefix=Config.GCS_PREFIX,
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
            bucket, covariate_name, exc,
        )
        return []


def get_user_list():
    """Return all users ordered by creation date (admin only)."""
    db = get_db()
    try:
        from models import User
        return db.query(User).order_by(User.created_at.desc()).all()
    finally:
        db.close()


def approve_user(user_id):
    """Approve a pending user account. Returns (success, message)."""
    db = get_db()
    try:
        from models import User
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return False, "User not found."
        if user.is_approved:
            return False, "User is already approved."
        user.is_approved = True
        user.updated_at = datetime.now(timezone.utc)
        db.commit()
        return True, f"User {user.email} approved."
    except Exception:
        db.rollback()
        return False, "Failed to approve user."
    finally:
        db.close()


def change_user_role(user_id, new_role):
    """Change a user's role. Returns (success, message)."""
    if new_role not in ("admin", "user"):
        return False, "Invalid role."
    db = get_db()
    try:
        from models import User
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return False, "User not found."
        user.role = new_role
        user.updated_at = datetime.now(timezone.utc)
        db.commit()
        return True, f"User {user.email} role changed to {new_role}."
    except Exception:
        db.rollback()
        return False, "Failed to change role."
    finally:
        db.close()


def delete_user(user_id):
    """Delete a user account and their analysis tasks. Returns (success, message)."""
    db = get_db()
    try:
        from models import User
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return False, "User not found."
        email = user.email
        # Delete the user's analysis tasks (cascades to sites/results via DB)
        tasks = db.query(AnalysisTask).filter(
            AnalysisTask.submitted_by == user_id
        ).all()
        for task in tasks:
            db.delete(task)
        db.delete(user)
        db.commit()
        return True, f"User {email} deleted."
    except Exception:
        db.rollback()
        return False, "Failed to delete user."
    finally:
        db.close()


def download_results_csv(task_id, result_type="by_site_year"):
    """Download result CSV from S3 for a completed task.

    Args:
        task_id: The task UUID.
        result_type: One of 'by_site_year', 'by_site_total', 'pixel_level'.

    Returns:
        CSV content as string, or None if not found.
    """
    filename_map = {
        "by_site_year": "results_by_site_year.csv",
        "by_site_total": "results_by_site_total.csv",
        "pixel_level": "results_pixel_level.csv",
    }
    filename = filename_map.get(result_type)
    if not filename:
        return None

    s3 = get_s3_client()
    key = f"{Config.S3_PREFIX}/tasks/{task_id}/output/{filename}"
    try:
        response = s3.get_object(Bucket=Config.S3_BUCKET, Key=key)
        return response["Body"].read().decode("utf-8")
    except s3.exceptions.NoSuchKey:
        return None


# ---------------------------------------------------------------------------
# Covariate inventory & COG merge functions
# ---------------------------------------------------------------------------


def force_reexport(covariate_name, user_id):
    """Force re-export a covariate from GEE.

    Deletes any existing S3 COG and GCS tiles, removes/resets the DB
    record, then starts a fresh GEE export.

    Parameters
    ----------
    covariate_name : str
        Covariate key from config.COVARIATES.
    user_id : uuid.UUID
        Admin user who triggered the action.

    Returns
    -------
    dict
        ``{"status": "ok", "export_id": …}`` on success.
    """
    from cog_merge import delete_gcs_tiles, delete_s3_cog

    # 1. Delete S3 COG (if exists)
    if Config.S3_BUCKET:
        cog_prefix = f"{Config.S3_PREFIX}/cog"
        try:
            delete_s3_cog(
                Config.S3_BUCKET, cog_prefix, covariate_name,
                region=Config.AWS_REGION,
            )
        except Exception:
            logger.warning("Failed to delete S3 COG for %s", covariate_name)

    # 2. Delete GCS tiles (if exists)
    if Config.GCS_BUCKET:
        try:
            delete_gcs_tiles(
                Config.GCS_BUCKET, Config.GCS_PREFIX, covariate_name,
            )
        except Exception:
            logger.warning("Failed to delete GCS tiles for %s", covariate_name)

    # 3. Remove old DB records for this covariate
    db = get_db()
    try:
        old_records = (
            db.query(Covariate)
            .filter(Covariate.covariate_name == covariate_name)
            .all()
        )
        for rec in old_records:
            db.delete(rec)
        db.commit()
    finally:
        db.close()

    # 4. Start a fresh GEE export
    export_ids = start_gee_export([covariate_name], user_id)
    return {"status": "ok", "export_id": export_ids[0] if export_ids else None}


def force_remerge(covariate_name, user_id):
    """Force re-merge GCS tiles to a new S3 COG.

    Deletes the existing S3 COG (if any), resets the DB record to
    ``pending_merge``, and dispatches a Celery merge task.

    Parameters
    ----------
    covariate_name : str
        Covariate key from config.COVARIATES.
    user_id : uuid.UUID
        Admin user who triggered the action.

    Returns
    -------
    dict
        ``{"status": "ok", "layer_id": …}`` on success.
    """
    from cog_merge import delete_s3_cog
    from tasks import run_cog_merge

    # 1. Delete existing S3 COG
    if Config.S3_BUCKET:
        cog_prefix = f"{Config.S3_PREFIX}/cog"
        try:
            delete_s3_cog(
                Config.S3_BUCKET, cog_prefix, covariate_name,
                region=Config.AWS_REGION,
            )
        except Exception:
            logger.warning("Failed to delete S3 COG for %s", covariate_name)

    # 2. Update or create DB record
    db = get_db()
    layer_id = None
    try:
        existing = (
            db.query(Covariate)
            .filter(Covariate.covariate_name == covariate_name)
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
            existing.output_prefix = f"{Config.S3_PREFIX}/cog"
            layer_id = str(existing.id)
        else:
            layer = Covariate(
                covariate_name=covariate_name,
                status="pending_merge",
                gcs_bucket=Config.GCS_BUCKET,
                gcs_prefix=Config.GCS_PREFIX,
                output_bucket=Config.S3_BUCKET,
                output_prefix=f"{Config.S3_PREFIX}/cog",
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
    run_cog_merge.delay(layer_id)
    return {"status": "ok", "layer_id": layer_id}


def get_covariate_inventory():
    """Build a comprehensive inventory of all covariates with GCS/S3 status.

    Scans GCS for exported tiles, S3 for merged COGs, and the database
    for export/merge status.  Returns one row per covariate defined in
    the GEE export config.

    Returns
    -------
    list[dict]
        Each dict has keys: covariate_name, category, description,
        gcs_tiles, on_s3, s3_url, status, gee_task_id, size_mb,
        merged_url, started_at, completed_at, error_message.
    """
    import importlib.util

    from cog_merge import list_all_gcs_tiles, list_s3_cog_objects

    # Load covariate definitions from GEE export config
    gee_config_path = os.path.join(
        os.path.dirname(__file__), "gee-export", "config.py"
    )
    spec = importlib.util.spec_from_file_location(
        "gee_export_config", gee_config_path
    )
    gee_config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gee_config)
    covariates = gee_config.COVARIATES

    cat_labels = {
        "climate": "Climate",
        "terrain": "Terrain",
        "accessibility": "Accessibility",
        "demographics": "Demographics",
        "biomass": "Biomass",
        "land_cover": "Land Cover",
        "forest_cover": "Forest Cover",
        "ecological": "Ecological",
        "administrative": "Administrative",
    }

    # 1. Scan GCS for tiles (single paginated API call)
    gcs_counts: dict[str, int] = {}
    try:
        if Config.GCS_BUCKET:
            gcs_counts = list_all_gcs_tiles(
                Config.GCS_BUCKET,
                Config.GCS_PREFIX,
                list(covariates.keys()),
            )
    except Exception:
        logger.exception("Failed to scan GCS for tiles")

    # 2. Scan S3 for merged COGs
    s3_cogs: dict[str, dict] = {}
    try:
        if Config.S3_BUCKET:
            cog_prefix = f"{Config.S3_PREFIX}/cog"
            for obj in list_s3_cog_objects(
                Config.S3_BUCKET, cog_prefix, Config.AWS_REGION
            ):
                s3_cogs[obj["covariate"]] = obj
    except Exception:
        logger.exception("Failed to scan S3 for COGs")

    # 3. Get most recent DB record per covariate
    db_records: dict[str, Covariate] = {}
    db = get_db()
    try:
        for rec in db.query(Covariate).all():
            existing = db_records.get(rec.covariate_name)
            if existing is None or (
                rec.started_at
                and (
                    existing.started_at is None
                    or rec.started_at > existing.started_at
                )
            ):
                db_records[rec.covariate_name] = rec
    finally:
        db.close()

    # 4. Build inventory rows
    def _fmt(dt):
        return dt.strftime("%Y-%m-%d %H:%M") if dt else ""

    rows = []
    for name, cfg in covariates.items():
        raw_cat = cfg.get("category", "other")
        gcs_tiles = gcs_counts.get(name, 0)
        s3_obj = s3_cogs.get(name)
        db_rec = db_records.get(name)

        row = {
            "covariate_name": name,
            "category": cat_labels.get(raw_cat, raw_cat),
            "description": cfg.get("description", ""),
            "gcs_tiles": gcs_tiles,
            "on_s3": bool(s3_obj),
            "status": db_rec.status if db_rec else "",
            "gee_task_id": (
                db_rec.gee_task_id if db_rec and db_rec.gee_task_id else ""
            ),
            "size_mb": (
                round(db_rec.size_bytes / (1024 * 1024), 1)
                if db_rec and db_rec.size_bytes
                else (
                    round(s3_obj["size"] / (1024 * 1024), 1)
                    if s3_obj
                    else None
                )
            ),
            "merged_url": (
                db_rec.merged_url
                if db_rec and db_rec.merged_url
                else (s3_obj["url"] if s3_obj else "")
            ),
            "started_at": _fmt(db_rec.started_at) if db_rec else "",
            "completed_at": _fmt(db_rec.completed_at) if db_rec else "",
            "error_message": (
                db_rec.error_message
                if db_rec and db_rec.error_message
                else ""
            ),
        }
        rows.append(row)

    return rows


def discover_existing_cogs():
    """Scan S3 for pre-existing merged COGs and import them into the DB.

    Lists all ``.tif`` files under the COG prefix in the S3 bucket,
    matches filenames to known covariates, and inserts ``Covariate`` rows
    (status=merged) for any that aren't already tracked.

    Returns
    -------
    list[str]
        Covariate names that were newly imported.
    """
    from cog_merge import list_s3_cog_objects

    bucket = Config.S3_BUCKET
    if not bucket:
        return []

    cog_prefix = f"{Config.S3_PREFIX}/cog"
    cog_objects = list_s3_cog_objects(bucket, cog_prefix, Config.AWS_REGION)
    if not cog_objects:
        return []

    db = get_db()
    imported = []
    try:
        # Get covariate names already tracked in the DB
        existing = {
            row.covariate_name
            for row in db.query(Covariate.covariate_name)
            .filter(Covariate.status.in_(["merged", "merging", "pending_merge"]))
            .all()
        }

        for obj in cog_objects:
            cov_name = obj["covariate"]
            if cov_name in existing:
                continue
            layer = Covariate(
                covariate_name=cov_name,
                status="merged",
                gcs_bucket=Config.GCS_BUCKET,
                gcs_prefix=Config.GCS_PREFIX or "avoided-emissions/covariates",
                output_bucket=bucket,
                output_prefix=cog_prefix,
                merged_url=obj["url"],
                size_bytes=obj["size"],
                completed_at=datetime.now(timezone.utc),
            )
            db.add(layer)
            imported.append(cov_name)
            existing.add(cov_name)  # prevent duplicates within batch

        if imported:
            db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    return imported


# -- Covariate presets -------------------------------------------------------


def get_covariate_presets(user_id):
    """Return all covariate presets for the given user, ordered by name.

    Each item is a dict with keys ``id``, ``name``, and ``covariates``.
    """
    db = get_db()
    try:
        presets = (
            db.query(CovariatePreset)
            .filter(CovariatePreset.user_id == user_id)
            .order_by(CovariatePreset.name)
            .all()
        )
        return [
            {"id": str(p.id), "name": p.name, "covariates": list(p.covariates)}
            for p in presets
        ]
    finally:
        db.close()


def save_covariate_preset(user_id, name, covariates):
    """Create or update a covariate preset for the given user.

    If a preset with the same *name* already exists for this user it is
    updated in-place; otherwise a new row is inserted.  Returns the
    preset ``id`` as a string.
    """
    db = get_db()
    try:
        existing = (
            db.query(CovariatePreset)
            .filter(
                CovariatePreset.user_id == user_id,
                CovariatePreset.name == name,
            )
            .first()
        )
        if existing:
            existing.covariates = list(covariates)
            existing.updated_at = datetime.now(timezone.utc)
            db.commit()
            return str(existing.id)

        preset = CovariatePreset(
            user_id=user_id,
            name=name,
            covariates=list(covariates),
        )
        db.add(preset)
        db.commit()
        return str(preset.id)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def delete_covariate_preset(preset_id, user_id):
    """Delete a covariate preset by id, scoped to the owning user.

    Returns ``True`` if a row was deleted, ``False`` otherwise.
    """
    db = get_db()
    try:
        preset = (
            db.query(CovariatePreset)
            .filter(
                CovariatePreset.id == preset_id,
                CovariatePreset.user_id == user_id,
            )
            .first()
        )
        if not preset:
            return False
        db.delete(preset)
        db.commit()
        return True
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
