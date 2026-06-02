"""Service layer for interacting with the trends.earth API and GEE.

Provides functions for submitting analysis tasks, checking job status,
uploading site files, and managing GEE covariate exports. Used by the
Dash callbacks to keep business logic out of the UI layer.
"""

import io
import json
import logging
import os
import shutil
import tarfile
import tempfile
import uuid
import zipfile
from datetime import datetime, timedelta, timezone
from time import monotonic

import boto3
from botocore.exceptions import ClientError
from pathlib import Path
import geopandas as gpd
import pandas as pd
from shapely import wkb
from sqlalchemy import text

from config import Config, report_exception
from models import (
    AnalysisTask,
    Covariate,
    CovariatePreset,
    MatchingSettingsPreset,
    TaskResult,
    TaskResultTotal,
    TaskSite,
    User,
    UserSiteUpload,
    UserSiteSet,
    get_db,
)

import tasks as webapp_tasks
from gee_export import gee_config

FC_YEAR_MIN = gee_config.FC_YEAR_MIN
FC_YEAR_MAX = gee_config.FC_YEAR_MAX

logger = logging.getLogger(__name__)

ALLOWED_MATCHING_JOB_QUEUES = {
    "ae-spot-gp3",
    "ae-ondemand-gp3",
}

DEFAULT_MATCHING_JOB_QUEUE = "ae-spot-gp3"

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

MAX_ARCHIVE_FILE_COUNT = 2_000
MAX_ARCHIVE_TOTAL_UNCOMPRESSED_BYTES = 200 * 1024 * 1024
MAX_ARCHIVE_MEMBER_UNCOMPRESSED_BYTES = 50 * 1024 * 1024
MAX_ARCHIVE_COMPRESSION_RATIO = 200.0

SITE_UPLOAD_STAGE_DIR = Path(tempfile.gettempdir()) / "ae_site_upload_stage"
SITE_UPLOAD_STAGE_TTL_SECONDS = 12 * 60 * 60
SITE_UPLOAD_STAGE_S3_PREFIX = f"{Config.S3_PREFIX.rstrip('/')}/site-upload-stage"
SITE_UPLOAD_INSERT_BATCH_SIZE = 1000
SITE_UPLOAD_INSERT_BATCH_MAX_BYTES = 8 * 1024 * 1024
SITE_UPLOAD_PROGRESS_BATCH_SIZE = 250
SITE_UPLOAD_PROGRESS_CHECK_ROW_INTERVAL = 25
SITE_UPLOAD_PROGRESS_INTERVAL_SECONDS = 10

# start_date is the only truly required field for analysis
# site_id and site_name can be auto-assigned if missing
REQUIRED_SITE_FIELDS = ("start_date",)
OPTIONAL_SITE_FIELDS = ("site_id", "site_name", "end_date")
ALL_SITE_FIELDS = ("site_id", "site_name", "start_date", "end_date")


def _site_upload_stage_paths(upload_token):
    token = str(upload_token or "").strip()
    if not token or not all(ch in "0123456789abcdef" for ch in token.lower()):
        raise ValueError("Invalid upload token.")

    data_path = SITE_UPLOAD_STAGE_DIR / f"{token}.bin"
    meta_path = SITE_UPLOAD_STAGE_DIR / f"{token}.json"
    return data_path, meta_path


def _site_upload_stage_s3_keys(upload_token):
    token = str(upload_token or "").strip()
    if not token or not all(ch in "0123456789abcdef" for ch in token.lower()):
        raise ValueError("Invalid upload token.")
    return (
        f"{SITE_UPLOAD_STAGE_S3_PREFIX}/{token}.bin",
        f"{SITE_UPLOAD_STAGE_S3_PREFIX}/{token}.json",
    )


def _use_shared_site_upload_stage():
    return Config.ENVIRONMENT in {"staging", "production"} and bool(Config.S3_BUCKET)


def _parse_staged_upload_created_at(meta):
    created_at = datetime.fromisoformat(meta.get("created_at", ""))
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    return created_at


def _load_staged_upload_meta(upload_token):
    if _use_shared_site_upload_stage():
        _, meta_key = _site_upload_stage_s3_keys(upload_token)
        try:
            response = get_s3_client().get_object(Bucket=Config.S3_BUCKET, Key=meta_key)
            return json.loads(response["Body"].read().decode("utf-8"))
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
                raise ValueError(
                    "Staged upload not found. Please upload the file again."
                ) from exc
            raise

    _, meta_path = _site_upload_stage_paths(upload_token)
    if not meta_path.exists():
        raise ValueError("Staged upload not found. Please upload the file again.")
    with open(meta_path, encoding="utf-8") as f:
        return json.load(f)


def _validate_staged_upload_access(upload_token, user_id):
    """Return staged metadata after ownership/TTL checks."""
    meta = _load_staged_upload_meta(upload_token)
    if str(meta.get("user_id")) != str(user_id):
        raise ValueError("You do not have access to this staged upload.")

    created_at = _parse_staged_upload_created_at(meta)
    age_seconds = (datetime.now(timezone.utc) - created_at).total_seconds()
    if age_seconds > SITE_UPLOAD_STAGE_TTL_SECONDS:
        discard_staged_site_upload(upload_token, user_id)
        raise ValueError("Staged upload expired. Please upload the file again.")

    return meta


def _materialize_staged_upload_path(upload_token, user_id):
    """Materialize staged upload to a local path and return (path, filename, size)."""
    meta = _validate_staged_upload_access(upload_token, user_id)
    filename = meta.get("filename") or "sites"
    ext = _get_file_extension(filename)

    if _use_shared_site_upload_stage():
        data_key, _ = _site_upload_stage_s3_keys(upload_token)
        with tempfile.NamedTemporaryFile(suffix=ext or ".bin", delete=False) as tmp:
            local_path = tmp.name
        try:
            with open(local_path, "wb") as out:
                get_s3_client().download_fileobj(Config.S3_BUCKET, data_key, out)
        except Exception:
            os.unlink(local_path)
            raise
        size_bytes = os.path.getsize(local_path)
        return local_path, filename, size_bytes

    data_path, _ = _site_upload_stage_paths(upload_token)
    if not data_path.exists():
        raise ValueError("Staged upload not found. Please upload the file again.")
    return str(data_path), filename, data_path.stat().st_size


def _parse_sites_geometry_file_from_path(file_path, filename):
    """Parse sites geometry using a local file path (low-memory path)."""
    errors = []
    gdf = None

    try:
        ext = _get_file_extension(filename)
        if ext in (".geojson", ".json", ".gpkg"):
            gdf = gpd.read_file(file_path)
        elif ext in (".zip", ".tar.gz", ".tgz"):
            with open(file_path, "rb") as f:
                gdf = _read_sites_from_archive(f.read(), filename)
        else:
            errors.append(f"Unsupported file format: {ext}")
            return None, errors
    except Exception as e:
        errors.append(f"Failed to read file: {str(e)}")
        return None, errors

    # Reuse existing validation/repair pipeline by operating on the parsed gdf.
    if gdf is not None and not gdf.empty:
        null_geom = gdf.geometry.is_empty | gdf.geometry.isna()
        if null_geom.any():
            feature_nums = [i + 1 for i in gdf[null_geom].index[:10]]
            errors.append(f"Features with missing/empty geometry: {feature_nums}")
        valid_mask = ~null_geom
        if valid_mask.any():
            bad_type = ~gdf.loc[valid_mask].geometry.geom_type.isin(
                ["Polygon", "MultiPolygon"]
            )
            if bad_type.any():
                bad_rows = gdf.loc[valid_mask][bad_type]
                details = [
                    f"Feature {idx + 1}: geometry type={row.geometry.geom_type}"
                    for idx, row in bad_rows.iterrows()
                ]
                errors.append(
                    "All geometries must be Polygon or MultiPolygon.\n"
                    + "\n".join(details[:10])
                )
        if gdf.crs and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)

        gdf = _repair_geometries(gdf)

    return gdf, errors


def _cleanup_expired_staged_uploads():
    if _use_shared_site_upload_stage():
        # Expired shared staged uploads are deleted lazily when retrieved.
        return

    SITE_UPLOAD_STAGE_DIR.mkdir(parents=True, exist_ok=True)
    cutoff = datetime.now(timezone.utc) - timedelta(
        seconds=SITE_UPLOAD_STAGE_TTL_SECONDS
    )

    for meta_path in SITE_UPLOAD_STAGE_DIR.glob("*.json"):
        token = meta_path.stem
        data_path = SITE_UPLOAD_STAGE_DIR / f"{token}.bin"
        try:
            with open(meta_path, encoding="utf-8") as f:
                meta = json.load(f)
            created_at = datetime.fromisoformat(meta.get("created_at", ""))
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
        except Exception:
            created_at = datetime.fromtimestamp(
                meta_path.stat().st_mtime, tz=timezone.utc
            )

        if created_at < cutoff:
            try:
                meta_path.unlink(missing_ok=True)
                data_path.unlink(missing_ok=True)
            except Exception:
                logger.warning("Failed cleaning expired staged upload: %s", token)


def stage_site_upload(file_content, filename, user_id):
    """Persist uploaded bytes to short-lived local staging and return a token."""
    _cleanup_expired_staged_uploads()

    token = uuid.uuid4().hex
    meta = {
        "user_id": str(user_id),
        "filename": filename,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    if _use_shared_site_upload_stage():
        data_key, meta_key = _site_upload_stage_s3_keys(token)
        s3 = get_s3_client()
        s3.put_object(Bucket=Config.S3_BUCKET, Key=data_key, Body=file_content)
        s3.put_object(
            Bucket=Config.S3_BUCKET,
            Key=meta_key,
            Body=json.dumps(meta).encode("utf-8"),
            ContentType="application/json",
        )
        return token

    SITE_UPLOAD_STAGE_DIR.mkdir(parents=True, exist_ok=True)
    data_path, meta_path = _site_upload_stage_paths(token)

    with open(data_path, "wb") as f:
        f.write(file_content)
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f)

    return token


def get_staged_site_upload(upload_token, user_id, consume=False):
    """Read staged upload bytes by token, enforcing ownership and TTL."""
    meta = _validate_staged_upload_access(upload_token, user_id)

    if _use_shared_site_upload_stage():
        data_key, _ = _site_upload_stage_s3_keys(upload_token)
        try:
            response = get_s3_client().get_object(Bucket=Config.S3_BUCKET, Key=data_key)
            content = response["Body"].read()
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
                raise ValueError(
                    "Staged upload not found. Please upload the file again."
                ) from exc
            raise
    else:
        data_path, _ = _site_upload_stage_paths(upload_token)
        if not data_path.exists():
            raise ValueError("Staged upload not found. Please upload the file again.")
        with open(data_path, "rb") as f:
            content = f.read()

    if consume:
        discard_staged_site_upload(upload_token, user_id)

    return {
        "filename": meta.get("filename") or "sites",
        "content": content,
    }


def discard_staged_site_upload(upload_token, user_id=None):
    """Delete staged upload bytes + metadata. Returns True if removed."""
    if _use_shared_site_upload_stage():
        data_key, meta_key = _site_upload_stage_s3_keys(upload_token)
        s3 = get_s3_client()
        try:
            if user_id is not None:
                meta = _load_staged_upload_meta(upload_token)
                if str(meta.get("user_id")) != str(user_id):
                    raise ValueError("You do not have access to this staged upload.")
        except ValueError:
            return False

        s3.delete_objects(
            Bucket=Config.S3_BUCKET,
            Delete={
                "Objects": [{"Key": data_key}, {"Key": meta_key}],
                "Quiet": True,
            },
        )
        return True

    data_path, meta_path = _site_upload_stage_paths(upload_token)
    if user_id is not None and meta_path.exists():
        try:
            with open(meta_path, encoding="utf-8") as f:
                meta = json.load(f)
            if str(meta.get("user_id")) != str(user_id):
                raise ValueError("You do not have access to this staged upload.")
        except FileNotFoundError:
            return False

    removed = False
    if meta_path.exists():
        meta_path.unlink(missing_ok=True)
        removed = True
    if data_path.exists():
        data_path.unlink(missing_ok=True)
        removed = True
    return removed


def stream_stage_site_upload(file_stream, filename, user_id):
    """Write an upload stream directly to staging in 1 MB chunks.

    Unlike ``stage_site_upload``, this never loads the full file content into
    Python memory, which is critical for files >200 MB.  Returns the token.
    """
    _cleanup_expired_staged_uploads()

    token = uuid.uuid4().hex

    try:
        meta = {
            "user_id": str(user_id),
            "filename": filename,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        if _use_shared_site_upload_stage():
            data_key, meta_key = _site_upload_stage_s3_keys(token)
            s3 = get_s3_client()
            s3.upload_fileobj(file_stream, Config.S3_BUCKET, data_key)

            size_bytes = s3.head_object(Bucket=Config.S3_BUCKET, Key=data_key).get(
                "ContentLength", 0
            )
            if int(size_bytes or 0) == 0:
                s3.delete_object(Bucket=Config.S3_BUCKET, Key=data_key)
                raise ValueError("Uploaded file is empty.")

            s3.put_object(
                Bucket=Config.S3_BUCKET,
                Key=meta_key,
                Body=json.dumps(meta).encode("utf-8"),
                ContentType="application/json",
            )
            return token

        SITE_UPLOAD_STAGE_DIR.mkdir(parents=True, exist_ok=True)
        data_path, meta_path = _site_upload_stage_paths(token)

        with open(data_path, "wb") as out:
            chunk_size = 1024 * 1024  # 1 MB
            while True:
                chunk = file_stream.read(chunk_size)
                if not chunk:
                    break
                out.write(chunk)

        if data_path.stat().st_size == 0:
            data_path.unlink(missing_ok=True)
            raise ValueError("Uploaded file is empty.")

        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f)
    except Exception:
        if _use_shared_site_upload_stage():
            try:
                data_key, meta_key = _site_upload_stage_s3_keys(token)
                get_s3_client().delete_objects(
                    Bucket=Config.S3_BUCKET,
                    Delete={
                        "Objects": [{"Key": data_key}, {"Key": meta_key}],
                        "Quiet": True,
                    },
                )
            except Exception:
                logger.warning("Failed cleaning staged upload from S3", exc_info=True)
        else:
            data_path, meta_path = _site_upload_stage_paths(token)
            data_path.unlink(missing_ok=True)
            meta_path.unlink(missing_ok=True)
        raise

    return token


def get_site_upload_mapping_preview_from_staged(token, user_id):
    """Return a column-mapping preview by reading the staged file directly.

    For GPKG/GeoJSON, reads only the first 10 rows so memory usage is O(1)
    rather than O(file size).  Feature count is obtained cheaply via fiona.
    For archive formats the full archive is still read (archives are small on
    disk due to compression) but the behaviour matches the existing fallback.
    """
    import fiona  # local import — only needed here

    try:
        meta = _load_staged_upload_meta(token)
    except ValueError as exc:
        return None, [str(exc)]

    if str(meta.get("user_id")) != str(user_id):
        return None, ["You do not have access to this staged upload."]

    created_at = _parse_staged_upload_created_at(meta)
    age_seconds = (datetime.now(timezone.utc) - created_at).total_seconds()
    if age_seconds > SITE_UPLOAD_STAGE_TTL_SECONDS:
        discard_staged_site_upload(token, user_id)
        return None, ["Staged upload expired. Please upload the file again."]

    filename = meta.get("filename") or "sites"
    ext = _get_file_extension(filename)

    gdf_sample = None
    n_features = 0

    preview_path = None
    try:
        if ext in (".gpkg", ".geojson", ".json"):
            # GDAL/pyogrio expects GeoPackage files to have a conformant
            # extension. Staged files are stored as .bin, so read through a
            # temporary path with the original suffix.
            with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
                preview_path = tmp.name

            if _use_shared_site_upload_stage():
                data_key, _ = _site_upload_stage_s3_keys(token)
                with open(preview_path, "wb") as out:
                    get_s3_client().download_fileobj(Config.S3_BUCKET, data_key, out)
            else:
                data_path, _ = _site_upload_stage_paths(token)
                if not data_path.exists():
                    return None, [
                        "Staged upload not found. Please upload the file again."
                    ]
                shutil.copyfile(data_path, preview_path)

            # Read a tiny sample — just enough to get column names + one value.
            gdf_sample = gpd.read_file(preview_path, rows=10)
            with fiona.open(preview_path) as src:
                n_features = len(src)
        elif ext in (".zip", ".tar.gz", ".tgz"):
            # Archives are compressed; read them fully (still small in RAM).
            if _use_shared_site_upload_stage():
                data_key, _ = _site_upload_stage_s3_keys(token)
                archive_bytes = (
                    get_s3_client()
                    .get_object(Bucket=Config.S3_BUCKET, Key=data_key)["Body"]
                    .read()
                )
            else:
                data_path, _ = _site_upload_stage_paths(token)
                if not data_path.exists():
                    return None, [
                        "Staged upload not found. Please upload the file again."
                    ]
                with open(data_path, "rb") as f:
                    archive_bytes = f.read()
            gdf_sample = _read_sites_from_archive(archive_bytes, filename)
            if gdf_sample is not None:
                n_features = len(gdf_sample)
        else:
            return None, [f"Unsupported file format: {ext}"]
    except Exception as exc:
        return None, [f"Failed to read file: {exc}"]
    finally:
        if preview_path:
            try:
                os.unlink(preview_path)
            except OSError:
                logger.debug(
                    "Failed to remove temporary preview file: %s", preview_path
                )

    if gdf_sample is None or gdf_sample.empty:
        return None, ["No features were found in the uploaded file."]

    non_geom_cols = [c for c in gdf_sample.columns if c != "geometry"]
    column_info = []
    for col in non_geom_cols:
        series = gdf_sample[col]
        sample_val = ""
        sample_series = series.dropna()
        if not sample_series.empty:
            sample_val = str(sample_series.iloc[0])
        column_info.append(
            {
                "name": col,
                "dtype": str(series.dtype),
                "sample": sample_val,
            }
        )

    return (
        {
            "column_info": column_info,
            "suggested_mapping": suggest_site_column_mapping(non_geom_cols),
            "n_features": n_features,
        },
        [],
    )


def get_s3_client():
    return boto3.client("s3", region_name=Config.AWS_REGION)


# Cost-allocation tag applied to every S3 object created by this app.
# Formatted as a URL query-string for the S3 ``Tagging`` header.
S3_COST_TAGGING = "Project=avoided-emissions"


def _get_file_extension(filename):
    lower_name = (filename or "").lower()
    if lower_name.endswith(".tar.gz"):
        return ".tar.gz"
    if lower_name.endswith(".tgz"):
        return ".tgz"
    return os.path.splitext(lower_name)[1]


def _is_within_directory(directory, target):
    abs_directory = os.path.abspath(directory)
    abs_target = os.path.abspath(target)
    return os.path.commonpath([abs_directory]) == os.path.commonpath(
        [abs_directory, abs_target]
    )


def _safe_extract_zip(archive_path, target_dir):
    with zipfile.ZipFile(archive_path, "r") as archive:
        file_count = 0
        total_uncompressed_bytes = 0

        for info in archive.infolist():
            member = info.filename
            if not member:
                continue
            destination = os.path.join(target_dir, member)
            if not _is_within_directory(target_dir, destination):
                raise ValueError("Archive contains invalid paths.")

            if info.is_dir():
                continue

            file_count += 1
            member_size = int(info.file_size or 0)
            compressed_size = int(info.compress_size or 0)

            if member_size > MAX_ARCHIVE_MEMBER_UNCOMPRESSED_BYTES:
                raise ValueError("Archive member is too large.")

            if compressed_size <= 0 and member_size > 0:
                raise ValueError("Archive contains an invalid compressed member.")

            if compressed_size > 0:
                ratio = member_size / compressed_size
                if ratio > MAX_ARCHIVE_COMPRESSION_RATIO:
                    raise ValueError("Archive contains suspiciously compressed data.")

            total_uncompressed_bytes += member_size
            if total_uncompressed_bytes > MAX_ARCHIVE_TOTAL_UNCOMPRESSED_BYTES:
                raise ValueError("Archive expands to too much data.")
            if file_count > MAX_ARCHIVE_FILE_COUNT:
                raise ValueError("Archive contains too many files.")

        archive.extractall(target_dir)


def _safe_extract_tar(archive_path, target_dir):
    with tarfile.open(archive_path, "r:gz") as archive:
        file_count = 0
        total_uncompressed_bytes = 0

        for member in archive.getmembers():
            destination = os.path.join(target_dir, member.name)
            if not _is_within_directory(target_dir, destination):
                raise ValueError("Archive contains invalid paths.")

            if not member.isfile():
                continue

            file_count += 1
            member_size = int(member.size or 0)

            if member_size > MAX_ARCHIVE_MEMBER_UNCOMPRESSED_BYTES:
                raise ValueError("Archive member is too large.")

            total_uncompressed_bytes += member_size
            if total_uncompressed_bytes > MAX_ARCHIVE_TOTAL_UNCOMPRESSED_BYTES:
                raise ValueError("Archive expands to too much data.")
            if file_count > MAX_ARCHIVE_FILE_COUNT:
                raise ValueError("Archive contains too many files.")

        archive.extractall(target_dir, filter="data")


def _find_supported_dataset_paths(directory):
    shapefiles = []
    geopackages = []
    geojsons = []

    for root, dirs, files in os.walk(directory):
        # Skip macOS resource fork directories
        dirs[:] = [d for d in dirs if d != "__MACOSX"]
        for filename in files:
            # Skip macOS resource fork files
            if filename.startswith("._"):
                continue
            lower_name = filename.lower()
            full_path = os.path.join(root, filename)
            if lower_name.endswith(".shp"):
                shapefiles.append(full_path)
            elif lower_name.endswith(".gpkg"):
                geopackages.append(full_path)
            elif lower_name.endswith(".geojson") or lower_name.endswith(".json"):
                geojsons.append(full_path)

    return sorted(shapefiles), sorted(geopackages), sorted(geojsons)


def _select_site_dataset_path(directory):
    shapefiles, geopackages, geojsons = _find_supported_dataset_paths(directory)
    candidate_count = len(shapefiles) + len(geopackages) + len(geojsons)

    if candidate_count == 0:
        raise ValueError(
            "No supported site dataset found in archive. Include a .shp, .gpkg, or .geojson/.json file."
        )

    if candidate_count > 1:
        raise ValueError(
            "Archive contains multiple supported datasets. Include exactly one site dataset per upload."
        )

    if shapefiles:
        return shapefiles[0]
    if geopackages:
        return geopackages[0]
    return geojsons[0]


def _extract_site_archive_dataset(archive_path, filename, target_dir):
    ext = _get_file_extension(filename)
    if ext == ".zip":
        _safe_extract_zip(archive_path, target_dir)
    else:
        _safe_extract_tar(archive_path, target_dir)
    return _select_site_dataset_path(target_dir)


def _read_shapefile(path):
    """Read a shapefile, preferring UTF-8 when no .cpg sidecar is present.

    Without a .cpg file, GDAL/Fiona defaults to ISO-8859-1 per the shapefile
    spec, which mangles text that is actually encoded as UTF-8. This function
    detects the missing .cpg case and tries UTF-8 first.
    """
    cpg_path = os.path.splitext(path)[0] + ".cpg"
    if not os.path.exists(cpg_path):
        try:
            return gpd.read_file(path, encoding="utf-8")
        except Exception:
            pass
    return gpd.read_file(path)


def _read_sites_from_archive(file_content, filename):
    ext = _get_file_extension(filename)

    with tempfile.TemporaryDirectory() as tmpdir:
        archive_name = filename or f"sites{ext}"
        archive_path = os.path.join(tmpdir, os.path.basename(archive_name))

        with open(archive_path, "wb") as archive_file:
            archive_file.write(file_content)

        dataset_path = _extract_site_archive_dataset(archive_path, filename, tmpdir)
        if dataset_path.lower().endswith(".shp"):
            return _read_shapefile(dataset_path)
        return gpd.read_file(dataset_path)


def _open_site_feature_source(path):
    import fiona  # noqa: PLC0415

    if str(path).lower().endswith(".shp"):
        cpg_path = os.path.splitext(path)[0] + ".cpg"
        if not os.path.exists(cpg_path):
            try:
                return fiona.open(path, encoding="utf-8")
            except Exception:
                logger.debug(
                    "Failed to open shapefile as UTF-8, falling back to GDAL defaults",
                    exc_info=True,
                )

    return fiona.open(path)


def _remove_duplicate_vertices(coords):
    """Remove consecutive duplicate vertices from a coordinate sequence."""
    cleaned = [coords[0]]
    for pt in coords[1:]:
        if pt != cleaned[-1]:
            cleaned.append(pt)
    return cleaned


def _clean_polygon(polygon):
    """Remove consecutive duplicate vertices from a Polygon's rings."""
    from shapely.geometry import Polygon as ShapelyPolygon

    ext = _remove_duplicate_vertices(list(polygon.exterior.coords))
    # A valid ring needs at least 4 coords (3 distinct + closing point)
    if len(ext) < 4:
        return polygon

    ints = []
    for ring in polygon.interiors:
        cleaned = _remove_duplicate_vertices(list(ring.coords))
        if len(cleaned) >= 4:
            ints.append(cleaned)

    return ShapelyPolygon(ext, ints)


def _repair_geometries(gdf):
    """Fix geometries that would be invalid under S2 spherical geometry.

    Applies two repairs to every non-null Polygon/MultiPolygon:
      1. Removes consecutive duplicate vertices (S2 "degenerate edge" errors).
      2. Applies ``shapely.validation.make_valid`` for self-intersections,
         then re-extracts Polygon/MultiPolygon parts.

    Returns a new GeoDataFrame with cleaned geometries.
    """
    from shapely.geometry import MultiPolygon as ShapelyMultiPolygon
    from shapely.validation import make_valid

    def _repair_one(geom):
        if geom is None or geom.is_empty:
            return geom

        # Step 1: strip consecutive duplicate vertices
        if geom.geom_type == "Polygon":
            geom = _clean_polygon(geom)
        elif geom.geom_type == "MultiPolygon":
            geom = ShapelyMultiPolygon([_clean_polygon(p) for p in geom.geoms])

        # Step 2: fix self-intersections / crossing edges
        if not geom.is_valid:
            geom = make_valid(geom)
            # make_valid can produce GeometryCollections; keep only polygons
            if geom.geom_type == "GeometryCollection":
                polys = [
                    g for g in geom.geoms if g.geom_type in ("Polygon", "MultiPolygon")
                ]
                if not polys:
                    return geom
                geom = (
                    polys[0]
                    if len(polys) == 1
                    else ShapelyMultiPolygon(
                        [
                            p
                            for g in polys
                            for p in (g.geoms if g.geom_type == "MultiPolygon" else [g])
                        ]
                    )
                )

        return geom

    gdf = gdf.copy()
    gdf["geometry"] = gdf["geometry"].apply(_repair_one)
    return gdf


def _repair_geometry_single(geom):
    """Repair a single geometry (streaming-friendly version)."""
    from shapely.geometry import MultiPolygon as ShapelyMultiPolygon
    from shapely.validation import make_valid

    if geom is None or geom.is_empty:
        return geom

    if geom.geom_type == "Polygon":
        geom = _clean_polygon(geom)
    elif geom.geom_type == "MultiPolygon":
        geom = ShapelyMultiPolygon([_clean_polygon(p) for p in geom.geoms])

    if not geom.is_valid:
        geom = make_valid(geom)
        if geom.geom_type == "GeometryCollection":
            polys = [
                g for g in geom.geoms if g.geom_type in ("Polygon", "MultiPolygon")
            ]
            if not polys:
                return geom
            geom = (
                polys[0]
                if len(polys) == 1
                else ShapelyMultiPolygon(
                    [
                        p
                        for g in polys
                        for p in (g.geoms if g.geom_type == "MultiPolygon" else [g])
                    ]
                )
            )

    return geom


def _parse_sites_geometry_file(file_content, filename):
    """Parse uploaded site files and validate geometry/CRS constraints."""
    errors = []
    gdf = None

    try:
        ext = _get_file_extension(filename)
        if ext in (".geojson", ".json"):
            gdf = gpd.read_file(io.BytesIO(file_content))
        elif ext == ".gpkg":
            with tempfile.NamedTemporaryFile(suffix=".gpkg", delete=False) as f:
                f.write(file_content)
                tmp_path = f.name
            gdf = gpd.read_file(tmp_path)
            os.unlink(tmp_path)
        elif ext in (".zip", ".tar.gz", ".tgz"):
            gdf = _read_sites_from_archive(file_content, filename)
        else:
            errors.append(f"Unsupported file format: {ext}")
            return None, errors
    except Exception as e:
        errors.append(f"Failed to read file: {str(e)}")
        return None, errors

    # Validate geometries
    if gdf is not None and not gdf.empty:
        null_geom = gdf.geometry.is_empty | gdf.geometry.isna()
        if null_geom.any():
            feature_nums = [i + 1 for i in gdf[null_geom].index[:10]]
            errors.append(f"Features with missing/empty geometry: {feature_nums}")
        valid_mask = ~null_geom
        if valid_mask.any():
            bad_type = ~gdf.loc[valid_mask].geometry.geom_type.isin(
                ["Polygon", "MultiPolygon"]
            )
            if bad_type.any():
                bad_rows = gdf.loc[valid_mask][bad_type]
                details = [
                    f"Feature {idx + 1}: geometry type={row.geometry.geom_type}"
                    for idx, row in bad_rows.iterrows()
                ]
                errors.append(
                    "All geometries must be Polygon or MultiPolygon.\n"
                    + "\n".join(details[:10])
                )
        # Ensure EPSG:4326
        if gdf.crs and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)

        # Repair geometries so they are valid under S2 (used by the R sf
        # package).  Two common issues:
        #   1. Consecutive duplicate vertices → "Edge N is degenerate"
        #   2. Self-intersecting rings → "Edge N crosses edge M"
        # ST_MakeValid in PostGIS fixes (2) but not (1), so we clean both
        # in Shapely before the data reaches PostGIS.
        gdf = _repair_geometries(gdf)

    return gdf, errors


def _normalize_col_name(name):
    return "".join(ch for ch in str(name or "").strip().lower() if ch.isalnum())


def suggest_site_column_mapping(columns):
    """Return best-effort source-column defaults for required site fields."""
    aliases = {
        "site_id": [
            "site_id",
            "siteid",
            "id",
            "site_code",
            "sitecode",
            "ci_id",
            "ciid",
            "objectid",
        ],
        "site_name": [
            "site_name",
            "sitename",
            "name",
            "site",
            "ci_name",
            "ciname",
        ],
        "start_date": [
            "start_date",
            "startdate",
            "start",
            "ci_start_date",
            "cistartdate",
            "intervention_start",
            "interventionstart",
        ],
        "end_date": [
            "end_date",
            "enddate",
            "end",
            "ci_end_date",
            "cienddate",
            "intervention_end",
            "interventionend",
        ],
    }

    by_norm = {_normalize_col_name(col): col for col in columns}
    suggested = {}
    for target, candidate_aliases in aliases.items():
        chosen = None
        for alias in candidate_aliases:
            col = by_norm.get(_normalize_col_name(alias))
            if col:
                chosen = col
                break
        suggested[target] = chosen
    return suggested


def _normalize_site_mapping(column_mapping):
    mapping = {field: None for field in ALL_SITE_FIELDS}
    if not isinstance(column_mapping, dict):
        return mapping

    for field in ALL_SITE_FIELDS:
        value = column_mapping.get(field)
        if value is None:
            continue
        as_str = str(value).strip()
        mapping[field] = as_str or None

    return mapping


def apply_site_column_mapping(gdf, column_mapping):
    """Map and coerce source columns into canonical site upload schema.

    Returns a tuple of (mapped_gdf, errors, warnings).
    """
    mapped = gdf.copy()
    errors = []
    warnings = []

    available_cols = {c for c in mapped.columns if c != "geometry"}
    mapping = _normalize_site_mapping(column_mapping)

    selected_sources = [mapping[field] for field in ALL_SITE_FIELDS if mapping[field]]
    duplicate_sources = sorted(
        {c for c in selected_sources if selected_sources.count(c) > 1}
    )
    if duplicate_sources:
        errors.append(
            "Each required field must use a different source column. "
            f"Duplicate selections: {', '.join(duplicate_sources)}"
        )

    for field in REQUIRED_SITE_FIELDS:
        src = mapping.get(field)
        if not src:
            errors.append(f"Please select a source column for '{field}'.")
        elif src not in available_cols:
            errors.append(f"Selected source column for '{field}' was not found: {src}")

    end_src = mapping.get("end_date")
    if end_src and end_src not in available_cols:
        errors.append(f"Selected source column for 'end_date' was not found: {end_src}")

    if errors:
        return None, errors, warnings

    # Copy source values to canonical field names.
    for field in ALL_SITE_FIELDS:
        src = mapping.get(field)
        if src:
            mapped[field] = mapped[src]
        elif field == "end_date":
            mapped[field] = pd.Series([None] * len(mapped), index=mapped.index)

    mapped["site_id"] = mapped["site_id"].astype("string").str.strip()
    mapped["site_name"] = mapped["site_name"].astype("string").str.strip()

    # Note: Blank site_id and site_name values will be auto-assigned during import
    # (site_id → "MissingID{n}", site_name → "Site_{n}"), so no validation error needed

    start_raw = mapped["start_date"].astype("string").str.strip()
    start_blank = start_raw.isna() | start_raw.eq("")
    start_parsed = pd.to_datetime(mapped["start_date"], errors="coerce")
    start_invalid = (~start_blank) & start_parsed.isna()
    if start_blank.any() or start_invalid.any():
        bad_idx = mapped[start_blank | start_invalid].index[:10]
        feature_nums = [i + 1 for i in bad_idx]
        errors.append(
            "Mapped 'start_date' has missing or unparseable values. "
            f"Affected features (first 10): {feature_nums}"
        )
    mapped["start_date"] = start_parsed.dt.date

    if mapping.get("end_date"):
        end_raw = mapped["end_date"].astype("string").str.strip()
        end_blank = end_raw.isna() | end_raw.eq("")
        end_parsed = pd.to_datetime(mapped["end_date"], errors="coerce")
        end_invalid = (~end_blank) & end_parsed.isna()
        if end_invalid.any():
            n_invalid = int(end_invalid.sum())
            warnings.append(
                "Some 'end_date' values could not be parsed and will be saved as empty. "
                f"Affected rows: {n_invalid}"
            )
        mapped["end_date"] = end_parsed.dt.date
    else:
        mapped["end_date"] = pd.Series([None] * len(mapped), index=mapped.index)

    if errors:
        return None, errors, warnings

    return mapped, errors, warnings


def validate_site_upload_mapping(file_content, filename, column_mapping):
    """Validate a proposed column mapping against uploaded content."""
    gdf, errors = _parse_sites_geometry_file(file_content, filename)
    if errors:
        return {"errors": errors, "warnings": [], "summary": {}}
    mapped_gdf, map_errors, warnings = apply_site_column_mapping(gdf, column_mapping)
    if map_errors:
        return {"errors": map_errors, "warnings": warnings, "summary": {}}

    summary = {
        "n_features": int(len(mapped_gdf)),
        "n_missing_end_date": int(mapped_gdf["end_date"].isna().sum()),
        "n_unique_site_id": int(mapped_gdf["site_id"].nunique(dropna=True)),
    }
    if summary["n_unique_site_id"] < summary["n_features"]:
        warnings.append(
            "Mapped 'site_id' contains duplicates. Duplicates are allowed but may cause confusion in reporting."
        )

    return {
        "errors": [],
        "warnings": warnings,
        "summary": summary,
    }


def _derive_site_set_name(filename):
    stem = os.path.splitext(os.path.basename(filename or "sites"))[0].strip()
    stem = stem or "sites"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{stem}_{timestamp}"


def _site_set_summary_row(row):
    meta = row.extra_metadata if isinstance(row.extra_metadata, dict) else {}
    return {
        "id": str(row.id),
        "name": row.name,
        "filename": row.original_filename,
        "uploaded_at": row.uploaded_at.isoformat() if row.uploaded_at else None,
        "n_sites": row.n_sites or 0,
        "file_size_bytes": int(row.file_size_bytes or 0),
        "file_format": row.file_format,
        "is_archived": bool(row.is_archived),
        "ingest_stats": meta.get("ingest_stats") if meta else None,
    }


def _site_upload_summary_row(row):
    meta = row.extra_metadata if isinstance(row.extra_metadata, dict) else {}
    return {
        "id": str(row.id),
        "filename": row.original_filename,
        "celery_task_id": row.celery_task_id,
        "site_set_id": str(row.site_set_id) if row.site_set_id else None,
        "site_set_name": row.site_set_name,
        "n_features": int(row.n_features or 0),
        "n_sites_imported": int(row.n_sites_imported or 0),
        "status": row.status,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "completed_at": row.completed_at.isoformat() if row.completed_at else None,
        "error_message": row.error_message,
        "ingest_stats": meta.get("ingest_stats"),
    }


def create_user_site_upload(
    user_id, filename, upload_token, column_mapping=None, n_features=0
):
    """Create and dispatch an asynchronous site import job.

    Parameters
    ----------
    user_id : UUID | str
        Owner of the staged upload and resulting site set.
    filename : str
        Original uploaded filename shown in the admin UI.
    upload_token : str
        Token pointing at the staged upload payload.
    column_mapping : dict | None
        Mapping from canonical site fields to source columns.
    n_features : int
        Preview feature count detected before dispatch.

    Returns
    -------
    dict
        Summary row for the queued upload job.
    """
    db = get_db()
    normalized_mapping = _normalize_site_mapping(column_mapping)
    try:
        upload = UserSiteUpload(
            user_id=user_id,
            original_filename=filename or "sites",
            n_features=int(n_features or 0),
            status="pending",
            extra_metadata={
                "upload_token": upload_token,
                "column_mapping": normalized_mapping,
            },
        )
        db.add(upload)
        db.flush()

        async_result = webapp_tasks.import_user_site_upload_task.apply_async(
            kwargs={
                "upload_id": str(upload.id),
                "user_id": str(user_id),
                "upload_token": str(upload_token),
                "column_mapping": normalized_mapping,
            }
        )
        upload.celery_task_id = async_result.id
        db.commit()
        return _site_upload_summary_row(upload)
    except Exception:
        db.rollback()
        try:
            discard_staged_site_upload(upload_token, user_id)
        except Exception:
            logger.warning(
                "Failed to discard staged upload after async dispatch error",
                exc_info=True,
            )
        raise
    finally:
        db.close()


def list_user_site_uploads(user_id, limit=50):
    """Return recent background site import jobs for a user.

    Parameters
    ----------
    user_id : UUID | str
        Owner whose upload jobs should be listed.
    limit : int
        Maximum number of recent jobs to return.

    Returns
    -------
    list[dict]
        Upload summary rows ordered from newest to oldest.
    """
    db = get_db()
    try:
        rows = (
            db.query(UserSiteUpload)
            .filter(UserSiteUpload.user_id == user_id)
            .order_by(UserSiteUpload.created_at.desc())
            .limit(max(1, int(limit or 50)))
            .all()
        )
        return [_site_upload_summary_row(row) for row in rows]
    finally:
        db.close()


def cancel_user_site_upload(upload_id, user_id):
    """Cancel a user-owned background site import job.

    Only uploads in ``pending`` or ``running`` state can be cancelled.
    The associated Celery task is revoked and the upload row is marked
    ``cancelled``.
    """
    db = get_db()
    try:
        upload = (
            db.query(UserSiteUpload)
            .filter(UserSiteUpload.id == upload_id, UserSiteUpload.user_id == user_id)
            .first()
        )
        if not upload:
            return False, "Upload job not found."

        if upload.status in {"completed", "failed", "cancelled"}:
            return False, f"Upload is already {upload.status}."

        if upload.status not in {"pending", "running"}:
            return False, f"Upload cannot be cancelled from status '{upload.status}'."

        if upload.celery_task_id:
            try:
                webapp_tasks.import_user_site_upload_task.AsyncResult(
                    upload.celery_task_id
                ).revoke(terminate=True, signal="SIGTERM")
            except Exception:
                logger.exception(
                    "Failed to revoke upload Celery task %s", upload.celery_task_id
                )
                return False, "Failed to cancel upload task."

        upload.status = "cancelled"
        upload.completed_at = datetime.now(timezone.utc)
        upload.error_message = "Cancelled by admin."
        db.commit()
        return True, "Upload cancelled."
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def update_user_site_upload_status(
    upload_id,
    *,
    status,
    started_at=None,
    completed_at=None,
    site_set_id=None,
    site_set_name=None,
    n_sites_imported=None,
    error_message=None,
    ingest_stats=None,
):
    """Update a background site import job row.

    Parameters
    ----------
    upload_id : UUID | str
        Upload job identifier.
    status : str
        New lifecycle status.
    started_at, completed_at : datetime | None
        Optional timestamps to persist.
    site_set_id : UUID | None
        Persisted site set identifier created by the upload.
    site_set_name : str | None
        Resulting site set name shown in the admin UI.
    n_sites_imported : int | None
        Number of valid sites imported into the database.
    error_message : str | None
        Failure details, truncated before storage.
    ingest_stats : dict | None
        Optional skipped-feature statistics copied from the site set metadata.

    Returns
    -------
    bool
        ``True`` when the row was updated, else ``False`` when it was not found.
    """
    db = get_db()
    try:
        upload = db.query(UserSiteUpload).filter(UserSiteUpload.id == upload_id).first()
        if not upload:
            return False

        if upload.status == "cancelled" and status != "cancelled":
            return False

        upload.status = status
        if started_at is not None:
            upload.started_at = started_at
        if completed_at is not None:
            upload.completed_at = completed_at
        if site_set_id is not None:
            upload.site_set_id = site_set_id
        if site_set_name is not None:
            upload.site_set_name = site_set_name
        if n_sites_imported is not None:
            upload.n_sites_imported = int(n_sites_imported)
        if error_message is not None:
            upload.error_message = error_message[:2000] if error_message else None
        if ingest_stats is not None:
            metadata = dict(upload.extra_metadata or {})
            metadata["ingest_stats"] = ingest_stats
            upload.extra_metadata = metadata

        db.commit()
        return True
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def save_user_site_set(user_id, filename, file_content, column_mapping=None):
    """Persist uploaded sites as a reusable PostGIS-backed user site set.

    Geometries are repaired with ``ST_MakeValid`` and coerced to
    ``MULTIPOLYGON`` before storage.
    """
    gdf, errors = _parse_sites_geometry_file(file_content, filename)
    if errors:
        raise ValueError("\n".join(errors))
    if gdf is None or gdf.empty:
        raise ValueError("No features were found in the uploaded file.")

    mapping_to_apply = _normalize_site_mapping(column_mapping)
    if any(mapping_to_apply.values()):
        gdf, mapping_errors, _mapping_warnings = apply_site_column_mapping(
            gdf, mapping_to_apply
        )
        if mapping_errors:
            raise ValueError("\n".join(mapping_errors))
    else:
        # Backward-compatible path for datasets already using canonical names.
        missing = set(REQUIRED_SITE_FIELDS) - set(gdf.columns)
        if missing:
            raise ValueError("Missing required columns: " + ", ".join(sorted(missing)))

    if not gdf.crs:
        gdf = gdf.set_crs(epsg=4326)
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    db = get_db()
    try:
        site_set = UserSiteSet(
            user_id=user_id,
            name=_derive_site_set_name(filename),
            original_filename=filename,
            file_size_bytes=len(file_content),
            file_format=_get_file_extension(filename).lstrip("."),
            n_sites=len(gdf),
            bounds={"bbox": list(gdf.total_bounds)} if len(gdf) > 0 else None,
            extra_metadata={
                "column_mapping": mapping_to_apply,
            }
            if any(mapping_to_apply.values())
            else None,
        )
        db.add(site_set)
        db.flush()

        insert_sql = text(
            """
            INSERT INTO user_site_features (
                id, site_set_id, site_id, site_name, start_date, end_date, area_ha, geom
            )
            VALUES (
                uuid_generate_v4(),
                :site_set_id,
                :site_id,
                :site_name,
                :start_date,
                :end_date,
                NULL,
                ST_Multi(
                    ST_CollectionExtract(
                        ST_Force2D(
                            ST_MakeValid(
                                ST_SetSRID(ST_GeomFromGeoJSON(:geom_geojson), 4326)
                            )
                        ),
                        3
                    )
                )
            )
            """
        )

        for _, row in gdf.iterrows():
            start_date = pd.to_datetime(row["start_date"]).date()
            end_date = (
                pd.to_datetime(row["end_date"]).date()
                if pd.notna(row.get("end_date")) and str(row.get("end_date"))
                else None
            )

            db.execute(
                insert_sql,
                {
                    "site_set_id": str(site_set.id),
                    "site_id": str(row["site_id"]),
                    "site_name": str(row.get("site_name", "")),
                    "start_date": start_date,
                    "end_date": end_date,
                    "geom_geojson": json.dumps(row.geometry.__geo_interface__),
                },
            )

        db.execute(
            text(
                """
                UPDATE user_site_features
                SET area_ha = ST_Area(geom::geography) / 10000.0
                WHERE site_set_id = :site_set_id
                """
            ),
            {"site_set_id": str(site_set.id)},
        )

        db.commit()
        return get_user_site_set_detail(site_set.id, user_id)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def save_user_site_set_from_staged(
    user_id, upload_token, column_mapping=None, consume=True, upload_id=None
):
    """Persist staged upload without loading full file content into memory.

    When ``upload_id`` is provided, periodically writes import progress back to
    the corresponding ``user_site_uploads`` row so the admin UI can surface it
    while the Celery worker is still processing the file.
    """
    local_path = None
    extracted_archive_dir = None
    try:
        local_path, filename, file_size_bytes = _materialize_staged_upload_path(
            upload_token, user_id
        )
        mapping_to_apply = _normalize_site_mapping(column_mapping)
        source_path = local_path
        source_name = filename
        ext = _get_file_extension(filename)

        if ext in {".zip", ".tar.gz", ".tgz"}:
            extracted_archive_dir = tempfile.mkdtemp(prefix="ae_site_upload_extract_")
            source_path = _extract_site_archive_dataset(
                local_path, filename, extracted_archive_dir
            )
            source_name = os.path.basename(source_path)
            ext = _get_file_extension(source_name)

        # Stream supported datasets directly so Celery only holds a small insert
        # batch in memory at a time, even when the upload arrived as an archive.
        use_streaming_ingest = ext in {".shp", ".gpkg", ".geojson", ".json"}

        db = get_db()
        try:
            site_set = UserSiteSet(
                user_id=user_id,
                name=_derive_site_set_name(filename),
                original_filename=filename,
                file_size_bytes=int(file_size_bytes or 0),
                file_format=_get_file_extension(filename).lstrip("."),
                n_sites=0,
                bounds=None,
                extra_metadata={"column_mapping": mapping_to_apply}
                if any(mapping_to_apply.values())
                else None,
            )
            db.add(site_set)
            db.flush()

            insert_sql = text(
                """
                INSERT INTO user_site_features (
                    id, site_set_id, site_id, site_name, start_date, end_date, area_ha, geom
                )
                VALUES (
                    uuid_generate_v4(),
                    :site_set_id,
                    :site_id,
                    :site_name,
                    :start_date,
                    :end_date,
                    NULL,
                    ST_Multi(
                        ST_CollectionExtract(
                            ST_Force2D(
                                ST_MakeValid(
                                    ST_SetSRID(ST_GeomFromGeoJSON(:geom_geojson), 4326)
                                )
                            ),
                            3
                        )
                    )
                )
                """
            )

            n_sites = 0
            bounds_minx = None
            bounds_miny = None
            bounds_maxx = None
            bounds_maxy = None
            skipped_missing_required = 0
            skipped_bad_start_date = 0
            skipped_bad_geometry = 0
            skipped_examples = []

            batch_rows = []
            batch_row_bytes = 0
            last_progress_sites = 0
            last_progress_at = monotonic()

            def _report_upload_progress(force=False):
                nonlocal last_progress_sites, last_progress_at
                if upload_id is None or n_sites <= 0:
                    return
                now = monotonic()
                if not force and (
                    n_sites - last_progress_sites < SITE_UPLOAD_PROGRESS_BATCH_SIZE
                    and now - last_progress_at < SITE_UPLOAD_PROGRESS_INTERVAL_SECONDS
                ):
                    return
                skipped_total = (
                    skipped_missing_required
                    + skipped_bad_start_date
                    + skipped_bad_geometry
                )
                ingest_stats = (
                    {
                        "skipped_total": int(skipped_total),
                        "skipped_missing_required": int(skipped_missing_required),
                        "skipped_bad_start_date": int(skipped_bad_start_date),
                        "skipped_bad_geometry": int(skipped_bad_geometry),
                    }
                    if skipped_total > 0
                    else None
                )
                update_user_site_upload_status(
                    upload_id,
                    status="running",
                    n_sites_imported=n_sites,
                    ingest_stats=ingest_stats,
                )
                last_progress_sites = n_sites
                last_progress_at = now

            def _maybe_report_upload_progress():
                if n_sites % SITE_UPLOAD_PROGRESS_CHECK_ROW_INTERVAL == 0:
                    _report_upload_progress()

            def _flush_batch_rows():
                nonlocal batch_row_bytes
                if not batch_rows:
                    return
                db.execute(insert_sql, batch_rows)
                batch_rows.clear()
                batch_row_bytes = 0
                _report_upload_progress()

            if use_streaming_ingest:
                from pyproj import Transformer  # noqa: PLC0415
                from shapely.geometry import shape  # noqa: PLC0415
                from shapely.ops import transform as shapely_transform  # noqa: PLC0415

                with _open_site_feature_source(source_path) as src:
                    source_columns = list(
                        (src.schema or {}).get("properties", {}).keys()
                    )

                    # Validate mapping against available columns once.
                    selected_sources = [
                        mapping_to_apply[field]
                        for field in ALL_SITE_FIELDS
                        if mapping_to_apply.get(field)
                    ]
                    duplicate_sources = sorted(
                        {c for c in selected_sources if selected_sources.count(c) > 1}
                    )
                    if duplicate_sources:
                        raise ValueError(
                            "Each required field must use a different source column. "
                            f"Duplicate selections: {', '.join(duplicate_sources)}"
                        )

                    for field in REQUIRED_SITE_FIELDS:
                        src_col = mapping_to_apply.get(field)
                        if not src_col:
                            raise ValueError(
                                f"Please select a source column for '{field}'."
                            )
                        if src_col not in source_columns:
                            raise ValueError(
                                f"Selected source column for '{field}' was not found: {src_col}"
                            )

                    end_src = mapping_to_apply.get("end_date")
                    if end_src and end_src not in source_columns:
                        raise ValueError(
                            f"Selected source column for 'end_date' was not found: {end_src}"
                        )

                    to_wgs84 = None
                    try:
                        if src.crs and src.crs.to_epsg() != 4326:
                            to_wgs84 = Transformer.from_crs(
                                src.crs, "EPSG:4326", always_xy=True
                            ).transform
                    except Exception:
                        # If CRS is unavailable/unparseable, trust source geometry.
                        to_wgs84 = None

                    # Counters for auto-assigned IDs
                    missing_site_id_count = 0
                    missing_site_name_count = 0

                    for feature_index, feature in enumerate(src, start=1):
                        props = feature.get("properties") or {}
                        raw_site_id = props.get(mapping_to_apply["site_id"])
                        raw_site_name = props.get(mapping_to_apply["site_name"])
                        raw_start = props.get(mapping_to_apply["start_date"])
                        raw_end = (
                            props.get(mapping_to_apply["end_date"])
                            if mapping_to_apply.get("end_date")
                            else None
                        )

                        site_id = (
                            str(raw_site_id).strip() if raw_site_id is not None else ""
                        )
                        site_name = (
                            str(raw_site_name).strip()
                            if raw_site_name is not None
                            else ""
                        )

                        # Auto-assign missing site_id
                        if not site_id:
                            missing_site_id_count += 1
                            site_id = f"MissingID{missing_site_id_count}"
                            if len(skipped_examples) < 10:
                                skipped_examples.append(
                                    f"Feature {feature_index}: auto-assigned site_id to '{site_id}'."
                                )

                        # Auto-assign missing site_name
                        if not site_name:
                            missing_site_name_count += 1
                            site_name = f"Missing_Site_Name_{missing_site_name_count}"
                            if len(skipped_examples) < 10:
                                skipped_examples.append(
                                    f"Feature {feature_index}: auto-assigned site_name to '{site_name}'."
                                )

                        # Only check for missing start_date (required for analysis)
                        if raw_start is None:
                            skipped_missing_required += 1
                            if len(skipped_examples) < 10:
                                skipped_examples.append(
                                    f"Feature {feature_index}: mapped 'start_date' is missing."
                                )
                            continue

                        start_dt = pd.to_datetime(raw_start, errors="coerce")
                        if pd.isna(start_dt):
                            skipped_bad_start_date += 1
                            if len(skipped_examples) < 10:
                                skipped_examples.append(
                                    "Feature "
                                    f"{feature_index}: mapped 'start_date' is missing or unparseable."
                                )
                            continue

                        start_date = start_dt.date()

                        if raw_end is None or str(raw_end).strip() == "":
                            end_date = None
                        else:
                            end_dt = pd.to_datetime(raw_end, errors="coerce")
                            end_date = None if pd.isna(end_dt) else end_dt.date()

                        geom_json = feature.get("geometry")
                        if not geom_json:
                            skipped_bad_geometry += 1
                            if len(skipped_examples) < 10:
                                skipped_examples.append(
                                    f"Feature {feature_index}: missing geometry."
                                )
                            continue

                        geom = shape(geom_json)
                        if to_wgs84 is not None:
                            geom = shapely_transform(to_wgs84, geom)
                        geom = _repair_geometry_single(geom)

                        if geom.is_empty:
                            skipped_bad_geometry += 1
                            if len(skipped_examples) < 10:
                                skipped_examples.append(
                                    f"Feature {feature_index}: empty geometry after repair."
                                )
                            continue

                        if geom.geom_type not in ("Polygon", "MultiPolygon"):
                            skipped_bad_geometry += 1
                            if len(skipped_examples) < 10:
                                skipped_examples.append(
                                    "Feature "
                                    f"{feature_index}: unsupported geometry type {geom.geom_type}."
                                )
                            continue

                        minx, miny, maxx, maxy = geom.bounds
                        bounds_minx = (
                            minx if bounds_minx is None else min(bounds_minx, minx)
                        )
                        bounds_miny = (
                            miny if bounds_miny is None else min(bounds_miny, miny)
                        )
                        bounds_maxx = (
                            maxx if bounds_maxx is None else max(bounds_maxx, maxx)
                        )
                        bounds_maxy = (
                            maxy if bounds_maxy is None else max(bounds_maxy, maxy)
                        )

                        batch_row = {
                            "site_set_id": str(site_set.id),
                            "site_id": site_id,
                            "site_name": site_name,
                            "start_date": start_date,
                            "end_date": end_date,
                            "geom_geojson": json.dumps(geom.__geo_interface__),
                        }
                        batch_rows.append(batch_row)
                        n_sites += 1
                        _maybe_report_upload_progress()
                        batch_row_bytes += len(
                            json.dumps(batch_row, default=str).encode("utf-8")
                        )
                        if (
                            len(batch_rows) >= SITE_UPLOAD_INSERT_BATCH_SIZE
                            or batch_row_bytes >= SITE_UPLOAD_INSERT_BATCH_MAX_BYTES
                        ):
                            _flush_batch_rows()
            else:
                gdf, errors = _parse_sites_geometry_file_from_path(local_path, filename)
                if errors:
                    raise ValueError("\n".join(errors))
                if gdf is None or gdf.empty:
                    raise ValueError("No features were found in the uploaded file.")

                if any(mapping_to_apply.values()):
                    gdf, mapping_errors, _mapping_warnings = apply_site_column_mapping(
                        gdf, mapping_to_apply
                    )
                    if mapping_errors:
                        raise ValueError("\n".join(mapping_errors))
                else:
                    missing = set(REQUIRED_SITE_FIELDS) - set(gdf.columns)
                    if missing:
                        raise ValueError(
                            "Missing required columns: " + ", ".join(sorted(missing))
                        )

                if not gdf.crs:
                    gdf = gdf.set_crs(epsg=4326)
                elif gdf.crs.to_epsg() != 4326:
                    gdf = gdf.to_crs(epsg=4326)

                if len(gdf) > 0:
                    minx, miny, maxx, maxy = gdf.total_bounds
                    bounds_minx, bounds_miny, bounds_maxx, bounds_maxy = (
                        minx,
                        miny,
                        maxx,
                        maxy,
                    )

                for _, row in gdf.iterrows():
                    start_date = pd.to_datetime(row["start_date"]).date()
                    end_date = (
                        pd.to_datetime(row["end_date"]).date()
                        if pd.notna(row.get("end_date")) and str(row.get("end_date"))
                        else None
                    )

                    batch_row = {
                        "site_set_id": str(site_set.id),
                        "site_id": str(row["site_id"]),
                        "site_name": str(row.get("site_name", "")),
                        "start_date": start_date,
                        "end_date": end_date,
                        "geom_geojson": json.dumps(row.geometry.__geo_interface__),
                    }
                    batch_rows.append(batch_row)
                    n_sites += 1
                    _maybe_report_upload_progress()
                    batch_row_bytes += len(
                        json.dumps(batch_row, default=str).encode("utf-8")
                    )
                    if (
                        len(batch_rows) >= SITE_UPLOAD_INSERT_BATCH_SIZE
                        or batch_row_bytes >= SITE_UPLOAD_INSERT_BATCH_MAX_BYTES
                    ):
                        _flush_batch_rows()

            _flush_batch_rows()
            _report_upload_progress(force=True)

            if n_sites == 0:
                skipped_total = (
                    skipped_missing_required
                    + skipped_bad_start_date
                    + skipped_bad_geometry
                )
                if skipped_total > 0:
                    details = (
                        "No valid features were found after applying the selected mapping. "
                        f"Skipped checks: missing required={skipped_missing_required}, "
                        f"bad start_date={skipped_bad_start_date}, "
                        f"bad geometry={skipped_bad_geometry}."
                    )
                    if skipped_examples:
                        details += " Examples: " + " ".join(skipped_examples)
                    raise ValueError(details)
                raise ValueError("No features were found in the uploaded file.")

            skipped_total = (
                skipped_missing_required + skipped_bad_start_date + skipped_bad_geometry
            )
            if skipped_total > 0:
                logger.warning(
                    "Site upload saved with skipped features: site_set_id=%s skipped_total=%s "
                    "missing_required=%s bad_start_date=%s bad_geometry=%s",
                    site_set.id,
                    skipped_total,
                    skipped_missing_required,
                    skipped_bad_start_date,
                    skipped_bad_geometry,
                )
                metadata = dict(site_set.extra_metadata or {})
                metadata["ingest_stats"] = {
                    "skipped_total": int(skipped_total),
                    "skipped_missing_required": int(skipped_missing_required),
                    "skipped_bad_start_date": int(skipped_bad_start_date),
                    "skipped_bad_geometry": int(skipped_bad_geometry),
                }
                if skipped_examples:
                    metadata["ingest_skip_examples"] = skipped_examples
                site_set.extra_metadata = metadata

            site_set.n_sites = n_sites
            if bounds_minx is not None:
                site_set.bounds = {
                    "bbox": [bounds_minx, bounds_miny, bounds_maxx, bounds_maxy]
                }

            db.execute(
                text(
                    """
                    UPDATE user_site_features
                    SET area_ha = ST_Area(geom::geography) / 10000.0
                    WHERE site_set_id = :site_set_id
                    """
                ),
                {"site_set_id": str(site_set.id)},
            )

            db.commit()
            return get_user_site_set_detail(site_set.id, user_id)
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()
    finally:
        if consume:
            try:
                discard_staged_site_upload(upload_token, user_id)
            except Exception:
                logger.warning(
                    "Failed to discard staged upload after save", exc_info=True
                )

        if (
            local_path
            and _use_shared_site_upload_stage()
            and os.path.exists(local_path)
        ):
            try:
                os.unlink(local_path)
            except OSError:
                logger.debug("Failed to remove temporary staged file: %s", local_path)

        if extracted_archive_dir:
            shutil.rmtree(extracted_archive_dir, ignore_errors=True)


def list_user_site_sets(user_id, include_archived=False):
    """Return reusable site sets for a user ordered by most recent first."""
    db = get_db()
    try:
        q = db.query(UserSiteSet).filter(UserSiteSet.user_id == user_id)
        if not include_archived:
            q = q.filter(UserSiteSet.is_archived.is_(False))
        site_sets = q.order_by(UserSiteSet.uploaded_at.desc()).all()
        return [_site_set_summary_row(row) for row in site_sets]
    finally:
        db.close()


def get_user_site_set_geojson(site_set_id):
    """Export a user site set from PostGIS as a GeoJSON FeatureCollection.

    Rows are fetched individually and assembled into the FeatureCollection on
    the Python side rather than using jsonb_agg() inside PostgreSQL.  The
    aggregation approach builds the entire JSON blob in database working memory
    (work_mem), which causes PostgreSQL backends to be killed by the OOM killer
    when site sets are large.
    """
    db = get_db()
    try:
        rows = db.execute(
            text(
                """
                SELECT
                    f.site_id,
                    f.site_name,
                    to_char(f.start_date, 'YYYY-MM-DD') AS start_date,
                    CASE
                        WHEN f.end_date IS NULL THEN NULL
                        ELSE to_char(f.end_date, 'YYYY-MM-DD')
                    END AS end_date,
                    f.area_ha,
                    ST_AsGeoJSON(f.geom) AS geom_json
                FROM user_site_features f
                WHERE f.site_set_id = :site_set_id
                ORDER BY f.site_id
                """
            ),
            {"site_set_id": str(site_set_id)},
        ).fetchall()
        # fetchall() retrieves all rows in a single round trip; PostgreSQL streams
        # the result set and Python collects it here.  JSON assembly happens on the
        # Python side so the database never has to hold the full FeatureCollection
        # as a single JSONB value (which would exhaust work_mem on large site sets).
        features = [
            {
                "type": "Feature",
                "geometry": json.loads(r.geom_json),
                "properties": {
                    "site_id": r.site_id,
                    "site_name": r.site_name,
                    "start_date": r.start_date,
                    "end_date": r.end_date,
                    "area_ha": r.area_ha,
                },
            }
            for r in rows
            if r.geom_json is not None
        ]
        return {"type": "FeatureCollection", "features": features}
    finally:
        db.close()


def _get_user_site_set_geojson_simplified(
    site_set_id, max_features=None, simplify_tolerance=0.01
):
    """Load simplified GeoJSON for large datasets, either by sampling or simplification.

    For datasets with more than max_features (default 5000), returns either:
    - A sample of evenly-spaced features if available
    - All features with simplified geometries to reduce payload size

    This prevents 504 Gateway Timeouts when loading very large site sets (>100k features).

    Parameters
    ----------
    site_set_id : UUID | str
        Site set to load
    max_features : int, optional
        If set, limit visualization to this many features (samples or simplified)
    simplify_tolerance : float, optional
        Simplification tolerance for geometries (degrees, ~111km per degree at equator)

    Returns
    -------
    dict
        GeoJSON FeatureCollection with sampled/simplified features
    """
    if max_features is None:
        max_features = 5000

    db = get_db()
    try:
        # First, get the total count
        count_result = db.execute(
            text(
                "SELECT COUNT(*) as cnt FROM user_site_features WHERE site_set_id = :site_set_id"
            ),
            {"site_set_id": str(site_set_id)},
        ).fetchone()
        total_count = count_result.cnt if count_result else 0

        if total_count <= max_features:
            # Dataset is small enough, load full fidelity
            return get_user_site_set_geojson(site_set_id)

        # Dataset is large - use sampling to show representative subset
        logger.info(
            f"Large site set detected: {total_count} features (max_features={max_features}). "
            "Loading simplified preview."
        )

        # Sample every nth feature to get approximately max_features
        sample_interval = max(1, total_count // max_features)

        rows = db.execute(
            text(
                """
                WITH ranked_features AS (
                    SELECT
                        ROW_NUMBER() OVER (ORDER BY site_id) as row_num,
                        site_id,
                        site_name,
                        to_char(start_date, 'YYYY-MM-DD') AS start_date,
                        CASE WHEN end_date IS NULL THEN NULL
                             ELSE to_char(end_date, 'YYYY-MM-DD')
                        END AS end_date,
                        area_ha,
                        ST_AsGeoJSON(ST_SimplifyPreserveTopology(geom, :simplify_tolerance)) AS geom_json
                    FROM user_site_features
                    WHERE site_set_id = :site_set_id
                )
                SELECT * FROM ranked_features WHERE row_num % :sample_interval = 1
                ORDER BY site_id
                """
            ),
            {
                "site_set_id": str(site_set_id),
                "simplify_tolerance": simplify_tolerance,
                "sample_interval": sample_interval,
            },
        ).fetchall()

        features = [
            {
                "type": "Feature",
                "geometry": json.loads(r.geom_json),
                "properties": {
                    "site_id": r.site_id,
                    "site_name": r.site_name,
                    "start_date": r.start_date,
                    "end_date": r.end_date,
                    "area_ha": r.area_ha,
                    "_is_sample": True,  # Mark as sampled
                },
            }
            for r in rows
            if r.geom_json is not None
        ]

        result = {"type": "FeatureCollection", "features": features}
        logger.info(f"Loaded {len(features)} sampled features (from {total_count})")
        return result
    finally:
        db.close()


def get_user_site_set_geojson_by_bounds_and_zoom(
    site_set_id, zoom, bounds_minx, bounds_miny, bounds_maxx, bounds_maxy
):
    """Load GeoJSON sampled adaptively based on zoom level and map bounds.

    Implements progressive refinement: as user zooms in, more detailed data is loaded
    within the current map bounds. This prevents rendering thousands of features at
    low zoom while still providing full detail when zoomed in.

    Sampling strategy:
    - Zoom 0-8 (world/continent): Heavy sampling (1 in 50)
    - Zoom 8-14 (region/province): Medium sampling (1 in 5)
    - Zoom 14+ (detailed): Minimal/no sampling, full geometry detail

    Parameters
    ----------
    site_set_id : UUID | str
        Site set to load
    zoom : int
        Current map zoom level (0-28)
    bounds_minx, bounds_miny, bounds_maxx, bounds_maxy : float
        Map bounds in EPSG:4326 (lon/lat)

    Returns
    -------
    dict
        GeoJSON FeatureCollection with zoom-adaptive sampled features
    """
    db = get_db()
    try:
        # Determine sampling rate and simplification based on zoom level
        if zoom <= 8:
            # Low zoom (world/continent view): heavy sampling, heavy simplification
            sample_interval = 50
            simplify_tolerance = 0.1  # ~11 km
        elif zoom <= 12:
            # Medium zoom (region view): moderate sampling, moderate simplification
            sample_interval = 5
            simplify_tolerance = 0.02  # ~2 km
        elif zoom <= 15:
            # High zoom (province/city view): light sampling, minimal simplification
            sample_interval = 2
            simplify_tolerance = 0.005  # ~500 m
        else:
            # Very high zoom (street level): no sampling, minimal simplification
            sample_interval = 1
            simplify_tolerance = 0.001  # ~100 m

        # Create a bounding box geometry in PostGIS
        bbox_wkt = f"POLYGON(({bounds_minx} {bounds_miny}, {bounds_maxx} {bounds_miny}, {bounds_maxx} {bounds_maxy}, {bounds_minx} {bounds_maxy}, {bounds_minx} {bounds_miny}))"

        rows = db.execute(
            text(
                """
                WITH ranked_features AS (
                    SELECT
                        ROW_NUMBER() OVER (ORDER BY site_id) as row_num,
                        site_id,
                        site_name,
                        to_char(start_date, 'YYYY-MM-DD') AS start_date,
                        CASE WHEN end_date IS NULL THEN NULL
                             ELSE to_char(end_date, 'YYYY-MM-DD')
                        END AS end_date,
                        area_ha,
                        ST_AsGeoJSON(ST_SimplifyPreserveTopology(geom, :simplify_tolerance)) AS geom_json
                    FROM user_site_features
                    WHERE site_set_id = :site_set_id
                      AND ST_Intersects(geom, ST_GeomFromText(:bbox_wkt, 4326))
                )
                SELECT * FROM ranked_features WHERE row_num % :sample_interval = 1
                ORDER BY site_id
                """
            ),
            {
                "site_set_id": str(site_set_id),
                "bbox_wkt": bbox_wkt,
                "simplify_tolerance": simplify_tolerance,
                "sample_interval": sample_interval,
            },
        ).fetchall()

        features = [
            {
                "type": "Feature",
                "geometry": json.loads(r.geom_json),
                "properties": {
                    "site_id": r.site_id,
                    "site_name": r.site_name,
                    "start_date": r.start_date,
                    "end_date": r.end_date,
                    "area_ha": r.area_ha,
                    "_is_sample": sample_interval
                    > 1,  # Mark as sampled if sampling applied
                    "_zoom_level": zoom,
                },
            }
            for r in rows
            if r.geom_json is not None
        ]

        result = {"type": "FeatureCollection", "features": features}
        logger.info(
            f"Loaded {len(features)} features at zoom {zoom} within bounds "
            f"(sampling 1:{sample_interval}, simplification {simplify_tolerance})"
        )
        return result
    finally:
        db.close()


def get_user_site_set_detail(site_set_id, user_id):
    """Return full details for one user-owned site set, including preview rows.

    For large datasets (>5000 features), returns a simplified/sampled GeoJSON
    preview to prevent 504 Gateway Timeouts. The full dataset can still be
    used for task submission via get_user_site_set_gdf().
    """
    db = get_db()
    try:
        site_set = (
            db.query(UserSiteSet)
            .filter(UserSiteSet.id == site_set_id, UserSiteSet.user_id == user_id)
            .first()
        )
        if not site_set:
            return None

        rows = db.execute(
            text(
                """
                SELECT site_id, site_name, start_date, end_date
                FROM user_site_features
                WHERE site_set_id = :site_set_id
                ORDER BY site_id
                """
            ),
            {"site_set_id": str(site_set_id)},
        ).fetchall()

        preview_rows = [
            {
                "preview_row_id": f"{idx}:{r.site_id}:{r.start_date.isoformat() if r.start_date else ''}:{r.end_date.isoformat() if r.end_date else ''}",
                "site_id": r.site_id,
                "site_name": r.site_name,
                "start_date": r.start_date.isoformat() if r.start_date else "",
                "end_date": r.end_date.isoformat() if r.end_date else "",
            }
            for idx, r in enumerate(rows)
        ]

        # Use simplified GeoJSON for map preview to avoid timeout on large datasets
        geojson_fc = _get_user_site_set_geojson_simplified(site_set_id)

        return {
            **_site_set_summary_row(site_set),
            "geojson": json.dumps(geojson_fc),
            "preview_rows": preview_rows,
        }
    finally:
        db.close()


def _get_site_set_min_start_year(site_set_id, db):
    """Return the earliest start_date year across all features in a site set.

    Used by the submit worker to compute ``fc_years`` without loading a full
    GeoDataFrame.  Returns ``None`` when the site set has no features.
    """
    row = db.execute(
        text(
            "SELECT EXTRACT(YEAR FROM MIN(start_date))::int AS min_year "
            "FROM user_site_features "
            "WHERE site_set_id = :site_set_id"
        ),
        {"site_set_id": str(site_set_id)},
    ).fetchone()
    return row.min_year if row and row.min_year is not None else None


def _stream_site_set_to_parquet_buf(site_set_id, db, batch_size=500):
    """Stream site geometries from PostGIS to an in-memory GeoParquet buffer.

    Uses server-side cursor iteration (``stream_results=True``) so that
    only *batch_size* rows are decoded at once in Python.  Each batch is
    converted to a tiny GeoDataFrame, serialised to an Arrow table via
    ``GeoDataFrame.to_arrow()``, and appended to a ``ParquetWriter``.
    The batch GeoDataFrame is released after each write, keeping peak
    Python memory at O(batch_size) geometry rows + O(n_sites) Parquet
    buffer — substantially less than the old O(n_sites) GeoDataFrame +
    O(n_sites) Parquet approach.

    Returns
    -------
    buf : io.BytesIO
        Seeked-to-0 in-memory GeoParquet file.  Callers that need a
        GeoDataFrame afterwards can do ``gpd.read_parquet(buf)`` then
        ``buf.seek(0)`` again for the S3 upload.
    row_count : int
        Total number of site features written.
    """
    import pyarrow.parquet as pq

    buf = io.BytesIO()
    writer = None
    total_rows = 0

    result = db.execute(
        text(
            """
            SELECT
                f.site_id,
                f.site_name,
                to_char(f.start_date, 'YYYY-MM-DD') AS start_date,
                CASE
                    WHEN f.end_date IS NULL THEN NULL
                    ELSE to_char(f.end_date, 'YYYY-MM-DD')
                END AS end_date,
                f.area_ha,
                ST_AsBinary(f.geom) AS geom_wkb
            FROM user_site_features f
            WHERE f.site_set_id = :site_set_id
            ORDER BY f.site_id
            """
        ),
        {"site_set_id": str(site_set_id)},
    ).execution_options(stream_results=True, max_row_buffer=batch_size)

    batch = []
    for row in result:
        if row.geom_wkb is None:
            continue
        batch.append(row)
        if len(batch) >= batch_size:
            records = [
                {
                    "site_id": r.site_id,
                    "site_name": r.site_name,
                    "start_date": r.start_date,
                    "end_date": r.end_date,
                    "area_ha": r.area_ha,
                    "geometry": wkb.loads(bytes(r.geom_wkb)),
                }
                for r in batch
            ]
            gdf_batch = gpd.GeoDataFrame(records, crs="EPSG:4326", geometry="geometry")
            gdf_batch["start_date"] = pd.to_datetime(gdf_batch["start_date"])
            gdf_batch["end_date"] = pd.to_datetime(gdf_batch["end_date"])
            arrow_batch = gdf_batch.to_arrow()
            if writer is None:
                writer = pq.ParquetWriter(buf, arrow_batch.schema)
            writer.write_table(arrow_batch)
            total_rows += len(batch)
            batch = []  # release batch GDF and rows

    if batch:
        records = [
            {
                "site_id": r.site_id,
                "site_name": r.site_name,
                "start_date": r.start_date,
                "end_date": r.end_date,
                "area_ha": r.area_ha,
                "geometry": wkb.loads(bytes(r.geom_wkb)),
            }
            for r in batch
        ]
        gdf_batch = gpd.GeoDataFrame(records, crs="EPSG:4326", geometry="geometry")
        gdf_batch["start_date"] = pd.to_datetime(gdf_batch["start_date"])
        gdf_batch["end_date"] = pd.to_datetime(gdf_batch["end_date"])
        arrow_batch = gdf_batch.to_arrow()
        if writer is None:
            writer = pq.ParquetWriter(buf, arrow_batch.schema)
        writer.write_table(arrow_batch)
        total_rows += len(batch)

    if writer:
        writer.close()

    if total_rows == 0:
        raise ValueError(
            f"Site set {site_set_id} has no valid geometries — cannot stream to Parquet."
        )

    buf.seek(0)
    return buf, total_rows


def get_user_site_set_gdf(site_set_id, user_id=None):
    """Load one site set as a GeoDataFrame."""
    db = get_db()
    try:
        if user_id is not None:
            exists = (
                db.query(UserSiteSet)
                .filter(UserSiteSet.id == site_set_id, UserSiteSet.user_id == user_id)
                .first()
            )
            if not exists:
                raise ValueError("Site set not found.")

        rows = db.execute(
            text(
                """
                SELECT
                    f.site_id,
                    f.site_name,
                    to_char(f.start_date, 'YYYY-MM-DD') AS start_date,
                    CASE
                        WHEN f.end_date IS NULL THEN NULL
                        ELSE to_char(f.end_date, 'YYYY-MM-DD')
                    END AS end_date,
                    f.area_ha,
                    ST_AsBinary(f.geom) AS geom_wkb
                FROM user_site_features f
                WHERE f.site_set_id = :site_set_id
                ORDER BY f.site_id
                """
            ),
            {"site_set_id": str(site_set_id)},
        ).fetchall()

        records = [
            {
                "site_id": row.site_id,
                "site_name": row.site_name,
                "start_date": row.start_date,
                "end_date": row.end_date,
                "area_ha": row.area_ha,
                "geometry": wkb.loads(bytes(row.geom_wkb)),
            }
            for row in rows
            if row.geom_wkb is not None
        ]
        gdf = gpd.GeoDataFrame(records, crs="EPSG:4326", geometry="geometry")
        if gdf.empty:
            raise ValueError("Selected site set has no site geometries.")
        gdf["start_date"] = pd.to_datetime(gdf["start_date"])
        gdf["end_date"] = pd.to_datetime(gdf["end_date"])
        return gdf
    finally:
        db.close()


def delete_user_site_set(site_set_id, user_id):
    """Delete a user-owned site set that is not referenced by any task."""
    db = get_db()
    try:
        site_set = (
            db.query(UserSiteSet)
            .filter(UserSiteSet.id == site_set_id, UserSiteSet.user_id == user_id)
            .first()
        )
        if not site_set:
            return False, "Site set not found."

        task_count = (
            db.query(AnalysisTask)
            .filter(AnalysisTask.site_set_id == site_set_id)
            .count()
        )
        if task_count > 0:
            return (
                False,
                "This site set is linked to submitted tasks and cannot be deleted."
                " Use Archive to hide it instead.",
            )

        db.delete(site_set)
        db.commit()
        return True, "Site set deleted."
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def rename_user_site_set(site_set_id, user_id, new_name):
    """Rename a user-owned site set.

    Parameters
    ----------
    site_set_id : UUID | str
        Site set to rename.
    user_id : UUID | str
        Owner of the site set.
    new_name : str
        Requested replacement name.

    Returns
    -------
    tuple[bool, str]
        Success flag and user-facing status message.
    """
    cleaned_name = (new_name or "").strip()
    if not cleaned_name:
        return False, "Enter a name for the site set."
    if len(cleaned_name) > 255:
        return False, "Site set names must be 255 characters or fewer."

    db = get_db()
    try:
        site_set = (
            db.query(UserSiteSet)
            .filter(UserSiteSet.id == site_set_id, UserSiteSet.user_id == user_id)
            .first()
        )
        if not site_set:
            return False, "Site set not found."

        site_set.name = cleaned_name
        db.commit()
        return True, "Site set renamed."
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def archive_user_site_set(site_set_id, user_id, archive=None):
    """Archive (or unarchive) a user-owned site set.

    When *archive* is ``None`` (default), the current state is toggled.
    Archived site sets are hidden from the dropdown but remain in the
    database for tasks that reference them.
    """
    db = get_db()
    try:
        site_set = (
            db.query(UserSiteSet)
            .filter(UserSiteSet.id == site_set_id, UserSiteSet.user_id == user_id)
            .first()
        )
        if not site_set:
            return False, "Site set not found."

        if archive is None:
            archive = not site_set.is_archived
        site_set.is_archived = archive
        db.commit()
        action = "archived" if archive else "restored"
        return True, f"Site set {action}."
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def upload_user_site_set_geojson_to_s3(site_set_id, task_id):
    """Export a persisted site set to GeoJSON (via PostGIS) and upload to S3."""
    site_fc = get_user_site_set_geojson(site_set_id)
    s3 = get_s3_client()
    key = f"{Config.S3_PREFIX}/tasks/{task_id}/sites.geojson"
    body = json.dumps(site_fc)
    s3.put_object(
        Bucket=Config.S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
        Tagging=S3_COST_TAGGING,
    )
    return f"s3://{Config.S3_BUCKET}/{key}"


def upload_sites_parquet_to_s3(gdf, task_id):
    """Upload a GeoDataFrame as GeoParquet to S3."""
    s3 = get_s3_client()
    key = f"{Config.S3_PREFIX}/tasks/{task_id}/sites.parquet"
    buf = io.BytesIO()
    gdf.to_parquet(buf, index=False)
    buf.seek(0)
    s3.put_object(
        Bucket=Config.S3_BUCKET,
        Key=key,
        Body=buf,
        ContentType="application/vnd.apache.parquet",
        Tagging=S3_COST_TAGGING,
    )
    return f"s3://{Config.S3_BUCKET}/{key}"


def upload_sites_to_geojson(gdf):
    """Serialize a GeoDataFrame to GeoJSON text without mutating it."""
    gdf_json = gdf.copy()
    for col in gdf_json.columns:
        if pd.api.types.is_datetime64_any_dtype(gdf_json[col]):
            gdf_json[col] = gdf_json[col].dt.strftime("%Y-%m-%d")
        elif gdf_json[col].apply(lambda v: isinstance(v, pd.Timestamp)).any():
            gdf_json[col] = gdf_json[col].apply(
                lambda v: v.strftime("%Y-%m-%d") if isinstance(v, pd.Timestamp) else v
            )
    return gdf_json.to_json()


def upload_sites_to_s3(gdf, task_id):
    """Upload a GeoDataFrame as GeoJSON to S3.

    Returns the S3 URI of the uploaded file.
    """
    s3 = get_s3_client()
    key = f"{Config.S3_PREFIX}/tasks/{task_id}/sites.geojson"
    body = upload_sites_to_geojson(gdf)
    s3.put_object(
        Bucket=Config.S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
        Tagging=S3_COST_TAGGING,
    )
    return f"s3://{Config.S3_BUCKET}/{key}"


# ---------------------------------------------------------------------------
# PostGIS matching-extent computation
# ---------------------------------------------------------------------------

# Maps exact-match variable names to their PostGIS table.  Variables
# not present here (e.g. ``pa``, which is binary) are skipped when
# computing the spatial extent because they don't constrain the search
# area to discrete polygon regions.
_EXTENT_TABLE_MAP: dict[str, str] = {
    "admin0": "geoboundaries_adm0",
    "admin1": "geoboundaries_adm1",
    "admin2": "geoboundaries_adm2",
    "ecoregion": "ecoregions",
}


_SAFE_TABLE_RE = __import__("re").compile(r"^[a-z_][a-z0-9_]*$")


def compute_matching_extent(
    gdf: gpd.GeoDataFrame | None,
    exact_match_vars: list[str],
    site_set_id: str | None = None,
) -> dict | None:
    """Compute the spatial extent for control-pixel selection.

    For each polygon-type exact-match variable the function queries
    PostGIS to find every polygon that intersects any of the treatment
    *sites*.  The per-layer polygons are collected, then all layers are
    intersected together.  The resulting geometry is the tightest
    bounding area in which a pixel could share the same exact-match
    attribute values as at least one treatment site.

    Binary variables (``pa``) do not contribute to the extent because
    they partition all of space into only two classes and therefore
    provide no spatial restriction.

    Performance notes
    -----------------
    * ``ST_MakeValid`` is **not** applied to stored geometries because
      the vector import pipeline (``import_vector_data._make_valid``)
      already validates every geometry before writing to PostGIS.
    * ``ST_Collect`` + ``ST_Buffer(geom, 0)`` is used instead of the
      much slower ``ST_Union`` aggregate.  ``ST_Collect`` simply groups
      geometries into a GeometryCollection (O(n)), and ``ST_Buffer(…,
      0)`` dissolves overlaps in a single GEOS pass — typically 5–10×
      faster than the iterative pair-wise merge that ``ST_Union``
      performs on large sets of complex polygons.

    Returns a GeoJSON-compatible dict (``{"type": "...", ...}``) or
    ``None`` when no polygon-type variables are selected.
    """
    import time as _time

    from shapely.geometry import mapping, shape
    from shapely.validation import make_valid
    from sqlalchemy import text

    polygon_vars = [v for v in exact_match_vars if v in _EXTENT_TABLE_MAP]
    if not polygon_vars:
        return None

    # When site_set_id is available, use a DB subquery so no site geometry
    # is materialised in Python — avoids the O(n²) unary_union topology
    # merge which is the primary OOM risk for large or complex site sets.
    # Falls back to GeoJSON for adopted tasks loaded from S3 (rare path).
    use_db_subquery = site_set_id is not None
    if not use_db_subquery:
        # Validate each geometry individually before union to avoid
        # GEOSException / TopologyException from invalid input geometries.
        valid_geoms = gdf.geometry.apply(
            lambda g: make_valid(g) if not g.is_valid else g
        )
        sites_union = valid_geoms.unary_union
        if not sites_union.is_valid:
            sites_union = make_valid(sites_union)
        sites_geojson = json.dumps(mapping(sites_union))

    t0 = _time.perf_counter()

    db = get_db()
    try:
        layer_extents = []
        for var_name in polygon_vars:
            table = _EXTENT_TABLE_MAP[var_name]
            # Safety: table comes from the hardcoded _EXTENT_TABLE_MAP —
            # assert it matches a safe identifier pattern to prevent SQL
            # injection if the dict is ever populated from external input.
            assert _SAFE_TABLE_RE.match(table), f"Unsafe table name: {table}"
            t1 = _time.perf_counter()
            if use_db_subquery:
                result = db.execute(
                    text(
                        f"SELECT ST_AsGeoJSON("
                        f"  ST_Buffer(ST_Collect(t.geom), 0)"
                        f") "
                        f"FROM {table} t "
                        f"WHERE ST_Intersects("
                        f"  t.geom, "
                        f"  (SELECT ST_SetSRID(ST_Extent(geom)::geometry, 4326) FROM user_site_features"
                        f"   WHERE site_set_id = :site_set_id)"
                        f")"
                    ),
                    {"site_set_id": str(site_set_id)},
                )
            else:
                result = db.execute(
                    text(
                        f"SELECT ST_AsGeoJSON("
                        f"  ST_Buffer(ST_Collect(geom), 0)"
                        f") "
                        f"FROM {table} "
                        f"WHERE ST_Intersects("
                        f"  geom, "
                        f"  ST_SetSRID(ST_GeomFromGeoJSON(:sites), 4326)"
                        f")"
                    ),
                    {"sites": sites_geojson},
                )
            row = result.fetchone()
            elapsed = _time.perf_counter() - t1
            logger.info(
                "[EXTENT] %s query took %.2fs",
                table,
                elapsed,
            )
            if row and row[0]:
                layer_extents.append(shape(json.loads(row[0])))

        if not layer_extents:
            return None

        # Intersect all layer extents to get the tightest envelope
        extent = layer_extents[0]
        for geom in layer_extents[1:]:
            extent = extent.intersection(geom)

        if extent.is_empty:
            logger.warning(
                "Matching extent is empty — the intersection of the "
                "selected exact-match layers does not cover any area."
            )
            return None

        logger.info(
            "[EXTENT] Total matching-extent computation: %.2fs",
            _time.perf_counter() - t0,
        )
        return mapping(extent)

    finally:
        db.close()


def compute_sites_exclusion_buffer(gdf, distance_km, site_set_id=None):
    """Compute a buffer around all site geometries using PostGIS geography.

    Uses ``ST_Buffer`` on a geography cast so the *distance_km* is
    interpreted in metres on the sphere — no S2 issues and correct
    at all latitudes.  Returns a GeoJSON-compatible dict or ``None``
    when buffering is disabled (distance_km <= 0).
    """
    if distance_km <= 0:
        return None

    from shapely.geometry import mapping
    from shapely.validation import make_valid

    # Same OOM-safe strategy as compute_matching_extent: use a DB subquery
    # when site_set_id is available to avoid Python-side unary_union.
    use_db_subquery = site_set_id is not None
    if not use_db_subquery:
        # Validate each geometry individually before union to avoid
        # GEOSException / TopologyException from invalid input geometries.
        valid_geoms = gdf.geometry.apply(
            lambda g: make_valid(g) if not g.is_valid else g
        )
        sites_union = valid_geoms.unary_union
        if not sites_union.is_valid:
            sites_union = make_valid(sites_union)
        sites_geojson = json.dumps(mapping(sites_union))

    db = get_db()
    try:
        if use_db_subquery:
            row = db.execute(
                text(
                    "SELECT ST_AsGeoJSON("
                    "  ST_Buffer("
                    "    (SELECT ST_SetSRID(ST_Extent(geom)::geometry, 4326) FROM user_site_features"
                    "     WHERE site_set_id = :site_set_id)::geography,"
                    "    :dist_m"
                    "  )::geometry"
                    ")"
                ),
                {"site_set_id": str(site_set_id), "dist_m": distance_km * 1000},
            ).fetchone()
        else:
            row = db.execute(
                text(
                    "SELECT ST_AsGeoJSON("
                    "  ST_Buffer("
                    "    ST_SetSRID(ST_GeomFromGeoJSON(:sites), 4326)::geography,"
                    "    :dist_m"
                    "  )::geometry"
                    ")"
                ),
                {"sites": sites_geojson, "dist_m": distance_km * 1000},
            ).fetchone()
        if row and row[0]:
            return json.loads(row[0])
        return None
    finally:
        db.close()


def compute_exact_match_groups_with_splitting(
    gdf: gpd.GeoDataFrame,
    exact_match_vars: list[str],
) -> tuple[gpd.GeoDataFrame, dict[int, list[tuple[str, int]]]]:
    """Split sites crossing exact-match boundaries and build group mapping.

    When ``group_by_exact_matches`` is enabled, sites spanning multiple
    exact-match regions (e.g., admin boundaries) are split into
    sub-polygons.  Each sub-polygon is assigned a separate exact-match
    group ID based on the combination of all polygon-type exact-match
    values it falls within.

    Small slivers (<10% of original site area) are merged into the
    largest sub-polygon of the same site to reduce fragmentation.

    Returns
    -------
    split_gdf : gpd.GeoDataFrame
        Modified GeoDataFrame with potential sub-site rows.  Added columns:
        - ``sub_site_index``: 0 for unsplit sites, 1+ for split fragments
        - ``is_sub_site``: True when site was split
        - ``original_area_ha``: Original site area before splitting
        - One column per polygon-type exact-match var with the region ID
    group_mapping : dict[int, list[tuple[str, int]]]
        Maps exact-match group ID → list of (site_id, sub_site_index).
        Group IDs are sequential integers starting from 1.

    Notes
    -----
    - Binary exact-match variables (e.g., ``pa``) are not used for
      splitting because they don't define discrete spatial regions.
    - Sites that don't intersect any exact-match layers are assigned to
      group 0 (ungrouped).
    - The function uses PostGIS ``ST_Intersection`` to split site
      geometries along exact-match boundaries in a single SQL query
      per layer, minimizing roundtrips.
    """
    import time as _time

    from shapely.geometry import mapping, shape
    from shapely.validation import make_valid

    # Filter to polygon-type exact-match variables
    polygon_vars = [v for v in exact_match_vars if v in _EXTENT_TABLE_MAP]
    if not polygon_vars:
        # No polygon variables → no splitting
        gdf_out = gdf.copy()
        gdf_out["sub_site_index"] = 0
        gdf_out["is_sub_site"] = False
        gdf_out["original_area_ha"] = None
        # All sites in one group
        group_mapping = {1: [(row["site_id"], 0) for _, row in gdf_out.iterrows()]}
        return gdf_out, group_mapping

    t0 = _time.perf_counter()
    db = get_db()

    try:
        # For each site, do a spatial overlay with each exact-match layer
        # to split the geometry and assign region IDs
        split_records = []

        for idx, site_row in gdf.iterrows():
            site_id = site_row["site_id"]
            site_geom = site_row["geometry"]

            # Validate geometry
            if not site_geom.is_valid:
                site_geom = make_valid(site_geom)

            site_geojson = json.dumps(mapping(site_geom))
            original_area_ha = site_row.get("area_ha") or (
                site_geom.area * 111_000 * 111_000 / 10_000
            )

            # Start with the site geometry as a single piece
            # We'll iteratively intersect with each exact-match layer
            pieces = [{"geometry": site_geom, "exact_match_values": {}}]

            for var_name in polygon_vars:
                table = _EXTENT_TABLE_MAP[var_name]
                assert _SAFE_TABLE_RE.match(table), f"Unsafe table name: {table}"

                # Query for all regions that intersect this site
                # Return the intersection geometry and the region identifier
                # Use shapely_name or region_id column depending on table
                id_col = (
                    "shape_name" if table.startswith("geoboundaries") else "eco_name"
                )

                result = db.execute(
                    text(
                        f"SELECT {id_col}, ST_AsGeoJSON(ST_Intersection("
                        f"  geom, "
                        f"  ST_SetSRID(ST_GeomFromGeoJSON(:site), 4326)"
                        f")) "
                        f"FROM {table} "
                        f"WHERE ST_Intersects("
                        f"  geom, "
                        f"  ST_SetSRID(ST_GeomFromGeoJSON(:site), 4326)"
                        f")"
                    ),
                    {"site": site_geojson},
                )

                intersections = result.fetchall()

                if not intersections:
                    # Site doesn't intersect this layer
                    # All pieces get NULL for this variable
                    for piece in pieces:
                        piece["exact_match_values"][var_name] = None
                    continue

                # For each existing piece, intersect it with each region
                # This can fragment pieces further
                new_pieces = []
                for piece in pieces:
                    piece_geom = piece["geometry"]
                    piece_covered = False

                    for region_id, intersection_geojson in intersections:
                        intersection_geom = shape(json.loads(intersection_geojson))
                        if intersection_geom.is_empty:
                            continue

                        # Check if this intersection overlaps the piece
                        overlap = piece_geom.intersection(intersection_geom)
                        if not overlap.is_empty and overlap.area > 0:
                            new_piece = piece["exact_match_values"].copy()
                            new_piece[var_name] = region_id
                            new_pieces.append(
                                {
                                    "geometry": overlap,
                                    "exact_match_values": new_piece,
                                }
                            )
                            piece_covered = True

                    # If piece wasn't covered by any region, keep it with NULL
                    if not piece_covered:
                        piece["exact_match_values"][var_name] = None
                        new_pieces.append(piece)

                pieces = new_pieces

            # Now merge small slivers (<10% of original area)
            if len(pieces) > 1:
                # Sort pieces by area descending
                pieces.sort(key=lambda p: p["geometry"].area, reverse=True)

                threshold_area = site_geom.area * 0.1  # 10% of original area
                main_pieces = []
                slivers = []

                for piece in pieces:
                    if piece["geometry"].area >= threshold_area:
                        main_pieces.append(piece)
                    else:
                        slivers.append(piece)

                # Merge slivers into the largest piece
                if slivers and main_pieces:
                    largest = main_pieces[0]
                    for sliver in slivers:
                        largest["geometry"] = largest["geometry"].union(
                            sliver["geometry"]
                        )

                    pieces = main_pieces
                elif not main_pieces:
                    # All pieces are slivers, keep the largest one
                    pieces = [pieces[0]]

            # Convert pieces to records
            for sub_idx, piece in enumerate(pieces):
                record = {
                    "site_id": site_id,
                    "site_name": site_row.get("site_name", site_id),
                    "start_date": site_row.get("start_date"),
                    "end_date": site_row.get("end_date"),
                    "geometry": piece["geometry"],
                    "area_ha": piece["geometry"].area * 111_000 * 111_000 / 10_000,
                    "sub_site_index": sub_idx,
                    "is_sub_site": len(pieces) > 1,
                    "original_area_ha": original_area_ha if len(pieces) > 1 else None,
                }
                # Add exact-match values
                record.update(piece["exact_match_values"])
                split_records.append(record)

        # Create output GeoDataFrame
        split_gdf = gpd.GeoDataFrame(split_records, crs=gdf.crs, geometry="geometry")

        # Build group mapping based on unique combinations of exact-match values
        # Group 0 is reserved for sites with all NULL exact-match values
        group_key_to_id = {}
        next_group_id = 1
        group_mapping = {}

        for _, row in split_gdf.iterrows():
            # Build a tuple of exact-match values for this site/sub-site
            key_values = tuple(row.get(var_name) for var_name in polygon_vars)

            # Check if all NULL
            if all(v is None for v in key_values):
                group_id = 0
            else:
                if key_values not in group_key_to_id:
                    group_key_to_id[key_values] = next_group_id
                    next_group_id += 1
                group_id = group_key_to_id[key_values]

            if group_id not in group_mapping:
                group_mapping[group_id] = []
            group_mapping[group_id].append((row["site_id"], row["sub_site_index"]))

        elapsed = _time.perf_counter() - t0
        logger.info(
            "[SPLIT] Exact-match site splitting: %d sites → %d pieces in %.2fs",
            len(gdf),
            len(split_gdf),
            elapsed,
        )
        logger.info("[SPLIT] Created %d exact-match groups", len(group_mapping))

        return split_gdf, group_mapping

    finally:
        db.close()


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

        config = task.config or {}
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

            # Step 2 & 3 — matching extent and exclusion buffer (DB subqueries).
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

            # Step 4 — stream site set to in-memory Parquet (batch-at-a-time).
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

        # Optionally split sites across exact-match boundaries.
        if group_by_exact_matches:
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

        # Create TaskSite rows and update n_sites on the task.
        task.n_sites = len(gdf)
        for i, (_, row) in enumerate(gdf_for_db.iterrows()):
            geom = row.geometry
            area_ha = (
                float(_areas_ha.iloc[i])
                if (geom is not None and not geom.is_empty)
                else None
            )

            site = TaskSite(
                task_id=task_id,
                site_id=str(row["site_id"]),
                site_name=str(row.get("site_name", "")),
                start_date=pd.to_datetime(row["start_date"]),
                end_date=pd.to_datetime(row["end_date"])
                if pd.notna(row.get("end_date"))
                else None,
                area_ha=area_ha,
                sub_site_index=row.get("sub_site_index", 0),
                is_sub_site=row.get("is_sub_site", False),
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
        if parquet_buf is not None and not group_by_exact_matches:
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
            **({"exact_match_group_mapping": group_mapping} if group_mapping else {}),
            **({"random_seed": random_seed} if random_seed is not None else {}),
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
                        {
                            "name": "match",
                            "command": ["match"],
                            "array_size": (
                                len(group_mapping) * n_replicates
                                if group_by_exact_matches and group_mapping
                                else len(gdf_for_db) * n_replicates
                            ),
                            "timeout_seconds": 14400,  # 4 h per element
                            "memory_mib": match_memory_mib,
                            "vcpus": 2,
                            "retry_attempts": 5,
                        },
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
                if (
                    (
                        group_by_exact_matches
                        and group_mapping
                        and len(group_mapping) > 1
                    )
                    or (not group_by_exact_matches and len(gdf) > 1)
                    or n_replicates > 1
                )
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

    # Optionally split sites crossing exact-match boundaries
    if group_by_exact_matches:
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

            site = TaskSite(
                task_id=task_id,
                site_id=str(row["site_id"]),
                site_name=str(row.get("site_name", "")),
                start_date=pd.to_datetime(row["start_date"]),
                end_date=pd.to_datetime(row["end_date"])
                if pd.notna(row.get("end_date"))
                else None,
                area_ha=area_ha,
                sub_site_index=row.get("sub_site_index", 0),
                is_sub_site=row.get("is_sub_site", False),
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
            **({"exact_match_group_mapping": group_mapping} if group_mapping else {}),
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
                        {
                            "name": "match",
                            "command": ["match"],
                            "array_size": (
                                len(group_mapping) * n_replicates
                                if group_by_exact_matches and group_mapping
                                else len(gdf_for_db) * n_replicates
                            ),
                            "timeout_seconds": 14400,  # 4 h per element
                            "memory_mib": match_memory_mib,
                            "vcpus": 2,
                            "retry_attempts": 5,
                        },
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
                if (
                    (
                        group_by_exact_matches
                        and group_mapping
                        and len(group_mapping) > 1
                    )
                    or (not group_by_exact_matches and len(gdf) > 1)
                    or n_replicates > 1
                )
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


def get_task_list(user_id=None, limit=50):
    """Get recent analysis tasks, optionally filtered by user.

    Uses ``load_only`` to skip the heavy ``extra_metadata`` JSON column
    that the task list view never reads.
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
        return query.limit(limit).all()
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


def get_task_detail(task_id):
    """Get full task details including sites and results."""
    db = get_db()
    try:
        task = db.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
        if not task:
            return None

        sites = db.query(TaskSite).filter(TaskSite.task_id == task_id).all()

        results = (
            db.query(TaskResult)
            .filter(TaskResult.task_id == task_id)
            .order_by(TaskResult.site_id, TaskResult.year)
            .all()
        )

        totals = (
            db.query(TaskResultTotal).filter(TaskResultTotal.task_id == task_id).all()
        )

        sites_geojson = None
        if task.site_set_id:
            sites_geojson = get_user_site_set_geojson(task.site_set_id)
        if not sites_geojson or not sites_geojson.get("features"):
            # Adopted tasks have no local site set — fall back to S3
            s3_uri = task.sites_s3_uri
            if not s3_uri:
                # Also check inside config/params (older adopted tasks)
                s3_uri = (task.config or {}).get("sites_s3_uri")
            if s3_uri:
                sites_geojson = _fetch_sites_geojson_from_s3(s3_uri)
            else:
                parquet_uri = (task.config or {}).get("sites_parquet_s3_uri")
                if parquet_uri:
                    parquet_gdf = _fetch_sites_parquet_from_s3(parquet_uri)
                    if parquet_gdf is not None and not parquet_gdf.empty:
                        sites_geojson = json.loads(upload_sites_to_geojson(parquet_gdf))

        return {
            "task": task,
            "sites": sites,
            "results": results,
            "totals": totals,
            "sites_geojson": sites_geojson,
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
    from models import TrendsEarthCredential, User

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

    from gee_export import gee_tasks

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


def get_user_list():
    """Return all users ordered by creation date (admin only)."""
    db = get_db()
    try:
        from models import User

        return db.query(User).order_by(User.created_at.desc()).all()
    finally:
        db.close()


def approve_user(user_id):
    """Approve a pending user account and email them a set-password link.

    Returns (success, message).
    """
    db = get_db()
    try:
        from models import PasswordResetToken, User

        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return False, "User not found."
        if user.is_approved:
            return False, "User is already approved."
        user.is_approved = True
        user.updated_at = datetime.now(timezone.utc)
        db.commit()

        # Send the newly-approved user a link to set their password.
        try:
            PasswordResetToken.invalidate_user_tokens(user.id, db)
            reset_token = PasswordResetToken(user_id=user.id)
            db.add(reset_token)
            db.commit()

            from config import Config

            set_pw_url = f"{Config.APP_URL}/reset-password?token={reset_token.token}"
            html_body = f"""
            <p>Hello {user.name},</p>

            <p>Your Avoided Emissions account has been approved! To get
            started, please set your password by clicking the link below.
            This link will expire in 1 hour.</p>

            <p><a href=\"{set_pw_url}\">Set Your Password</a></p>

            <p>If you cannot click the link, copy and paste this URL into
            your browser:</p>
            <p>{set_pw_url}</p>
            """
            from email_service import send_html_email

            send_html_email(
                recipients=[user.email],
                html=html_body,
                subject="[Avoided Emissions] Account Approved — Set Your Password",
            )
        except Exception:
            logger.exception(
                "Failed to send set-password email to newly approved user %s",
                user.email,
            )
            report_exception(approved_user_email=user.email)

        return True, f"User {user.email} approved."
    except Exception:
        db.rollback()
        return False, "Failed to approve user."
    finally:
        db.close()


def change_user_role(user_id, new_role, acting_user_id=None):
    """Change a user's role. Returns (success, message)."""
    if new_role not in ("admin", "user"):
        return False, "Invalid role."
    if acting_user_id and str(acting_user_id) == str(user_id) and new_role == "user":
        return False, "You cannot change your own role to user."
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


# ---------------------------------------------------------------------------
# trends.earth script access management
# ---------------------------------------------------------------------------


def _get_te_admin_client():
    """Return a ``TrendsEarthClient`` authenticated with the webapp's
    service-account credentials (``TRENDSEARTH_CLIENT_ID`` /
    ``TRENDSEARTH_CLIENT_SECRET``).

    These credentials must belong to a trends.earth ADMIN or SUPERADMIN
    user so that the script access endpoints are accessible.

    Returns ``None`` if the service-account credentials or script ID are
    not configured.
    """
    from trendsearth_client import TrendsEarthClient

    client_id = Config.TRENDSEARTH_CLIENT_ID
    client_secret = Config.TRENDSEARTH_CLIENT_SECRET
    script_id = Config.TRENDSEARTH_SCRIPT_ID

    if not client_id or not client_secret or not script_id:
        logger.debug(
            "TE admin client not available — TRENDSEARTH_CLIENT_ID, "
            "TRENDSEARTH_CLIENT_SECRET, or TRENDSEARTH_SCRIPT_ID is not set."
        )
        return None

    return TrendsEarthClient.from_oauth2_credentials(
        api_url=Config.TRENDSEARTH_API_URL,
        client_id=client_id,
        client_secret=client_secret,
    )


def grant_te_script_access(user_id):
    """Grant a webapp user access to the avoided-emissions TE API script.

    Looks up the user's ``te_user_id`` from the stored credential and
    adds that ID to the script's allowed-users list on the TE API using
    the webapp's service-account credentials.

    Does nothing (and logs a warning) if:
    - The user has no linked TE credential or no ``te_user_id``.
    - The webapp service-account credentials are not configured.

    Raises on HTTP errors so callers can decide whether to treat the
    failure as blocking or best-effort.
    """
    from credential_store import get_credential

    cred = get_credential(user_id)
    if not cred or not cred.te_user_id:
        logger.warning(
            "Cannot grant TE script access for user %s — no te_user_id", user_id
        )
        return

    client = _get_te_admin_client()
    if not client:
        logger.warning(
            "Cannot grant TE script access — webapp service-account "
            "credentials are not configured."
        )
        return

    script_id = Config.TRENDSEARTH_SCRIPT_ID
    logger.info(
        "Granting TE script %s access to TE user %s (webapp user %s)",
        script_id,
        cred.te_user_id,
        user_id,
    )
    client.add_user_to_script(script_id, cred.te_user_id)


def revoke_te_script_access(user_id):
    """Revoke a webapp user's access to the avoided-emissions TE API script.

    Looks up the user's ``te_user_id`` from the stored credential and
    removes that ID from the script's allowed-users list on the TE API
    using the webapp's service-account credentials.

    Does nothing (and logs a warning) if:
    - The user has no linked TE credential or no ``te_user_id``.
    - The webapp service-account credentials are not configured.

    Raises on HTTP errors so callers can decide whether to treat the
    failure as blocking or best-effort.
    """
    from credential_store import get_credential

    cred = get_credential(user_id)
    if not cred or not cred.te_user_id:
        logger.warning(
            "Cannot revoke TE script access for user %s — no te_user_id", user_id
        )
        return

    client = _get_te_admin_client()
    if not client:
        logger.warning(
            "Cannot revoke TE script access — webapp service-account "
            "credentials are not configured."
        )
        return

    script_id = Config.TRENDSEARTH_SCRIPT_ID
    logger.info(
        "Revoking TE script %s access from TE user %s (webapp user %s)",
        script_id,
        cred.te_user_id,
        user_id,
    )
    client.remove_user_from_script(script_id, cred.te_user_id)


def delete_user(user_id):
    """Delete a user account and their analysis tasks. Returns (success, message)."""
    # Revoke TE script access *before* deleting the DB row so that
    # the credential lookup still works.
    try:
        revoke_te_script_access(user_id)
    except Exception:
        logger.warning(
            "Failed to revoke TE script access for user %s during deletion "
            "(continuing with deletion)",
            user_id,
            exc_info=True,
        )

    db = get_db()
    try:
        from models import User

        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return False, "User not found."
        email = user.email
        # Delete the user's analysis tasks (cascades to sites/results via DB)
        tasks = (
            db.query(AnalysisTask).filter(AnalysisTask.submitted_by == user_id).all()
        )
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


def get_covariate_presets(user_id):
    """Return all covariate presets for the given user, ordered by name.

    Each item is a dict with keys ``id``, ``name``, ``covariates``, and
    ``exact_match_vars``.
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
            {
                "id": str(p.id),
                "name": p.name,
                "covariates": list(p.covariates),
                "exact_match_vars": list(p.exact_match_vars)
                if p.exact_match_vars
                else [],
            }
            for p in presets
        ]
    finally:
        db.close()


def save_covariate_preset(user_id, name, covariates, exact_match_vars=None):
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
            existing.exact_match_vars = (
                list(exact_match_vars) if exact_match_vars else []
            )
            existing.updated_at = datetime.now(timezone.utc)
            db.commit()
            return str(existing.id)

        preset = CovariatePreset(
            user_id=user_id,
            name=name,
            covariates=list(covariates),
            exact_match_vars=list(exact_match_vars) if exact_match_vars else [],
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


# ---------------------------------------------------------------------------
# Matching settings presets
# ---------------------------------------------------------------------------


def get_matching_settings_presets(user_id):
    """Return all matching settings presets for the given user, ordered by name.

    Each item is a dict with keys ``id``, ``name``, and ``settings``.
    """
    db = get_db()
    try:
        presets = (
            db.query(MatchingSettingsPreset)
            .filter(MatchingSettingsPreset.user_id == user_id)
            .order_by(MatchingSettingsPreset.name)
            .all()
        )
        return [
            {
                "id": str(p.id),
                "name": p.name,
                "settings": dict(p.settings) if p.settings else {},
            }
            for p in presets
        ]
    finally:
        db.close()


def save_matching_settings_preset(user_id, name, settings):
    """Create or update a matching settings preset for the given user.

    If a preset with the same *name* already exists for this user it is
    updated in-place; otherwise a new row is inserted.  Returns the
    preset ``id`` as a string.
    """
    db = get_db()
    try:
        existing = (
            db.query(MatchingSettingsPreset)
            .filter(
                MatchingSettingsPreset.user_id == user_id,
                MatchingSettingsPreset.name == name,
            )
            .first()
        )
        if existing:
            existing.settings = dict(settings)
            existing.updated_at = datetime.now(timezone.utc)
            db.commit()
            return str(existing.id)

        preset = MatchingSettingsPreset(
            user_id=user_id,
            name=name,
            settings=dict(settings),
        )
        db.add(preset)
        db.commit()
        return str(preset.id)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def delete_matching_settings_preset(preset_id, user_id):
    """Delete a matching settings preset by id, scoped to the owning user.

    Returns ``True`` if a row was deleted, ``False`` otherwise.
    """
    db = get_db()
    try:
        preset = (
            db.query(MatchingSettingsPreset)
            .filter(
                MatchingSettingsPreset.id == preset_id,
                MatchingSettingsPreset.user_id == user_id,
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


# ---------------------------------------------------------------------------
# Task share links
# ---------------------------------------------------------------------------


def create_share_link(task_id, user_id, expiry_days=7):
    """Create a shareable link for a task.

    Parameters
    ----------
    task_id : str
        UUID of the ``AnalysisTask``.
    user_id : str
        UUID of the user creating the link.
    expiry_days : int
        Number of days until the link expires (default 7, max 90).

    Returns
    -------
    dict
        ``{"token": ..., "expires_at": ..., "id": ...}`` on success.
    """
    from models import TaskShareLink

    # Clamp expiry to a reasonable range (1–90 days)
    expiry_days = max(1, min(int(expiry_days), 90))

    db = get_db()
    try:
        from models import User

        task = db.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
        if not task:
            raise ValueError("Task not found.")

        user = (
            db.query(User).filter(User.id == user_id, User.is_active.is_(True)).first()
        )
        if not user:
            raise PermissionError("User is not authorized to manage share links.")

        if user.role != "admin" and str(task.submitted_by) != str(user_id):
            raise PermissionError("User is not authorized to manage share links.")

        link = TaskShareLink(
            task_id=task_id,
            created_by=user_id,
            expires_at=datetime.now(timezone.utc) + timedelta(days=expiry_days),
        )
        db.add(link)
        db.commit()
        return {
            "token": link.token,
            "expires_at": link.expires_at.isoformat(),
            "id": str(link.id),
        }
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def list_share_links(task_id, user_id=None):
    """Return active share links for a task.

    Returns
    -------
    list[dict]
        Each dict has ``id``, ``token``, ``created_at``, ``expires_at``,
        ``is_active``, ``access_count``.
    """
    from models import TaskShareLink

    db = get_db()
    try:
        if user_id is not None:
            from models import User

            task = db.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
            if not task:
                return []

            user = (
                db.query(User)
                .filter(User.id == user_id, User.is_active.is_(True))
                .first()
            )
            if not user:
                return []
            if user.role != "admin" and str(task.submitted_by) != str(user_id):
                return []

        links = (
            db.query(TaskShareLink)
            .filter(TaskShareLink.task_id == task_id)
            .order_by(TaskShareLink.created_at.desc())
            .all()
        )
        return [
            {
                "id": str(lnk.id),
                "token": lnk.token,
                "created_at": lnk.created_at.isoformat() if lnk.created_at else None,
                "expires_at": lnk.expires_at.isoformat() if lnk.expires_at else None,
                "is_active": lnk.is_active,
                "is_valid": lnk.is_valid,
                "access_count": lnk.access_count or 0,
            }
            for lnk in links
        ]
    finally:
        db.close()


def revoke_share_link(link_id, user_id, task_id=None):
    """Revoke a share link by setting ``is_active`` to False.

    Validates that the link belongs to the expected *task_id* (if
    provided) to prevent cross-object attacks where a forged request
    pairs a valid task_id with a foreign link_id.

    Returns ``True`` if the link was found and revoked.
    """
    from models import TaskShareLink

    db = get_db()
    try:
        from models import User

        link = db.query(TaskShareLink).filter(TaskShareLink.id == link_id).first()
        if not link:
            return False

        user = (
            db.query(User).filter(User.id == user_id, User.is_active.is_(True)).first()
        )
        if not user:
            return False

        task = db.query(AnalysisTask).filter(AnalysisTask.id == link.task_id).first()
        if not task:
            return False

        if user.role != "admin" and str(task.submitted_by) != str(user_id):
            return False

        # Cross-validate: link must belong to the task the caller has
        # access to.  Without this check a user who can view task A
        # could revoke a link belonging to task B.
        if task_id is not None and str(link.task_id) != str(task_id):
            return False
        link.is_active = False
        db.commit()
        return True
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def validate_share_token(token, record_access=True):
    """Validate a share token and return the associated task_id.

    Parameters
    ----------
    token : str
        The URL-safe share token.
    record_access : bool
        When *True* (the default), increments the access counter on the
        link.  Pass *False* for lightweight authorisation checks that
        should not inflate the counter (e.g. periodic callback ticks).

    Returns
    -------
    str or None
        The task UUID as a string, or ``None``.
    """
    from models import TaskShareLink

    db = get_db()
    try:
        link = TaskShareLink.get_valid_link(token, db)
        if not link:
            return None
        if record_access:
            link.record_access()
            db.commit()
        return str(link.task_id)
    except Exception:
        db.rollback()
        return None
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Recompute (resubmit with new random seed)
# ---------------------------------------------------------------------------


def get_recompute_config(task_id, user_id):
    """Return the configuration of a task for pre-filling the submit form.

    Loads the original task's settings and returns them as a plain dict
    suitable for pre-populating the task-submission page.  A new random
    seed is generated so the user starts with a fresh value.

    Parameters
    ----------
    task_id : str
        UUID of the ``AnalysisTask`` to recompute.
    user_id : str
        UUID of the user requesting the recompute.

    Returns
    -------
    dict
        Keys: ``task_name``, ``description``, ``covariates``,
        ``exact_match_vars``, ``max_treatment_pixels``,
        ``control_multiplier``, ``min_site_area_ha``,
        ``min_glm_treatment_pixels``, ``caliper_width``,
        ``max_controls_per_treatment``, ``random_seed``,
        ``match_memory_gb``, ``matching_job_queue``, ``site_set_id``.

    Raises
    ------
    ValueError
        If the task is not found or the user is not authorised.
    """
    import random as _random

    db = get_db()
    try:
        task = db.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
        if not task:
            raise ValueError("Task not found.")

        from models import User

        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            raise ValueError("User not found.")
        if not user.is_admin and str(task.submitted_by) != str(user_id):
            raise ValueError("You can only recompute your own tasks.")

        config = task.config or {}
        match_memory_mib = config.get("match_memory_mib", 30720)

        return {
            "task_name": f"{task.name} (recompute)",
            "description": task.description or "",
            "covariates": list(task.covariates or []),
            "exact_match_vars": config.get("exact_match_vars", []),
            "max_treatment_pixels": config.get("max_treatment_pixels", 1000),
            "control_multiplier": config.get("control_multiplier", 50),
            "min_site_area_ha": config.get("min_site_area_ha", 100),
            "min_glm_treatment_pixels": config.get("min_glm_treatment_pixels", 15),
            "caliper_width": config.get("caliper_width", 0.2),
            "max_controls_per_treatment": config.get("max_controls_per_treatment", 1),
            "min_control_distance_km": config.get("min_control_distance_km", 10),
            "separation_fallback_mahalanobis": config.get(
                "separation_fallback_mahalanobis", False
            ),
            "group_by_exact_matches": config.get("group_by_exact_matches", False),
            "matching_method": config.get("matching_method", "optimal"),
            "n_replicates": config.get("n_replicates", 1),
            "random_seed": _random.randint(1, 2_147_483_647),
            "match_memory_gb": max(1, match_memory_mib // 1024),
            "matching_job_queue": config.get("matching_job_queue", "ae-spot-gp3"),
            "site_set_id": str(task.site_set_id) if task.site_set_id else None,
        }
    finally:
        db.close()


def resubmit_analysis_task(task_id, user_id):
    """Resubmit a previously submitted task with a new random seed.

    Looks up the original task's configuration, generates a fresh random
    seed, and creates a brand-new ``AnalysisTask`` via
    :func:`queue_analysis_task`.  The new task starts in
    ``status='submitting'`` and a Celery worker handles the slow parts
    (PostGIS computations, S3 upload, API call) asynchronously.

    Parameters
    ----------
    task_id : str
        UUID of the original ``AnalysisTask`` to recompute.
    user_id : str
        UUID of the user requesting the recompute.

    Returns
    -------
    str
        UUID of the newly created task.

    Raises
    ------
    ValueError
        If the task is not found, not owned by the user, or its sites
        cannot be recovered.
    """
    import random as _random

    db = get_db()
    try:
        task = db.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
        if not task:
            raise ValueError("Task not found.")

        # Ownership check (admins bypass)
        from models import User

        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            raise ValueError("User not found.")
        if not user.is_admin and str(task.submitted_by) != str(user_id):
            raise ValueError("You can only recompute your own tasks.")

        # Verify that site data can be recovered — either via the local
        # site set or an S3 URI.  We don't load the GDF here; the Celery
        # worker will do that.
        source_sites_s3_uri = None
        source_sites_parquet_s3_uri = (task.config or {}).get("sites_parquet_s3_uri")
        if not task.site_set_id:
            source_sites_s3_uri = task.sites_s3_uri or (task.config or {}).get(
                "sites_s3_uri"
            )
            if not source_sites_s3_uri and not source_sites_parquet_s3_uri:
                raise ValueError(
                    "Cannot recover sites for this task. The original site "
                    "data is no longer available."
                )

        config = task.config or {}
        new_seed = _random.randint(1, 2_147_483_647)
        match_memory_mib = config.get("match_memory_mib", 30720)
        new_task_name = f"{task.name} (recompute)"

        return queue_analysis_task(
            task_name=new_task_name,
            description=task.description or "",
            user_id=user_id,
            site_set_id=str(task.site_set_id) if task.site_set_id else None,
            covariates=list(task.covariates or []),
            exact_match_vars=config.get("exact_match_vars", []),
            max_treatment_pixels=config.get("max_treatment_pixels", 1000),
            control_multiplier=config.get("control_multiplier", 50),
            min_site_area_ha=config.get("min_site_area_ha", 100),
            min_glm_treatment_pixels=config.get("min_glm_treatment_pixels", 15),
            caliper_width=config.get("caliper_width", 0.2),
            max_controls_per_treatment=config.get("max_controls_per_treatment", 1),
            min_control_distance_km=config.get("min_control_distance_km", 10),
            separation_fallback_mahalanobis=config.get(
                "separation_fallback_mahalanobis", False
            ),
            group_by_exact_matches=config.get("group_by_exact_matches", False),
            matching_method=config.get("matching_method", "optimal"),
            n_replicates=config.get("n_replicates", 1),
            random_seed=new_seed,
            match_memory_mib=match_memory_mib,
            matching_job_queue=config.get("matching_job_queue", "ae-spot-gp3"),
            resolution_m=config.get("resolution_m", ANALYSIS_DEFAULTS["resolution_m"]),
            source_sites_s3_uri=source_sites_s3_uri,
            source_sites_parquet_s3_uri=source_sites_parquet_s3_uri,
        )
    finally:
        db.close()
