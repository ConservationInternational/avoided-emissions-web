"""Site upload staging, file parsing, column mapping, and DB persistence."""

import csv
import io
import json
import logging
import os
import shutil
import tarfile
import tempfile
import uuid
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic

import geopandas as gpd
import pandas as pd
import tasks as webapp_tasks
from botocore.exceptions import ClientError
from config import Config
from models import (
    UserSiteSet,
    UserSiteUpload,
    get_db,
)
from sqlalchemy import text

from services.s3 import get_s3_client
from services.site_set import get_user_site_set_detail
from services.site_upload_validation import (
    normalize_site_date,
    normalize_site_text,
    validate_wgs84_polygon,
)

logger = logging.getLogger(__name__)

MAX_ARCHIVE_FILE_COUNT = 2_000
MAX_ARCHIVE_TOTAL_UNCOMPRESSED_BYTES = 1024 * 1024 * 1024
MAX_ARCHIVE_MEMBER_UNCOMPRESSED_BYTES = 512 * 1024 * 1024
MAX_ARCHIVE_COMPRESSION_RATIO = 200.0

SITE_UPLOAD_STAGE_DIR = Path(tempfile.gettempdir()) / "ae_site_upload_stage"
SITE_UPLOAD_STAGE_TTL_SECONDS = 12 * 60 * 60
SITE_UPLOAD_STAGE_S3_PREFIX = f"{Config.S3_PREFIX.rstrip('/')}/site-upload-stage"
SITE_UPLOAD_INSERT_BATCH_SIZE = 1000
SITE_UPLOAD_INSERT_BATCH_MAX_BYTES = 8 * 1024 * 1024
SITE_UPLOAD_PROGRESS_BATCH_SIZE = 250
SITE_UPLOAD_PROGRESS_CHECK_ROW_INTERVAL = 25
SITE_UPLOAD_PROGRESS_INTERVAL_SECONDS = 10
# Recovery must outlive the worker's 7,500-second hard time limit.
SITE_UPLOAD_WORKER_LEASE_SECONDS = 8400
SITE_UPLOAD_DISPATCH_INTERVAL_SECONDS = 600
SITE_UPLOAD_MAX_DISPATCH_ATTEMPTS = 6
SITE_UPLOAD_PENDING_EXPIRY_SECONDS = 24 * 60 * 60

# start_date is the only truly required field for analysis
# site_id and site_name can be auto-assigned if missing
REQUIRED_SITE_FIELDS = ("start_date",)
OPTIONAL_SITE_FIELDS = ("site_id", "site_name", "end_date")
ALL_SITE_FIELDS = ("site_id", "site_name", "start_date", "end_date")


class _SiteUploadCancelled(Exception):
    """Raised when a user cancels an active import."""


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
    # Use S3 whenever a bucket is configured so that the webapp and Celery
    # worker containers (which have separate filesystems) share access to the
    # staged upload.  The local-filesystem fallback is only safe when both
    # processes run in the same container or the same filesystem namespace.
    return bool(Config.S3_BUCKET)


def _parse_staged_upload_created_at(meta):
    created_at = datetime.fromisoformat(meta.get("created_at", ""))
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)
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
    age_seconds = (datetime.now(UTC) - created_at).total_seconds()
    if age_seconds > SITE_UPLOAD_STAGE_TTL_SECONDS and not _site_upload_stage_in_use(
        upload_token
    ):
        discard_staged_site_upload(upload_token, user_id)
        raise ValueError("Staged upload expired. Please upload the file again.")

    return meta


def _materialize_staged_upload_path(upload_token, user_id, progress_callback=None):
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
                get_s3_client().download_fileobj(
                    Config.S3_BUCKET,
                    data_key,
                    out,
                    Callback=progress_callback,
                )
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
    from fiona.errors import FionaError
    from pyproj.exceptions import ProjError
    from shapely.errors import GEOSException

    errors = []
    gdf = None

    try:
        ext = _get_file_extension(filename)
        if ext in (".geojson", ".json", ".gpkg"):
            gdf = gpd.read_file(file_path)
        elif ext in (".zip", ".tar.gz", ".tgz"):
            archive_dir = tempfile.mkdtemp(prefix="ae_site_archive_parse_")
            source_path = _extract_site_archive_dataset(
                file_path, filename, archive_dir
            )
            gdf = (
                _read_shapefile(source_path)
                if source_path.endswith(".shp")
                else gpd.read_file(source_path)
            )
        else:
            errors.append(f"Unsupported file format: {ext}")
            return None, errors
    except (FionaError, OSError, ValueError, RuntimeError) as e:
        errors.append(f"Failed to read file: {e!s}")
        return None, errors
    finally:
        if "archive_dir" in locals():
            shutil.rmtree(archive_dir, ignore_errors=True)

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
        if not gdf.crs:
            if _get_file_extension(filename) in {".geojson", ".json"}:
                # RFC 7946 GeoJSON coordinates are WGS84 longitude/latitude.
                gdf = gdf.set_crs(epsg=4326)
            else:
                errors.append("The uploaded dataset has no known CRS.")
        else:
            try:
                if gdf.crs.to_epsg() != 4326:
                    gdf = gdf.to_crs(epsg=4326)
            except (ValueError, ProjError) as exc:
                errors.append(f"Failed to transform geometry to EPSG:4326: {exc}")

        gdf = _repair_geometries(gdf)
        geometry_errors = []
        for index, geometry in gdf.geometry.items():
            try:
                validate_wgs84_polygon(geometry, row_number=index + 1)
            except (TypeError, ValueError, GEOSException) as exc:
                geometry_errors.append(str(exc))
                if len(geometry_errors) >= 10:
                    break
        errors.extend(geometry_errors)

    return gdf, errors


def _cleanup_expired_staged_uploads():
    if _use_shared_site_upload_stage():
        # Expired shared staged uploads are deleted lazily when retrieved.
        return

    audit_local_site_upload_stages(apply=True)


def _lock_site_upload_stage(db, token, *, wait=True):
    """Serialize local-stage deletion with durable job creation."""
    statement = (
        "SELECT pg_advisory_xact_lock(hashtextextended(:token, 0))"
        if wait
        else "SELECT pg_try_advisory_xact_lock(hashtextextended(:token, 0))"
    )
    result = db.execute(
        text(statement),
        {"token": f"site-upload-stage:{token}"},
    )
    return True if wait else result.scalar_one()


def _site_upload_stage_in_use(token, db=None):
    owns_session = db is None
    db = db or get_db()
    try:
        return (
            db.query(UserSiteUpload.id)
            .filter(
                UserSiteUpload.status.in_(("pending", "running")),
                UserSiteUpload.extra_metadata["upload_token"].as_string() == str(token),
            )
            .first()
            is not None
        )
    finally:
        if owns_session:
            db.close()


def audit_local_site_upload_stages(*, apply=False, now=None):
    """Audit expired local files, retaining all pending/running job inputs.

    Failure to check the database fails closed. No cloud API is called.
    """
    cutoff = (now or datetime.now(UTC)) - timedelta(
        seconds=SITE_UPLOAD_STAGE_TTL_SECONDS
    )
    result = {"expired_local_tokens": [], "protected_local_tokens": []}
    if not SITE_UPLOAD_STAGE_DIR.exists():
        return result
    db = get_db()
    try:
        for meta_path in SITE_UPLOAD_STAGE_DIR.glob("*.json"):
            token = meta_path.stem
            data_path, _ = _site_upload_stage_paths(token)
            if not _lock_site_upload_stage(db, token, wait=False):
                result["protected_local_tokens"].append(token)
                db.rollback()
                continue
            try:
                try:
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                    created_at = _parse_staged_upload_created_at(meta)
                except (OSError, KeyError, TypeError, ValueError, AttributeError):
                    created_at = datetime.fromtimestamp(meta_path.stat().st_mtime, UTC)
                if created_at >= cutoff:
                    continue
                if _site_upload_stage_in_use(token, db):
                    result["protected_local_tokens"].append(token)
                    continue
                result["expired_local_tokens"].append(token)
                if apply:
                    meta_path.unlink(missing_ok=True)
                    data_path.unlink(missing_ok=True)
            except OSError:
                logger.warning(
                    "Failed auditing local upload stage %s", token, exc_info=True
                )
            finally:
                db.rollback()
    finally:
        db.close()
    return result


def stage_site_upload(file_content, filename, user_id):
    """Persist uploaded bytes to short-lived local staging and return a token."""
    _cleanup_expired_staged_uploads()

    token = uuid.uuid4().hex
    meta = {
        "user_id": str(user_id),
        "filename": filename,
        "created_at": datetime.now(UTC).isoformat(),
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
            "created_at": datetime.now(UTC).isoformat(),
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

    Direct files and archives are read from paths. Only a small feature sample
    is materialized; archive feature counts remain unknown because obtaining
    them may require scanning every member.
    """
    from fiona.errors import FionaError
    from shapely.geometry import shape

    try:
        local_path, filename, _size = _materialize_staged_upload_path(token, user_id)
    except (OSError, ValueError) as exc:
        return None, [str(exc)]

    ext = _get_file_extension(filename)
    preview_path = local_path
    extracted_dir = None
    owned_preview_path = False
    try:
        if ext not in {".gpkg", ".geojson", ".json", ".zip", ".tar.gz", ".tgz"}:
            return None, [f"Unsupported file format: {ext}"]

        if ext in {".zip", ".tar.gz", ".tgz"}:
            extracted_dir = tempfile.mkdtemp(prefix="ae_site_upload_preview_")
            preview_path = _extract_site_archive_dataset(
                local_path, filename, extracted_dir
            )
        elif not local_path.lower().endswith(ext):
            with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as temp_file:
                preview_path = temp_file.name
                owned_preview_path = True
            shutil.copyfile(local_path, preview_path)

        with _open_site_feature_source(preview_path) as src:
            source_crs = src.crs_wkt or src.crs
            if not source_crs and ext not in {".geojson", ".json"}:
                return None, ["The uploaded dataset has no known CRS."]
            if source_crs:
                try:
                    from pyproj import CRS

                    CRS.from_user_input(source_crs)
                except (ValueError, TypeError) as exc:
                    return None, [f"Invalid source CRS: {exc}"]
            sample_features = []
            for feature in src:
                if feature is None:
                    continue
                properties = dict(feature.get("properties") or {})
                geometry = feature.get("geometry")
                properties["geometry"] = (
                    shape(geometry) if geometry is not None else None
                )
                sample_features.append(properties)
                if len(sample_features) >= 10:
                    break

            if not sample_features:
                return None, ["No features were found in the uploaded file."]
            gdf_sample = gpd.GeoDataFrame(sample_features, geometry="geometry")
            if source_crs:
                gdf_sample = gdf_sample.set_crs(source_crs)
            else:
                gdf_sample = gdf_sample.set_crs("EPSG:4326")
            n_features = None
            if ext not in {".zip", ".tar.gz", ".tgz", ".geojson", ".json"}:
                try:
                    n_features = len(src)
                except (TypeError, ValueError):
                    n_features = None
    except (FionaError, OSError, ValueError, RuntimeError, TypeError) as exc:
        return None, [f"Failed to read file: {exc}"]
    finally:
        if owned_preview_path:
            try:
                os.unlink(preview_path)
            except OSError:
                logger.debug(
                    "Failed to remove temporary preview file: %s", preview_path
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
        if extracted_dir:
            shutil.rmtree(extracted_dir, ignore_errors=True)

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


def _copy_archive_member(source, destination, cancel_check=None):
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    with open(destination, "wb") as target:
        while chunk := source.read(1024 * 1024):
            if cancel_check:
                cancel_check()
            target.write(chunk)


def _safe_extract_zip(archive_path, target_dir, cancel_check=None):
    with zipfile.ZipFile(archive_path, "r") as archive:
        file_count = 0
        total_uncompressed_bytes = 0

        for info in archive.infolist():
            if cancel_check:
                cancel_check()
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

        for info in archive.infolist():
            if cancel_check:
                cancel_check()
            member_path = os.path.join(target_dir, info.filename)
            if info.is_dir():
                os.makedirs(member_path, exist_ok=True)
            else:
                with archive.open(info, "r") as source:
                    _copy_archive_member(source, member_path, cancel_check)


def _safe_extract_tar(archive_path, target_dir, cancel_check=None):
    with tarfile.open(archive_path, "r:gz") as archive:
        file_count = 0
        total_uncompressed_bytes = 0

        for member in archive.getmembers():
            if cancel_check:
                cancel_check()
            destination = os.path.join(target_dir, member.name)
            if not _is_within_directory(target_dir, destination):
                raise ValueError("Archive contains invalid paths.")

            if member.issym() or member.islnk():
                raise ValueError("Archive links are not supported.")
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

        for member in archive.getmembers():
            if cancel_check:
                cancel_check()
            if member.isdir():
                os.makedirs(os.path.join(target_dir, member.name), exist_ok=True)
                continue
            if not member.isfile():
                continue
            source = archive.extractfile(member)
            if source is not None:
                with source:
                    _copy_archive_member(
                        source,
                        os.path.join(target_dir, member.name),
                        cancel_check,
                    )


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
            elif lower_name.endswith((".geojson", ".json")):
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


def _extract_site_archive_dataset(
    archive_path, filename, target_dir, cancel_check=None
):
    ext = _get_file_extension(filename)
    if ext == ".zip":
        _safe_extract_zip(archive_path, target_dir, cancel_check)
    else:
        _safe_extract_tar(archive_path, target_dir, cancel_check)
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
        except Exception:  # noqa: BLE001, S110
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
    import fiona

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
    from fiona.errors import FionaError
    from pyproj.exceptions import ProjError
    from shapely.errors import GEOSException

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
    except (FionaError, OSError, ValueError, RuntimeError) as e:
        errors.append(f"Failed to read file: {e!s}")
        return None, errors

    # Validate CRS and normalize all valid inputs to WGS84. GeoJSON follows
    # RFC 7946 when the driver omits CRS metadata.
    if gdf is not None and not gdf.empty:
        if not gdf.crs:
            if _get_file_extension(filename) in {".geojson", ".json"}:
                gdf = gdf.set_crs(epsg=4326)
            else:
                errors.append("The uploaded dataset has no known CRS.")
        else:
            try:
                if gdf.crs.to_epsg() != 4326:
                    gdf = gdf.to_crs(epsg=4326)
            except (ValueError, ProjError) as exc:
                errors.append(f"Failed to transform geometry to EPSG:4326: {exc}")

        # Repair geometries so they are valid under S2 (used by the R sf
        # package).  Two common issues:
        #   1. Consecutive duplicate vertices → "Edge N is degenerate"
        #   2. Self-intersecting rings → "Edge N crosses edge M"
        # ST_MakeValid in PostGIS fixes (2) but not (1), so we clean both
        # in Shapely before the data reaches PostGIS.
        gdf = _repair_geometries(gdf)
        geometry_errors = []
        for position, geometry in enumerate(gdf.geometry, start=1):
            try:
                validate_wgs84_polygon(geometry, row_number=position)
            except (TypeError, ValueError, GEOSException) as exc:
                geometry_errors.append(str(exc))
                if len(geometry_errors) >= 10:
                    break
        errors.extend(geometry_errors)

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
    if column_mapping is None:
        return mapping
    if not isinstance(column_mapping, dict):
        raise TypeError("Column mapping must be an object.")

    unknown_fields = set(column_mapping) - set(ALL_SITE_FIELDS)
    if unknown_fields:
        raise ValueError(
            "Unknown site mapping field(s): "
            + ", ".join(sorted(str(field) for field in unknown_fields))
        )

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
    source = gdf.copy(deep=True)
    mapped = gdf.copy(deep=True)
    errors = []
    warnings = []

    available_cols = {c for c in source.columns if c != "geometry"}
    try:
        mapping = _normalize_site_mapping(column_mapping)
    except (TypeError, ValueError) as exc:
        return None, [str(exc)], warnings

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

    # Read every field from the same immutable snapshot so swapped mappings
    # cannot overwrite a source field before the next field is copied.
    for field in ALL_SITE_FIELDS:
        src = mapping.get(field)
        if src:
            mapped[field] = source[src].copy()
        else:
            mapped[field] = pd.Series([None] * len(mapped), index=mapped.index)

    row_errors = []
    start_dates = []
    end_dates = []
    site_ids = []
    site_names = []
    for position, (index, row) in enumerate(source.iterrows(), start=1):
        error = None
        try:
            start_date = normalize_site_date(
                row[mapping["start_date"]],
                field="start_date",
                row_number=position,
                required=True,
            )
            end_date = normalize_site_date(
                row[mapping["end_date"]] if mapping["end_date"] else None,
                field="end_date",
                row_number=position,
                required=False,
            )
            if end_date is not None and end_date < start_date:
                raise ValueError(f"row {position}: end_date precedes start_date")
            site_id = normalize_site_text(
                row[mapping["site_id"]] if mapping["site_id"] else None,
                field="site_id",
                row_number=position,
            )
            site_name = normalize_site_text(
                row[mapping["site_name"]] if mapping["site_name"] else None,
                field="site_name",
                row_number=position,
            )
        except (TypeError, ValueError) as exc:
            error = str(exc)
            start_date = end_date = None
            site_id = site_name = None
        start_dates.append(start_date)
        end_dates.append(end_date)
        site_ids.append(site_id)
        site_names.append(site_name)
        row_errors.append(error)

    mapped["site_id"] = pd.Series(site_ids, index=mapped.index, dtype="string")
    mapped["site_name"] = pd.Series(site_names, index=mapped.index, dtype="string")
    mapped["start_date"] = pd.Series(start_dates, index=mapped.index, dtype=object)
    mapped["end_date"] = pd.Series(end_dates, index=mapped.index, dtype=object)
    mapped["_site_upload_row_error"] = pd.Series(
        row_errors, index=mapped.index, dtype="string"
    )
    invalid_rows = [(i + 1, error) for i, error in enumerate(row_errors) if error]
    if invalid_rows:
        detail = "; ".join(f"row {row}: {error}" for row, error in invalid_rows[:10])
        warnings.append(
            f"{len(invalid_rows)} feature(s) contain invalid mapped attributes and "
            f"will be skipped during import. {detail}"
        )

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
        "n_features": len(mapped_gdf),
        "n_missing_end_date": int(mapped_gdf["end_date"].isna().sum()),
        "n_unique_site_id": int(mapped_gdf["site_id"].nunique(dropna=True)),
        "n_missing_site_id": int(mapped_gdf["site_id"].isna().sum()),
        "n_missing_site_name": int(mapped_gdf["site_name"].isna().sum()),
        "n_invalid_features": int(mapped_gdf["_site_upload_row_error"].notna().sum()),
    }
    if summary["n_unique_site_id"] < (
        summary["n_features"] - summary["n_missing_site_id"]
    ):
        warnings.append(
            "Mapped 'site_id' contains duplicate display labels. These remain separate "
            "features with distinct internal UUID identities."
        )

    return {
        "errors": [],
        "warnings": warnings,
        "summary": summary,
    }


def _derive_site_set_name(filename):
    stem = os.path.splitext(os.path.basename(filename or "sites"))[0].strip()
    stem = stem or "sites"
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    return f"{stem}_{timestamp}"


def _upload_skip_errors_csv(upload_id, csv_path):
    """Upload a local skip-errors CSV file to S3 and return the key, or None on failure."""
    try:
        s3_key = f"{Config.S3_PREFIX.rstrip('/')}/site-upload-errors/{upload_id}.csv"
        s3 = get_s3_client()
        s3.upload_file(
            csv_path,
            Config.S3_BUCKET,
            s3_key,
            ExtraArgs={
                "ContentType": "text/csv",
                "ContentDisposition": f'attachment; filename="skip_errors_{upload_id}.csv"',
            },
        )
        return s3_key
    except Exception:
        logger.warning(
            "Failed to upload skip errors CSV to S3 for upload %s",
            upload_id,
            exc_info=True,
        )
        return None


def _site_upload_summary_row(row):
    meta = row.extra_metadata if isinstance(row.extra_metadata, dict) else {}
    return {
        "id": str(row.id),
        "filename": row.original_filename,
        "celery_task_id": row.celery_task_id,
        "site_set_id": str(row.site_set_id) if row.site_set_id else None,
        "site_set_name": row.site_set_name,
        "n_features": int(row.n_features) if row.n_features is not None else None,
        "n_sites_imported": int(row.n_sites_imported or 0),
        "phase": row.phase,
        "rows_scanned": int(row.rows_scanned or 0),
        "rows_accepted": int(row.rows_accepted or 0),
        "rows_skipped": int(row.rows_skipped or 0),
        "heartbeat_at": row.heartbeat_at.isoformat() if row.heartbeat_at else None,
        "status": row.status,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "completed_at": row.completed_at.isoformat() if row.completed_at else None,
        "error_message": row.error_message,
        "ingest_stats": meta.get("ingest_stats"),
        "diagnostics": meta.get("diagnostics"),
        "skip_errors_available": bool(meta.get("skip_errors_s3_key")),
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
    task_id = str(uuid.uuid4())
    try:
        _lock_site_upload_stage(db, upload_token)
        if not _use_shared_site_upload_stage():
            data_path, meta_path = _site_upload_stage_paths(upload_token)
            if not data_path.exists() or not meta_path.exists():
                raise ValueError(
                    "Staged upload not found. Please upload the file again."
                )
        upload = UserSiteUpload(
            user_id=user_id,
            original_filename=filename or "sites",
            n_features=int(n_features) if n_features is not None else None,
            status="pending",
            celery_task_id=task_id,
            extra_metadata={
                "upload_token": upload_token,
                "column_mapping": normalized_mapping,
                "dispatch_at": datetime.now(UTC).isoformat(),
                "dispatch_attempts": 1,
            },
        )
        db.add(upload)
        db.flush()
        upload_id = str(upload.id)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    try:
        webapp_tasks.import_user_site_upload_task.apply_async(
            task_id=task_id,
            kwargs={
                "upload_id": upload_id,
                "user_id": str(user_id),
                "upload_token": str(upload_token),
                "column_mapping": normalized_mapping,
            },
        )
    except Exception as exc:
        logger.exception("Failed to dispatch site upload %s", upload_id)
        record_site_upload_dispatch(upload_id, task_id, error=str(exc))
    else:
        record_site_upload_dispatch(upload_id, task_id)
    db = get_db()
    try:
        upload = db.query(UserSiteUpload).filter(UserSiteUpload.id == upload_id).first()
        if upload:
            return _site_upload_summary_row(upload)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    raise ValueError("Upload job disappeared after it was queued.")


def record_site_upload_dispatch(upload_id, task_id, *, error=None):
    """Record a broker outcome without changing lifecycle or newer reservations."""
    db = get_db()
    try:
        upload = (
            db.query(UserSiteUpload)
            .filter(
                UserSiteUpload.id == upload_id,
                UserSiteUpload.celery_task_id == task_id,
                UserSiteUpload.status.in_(("pending", "running")),
            )
            .with_for_update(key_share=True)
            .first()
        )
        if upload:
            metadata = dict(upload.extra_metadata or {})
            if error is None:
                metadata["dispatch_confirmed_at"] = datetime.now(UTC).isoformat()
                metadata.pop("dispatch_error", None)
                upload.error_message = None
            else:
                metadata["dispatch_error"] = error[:2000]
                upload.error_message = (
                    f"Task dispatch failed; recovery will retry: {error}"[:2000]
                )
            upload.extra_metadata = metadata
            db.commit()
    except Exception:
        db.rollback()
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


def delete_user_site_upload(upload_id, user_id):
    """Delete a terminal (cancelled or failed) upload job record.

    Only uploads in ``cancelled`` or ``failed`` state with no associated site
    set can be deleted.  Completed uploads are managed via their site set.
    """
    db = get_db()
    try:
        upload = (
            db.query(UserSiteUpload)
            .filter(UserSiteUpload.id == upload_id, UserSiteUpload.user_id == user_id)
            .with_for_update(key_share=True)
            .first()
        )
        if not upload:
            return False, "Upload job not found."

        if upload.status not in {"cancelled", "failed"}:
            return (
                False,
                f"Only cancelled or failed uploads can be deleted (current status: '{upload.status}').",
            )

        if upload.site_set_id:
            return (
                False,
                "This upload has an associated site set. Delete the site set instead.",
            )

        db.delete(upload)
        db.commit()
        return True, "Upload record deleted."
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def cancel_user_site_upload(upload_id, user_id):
    """Cancel a user-owned background site import job.

    Cancellation is durable and cooperative. The task is revoked without
    terminating its worker process; the importer checks the status between
    phases and before atomic publication.
    """
    task_id = None
    db = get_db()
    try:
        upload = (
            db.query(UserSiteUpload)
            .filter(UserSiteUpload.id == upload_id, UserSiteUpload.user_id == user_id)
            .with_for_update(key_share=True)
            .first()
        )
        if not upload:
            return False, "Upload job not found."

        if upload.status in {"completed", "failed", "cancelled"}:
            return False, f"Upload is already {upload.status}."

        if upload.status not in {"pending", "running"}:
            return False, f"Upload cannot be cancelled from status '{upload.status}'."

        task_id = upload.celery_task_id
        upload.status = "cancelled"
        upload.phase = "cancelled"
        upload.completed_at = datetime.now(UTC)
        upload.error_message = "Cancelled by admin."
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    if task_id:
        try:
            webapp_tasks.import_user_site_upload_task.AsyncResult(task_id).revoke(
                terminate=False
            )
        except Exception:
            logger.warning(
                "Failed to revoke queued upload task %s", task_id, exc_info=True
            )
    return True, "Upload cancelled."


def claim_user_site_upload(
    upload_id,
    user_id,
    *,
    lease_seconds=SITE_UPLOAD_WORKER_LEASE_SECONDS,
    max_attempts=3,
):
    """Atomically claim a pending upload or one with an expired worker lease."""
    db = get_db()
    try:
        claimed = db.execute(
            text(
                """
                UPDATE user_site_uploads
                SET status = 'running',
                    phase = 'loading',
                    started_at = COALESCE(started_at, now()),
                    heartbeat_at = now(),
                    attempt_count = attempt_count + 1
                WHERE id = :upload_id
                  AND user_id = :user_id
                  AND attempt_count < :max_attempts
                  AND (
                    status = 'pending'
                    OR (
                      status = 'running'
                      AND (
                        COALESCE(heartbeat_at, started_at, created_at)
                            < now() - (:lease_seconds * interval '1 second')
                      )
                    )
                  )
                RETURNING id
                """
            ),
            {
                "upload_id": str(upload_id),
                "user_id": str(user_id),
                "max_attempts": max_attempts,
                "lease_seconds": lease_seconds,
            },
        ).scalar_one_or_none()
        if claimed:
            db.commit()
            return {"claimed": True, "status": "running"}

        upload = (
            db.query(UserSiteUpload)
            .filter(
                UserSiteUpload.id == upload_id,
                UserSiteUpload.user_id == user_id,
            )
            .with_for_update(key_share=True)
            .first()
        )
        if not upload:
            return {"claimed": False, "status": "missing"}

        if (
            upload.status == "running"
            and upload.attempt_count >= max_attempts
            and (upload.heartbeat_at or upload.started_at or upload.created_at)
            < datetime.now(UTC) - timedelta(seconds=lease_seconds)
        ):
            upload.status = "failed"
            upload.phase = "failed"
            upload.completed_at = datetime.now(UTC)
            upload.error_message = (
                "Upload worker stopped and exhausted its retry limit."
            )
            db.commit()
            return {"claimed": False, "status": "failed"}
        return {
            "claimed": False,
            "status": upload.status,
            "site_set_id": str(upload.site_set_id) if upload.site_set_id else None,
            "site_set_name": upload.site_set_name,
            "n_sites": int(upload.n_sites_imported or 0),
        }
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _assert_site_upload_running(upload_id):
    """Raise when a durable upload job was cancelled or disappeared."""
    if upload_id is None:
        return
    db = get_db()
    try:
        status = db.execute(
            text("SELECT status FROM user_site_uploads WHERE id = :upload_id"),
            {"upload_id": str(upload_id)},
        ).scalar_one_or_none()
    finally:
        db.close()
    if status != "running":
        raise _SiteUploadCancelled(f"Upload {upload_id} is no longer active.")


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
    phase=None,
    rows_scanned=None,
    rows_accepted=None,
    rows_skipped=None,
    heartbeat_at=None,
    diagnostics=None,
    skip_errors_s3_key=None,
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
    phase : str | None
        Current import phase.
    rows_scanned, rows_accepted, rows_skipped : int | None
        Import progress counters.
    heartbeat_at : datetime | None
        Latest worker heartbeat for lease recovery.
    diagnostics : dict | None
        Bounded examples and failure context, stored as JSON metadata.

    Returns
    -------
    bool
        ``True`` when updated; ``False`` when missing or terminal-state guarded.
    """
    db = get_db()
    try:
        # NO KEY UPDATE coexists with the import transaction's source-upload FK.
        upload = (
            db.query(UserSiteUpload)
            .filter(UserSiteUpload.id == upload_id)
            .with_for_update(key_share=True)
            .first()
        )
        if not upload:
            return False

        terminal_statuses = {"completed", "failed", "cancelled"}
        if upload.status in terminal_statuses and status != upload.status:
            return False
        if status == "running" and upload.status not in {"pending", "running"}:
            return False

        upload.status = status
        if phase is not None:
            upload.phase = phase
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
        if rows_scanned is not None:
            upload.rows_scanned = int(rows_scanned)
        if rows_accepted is not None:
            upload.rows_accepted = int(rows_accepted)
        if rows_skipped is not None:
            upload.rows_skipped = int(rows_skipped)
        if heartbeat_at is not None:
            upload.heartbeat_at = heartbeat_at
        if error_message is not None:
            upload.error_message = error_message[:2000] if error_message else None
        if (
            ingest_stats is not None
            or diagnostics is not None
            or skip_errors_s3_key is not None
        ):
            metadata = dict(upload.extra_metadata or {})
            if ingest_stats is not None:
                metadata["ingest_stats"] = ingest_stats
            if diagnostics is not None:
                metadata["diagnostics"] = diagnostics
            if skip_errors_s3_key is not None:
                metadata["skip_errors_s3_key"] = skip_errors_s3_key
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
    _skip_csv_path = (
        None  # temp file for incremental skip-error CSV; cleaned up in finally
    )
    _skip_csv_file = (
        None  # file handle; kept in outer scope so finally can close on exception
    )
    try:
        _assert_site_upload_running(upload_id)
        bytes_since_download_check = 0

        def _download_progress(bytes_transferred):
            nonlocal bytes_since_download_check
            bytes_since_download_check += bytes_transferred
            if bytes_since_download_check >= 8 * 1024 * 1024:
                _assert_site_upload_running(upload_id)
                if upload_id is not None:
                    update_user_site_upload_status(
                        upload_id,
                        status="running",
                        phase="downloading",
                        heartbeat_at=datetime.now(UTC),
                    )
                bytes_since_download_check = 0

        if upload_id is not None:
            update_user_site_upload_status(
                upload_id,
                status="running",
                phase="downloading",
                heartbeat_at=datetime.now(UTC),
            )
        local_path, filename, file_size_bytes = _materialize_staged_upload_path(
            upload_token, user_id, progress_callback=_download_progress
        )
        _assert_site_upload_running(upload_id)
        mapping_to_apply = _normalize_site_mapping(column_mapping)
        source_path = local_path
        source_name = filename
        ext = _get_file_extension(filename)

        if ext in {".zip", ".tar.gz", ".tgz"}:
            if upload_id is not None:
                update_user_site_upload_status(
                    upload_id,
                    status="running",
                    phase="extracting",
                    heartbeat_at=datetime.now(UTC),
                )

            last_archive_check = monotonic()

            def _check_archive_cancellation():
                nonlocal last_archive_check
                if monotonic() - last_archive_check < 5:
                    return
                _assert_site_upload_running(upload_id)
                if upload_id is not None:
                    update_user_site_upload_status(
                        upload_id,
                        status="running",
                        phase="extracting",
                        heartbeat_at=datetime.now(UTC),
                    )
                last_archive_check = monotonic()

            extracted_archive_dir = tempfile.mkdtemp(prefix="ae_site_upload_extract_")
            source_path = _extract_site_archive_dataset(
                local_path,
                filename,
                extracted_archive_dir,
                cancel_check=_check_archive_cancellation,
            )
            source_name = os.path.basename(source_path)
            ext = _get_file_extension(source_name)
        _assert_site_upload_running(upload_id)

        # Stream supported datasets directly so Celery only holds a small insert
        # batch in memory at a time, even when the upload arrived as an archive.
        use_streaming_ingest = ext in {".shp", ".gpkg", ".geojson", ".json"}

        db = get_db()
        try:
            if upload_id is not None:
                existing_site_set = (
                    db.query(UserSiteSet)
                    .filter(UserSiteSet.source_upload_id == upload_id)
                    .first()
                )
                if existing_site_set:
                    db.rollback()
                    return get_user_site_set_detail(existing_site_set.id, user_id)
            site_set = UserSiteSet(
                user_id=user_id,
                name=_derive_site_set_name(filename),
                original_filename=filename,
                file_size_bytes=int(file_size_bytes or 0),
                file_format=_get_file_extension(filename).lstrip("."),
                n_sites=0,
                bounds=None,
                source_upload_id=upload_id,
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
            rows_scanned = 0
            rows_skipped = 0
            bounds_minx = None
            bounds_miny = None
            bounds_maxx = None
            bounds_maxy = None
            skipped_by_reason = {}
            _skip_error_examples = []
            _skip_csv_writer = None
            if upload_id is not None and Config.S3_BUCKET:
                try:
                    fd, _skip_csv_path = tempfile.mkstemp(
                        suffix=".csv", prefix="ae_skip_errors_"
                    )
                    _skip_csv_file = os.fdopen(fd, "w", encoding="utf-8", newline="")
                    _skip_csv_writer = csv.DictWriter(
                        _skip_csv_file,
                        fieldnames=[
                            "row_number",
                            "site_id",
                            "site_name",
                            "reason",
                            "value",
                        ],
                    )
                    _skip_csv_writer.writeheader()
                except Exception:
                    logger.warning(
                        "Failed to create skip errors temp file", exc_info=True
                    )

            batch_rows = []
            batch_row_bytes = 0
            last_progress_rows = 0
            last_progress_at = monotonic()

            def _report_upload_progress(force=False):
                nonlocal last_progress_rows, last_progress_at
                if upload_id is None:
                    return
                now = monotonic()
                if not force and (
                    rows_scanned - last_progress_rows < SITE_UPLOAD_PROGRESS_BATCH_SIZE
                    and now - last_progress_at < SITE_UPLOAD_PROGRESS_INTERVAL_SECONDS
                ):
                    return
                ingest_stats = {
                    "rows_scanned": int(rows_scanned),
                    "rows_accepted": int(n_sites),
                    "rows_skipped": int(rows_skipped),
                    "skipped_by_reason": dict(skipped_by_reason),
                }
                update_user_site_upload_status(
                    upload_id,
                    status="running",
                    phase="importing",
                    n_sites_imported=n_sites,
                    rows_scanned=rows_scanned,
                    rows_accepted=n_sites,
                    rows_skipped=rows_skipped,
                    heartbeat_at=datetime.now(UTC),
                    ingest_stats=ingest_stats,
                )
                _assert_site_upload_running(upload_id)
                last_progress_rows = rows_scanned
                last_progress_at = now

            def _maybe_report_upload_progress():
                if rows_scanned % SITE_UPLOAD_PROGRESS_CHECK_ROW_INTERVAL == 0:
                    _report_upload_progress()

            def _record_skip(
                feature_index, reason, *, category, site_id="", site_name="", value=""
            ):
                nonlocal rows_skipped
                rows_skipped += 1
                skipped_by_reason[category] = skipped_by_reason.get(category, 0) + 1
                if len(_skip_error_examples) < 10:
                    _skip_error_examples.append(f"row {feature_index}: {reason}")
                if _skip_csv_writer is not None:

                    def _as_text(value):
                        if value is None:
                            return ""
                        try:
                            missing = pd.isna(value)
                            if not hasattr(missing, "__len__") and bool(missing):
                                return ""
                        except (TypeError, ValueError):
                            pass
                        return str(value)

                    _skip_csv_writer.writerow(
                        {
                            "row_number": feature_index,
                            "site_id": _as_text(site_id)[:100],
                            "site_name": _as_text(site_name)[:255],
                            "reason": reason,
                            "value": _as_text(value)[:500],
                        }
                    )

            def _flush_batch_rows():
                nonlocal batch_row_bytes
                if not batch_rows:
                    return
                _assert_site_upload_running(upload_id)
                db.execute(insert_sql, batch_rows)
                batch_rows.clear()
                batch_row_bytes = 0
                _report_upload_progress()

            if use_streaming_ingest:
                from pyproj import Transformer
                from pyproj.exceptions import ProjError
                from shapely.errors import GEOSException
                from shapely.geometry import shape
                from shapely.ops import transform as shapely_transform

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

                    for field in ALL_SITE_FIELDS:
                        src_col = mapping_to_apply.get(field)
                        if src_col and src_col not in source_columns:
                            raise ValueError(
                                f"Selected source column for '{field}' was not found: "
                                f"{src_col}"
                            )

                    source_crs = src.crs_wkt or src.crs
                    if not source_crs:
                        if ext in {".geojson", ".json"}:
                            source_crs = "EPSG:4326"
                        else:
                            raise ValueError("The uploaded dataset has no known CRS.")
                    try:
                        to_wgs84 = Transformer.from_crs(
                            source_crs, "EPSG:4326", always_xy=True
                        ).transform
                    except Exception as exc:
                        raise ValueError(f"Invalid source CRS: {exc}") from exc

                    reserved_site_ids = set()
                    if mapping_to_apply.get("site_id"):
                        last_identifier_check = monotonic()
                        for feature_index, feature in enumerate(src, start=1):
                            value = (feature.get("properties") or {}).get(
                                mapping_to_apply["site_id"]
                            )
                            if value is not None and str(value).strip():
                                reserved_site_ids.add(str(value).strip())
                            if (
                                feature_index % 1000 == 0
                                or monotonic() - last_identifier_check >= 5
                            ):
                                _assert_site_upload_running(upload_id)
                                if upload_id is not None:
                                    update_user_site_upload_status(
                                        upload_id,
                                        status="running",
                                        phase="reserving_ids",
                                        heartbeat_at=datetime.now(UTC),
                                    )
                                last_identifier_check = monotonic()
                    generated_site_ids = set()
                    generated_id_counter = 0
                    missing_site_name_count = 0

                if upload_id is not None:
                    update_user_site_upload_status(
                        upload_id,
                        status="running",
                        phase="validating",
                        heartbeat_at=datetime.now(UTC),
                    )

                with _open_site_feature_source(source_path) as src:
                    for feature_index, feature in enumerate(src, start=1):
                        rows_scanned = feature_index
                        props = dict(feature.get("properties") or {})
                        raw_site_id = props.get(mapping_to_apply.get("site_id"))
                        raw_site_name = props.get(mapping_to_apply.get("site_name"))

                        try:
                            site_id = normalize_site_text(
                                raw_site_id,
                                field="site_id",
                                row_number=feature_index,
                            )
                            site_name = normalize_site_text(
                                raw_site_name,
                                field="site_name",
                                row_number=feature_index,
                            )
                            start_date = normalize_site_date(
                                props.get(mapping_to_apply["start_date"]),
                                field="start_date",
                                row_number=feature_index,
                                required=True,
                            )
                            end_date = normalize_site_date(
                                props.get(mapping_to_apply["end_date"])
                                if mapping_to_apply.get("end_date")
                                else None,
                                field="end_date",
                                row_number=feature_index,
                                required=False,
                            )
                            if end_date is not None and end_date < start_date:
                                raise ValueError(
                                    f"row {feature_index}: end_date precedes start_date"
                                )
                        except (TypeError, ValueError) as exc:
                            message = str(exc)
                            if "start_date is required" in message:
                                category = "missing_required"
                            elif "start_date" in message or "end_date" in message:
                                category = "bad_date"
                            else:
                                category = "bad_text"
                            _record_skip(
                                feature_index,
                                message,
                                category=category,
                                site_id=raw_site_id,
                                site_name=raw_site_name,
                            )
                            _maybe_report_upload_progress()
                            continue

                        if site_id is None:
                            while True:
                                generated_id_counter += 1
                                candidate = f"MissingID{generated_id_counter}"
                                if (
                                    candidate not in reserved_site_ids
                                    and candidate not in generated_site_ids
                                ):
                                    site_id = candidate
                                    generated_site_ids.add(candidate)
                                    break
                        if site_name is None:
                            missing_site_name_count += 1
                            site_name = f"Missing_Site_Name_{missing_site_name_count}"

                        geom_json = feature.get("geometry")
                        if not geom_json:
                            _record_skip(
                                feature_index,
                                "Missing geometry",
                                category="bad_geometry",
                                site_id=site_id,
                                site_name=site_name,
                            )
                            _maybe_report_upload_progress()
                            continue

                        try:
                            geom = shape(geom_json)
                            geom = shapely_transform(to_wgs84, geom)
                            geom = _repair_geometry_single(geom)
                            validate_wgs84_polygon(geom, row_number=feature_index)
                        except (ValueError, TypeError, ProjError, GEOSException) as exc:
                            _record_skip(
                                feature_index,
                                f"Invalid polygon geometry: {exc}",
                                category="bad_geometry",
                                site_id=site_id,
                                site_name=site_name,
                            )
                            _maybe_report_upload_progress()
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
                raise ValueError(
                    f"Unsupported site upload dataset format for streaming: {ext}"
                )

            _flush_batch_rows()
            _report_upload_progress(force=True)

            # Close the incremental CSV file (if open) before any post-loop logic.
            if _skip_csv_file is not None:
                _skip_csv_file.close()
                _skip_csv_file = None

            skipped_total = rows_skipped
            ingest_stats = {
                "rows_scanned": int(rows_scanned),
                "rows_accepted": int(n_sites),
                "rows_skipped": int(rows_skipped),
                "skipped_by_reason": dict(skipped_by_reason),
            }
            diagnostics = {
                "examples": _skip_error_examples,
                "input_rows": int(rows_scanned),
                "valid_rows": int(n_sites),
            }
            skip_errors_s3_key = None
            if (
                skipped_total > 0
                and _skip_csv_path is not None
                and upload_id is not None
            ):
                skip_errors_s3_key = _upload_skip_errors_csv(upload_id, _skip_csv_path)

            if upload_id is not None:
                update_user_site_upload_status(
                    upload_id,
                    status="running",
                    phase="finalizing" if n_sites else "validating",
                    n_sites_imported=n_sites,
                    rows_scanned=rows_scanned,
                    rows_accepted=n_sites,
                    rows_skipped=rows_skipped,
                    heartbeat_at=datetime.now(UTC),
                    ingest_stats=ingest_stats,
                    diagnostics=diagnostics,
                    skip_errors_s3_key=skip_errors_s3_key,
                )
                _assert_site_upload_running(upload_id)

            if n_sites == 0:
                if skipped_total > 0:
                    details = (
                        "No valid features were found after applying the selected mapping. "
                        f"Scanned {rows_scanned}; skipped {rows_skipped}: "
                        f"{skipped_by_reason}."
                    )
                    if _skip_error_examples:
                        details += f" First examples: {'; '.join(_skip_error_examples)}"
                    raise ValueError(details)
                raise ValueError("No features were found in the uploaded file.")

            if skipped_total > 0:
                logger.warning(
                    "Site upload saved with skipped features: site_set_id=%s "
                    "scanned=%s accepted=%s skipped=%s reasons=%s",
                    site_set.id,
                    rows_scanned,
                    n_sites,
                    skipped_total,
                    skipped_by_reason,
                )
                metadata = dict(site_set.extra_metadata or {})
                metadata["ingest_stats"] = ingest_stats
                metadata["diagnostics"] = diagnostics
                site_set.extra_metadata = metadata

            site_set.n_sites = n_sites
            if bounds_minx is not None:
                site_set.bounds = {
                    "west": bounds_minx,
                    "south": bounds_miny,
                    "east": bounds_maxx,
                    "north": bounds_maxy,
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

            if upload_id is not None:
                upload_row = (
                    db.query(UserSiteUpload)
                    .filter(UserSiteUpload.id == upload_id)
                    .with_for_update(key_share=True)
                    .first()
                )
                if upload_row is None or upload_row.status != "running":
                    raise _SiteUploadCancelled(
                        f"Upload {upload_id} was cancelled before publication."
                    )
                upload_row.status = "completed"
                upload_row.phase = "completed"
                upload_row.completed_at = datetime.now(UTC)
                upload_row.heartbeat_at = upload_row.completed_at
                upload_row.site_set_id = site_set.id
                upload_row.site_set_name = site_set.name
                upload_row.n_sites_imported = n_sites
                upload_row.rows_scanned = rows_scanned
                upload_row.rows_accepted = n_sites
                upload_row.rows_skipped = rows_skipped
                upload_row.error_message = None
                upload_metadata = dict(upload_row.extra_metadata or {})
                upload_metadata["ingest_stats"] = ingest_stats
                upload_metadata["diagnostics"] = diagnostics
                if skip_errors_s3_key:
                    upload_metadata["skip_errors_s3_key"] = skip_errors_s3_key
                upload_row.extra_metadata = upload_metadata

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

        if _skip_csv_file is not None:
            try:
                _skip_csv_file.close()
            except Exception:  # noqa: BLE001, S110
                pass
        if _skip_csv_path and os.path.exists(_skip_csv_path):
            try:
                os.unlink(_skip_csv_path)
            except OSError:
                logger.debug(
                    "Failed to remove skip errors temp file: %s", _skip_csv_path
                )
