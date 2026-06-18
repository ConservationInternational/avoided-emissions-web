"""Site upload staging, file parsing, column mapping, and DB persistence."""

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
from pathlib import Path
from time import monotonic

import geopandas as gpd
import pandas as pd
from botocore.exceptions import ClientError
from sqlalchemy import text

from config import Config
from models import (
    UserSiteSet,
    UserSiteUpload,
    get_db,
)

from services.s3 import get_s3_client
from services.site_set import get_user_site_set_detail
import tasks as webapp_tasks

logger = logging.getLogger(__name__)

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
    # Use S3 whenever a bucket is configured so that the webapp and Celery
    # worker containers (which have separate filesystems) share access to the
    # staged upload.  The local-filesystem fallback is only safe when both
    # processes run in the same container or the same filesystem namespace.
    return bool(Config.S3_BUCKET)


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
