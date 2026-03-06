"""Service layer for interacting with the trends.earth API and GEE.

Provides functions for submitting analysis tasks, checking job status,
uploading site files, and managing GEE covariate exports. Used by the
Dash callbacks to keep business logic out of the UI layer.
"""

import io
import json
import logging
import os
import tarfile
import tempfile
import uuid
import zipfile
from datetime import datetime, timedelta, timezone

import boto3
import geopandas as gpd
import pandas as pd
from sqlalchemy import text

from config import Config, report_exception
from models import (
    AnalysisTask,
    Covariate,
    CovariatePreset,
    TaskResult,
    TaskResultTotal,
    TaskSite,
    UserSiteSet,
    get_db,
)

logger = logging.getLogger(__name__)

ALLOWED_MATCHING_JOB_QUEUES = {
    "spot_fleet_1TB-io2-disk",
    "ondemand_fleet_1TB-io2-disk",
}

DEFAULT_MATCHING_JOB_QUEUE = "spot_fleet_1TB-io2-disk"

MAX_ARCHIVE_FILE_COUNT = 2_000
MAX_ARCHIVE_TOTAL_UNCOMPRESSED_BYTES = 200 * 1024 * 1024
MAX_ARCHIVE_MEMBER_UNCOMPRESSED_BYTES = 50 * 1024 * 1024
MAX_ARCHIVE_COMPRESSION_RATIO = 200.0


def get_s3_client():
    return boto3.client("s3", region_name=Config.AWS_REGION)


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

    for root, _dirs, files in os.walk(directory):
        for filename in files:
            lower_name = filename.lower()
            full_path = os.path.join(root, filename)
            if lower_name.endswith(".shp"):
                shapefiles.append(full_path)
            elif lower_name.endswith(".gpkg"):
                geopackages.append(full_path)
            elif lower_name.endswith(".geojson") or lower_name.endswith(".json"):
                geojsons.append(full_path)

    return sorted(shapefiles), sorted(geopackages), sorted(geojsons)


def _read_sites_from_archive(file_content, filename):
    ext = _get_file_extension(filename)

    with tempfile.TemporaryDirectory() as tmpdir:
        archive_name = filename or f"sites{ext}"
        archive_path = os.path.join(tmpdir, os.path.basename(archive_name))

        with open(archive_path, "wb") as archive_file:
            archive_file.write(file_content)

        if ext == ".zip":
            _safe_extract_zip(archive_path, tmpdir)
        else:
            _safe_extract_tar(archive_path, tmpdir)

        shapefiles, geopackages, geojsons = _find_supported_dataset_paths(tmpdir)
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
            return gpd.read_file(shapefiles[0])
        if geopackages:
            return gpd.read_file(geopackages[0])
        return gpd.read_file(geojsons[0], driver="GeoJSON")


def parse_sites_file(file_content, filename):
    """Parse uploaded site files into a GeoDataFrame.

    Validates required columns and geometry types. Returns the GeoDataFrame
    and a list of validation errors (empty if valid).
    """
    errors = []
    gdf = None

    try:
        ext = _get_file_extension(filename)
        if ext in (".geojson", ".json"):
            gdf = gpd.read_file(io.BytesIO(file_content), driver="GeoJSON")
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

    # Validate required columns
    required = {"site_id", "site_name", "start_date"}
    missing = required - set(gdf.columns)
    if missing:
        errors.append(f"Missing required columns: {', '.join(missing)}")

    # Validate geometries
    if gdf is not None and not gdf.empty:
        bad_type = ~gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])
        if bad_type.any():
            bad_rows = gdf[bad_type]
            details = [
                f"Feature {idx}: geometry type={row.geometry.geom_type}"
                for idx, row in bad_rows.iterrows()
            ]
            errors.append(
                "All geometries must be Polygon or MultiPolygon.\n"
                + "\n".join(details[:10])
            )
        # Ensure EPSG:4326
        if gdf.crs and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)

    return gdf, errors


def _derive_site_set_name(filename):
    stem = os.path.splitext(os.path.basename(filename or "sites"))[0].strip()
    stem = stem or "sites"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{stem}_{timestamp}"


def _site_set_summary_row(row):
    return {
        "id": str(row.id),
        "name": row.name,
        "filename": row.original_filename,
        "uploaded_at": row.uploaded_at.isoformat() if row.uploaded_at else None,
        "n_sites": row.n_sites or 0,
        "file_size_bytes": int(row.file_size_bytes or 0),
        "file_format": row.file_format,
    }


def save_user_site_set(user_id, filename, file_content):
    """Persist uploaded sites as a reusable PostGIS-backed user site set.

    Geometries are repaired with ``ST_MakeValid`` and coerced to
    ``MULTIPOLYGON`` before storage.
    """
    gdf, errors = parse_sites_file(file_content, filename)
    if errors:
        raise ValueError("\n".join(errors))
    if gdf is None or gdf.empty:
        raise ValueError("No features were found in the uploaded file.")

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


def list_user_site_sets(user_id):
    """Return reusable site sets for a user ordered by most recent first."""
    db = get_db()
    try:
        site_sets = (
            db.query(UserSiteSet)
            .filter(UserSiteSet.user_id == user_id)
            .order_by(UserSiteSet.uploaded_at.desc())
            .all()
        )
        return [_site_set_summary_row(row) for row in site_sets]
    finally:
        db.close()


def get_user_site_set_geojson(site_set_id):
    """Export a user site set from PostGIS as a GeoJSON FeatureCollection."""
    db = get_db()
    try:
        row = db.execute(
            text(
                """
                SELECT jsonb_build_object(
                    'type', 'FeatureCollection',
                    'features', COALESCE(jsonb_agg(
                        jsonb_build_object(
                            'type', 'Feature',
                            'geometry', ST_AsGeoJSON(f.geom)::jsonb,
                            'properties', jsonb_build_object(
                                'site_id', f.site_id,
                                'site_name', f.site_name,
                                'start_date', to_char(f.start_date, 'YYYY-MM-DD'),
                                'end_date', CASE
                                    WHEN f.end_date IS NULL THEN NULL
                                    ELSE to_char(f.end_date, 'YYYY-MM-DD')
                                END,
                                'area_ha', f.area_ha
                            )
                        )
                        ORDER BY f.site_id
                    ), '[]'::jsonb)
                )
                FROM user_site_features f
                WHERE f.site_set_id = :site_set_id
                """
            ),
            {"site_set_id": str(site_set_id)},
        ).fetchone()
        return (
            row[0] if row and row[0] else {"type": "FeatureCollection", "features": []}
        )
    finally:
        db.close()


def get_user_site_set_detail(site_set_id, user_id):
    """Return full details for one user-owned site set, including preview rows."""
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
                "site_id": r.site_id,
                "site_name": r.site_name,
                "start_date": r.start_date.isoformat() if r.start_date else "",
                "end_date": r.end_date.isoformat() if r.end_date else "",
            }
            for r in rows
        ]

        geojson_fc = get_user_site_set_geojson(site_set_id)

        return {
            **_site_set_summary_row(site_set),
            "geojson": json.dumps(geojson_fc),
            "preview_rows": preview_rows,
        }
    finally:
        db.close()


def get_user_site_set_gdf(site_set_id, user_id=None):
    """Load one site set as a GeoDataFrame."""
    geojson_fc = get_user_site_set_geojson(site_set_id)
    gdf = gpd.GeoDataFrame.from_features(
        geojson_fc.get("features", []), crs="EPSG:4326"
    )
    if gdf.empty:
        raise ValueError("Selected site set has no site geometries.")
    if user_id is not None:
        db = get_db()
        try:
            exists = (
                db.query(UserSiteSet)
                .filter(UserSiteSet.id == site_set_id, UserSiteSet.user_id == user_id)
                .first()
            )
            if not exists:
                raise ValueError("Site set not found.")
        finally:
            db.close()
    return gdf


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
                "This site set is linked to submitted tasks and cannot be deleted.",
            )

        db.delete(site_set)
        db.commit()
        return True, "Site set deleted."
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
    )
    return f"s3://{Config.S3_BUCKET}/{key}"


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


def compute_matching_extent(
    gdf: gpd.GeoDataFrame,
    exact_match_vars: list[str],
) -> dict | None:
    """Compute the spatial extent for control-pixel selection.

    For each polygon-type exact-match variable the function queries
    PostGIS to find every polygon that intersects any of the treatment
    *sites*.  The per-layer polygons are unioned, then all layers are
    intersected together.  The resulting geometry is the tightest
    bounding area in which a pixel could share the same exact-match
    attribute values as at least one treatment site.

    Binary variables (``pa``) do not contribute to the extent because
    they partition all of space into only two classes and therefore
    provide no spatial restriction.

    Returns a GeoJSON-compatible dict (``{"type": "...", ...}``) or
    ``None`` when no polygon-type variables are selected.
    """
    from shapely.geometry import mapping, shape
    from sqlalchemy import text

    polygon_vars = [v for v in exact_match_vars if v in _EXTENT_TABLE_MAP]
    if not polygon_vars:
        return None

    # Build a single GeoJSON geometry representing all sites
    sites_geojson = json.dumps(mapping(gdf.unary_union))

    _SAFE_ID = __import__("re").compile(r"^[a-z_][a-z0-9_]*$")

    db = get_db()
    try:
        layer_extents = []
        for var_name in polygon_vars:
            table = _EXTENT_TABLE_MAP[var_name]
            # Safety: table comes from the hardcoded _EXTENT_TABLE_MAP —
            # assert it matches a safe identifier pattern to prevent SQL
            # injection if the dict is ever populated from external input.
            assert _SAFE_ID.match(table), f"Unsafe table name: {table}"
            result = db.execute(
                text(
                    f"SELECT ST_AsGeoJSON(ST_Union(ST_MakeValid(geom))) "
                    f"FROM {table} "
                    f"WHERE ST_Intersects("
                    f"  ST_MakeValid(geom), "
                    f"  ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON(:sites), 4326))"
                    f")"
                ),
                {"sites": sites_geojson},
            )
            row = result.fetchone()
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

        return mapping(extent)

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
    max_treatment_pixels=1000,
    control_multiplier=50,
    min_site_area_ha=100,
    min_glm_treatment_pixels=15,
    caliper_width=0.2,
    max_controls_per_treatment=1,
    match_memory_mib=30720,
    matching_job_queue=DEFAULT_MATCHING_JOB_QUEUE,
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
            "submitting analysis tasks.  Go to Settings \u2192 "
            "trends.earth Integration to connect your account."
        )
    script_id = Config.TRENDSEARTH_SCRIPT_ID
    if not script_id:
        raise ValueError(
            "TRENDSEARTH_SCRIPT_ID is not configured. Set this "
            "environment variable to the UUID of the avoided-emissions "
            "script registered on the trends.earth API."
        )

    # Compute the matching extent polygon from PostGIS
    matching_extent = compute_matching_extent(gdf, exact_match_vars)

    if fc_years is None:
        fc_years = list(range(2000, 2024))

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
                "match_memory_mib": match_memory_mib,
                "matching_job_queue": matching_job_queue,
            },
            covariates=covariates,
            n_sites=len(gdf),
        )
        db.add(task)

        for _, row in gdf.iterrows():
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
            )
            db.add(site)
        db.commit()

        logger.info(
            "[SUBMIT] Task %s: DB record created, uploading sites to S3", task_id
        )

        # Upload sites to S3 (prefer PostGIS-exported GeoJSON from a persisted set)
        if site_set_id:
            sites_uri = upload_user_site_set_geojson_to_s3(site_set_id, task_id)
        else:
            sites_uri = upload_sites_to_s3(gdf, task_id)
        logger.info("[SUBMIT] Task %s: sites uploaded to %s", task_id, sites_uri)

        # Build params matching AvoidedEmissionsParams schema
        params = {
            "task_id": task_id,
            "sites_s3_uri": sites_uri,
            "cog_bucket": Config.S3_BUCKET,
            "cog_prefix": f"{Config.S3_PREFIX}/cog",
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
            # For single-site jobs, skip the pipeline entirely and run
            # all steps in one container (step="all") — this avoids
            # the overhead of S3 intermediate data transfer and extra
            # job scheduling.  The R analysis container handles both
            # modes via the ``step`` parameter.
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
                            "array_size": len(gdf),
                            "timeout_seconds": 14400,  # 4 h per site
                            "memory_mib": match_memory_mib,
                            "vcpus": max(2, match_memory_mib // 15360),
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
                if len(gdf) > 1
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
        client_id, client_secret = user_creds
        client = TrendsEarthClient.from_oauth2_credentials(
            api_url=Config.TRENDSEARTH_API_URL,
            client_id=client_id,
            client_secret=client_secret,
        )
        logger.info(
            "[SUBMIT] Task %s: calling trends.earth API (script=%s, "
            "api_url=%s, batch_overrides=%s)",
            task_id,
            script_id,
            Config.TRENDSEARTH_API_URL,
            batch_overrides if batch_overrides else "none",
        )
        execution = client.create_execution(script_id, params)

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
        task.results_s3_uri = params["results_s3_uri"]
        task.status = "submitted"
        task.submitted_at = datetime.now(timezone.utc)
        # Store the API execution ID in a new-ish field; reuse
        # extract_job_id since we no longer need the Batch job IDs.
        task.extract_job_id = f"api:{exec_id}"
        db.commit()
        logger.info(
            "[SUBMIT] Task %s: status → submitted (tracking as api:%s)",
            task_id,
            exec_id,
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


def get_task_list(user_id=None, limit=50):
    """Get recent analysis tasks, optionally filtered by user."""
    db = get_db()
    try:
        query = db.query(AnalysisTask).order_by(AnalysisTask.created_at.desc())
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
        without_scheme = sites_s3_uri[5:]
        bucket, _, key = without_scheme.partition("/")
        if not bucket or not key:
            return None
        s3 = get_s3_client()
        response = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except Exception:
        logger.warning(
            "Failed to download sites GeoJSON from %s", sites_s3_uri, exc_info=True
        )
        return None


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
            task_obj.extra_metadata = meta

        # Delete any existing results (idempotent re-import)
        db.query(TaskResult).filter(TaskResult.task_id == task_id).delete()
        db.query(TaskResultTotal).filter(TaskResultTotal.task_id == task_id).delete()

        # --- per-site-year time series → TaskResult ---
        time_series = results_payload.get("time_series") or []
        for ts in time_series:
            values = ts.get("values", {})
            metadata = ts.get("metadata", {})
            db.add(
                TaskResult(
                    task_id=task_id,
                    site_id=ts["entity_id"],
                    year=ts["year"],
                    forest_loss_avoided_ha=values.get("forest_loss_avoided_ha"),
                    emissions_avoided_mgco2e=values.get("emissions_avoided_mgco2e"),
                    treatment_defor_ha=values.get("treatment_defor_ha"),
                    control_defor_ha=values.get("control_defor_ha"),
                    treatment_emissions_mgco2e=values.get("treatment_emissions_mgco2e"),
                    control_emissions_mgco2e=values.get("control_emissions_mgco2e"),
                    is_pre_intervention=bool(
                        metadata.get("is_pre_intervention", False)
                    ),
                    n_matched_pixels=metadata.get("n_matched_pixels"),
                    sampled_fraction=metadata.get("sampled_fraction"),
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
                    forest_loss_avoided_ha=values.get("forest_loss_avoided_ha"),
                    emissions_avoided_mgco2e=values.get("emissions_avoided_mgco2e"),
                    area_ha=values.get("area_ha"),
                    n_matched_pixels=metadata.get("n_matched_pixels"),
                    sampled_fraction=metadata.get("sampled_fraction"),
                    first_year=rec.get("period_start"),
                    last_year=rec.get("period_end"),
                    n_years=metadata.get("n_years"),
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

    # Reconstruct n_sites from the pipeline array_size if available
    n_sites = 1
    pipeline = params.get("pipeline") or []
    for step in pipeline:
        if isinstance(step, dict) and step.get("array_size"):
            n_sites = step["array_size"]
            break

    task = AnalysisTask(
        id=uuid.uuid4(),
        name=params.get("task_id", f"Discovered: {exec_id[:8]}"),
        description=f"Auto-discovered from trends.earth API execution {exec_id}",
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
        extra_metadata={"discovered_from_api": True, "api_exec_id": exec_id},
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


def _cleanup_covariate_downstream(covariate_name, db):
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
    """
    from cog_merge import delete_gcs_tiles, delete_s3_cog

    # 1. Delete S3 COG (if exists)
    if Config.S3_BUCKET:
        cog_prefix = f"{Config.S3_PREFIX}/cog"
        try:
            delete_s3_cog(
                Config.S3_BUCKET,
                cog_prefix,
                covariate_name,
                region=Config.AWS_REGION,
            )
        except Exception:
            logger.warning("Failed to delete S3 COG for %s", covariate_name)

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

    # 3. Remove old DB records for this covariate.
    #    Flush + commit-worthy so that a concurrent merge worker sees
    #    the deletion immediately and can bail out.
    old_records = (
        db.query(Covariate).filter(Covariate.covariate_name == covariate_name).all()
    )
    for rec in old_records:
        db.delete(rec)
    db.flush()


def start_gee_export(covariate_names, user_id):
    """Start GEE export tasks for the specified covariates.

    Any existing downstream artefacts (GCS tiles, S3 COGs, DB records)
    are cleaned up before starting the new export so that re-exports
    always produce a consistent fresh state.

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
            # Clean up any existing downstream artefacts before re-export
            _cleanup_covariate_downstream(name, db)

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
            'match_covariates', 'balance', 'propensity_scores'.
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
        "pixel_level": "results_pixel_level.csv",
        "match_covariates": "results_match_covariates.csv",
        "balance": "results_balance.csv",
        "propensity_scores": "results_propensity_scores.csv",
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


# ---------------------------------------------------------------------------
# Covariate inventory & COG merge functions
# ---------------------------------------------------------------------------


def force_reexport(covariate_name, user_id):
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

    Returns
    -------
    dict
        ``{"status": "ok", "export_id": …}`` on success.
    """
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
                Config.S3_BUCKET,
                cog_prefix,
                covariate_name,
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
    gee_config_path = os.path.join(os.path.dirname(__file__), "gee-export", "config.py")
    spec = importlib.util.spec_from_file_location("gee_export_config", gee_config_path)
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
                    existing.started_at is None or rec.started_at > existing.started_at
                )
            ):
                db_records[rec.covariate_name] = rec
    finally:
        db.close()

    # 4. Build inventory rows
    def _fmt(dt):
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt else ""

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


def get_ready_covariate_names():
    """Return covariate names that are fully merged and ready for submission.

    A covariate is considered ready when its most recent record in
    ``covariates`` has status ``merged`` and a non-empty ``merged_url``.
    Forest-cover year layers (``fc_*``) are excluded because they are
    handled automatically by the analysis pipeline via ``fc_years``.
    The returned order follows the GEE export config definition.
    """
    import importlib.util

    gee_config_path = os.path.join(os.path.dirname(__file__), "gee-export", "config.py")
    spec = importlib.util.spec_from_file_location("gee_export_config", gee_config_path)
    gee_config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gee_config)
    covariate_order = list(gee_config.COVARIATES.keys())

    latest_records: dict[str, Covariate] = {}
    db = get_db()
    try:
        for rec in db.query(Covariate).all():
            existing = latest_records.get(rec.covariate_name)
            if existing is None or (
                rec.started_at
                and (
                    existing.started_at is None or rec.started_at > existing.started_at
                )
            ):
                latest_records[rec.covariate_name] = rec
    finally:
        db.close()

    ready_names = []
    for covariate_name in covariate_order:
        if covariate_name.startswith("fc_"):
            continue
        record = latest_records.get(covariate_name)
        if record and record.status == "merged" and record.merged_url:
            ready_names.append(covariate_name)

    # Dual-purpose variables (ecoregion, pa) are rasterized from vector
    # data and uploaded to S3 on startup.  They are always available as
    # covariates once the rasterize-vectors task has completed.
    from layouts import DUAL_PURPOSE_VARS

    for var_name in DUAL_PURPOSE_VARS:
        if var_name not in ready_names:
            ready_names.append(var_name)

    return ready_names


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
