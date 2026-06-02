"""User site set CRUD, GeoJSON/Parquet export, and S3 upload."""

import io
import json
import logging

import geopandas as gpd
import pandas as pd
from shapely import wkb
from sqlalchemy import text

from config import Config
from models import (
    AnalysisTask,
    UserSiteSet,
    get_db,
)

from services.s3 import S3_COST_TAGGING, get_s3_client

logger = logging.getLogger(__name__)


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
        ).execution_options(stream_results=True, max_row_buffer=batch_size),
        {"site_set_id": str(site_set_id)},
    )

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
