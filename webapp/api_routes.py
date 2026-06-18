"""Flask Blueprint for REST API endpoints.

Contains all non-Dash HTTP routes served by the Flask server.  The blueprint is
created via :func:`create_api_blueprint` which accepts the app-level ``limiter``
so that rate-limiting decorators can be applied without creating a circular
dependency on ``app.py``.
"""

import json
import logging
import re

import boto3
import flask_login
from flask import Blueprint, Response, jsonify, request
from sqlalchemy import text as sa_text

from config import Config
from gee_export import gee_config
from layer_config import get_style
from models import AnalysisTask, Covariate, UserSiteSet, get_db
from services import (
    discard_staged_site_upload,
    download_results_csv,
    get_site_upload_mapping_preview_from_staged,
    get_user_site_set_centroids_geojson,
    stream_stage_site_upload,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Vector overlay layer configuration: maps layer name to DB table and
# the column that contains the human-readable label for each polygon.
# "limit" controls max features returned (lower for complex geometries).
# ---------------------------------------------------------------------------
_VECTOR_LAYERS = {
    "admin0": {
        "table": "geoboundaries_adm0",
        "label_col": "shape_name",
        "description": "Country boundaries (ADM0)",
        "category": "boundaries",
        "limit": 200,
    },
    "admin1": {
        "table": "geoboundaries_adm1",
        "label_col": "shape_name",
        "description": "Admin level 1 boundaries",
        "category": "boundaries",
        "limit": 300,
    },
    "admin2": {
        "table": "geoboundaries_adm2",
        "label_col": "shape_name",
        "description": "Admin level 2 boundaries",
        "category": "boundaries",
        "limit": 500,
    },
    "ecoregion": {
        "table": "ecoregions",
        "label_col": "eco_name",
        "description": "RESOLVE Ecoregions",
        "category": "ecological",
        "limit": 150,  # Ecoregions have very complex geometries
    },
    "pa": {
        "table": "wdpa",
        "label_col": "name_eng",
        "description": "Protected Areas (WDPA)",
        "category": "ecological",
        "limit": 300,
    },
}

_SAFE_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


def create_api_blueprint(limiter):
    """Create and return the Flask Blueprint for all API routes.

    Parameters
    ----------
    limiter : flask_limiter.Limiter
        The app-level rate-limiter instance, used to decorate endpoints that
        need request-rate protection.
    """
    api_bp = Blueprint("api", __name__)

    # Health endpoint (used by Docker healthcheck to confirm app + migrations are ready)
    @api_bp.route("/health")
    def health_check():
        return "ok", 200

    # -- Session-check endpoint (called by client-side interval) -------------
    @api_bp.route("/api/session-check")
    def session_check():
        """Return whether the current user is still authenticated.

        Called by a ``dcc.Interval`` in the client to detect inactivity
        logouts and redirect to ``/login``.
        """
        if flask_login.current_user.is_authenticated:
            return jsonify({"authenticated": True})
        return jsonify({"authenticated": False}), 401

    @api_bp.route("/api/site-upload/stream-preview", methods=["POST"])
    @flask_login.login_required
    @limiter.limit("20 per minute")
    def site_upload_stream_preview():
        """Receive a streamed multipart upload and return mapping preview + token."""
        file = request.files.get("file")
        if not file or not file.filename:
            return jsonify({"ok": False, "errors": ["No file was provided."]}), 400

        filename = file.filename

        # Stream directly to disk in 1 MB chunks — never buffer the full file in
        # Python memory so that large uploads (100 MB+) don't exhaust the worker.
        try:
            upload_token = stream_stage_site_upload(
                file_stream=file.stream,
                filename=filename,
                user_id=flask_login.current_user.id,
            )
        except ValueError as exc:
            return jsonify({"ok": False, "errors": [str(exc)]}), 400

        # Read only the first 10 rows for the column-mapping preview.
        preview, errors = get_site_upload_mapping_preview_from_staged(
            upload_token, flask_login.current_user.id
        )
        if errors:
            discard_staged_site_upload(upload_token, flask_login.current_user.id)
            return jsonify({"ok": False, "errors": errors}), 400

        return jsonify(
            {
                "ok": True,
                "filename": filename,
                "upload_token": upload_token,
                "preview": preview,
            }
        )

    # -- COG layer API -------------------------------------------------------
    # Returns available covariate COG layers with pre-signed S3 URLs and style
    # config so the OpenLayers map can render them as toggleable overlays.

    @api_bp.route("/api/cog-layers")
    @flask_login.login_required
    def cog_layers():
        """Return merged covariate layers with pre-signed URLs and styles.

        Accepts an optional ``resolution`` query parameter (1000 or 250) to
        return COGs for a specific resolution.  Defaults to 1000 (1 km).
        """
        # Determine requested resolution
        try:
            resolution_m = int(request.args.get("resolution", "1000"))
        except (ValueError, TypeError):
            resolution_m = 1000
        if resolution_m not in (1000, 250):
            resolution_m = 1000

        _cog_suffixes = {1000: "_1km", 250: "_250m"}
        cog_suffix = _cog_suffixes.get(resolution_m, "_1km")

        # Load gee-export config for descriptions and categories
        cog_prefix = f"{Config.S3_PREFIX}/cog{cog_suffix}"
        # Backwards-compat: legacy COGs without a suffix are treated as 1 km.
        legacy_cog_prefix = f"{Config.S3_PREFIX}/cog" if resolution_m == 1000 else None

        # Get latest merged covariates from DB
        db = get_db()
        try:
            latest: dict[str, Covariate] = {}
            for rec in db.query(Covariate).filter(Covariate.status == "merged").all():
                existing = latest.get(rec.covariate_name)
                if existing is None or (
                    rec.started_at
                    and (
                        existing.started_at is None
                        or rec.started_at > existing.started_at
                    )
                ):
                    latest[rec.covariate_name] = rec
        finally:
            db.close()

        if not Config.S3_BUCKET:
            return jsonify({"layers": []})

        s3 = boto3.client("s3", region_name=Config.AWS_REGION)
        layers = []

        for name, rec in sorted(latest.items()):
            if not rec.merged_url:
                continue
            cfg = gee_config.COVARIATES.get(name, {})
            category = cfg.get("category", "")

            # Generate a 1-hour pre-signed URL for the COG.
            # Try the resolution-specific key first; for 1 km fall back to
            # the legacy prefix (cog/) if the new key (cog_1km/) is missing.
            s3_key = f"{cog_prefix}/{name}.tif"
            try:
                s3.head_object(Bucket=Config.S3_BUCKET, Key=s3_key)
            except Exception:
                if legacy_cog_prefix:
                    s3_key = f"{legacy_cog_prefix}/{name}.tif"
                    try:
                        s3.head_object(Bucket=Config.S3_BUCKET, Key=s3_key)
                    except Exception:
                        continue
                else:
                    continue
            try:
                url = s3.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": Config.S3_BUCKET, "Key": s3_key},
                    ExpiresIn=3600,
                )
            except Exception:
                continue

            style = get_style(name, category)
            layers.append(
                {
                    "name": name,
                    "description": cfg.get("description", name),
                    "category": category,
                    "url": url,
                    "style": style,
                }
            )

        return jsonify({"layers": layers, "resolution_m": resolution_m})

    @api_bp.route("/api/vector-layer/<layer_name>")
    @flask_login.login_required
    def vector_layer(layer_name):
        """Return simplified GeoJSON for a vector overlay within a bounding box.

        Query parameters:
            bbox  – comma-separated west,south,east,north in EPSG:4326
            simplify – optional tolerance in degrees (default 0.01)
        """
        from sqlalchemy import text as sa_text
        from sqlalchemy.exc import OperationalError

        cfg = _VECTOR_LAYERS.get(layer_name)
        if not cfg:
            return jsonify({"error": "Unknown layer"}), 404

        bbox_str = request.args.get("bbox")
        if not bbox_str:
            return jsonify({"error": "bbox parameter required"}), 400

        try:
            west, south, east, north = (float(v) for v in bbox_str.split(","))
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid bbox format"}), 400

        simplify = float(request.args.get("simplify", "0.01"))
        table = cfg["table"]
        label_col = cfg["label_col"]

        # Safety: table and label_col come from the hardcoded _VECTOR_LAYERS
        # dict (never from user input).  Assert this to prevent future
        # regressions if the dict is ever populated dynamically.
        assert _SAFE_IDENTIFIER.match(table), f"Unsafe table name: {table}"  # nosec B101
        assert _SAFE_IDENTIFIER.match(label_col), f"Unsafe column name: {label_col}"  # nosec B101

        # Lower limit for layers with complex geometries to reduce memory usage.
        # Ecoregions in particular have very detailed polygons.
        limit = cfg.get("limit", 500)

        # Use ST_Simplify (faster, Douglas-Peucker) instead of
        # ST_SimplifyPreserveTopology for better performance on large geometries.
        # Clip geometries to bbox first to reduce data volume before simplification.
        sql = sa_text(
            f"""
            SELECT
                {label_col} AS name,
                ST_AsGeoJSON(
                    ST_Simplify(
                        ST_Intersection(geom, ST_MakeEnvelope(:w, :s, :e, :n, 4326)),
                        :tol
                    ),
                    5
                ) AS geojson
            FROM {table}
            WHERE geom && ST_MakeEnvelope(:w, :s, :e, :n, 4326)
            LIMIT :lim
            """
        )

        db = get_db()
        try:
            # Set statement timeout to prevent runaway queries (15 seconds)
            db.execute(sa_text("SET LOCAL statement_timeout = '15s'"))
            rows = db.execute(
                sql,
                {
                    "tol": simplify,
                    "w": west,
                    "s": south,
                    "e": east,
                    "n": north,
                    "lim": limit,
                },
            ).fetchall()
        except OperationalError as e:
            logger.warning("Vector layer query failed for %s: %s", layer_name, e)
            db.rollback()
            return jsonify({"error": "Query timeout or server error"}), 503
        finally:
            db.close()

        features = []
        for row in rows:
            geom = json.loads(row.geojson) if row.geojson else None
            if not geom:
                continue
            features.append(
                {
                    "type": "Feature",
                    "properties": {"name": row.name or ""},
                    "geometry": geom,
                }
            )

        return jsonify({"type": "FeatureCollection", "features": features})

    @api_bp.route("/api/vector-layers")
    @flask_login.login_required
    def vector_layers_list():
        """Return the list of available vector overlay layers."""
        layers = []
        for name, cfg in _VECTOR_LAYERS.items():
            layers.append(
                {
                    "name": name,
                    "description": cfg["description"],
                    "category": cfg["category"],
                }
            )
        return jsonify({"layers": layers})

    @api_bp.route("/api/matched-pixels/<task_id>")
    @flask_login.login_required
    def matched_pixels(task_id):
        """Return matched treatment/control pixel locations as GeoJSON.

        Reads ``results_pixel_locations.csv`` from S3 for the given task and
        returns a GeoJSON FeatureCollection of Point features.  Each feature
        has properties ``site_id``, ``treatment`` (bool), and ``match_group``.

        Query parameters:
            site_id – optional, filter to a single site.
        """
        import csv as csv_mod
        import io

        csv_text = download_results_csv(task_id, "matched_pixels")
        if not csv_text:
            return jsonify({"type": "FeatureCollection", "features": []})

        site_filter = request.args.get("site_id")
        features = []
        reader = csv_mod.DictReader(io.StringIO(csv_text))
        for row in reader:
            if site_filter and row.get("site_id") != site_filter:
                continue
            try:
                lon = float(row["lon"])
                lat = float(row["lat"])
            except (ValueError, KeyError):
                continue
            features.append(
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [lon, lat]},
                    "properties": {
                        "site_id": row.get("site_id", ""),
                        "treatment": row.get("treatment", "").upper() == "TRUE",
                        "match_group": row.get("match_group", ""),
                    },
                }
            )

        return jsonify({"type": "FeatureCollection", "features": features})

    # -- MVT tile endpoints --------------------------------------------------
    # Serve Mapbox Vector Tiles for site geometries.  The rendering layer in
    # OpenLayers fetches these directly; no Dash callback round-trip is needed
    # for zoom/pan.  At zoom < 12 the endpoint returns centroid Points for all
    # sites in the tile; at zoom >= 12 it returns simplified Polygons.

    def _mvt_simplify_tol(z):
        """Geometry simplification tolerance in degrees, halving each zoom level."""
        return max(0.0001, 0.00001 * (2 ** (18 - z)))

    _MVT_SQL_SITE_SET = sa_text(
        """
        WITH tile AS (
            SELECT
                ST_TileEnvelope(:z, :x, :y)                          AS env3857,
                ST_Transform(ST_TileEnvelope(:z, :x, :y), 4326)      AS env4326
        )
        SELECT ST_AsMVT(q, 'sites', 4096, 'geom') AS mvt
        FROM (
            SELECT
                f.site_id,
                f.site_name,
                f.area_ha,
                CASE WHEN :z < 12
                    THEN ST_AsMVTGeom(
                             ST_Transform(ST_Centroid(f.geom), 3857),
                             t.env3857, 4096, 256, true)
                    ELSE ST_AsMVTGeom(
                             ST_Transform(
                                 ST_SimplifyPreserveTopology(f.geom, :simplify_tol),
                                 3857),
                             t.env3857, 4096, 256, true)
                END AS geom
            FROM user_site_features f, tile t
            WHERE f.site_set_id = :site_set_id
              AND f.geom && t.env4326
        ) q
        WHERE q.geom IS NOT NULL
        """
    )

    _MVT_SQL_TASK = sa_text(
        """
        WITH tile AS (
            SELECT
                ST_TileEnvelope(:z, :x, :y)                          AS env3857,
                ST_Transform(ST_TileEnvelope(:z, :x, :y), 4326)      AS env4326
        )
        SELECT ST_AsMVT(q, 'sites', 4096, 'geom') AS mvt
        FROM (
            SELECT
                f.site_id,
                f.site_name,
                f.area_ha,
                tr.extrapolated_emissions_avoided_mgco2e AS emissions_avoided_mgco2e,
                tr.extrapolated_forest_loss_avoided_ha  AS forest_loss_avoided_ha,
                CASE WHEN :z < 12
                    THEN ST_AsMVTGeom(
                             ST_Transform(ST_Centroid(f.geom), 3857),
                             t.env3857, 4096, 256, true)
                    ELSE ST_AsMVTGeom(
                             ST_Transform(
                                 ST_SimplifyPreserveTopology(f.geom, :simplify_tol),
                                 3857),
                             t.env3857, 4096, 256, true)
                END AS geom
            FROM user_site_features f
            JOIN tile t ON true
            LEFT JOIN task_results_total tr
                   ON tr.site_id = f.site_id AND tr.task_id = :task_id
            WHERE f.site_set_id = :site_set_id
              AND f.geom && t.env4326
        ) q
        WHERE q.geom IS NOT NULL
        """
    )

    def _mvt_response(mvt_bytes):
        resp = Response(mvt_bytes, status=200)
        resp.headers["Content-Type"] = "application/vnd.mapbox-vector-tile"
        resp.headers["Cache-Control"] = "private, max-age=300"
        return resp

    @api_bp.route("/api/sites-tiles/<site_set_id>/<int:z>/<int:x>/<int:y>")
    @flask_login.login_required
    @limiter.limit("300 per minute")
    def sites_tiles(site_set_id, z, x, y):
        """Serve MVT tiles for a user-owned site set.

        Returns centroid Points at zoom < 12; simplified Polygons at zoom >= 12.
        The tile is empty (zero-length body) when no sites intersect the tile.
        """
        db = get_db()
        try:
            exists = (
                db.query(UserSiteSet)
                .filter(
                    UserSiteSet.id == site_set_id,
                    UserSiteSet.user_id == flask_login.current_user.id,
                )
                .first()
            )
            if not exists:
                return jsonify({"error": "Site set not found"}), 404

            row = db.execute(
                _MVT_SQL_SITE_SET,
                {
                    "z": z,
                    "x": x,
                    "y": y,
                    "site_set_id": str(site_set_id),
                    "simplify_tol": _mvt_simplify_tol(z),
                },
            ).fetchone()
        finally:
            db.close()

        mvt_bytes = bytes(row.mvt) if row and row.mvt else b""
        return _mvt_response(mvt_bytes)

    @api_bp.route("/api/task-sites-tiles/<task_id>/<int:z>/<int:x>/<int:y>")
    @flask_login.login_required
    @limiter.limit("300 per minute")
    def task_sites_tiles(task_id, z, x, y):
        """Serve MVT tiles for a task's sites with per-site emissions properties.

        Joins ``task_results_total`` so that ``emissions_avoided_mgco2e`` and
        ``forest_loss_avoided_ha`` are available as tile feature properties for
        colour-coding in the results map.  Returns 204 when the task has no
        linked site set (adopted tasks with geometry only in S3 parquet).
        """
        db = get_db()
        try:
            task = (
                db.query(AnalysisTask)
                .filter(
                    AnalysisTask.id == task_id,
                    AnalysisTask.user_id == flask_login.current_user.id,
                )
                .first()
            )
            if not task:
                return jsonify({"error": "Task not found"}), 404
            if not task.site_set_id:
                # Adopted task — geometry only in S3 parquet; no tiles available.
                return Response(status=204)

            row = db.execute(
                _MVT_SQL_TASK,
                {
                    "z": z,
                    "x": x,
                    "y": y,
                    "site_set_id": str(task.site_set_id),
                    "task_id": str(task_id),
                    "simplify_tol": _mvt_simplify_tol(z),
                },
            ).fetchone()
        finally:
            db.close()

        mvt_bytes = bytes(row.mvt) if row and row.mvt else b""
        return _mvt_response(mvt_bytes)

    @api_bp.route("/api/site-centroids/<site_set_id>")
    @flask_login.login_required
    @limiter.limit("30 per minute")
    def site_centroids(site_set_id):
        """Return all sites as centroid Point GeoJSON for the companion vector source.

        Used by the OpenLayers map to populate ``_featureBySiteId`` for
        click-to-zoom and table\u2194map selection.  Returns every site’s centroid
        regardless of dataset size; points are tiny so even 50 k sites is
        only ~4 MB of JSON.
        """
        db = get_db()
        try:
            exists = (
                db.query(UserSiteSet)
                .filter(
                    UserSiteSet.id == site_set_id,
                    UserSiteSet.user_id == flask_login.current_user.id,
                )
                .first()
            )
        finally:
            db.close()

        if not exists:
            return jsonify({"error": "Site set not found"}), 404

        fc = get_user_site_set_centroids_geojson(site_set_id)
        return jsonify(fc)

    return api_bp
