"""Reference layer S3 export and PostGIS spatial computation helpers."""

import json
import logging
import tempfile
from datetime import datetime, timezone

import geopandas as gpd
from sqlalchemy import text

from config import Config
from models import (
    ReferenceLayerExport,
    get_db,
)

from services.s3 import S3_COST_TAGGING, get_s3_client

logger = logging.getLogger(__name__)

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


def export_reference_layers_to_s3() -> dict[str, str]:
    """Export PostGIS reference layers to S3 as GeoParquet files.

    Queries each table in :data:`_EXTENT_TABLE_MAP` from PostGIS, writes
    the result (with geometries simplified to ~100 m tolerance) to a
    temporary GeoParquet file, uploads it to S3, and upserts a
    :class:`~models.ReferenceLayerExport` row so subsequent runs know
    the current S3 URI.

    The exported GeoParquets are consumed by the Batch ``prep`` step to
    compute matching extents and exclusion buffers without accessing
    PostGIS at all.

    Returns
    -------
    dict[str, str]
        Mapping of *layer_name* → *s3_uri* for each exported layer.
    """
    import os as _os

    from shapely.geometry import shape as _shape

    exported: dict[str, str] = {}
    s3 = get_s3_client()
    now = datetime.now(timezone.utc)

    for layer_name, table in _EXTENT_TABLE_MAP.items():
        assert _SAFE_TABLE_RE.match(table), f"Unsafe table name: {table}"  # nosec B101

        logger.info("[EXPORT-REF] Exporting layer %s (table=%s)", layer_name, table)

        db = get_db()
        try:
            # Stream all rows with simplified geometry (one row at a time via
            # yield_per to keep peak Python memory near O(batch) not O(total)).
            result = db.execute(
                text(
                    f"SELECT "
                    f"  ST_AsGeoJSON(ST_SimplifyPreserveTopology(geom, 0.001)) AS geojson,"
                    f"  geom IS NOT NULL AS has_geom "
                    f"FROM {table}"
                )
            )
            geoms = []
            for row in result:
                if row[0]:
                    geoms.append(_shape(json.loads(row[0])))
        finally:
            db.close()

        if not geoms:
            logger.warning("[EXPORT-REF] Table %s is empty — skipping", table)
            continue

        gdf = gpd.GeoDataFrame(geometry=geoms, crs="EPSG:4326")
        feature_count = len(gdf)
        logger.info(
            "[EXPORT-REF] Layer %s: %d features, writing GeoParquet",
            layer_name,
            feature_count,
        )

        # Write to a temporary file, then upload to S3.
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".parquet")
        try:
            _os.close(tmp_fd)
            gdf.to_parquet(tmp_path)
            parquet_key = f"{Config.S3_PREFIX}/reference/{layer_name}.parquet"
            s3.upload_file(
                tmp_path,
                Config.S3_BUCKET,
                parquet_key,
                ExtraArgs={"Tagging": S3_COST_TAGGING},
            )
        finally:
            _os.unlink(tmp_path)

        s3_uri = f"s3://{Config.S3_BUCKET}/{parquet_key}"
        exported[layer_name] = s3_uri

        # Upsert ReferenceLayerExport row.
        db2 = get_db()
        try:
            existing = (
                db2.query(ReferenceLayerExport)
                .filter(ReferenceLayerExport.layer_name == layer_name)
                .first()
            )
            if existing:
                existing.s3_uri = s3_uri
                existing.feature_count = feature_count
                existing.exported_at = now
            else:
                db2.add(
                    ReferenceLayerExport(
                        layer_name=layer_name,
                        s3_uri=s3_uri,
                        feature_count=feature_count,
                        exported_at=now,
                    )
                )
            db2.commit()
        finally:
            db2.close()

        logger.info(
            "[EXPORT-REF] Layer %s: %d features → %s",
            layer_name,
            feature_count,
            s3_uri,
        )

    return exported


def get_reference_layer_uris() -> dict[str, str]:
    """Return the current S3 URIs for all exported reference layers.

    Reads from the ``reference_layer_exports`` table.  Returns an empty
    dict when no layers have been exported yet.
    """
    db = get_db()
    try:
        rows = db.query(ReferenceLayerExport).all()
        return {r.layer_name: r.s3_uri for r in rows}
    finally:
        db.close()


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
                # Stream one simplified polygon per matching reference row.
                # No ST_Collect aggregate means PostgreSQL memory stays O(1) —
                # each row is processed and sent individually.  We apply
                # ST_SimplifyPreserveTopology(geom, 0.001) (~100 m tolerance)
                # in PostGIS before streaming to reduce vertex count; the
                # resulting shapes are still the actual polygon boundaries, not
                # bounding boxes.  Python then unions them with unary_union.
                from shapely.ops import unary_union as _unary_union

                result = db.execute(
                    text(
                        f"SELECT ST_AsGeoJSON("
                        f"  ST_SimplifyPreserveTopology(t.geom, 0.001)"
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
                geoms = [shape(json.loads(r[0])) for r in result if r[0]]
                elapsed = _time.perf_counter() - t1
                logger.info(
                    "[EXTENT] %s query took %.2fs (%d polygons)",
                    table,
                    elapsed,
                    len(geoms),
                )
                if geoms:
                    layer_extents.append(_unary_union(geoms))
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

                        # Ensure both geometries are topologically valid before
                        # calling intersection() — ST_Intersection results from
                        # PostGIS and prior overlap fragments can carry slight
                        # precision errors that cause GEOSException.
                        if not intersection_geom.is_valid:
                            intersection_geom = make_valid(intersection_geom)
                        _piece_geom = (
                            make_valid(piece_geom)
                            if not piece_geom.is_valid
                            else piece_geom
                        )

                        # Check if this intersection overlaps the piece
                        overlap = _piece_geom.intersection(intersection_geom)
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
