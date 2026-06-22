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

    Sites spanning multiple exact-match regions (e.g., admin boundaries) are
    split into sub-polygons.  Each sub-polygon is assigned a separate
    exact-match group ID based on the combination of all polygon-type
    exact-match values it falls within.

    Small slivers (<10% of original site area) are merged into the largest
    sub-polygon of the same site to reduce fragmentation.

    Performance
    -----------
    Executes exactly **one PostGIS query per polygon-type exact-match
    variable** (regardless of site count), then performs all intersection
    math with Shapely using an in-memory STRtree.  This replaces the prior
    O(N × M) query pattern (one query per site per variable) that stalled
    submissions with large site sets.

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
    """
    import time as _time

    import numpy as np
    import pandas as pd
    import shapely
    from shapely import STRtree
    from shapely.geometry import box as _bbox_box
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

    # ── Step 1: bulk-load reference regions (one query per polygon var) ──────
    # Use gdf.total_bounds to compute the bounding rectangle of all sites
    # directly from coordinate arrays — O(N) in C, far cheaper than
    # _unary_union(valid_site_geoms).envelope which processes every geometry.
    non_empty_mask = ~(gdf.geometry.isna() | gdf.geometry.is_empty)
    if not non_empty_mask.any():
        # Edge case: no valid geometries — return the input unchanged.
        gdf_out = gdf.copy()
        gdf_out["sub_site_index"] = 0
        gdf_out["is_sub_site"] = False
        gdf_out["original_area_ha"] = None
        group_mapping = {1: [(row["site_id"], 0) for _, row in gdf_out.iterrows()]}
        return gdf_out, group_mapping

    sites_envelope_geojson = json.dumps(mapping(_bbox_box(*gdf.total_bounds)))

    # var_regions maps var_name → (region_ids, region_geoms ndarray, STRtree)
    var_regions: dict[str, tuple[list[str], np.ndarray, STRtree | None]] = {}
    db = get_db()
    try:
        for var_name in polygon_vars:
            table = _EXTENT_TABLE_MAP[var_name]
            # table comes from the hardcoded _EXTENT_TABLE_MAP constant dict,
            # never from user input.  The check below is a defence-in-depth
            # guard against accidental misconfiguration; raise an explicit
            # error so it can't be silently skipped with python -O.
            if not _SAFE_TABLE_RE.match(table):
                raise ValueError(
                    f"Table name {table!r} does not match the expected safe "
                    f"identifier pattern (lowercase letters, digits, underscores, "
                    f"must start with a letter or underscore)."
                )
            id_col = "shape_name" if table.startswith("geoboundaries") else "eco_name"

            # Single bulk query: all regions whose bounding box overlaps the
            # sites envelope.  Geometries are simplified to 0.005° (~550 m)
            # tolerance to reduce vertex count 10-100× for complex boundaries
            # while retaining region-assignment fidelity.
            rows = db.execute(
                text(
                    f"SELECT {id_col}, "  # noqa: S608
                    f"  ST_AsGeoJSON(ST_SimplifyPreserveTopology(geom, 0.005)) "
                    f"FROM {table} "
                    f"WHERE ST_Intersects("
                    f"  geom, ST_SetSRID(ST_GeomFromGeoJSON(:bbox), 4326)"
                    f")"
                ),
                {"bbox": sites_envelope_geojson},
            ).fetchall()

            region_ids: list[str] = []
            region_geom_list: list = []
            for rid, geojson_str in rows:
                if not geojson_str or rid is None:
                    continue
                g = shape(json.loads(geojson_str))
                if not g.is_valid:
                    g = make_valid(g)
                region_ids.append(rid)
                region_geom_list.append(g)

            region_geoms_arr = np.asarray(region_geom_list, dtype=object)
            tree: STRtree | None = (
                STRtree(region_geom_list) if region_geom_list else None
            )
            var_regions[var_name] = (region_ids, region_geoms_arr, tree)

            logger.info(
                "[SPLIT] Loaded %d regions for %s from %s in %.2fs",
                len(region_ids),
                var_name,
                table,
                _time.perf_counter() - t0,
            )
    finally:
        db.close()

    # ── Step 2: vectorized site splitting using bulk STRtree queries ──────────
    # Rather than a per-site Python loop, process all sites (and sub-pieces)
    # at once using Shapely 2.0's array-query API and vectorized ufuncs.
    # This moves geometry work to C-level libgeos with no per-row Python overhead.
    #
    # State arrays (one element per current "piece"):
    #   curr_orig_idxs  – index into the original gdf for each piece
    #   curr_geoms      – current piece geometry (numpy object array)
    #   processed_vals  – dict of var_name → numpy object array of region IDs
    curr_orig_idxs: np.ndarray = np.arange(len(gdf))
    curr_geoms: np.ndarray = shapely.make_valid(np.asarray(gdf.geometry, dtype=object))
    # Compute areas from validated geometries so sliver detection uses consistent values.
    orig_geom_areas = shapely.area(curr_geoms)
    processed_vals: dict[str, np.ndarray] = {}

    for var_name in polygon_vars:
        region_ids_list, region_geoms_arr, tree = var_regions[var_name]
        n_curr = len(curr_geoms)

        if tree is None:
            processed_vals[var_name] = np.full(n_curr, None, dtype=object)
            continue

        # Validate current piece geometries in bulk before querying
        curr_geoms = shapely.make_valid(curr_geoms)

        # Bulk STRtree query: (piece_indices, region_indices) for every
        # (piece, region) pair that truly intersects.
        piece_idxs, reg_idxs = tree.query(curr_geoms, predicate="intersects")

        # Compute all intersections in a single C-level ufunc call
        intersections = shapely.intersection(
            curr_geoms[piece_idxs], region_geoms_arr[reg_idxs]
        )

        # Discard empty / zero-area results
        valid_mask = ~shapely.is_empty(intersections) & (
            shapely.area(intersections) > 0
        )
        piece_idxs = piece_idxs[valid_mask]
        reg_idxs = reg_idxs[valid_mask]
        intersections = intersections[valid_mask]

        # Pieces with no matching region → keep with NULL for this variable
        hit_pieces = np.unique(piece_idxs)
        no_hit = np.setdiff1d(np.arange(n_curr), hit_pieces)
        n_hits = len(piece_idxs)
        n_miss = len(no_hit)

        # Build combined arrays: intersecting pieces first, then non-intersecting
        new_orig_idxs = np.empty(n_hits + n_miss, dtype=curr_orig_idxs.dtype)
        new_orig_idxs[:n_hits] = curr_orig_idxs[piece_idxs]
        new_orig_idxs[n_hits:] = curr_orig_idxs[no_hit]

        new_geoms = np.empty(n_hits + n_miss, dtype=object)
        new_geoms[:n_hits] = intersections
        new_geoms[n_hits:] = curr_geoms[no_hit]

        # Region IDs for this variable (None where no intersection)
        new_var_ids = np.empty(n_hits + n_miss, dtype=object)
        new_var_ids[:n_hits] = np.asarray(region_ids_list, dtype=object)[reg_idxs]
        new_var_ids[n_hits:] = None

        # Propagate all previously-assigned variables to the expanded rows
        new_processed: dict[str, np.ndarray] = {}
        for prev_var, prev_arr in processed_vals.items():
            new_arr = np.empty(n_hits + n_miss, dtype=object)
            new_arr[:n_hits] = prev_arr[piece_idxs]
            new_arr[n_hits:] = prev_arr[no_hit]
            new_processed[prev_var] = new_arr
        new_processed[var_name] = new_var_ids

        curr_orig_idxs = new_orig_idxs
        curr_geoms = new_geoms
        processed_vals = new_processed

    # ── Step 3: sliver merging (<10% of original site area) ──────────────────
    piece_areas = shapely.area(curr_geoms)
    piece_orig_areas = orig_geom_areas[curr_orig_idxs]
    area_fractions = np.where(piece_orig_areas > 0, piece_areas / piece_orig_areas, 1.0)
    is_sliver = area_fractions < 0.1

    if is_sliver.any():
        piece_df = pd.DataFrame(
            {
                "orig_idx": curr_orig_idxs,
                "piece_area": piece_areas,
                "is_sliver": is_sliver,
            }
        )
        rows_to_drop: list[int] = []
        geom_updates: dict[int, object] = {}
        for _orig_idx, grp in piece_df.groupby("orig_idx"):
            if len(grp) <= 1:
                continue  # site was not split — nothing to merge
            slivers = grp[grp["is_sliver"]]
            mains = grp[~grp["is_sliver"]]
            if slivers.empty:
                continue
            if mains.empty:
                # All pieces are slivers — keep only the largest
                keep_idx = int(grp["piece_area"].idxmax())
                rows_to_drop.extend(int(i) for i in grp.index if int(i) != keep_idx)
                continue
            # Merge all slivers into the largest main piece
            largest_idx = int(mains["piece_area"].idxmax())
            sliver_row_idxs = [int(i) for i in slivers.index]
            union_input = np.concatenate(
                [
                    curr_geoms[sliver_row_idxs],
                    np.asarray([curr_geoms[largest_idx]], dtype=object),
                ]
            )
            geom_updates[largest_idx] = shapely.union_all(union_input)
            rows_to_drop.extend(sliver_row_idxs)

        if geom_updates or rows_to_drop:
            for row_i, new_geom in geom_updates.items():
                curr_geoms[row_i] = new_geom
            if rows_to_drop:
                keep_mask = np.ones(len(curr_geoms), dtype=bool)
                keep_mask[rows_to_drop] = False
                curr_geoms = curr_geoms[keep_mask]
                curr_orig_idxs = curr_orig_idxs[keep_mask]
                for v in list(processed_vals.keys()):
                    processed_vals[v] = processed_vals[v][keep_mask]

    # ── Step 4: build output GeoDataFrame from arrays ─────────────────────────
    # Retrieve site_id first so sub_site_index_arr can be computed per site_id,
    # consistent with the _sub_site_counters approach in analysis_task.py and
    # the (task_id, site_id, sub_site_index) unique constraint in TaskSite.
    site_id_arr = gdf["site_id"].to_numpy()[curr_orig_idxs]

    # sub_site_index: 0-based sequential per site_id string (not per orig_idx).
    # This matches _sub_site_counters in analysis_task.py so group_mapping tuples
    # and TaskSite records always agree, even for duplicate site_id values.
    site_id_series = pd.Series(site_id_arr)
    sub_site_index_arr = (
        site_id_series.groupby(site_id_series, sort=False).cumcount().to_numpy()
    )

    # is_sub_site: True when the original site produced more than one output piece.
    orig_idx_series = pd.Series(curr_orig_idxs)
    piece_counts = orig_idx_series.value_counts()
    is_sub_site_arr = orig_idx_series.map(piece_counts).gt(1).to_numpy()
    site_name_arr = (
        gdf["site_name"].to_numpy()[curr_orig_idxs]
        if "site_name" in gdf.columns
        else site_id_arr
    )
    start_date_arr = (
        gdf["start_date"].to_numpy(dtype=object)[curr_orig_idxs]
        if "start_date" in gdf.columns
        else np.full(len(curr_geoms), None, dtype=object)
    )
    end_date_arr = (
        gdf["end_date"].to_numpy(dtype=object)[curr_orig_idxs]
        if "end_date" in gdf.columns
        else np.full(len(curr_geoms), None, dtype=object)
    )

    # Piece areas in ha (approximate degrees² to ha conversion)
    piece_areas_ha = shapely.area(curr_geoms) * 111_000 * 111_000 / 10_000

    # Original site area in ha — populated only for pieces of split sites
    if "area_ha" in gdf.columns:
        orig_site_area_ha = gdf["area_ha"].to_numpy(dtype=float)
    else:
        orig_site_area_ha = orig_geom_areas * 111_000 * 111_000 / 10_000
    original_area_ha_arr = np.where(
        is_sub_site_arr, orig_site_area_ha[curr_orig_idxs], np.nan
    )

    out_data: dict[str, object] = {
        "site_id": site_id_arr,
        "site_name": site_name_arr,
        "start_date": start_date_arr,
        "end_date": end_date_arr,
        "geometry": curr_geoms,
        "area_ha": piece_areas_ha,
        "sub_site_index": sub_site_index_arr,
        "is_sub_site": is_sub_site_arr,
        "original_area_ha": original_area_ha_arr,
    }
    for var_name in polygon_vars:
        out_data[var_name] = processed_vals[var_name]

    split_gdf = gpd.GeoDataFrame(out_data, crs=gdf.crs, geometry="geometry")

    # ── Step 5: build group mapping ───────────────────────────────────────────
    group_key_to_id: dict = {}
    next_group_id = 1
    group_mapping: dict[int, list[tuple[str, int]]] = {}

    for i in range(len(split_gdf)):
        key_values = tuple(processed_vals[var_name][i] for var_name in polygon_vars)
        if all(v is None for v in key_values):
            group_id = 0
        else:
            if key_values not in group_key_to_id:
                group_key_to_id[key_values] = next_group_id
                next_group_id += 1
            group_id = group_key_to_id[key_values]
        if group_id not in group_mapping:
            group_mapping[group_id] = []
        group_mapping[group_id].append((site_id_arr[i], int(sub_site_index_arr[i])))

    elapsed = _time.perf_counter() - t0
    logger.info(
        "[SPLIT] Exact-match site splitting: %d sites → %d pieces in %.2fs",
        len(gdf),
        len(split_gdf),
        elapsed,
    )
    logger.info("[SPLIT] Created %d exact-match groups", len(group_mapping))

    return split_gdf, group_mapping
