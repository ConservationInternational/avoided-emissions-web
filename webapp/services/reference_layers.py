"""Reference layer S3 export and PostGIS spatial computation helpers."""

import hashlib
import json
import logging
import tempfile
from datetime import UTC, datetime

import geopandas as gpd
from config import Config
from models import (
    ReferenceLayerExport,
    VectorImportMetadata,
    get_db,
)
from sqlalchemy import text

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
_REGION_ID_SQL = {
    "admin0": "shape_group",
    "admin1": "shape_group || ':' || shape_id",
    "admin2": "shape_group || ':' || shape_id",
    "ecoregion": "eco_id::text",
}

_SAFE_TABLE_RE = __import__("re").compile(r"^[a-z_][a-z0-9_]*$")


def _write_reference_parquet(db, layer_name, path, batch_size=500):
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = _EXTENT_TABLE_MAP[layer_name]
    identity = _REGION_ID_SQL[layer_name]
    schema = pa.schema(
        [("region_id", pa.string()), ("geometry", pa.binary())],
        metadata={
            b"geo": json.dumps(
                {
                    "version": "1.0.0",
                    "primary_column": "geometry",
                    "columns": {
                        "geometry": {
                            "encoding": "WKB",
                            "geometry_types": ["MultiPolygon", "Polygon"],
                            "crs": gpd.GeoSeries(crs=4326).crs.to_json_dict(),
                        }
                    },
                }
            ).encode()
        },
    )
    result = db.execute(
        text(
            f"SELECT {identity}, ST_AsBinary(geom) FROM {table} "
            "WHERE geom IS NOT NULL ORDER BY id"
        ).execution_options(stream_results=True, yield_per=batch_size)
    )
    count = 0
    try:
        with pq.ParquetWriter(path, schema) as writer:
            while rows := result.fetchmany(batch_size):
                records = []
                for region_id, geometry in rows:
                    if region_id is None:
                        raise ValueError(f"Missing reference identity in {layer_name}.")
                    records.append(
                        {"region_id": str(region_id), "geometry": bytes(geometry)}
                    )
                writer.write_table(pa.Table.from_pylist(records, schema=schema))
                count += len(records)
    finally:
        result.close()
    return count


def export_reference_layers_to_s3() -> dict[str, str]:
    """Export PostGIS reference layers to S3 as GeoParquet files.

    Queries each table in :data:`_EXTENT_TABLE_MAP` from PostGIS, writes
    the authoritative source geometries to a
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

    exported: dict[str, str] = {}
    s3 = get_s3_client()
    now = datetime.now(UTC)

    for layer_name, table in _EXTENT_TABLE_MAP.items():
        assert _SAFE_TABLE_RE.match(table), f"Unsafe table name: {table}"  # nosec B101

        logger.info("[EXPORT-REF] Exporting layer %s (table=%s)", layer_name, table)

        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".parquet")
        try:
            _os.close(tmp_fd)
            db = get_db()
            try:
                feature_count = _write_reference_parquet(db, layer_name, tmp_path)
            finally:
                db.close()
            if not feature_count:
                logger.warning("[EXPORT-REF] Table %s is empty; not ready", table)
                continue
            with open(tmp_path, "rb") as source:
                digest = hashlib.file_digest(source, "sha256").hexdigest()
            parquet_key = f"{Config.S3_PREFIX}/reference/{layer_name}/{digest}.parquet"
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
                existing.schema_version = 2
                existing.exported_at = now
            else:
                db2.add(
                    ReferenceLayerExport(
                        layer_name=layer_name,
                        s3_uri=s3_uri,
                        feature_count=feature_count,
                        schema_version=2,
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


def get_reference_layer_uris(required_layers=None) -> dict[str, str]:
    """Return the current S3 URIs for all exported reference layers.

    Reads from the ``reference_layer_exports`` table.  Returns an empty
    dict when no layers have been exported yet.
    """
    db = get_db()
    try:
        rows = db.query(ReferenceLayerExport).all()
        imports = {
            row.table_name: row.completed_at
            for row in db.query(VectorImportMetadata).all()
        }
        uris = {}
        for row in rows:
            completed = imports.get(_EXTENT_TABLE_MAP.get(row.layer_name))
            if row.schema_version != 2 or (
                completed is not None and row.exported_at < completed
            ):
                logger.warning("Ignoring stale reference export: %s", row.layer_name)
                continue
            uris[row.layer_name] = row.s3_uri
        if required_layers is not None:
            required = set(required_layers).intersection(_EXTENT_TABLE_MAP)
            missing = required.difference(uris)
            if missing:
                logger.warning("Reference exports not ready: %s", sorted(missing))
                return {}
            uris = {key: value for key, value in uris.items() if key in required}
        return uris
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
                        f"SELECT ST_AsGeoJSON(t.geom) "
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
                    "    (SELECT ST_UnaryUnion(ST_Collect(geom)) FROM user_site_features"
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

    All positive polygonal coverage is retained, including slivers and
    unassigned residuals. Overlapping reference coverage is rejected.

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
    from shapely.geometry import box as _bbox_box
    from shapely.geometry import mapping
    from shapely.validation import make_valid

    from services.polygon_partition import (
        equal_area_ha,
        partition_by_regions,
        polygonal,
    )

    # Filter to polygon-type exact-match variables
    polygon_vars = sorted({v for v in exact_match_vars if v in _EXTENT_TABLE_MAP})
    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        raise ValueError("Site splitting requires validated EPSG:4326 geometry.")
    if gdf.empty:
        raise ValueError("No sites available for splitting.")
    if gdf["site_id"].isna().any() or gdf["site_id"].duplicated().any():
        raise ValueError("Analysis site_id must be a unique immutable feature key.")
    gdf = gdf.copy()
    gdf["geometry"] = [polygonal(geom) for geom in gdf.geometry]
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

    non_empty_mask = ~(gdf.geometry.isna() | gdf.geometry.is_empty)
    if not non_empty_mask.any():
        raise ValueError("No valid polygon geometries available for splitting.")

    bounds = gdf.geometry.bounds.to_numpy()
    buckets = np.floor((bounds[:, :2] + bounds[:, 2:]) / 10).astype(int)
    envelopes = {}
    for bucket, site_bounds in zip(buckets, bounds, strict=True):
        key = tuple(bucket)
        if key not in envelopes:
            envelopes[key] = site_bounds.copy()
        else:
            envelopes[key][:2] = np.minimum(envelopes[key][:2], site_bounds[:2])
            envelopes[key][2:] = np.maximum(envelopes[key][2:], site_bounds[2:])
    sites_envelopes_geojson = json.dumps(
        [mapping(_bbox_box(*extent)) for extent in envelopes.values()]
    )

    var_regions: dict[str, tuple[list[str], np.ndarray]] = {}
    reference_digests = {}
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
            id_col = _REGION_ID_SQL[var_name]
            rows = db.execute(
                text(
                    f"WITH candidate_ids AS (SELECT DISTINCT r.id "
                    f"FROM jsonb_array_elements(CAST(:bboxes AS jsonb)) b "
                    f"JOIN {table} r ON ST_Intersects(r.geom, "
                    f"ST_SetSRID(ST_GeomFromGeoJSON(b::text), 4326))) "
                    f"SELECT {id_col}, ST_AsBinary(geom) "
                    f"FROM {table} r JOIN candidate_ids c ON c.id = r.id "
                    f"ORDER BY 1"
                ),
                {"bboxes": sites_envelopes_geojson},
            ).fetchall()

            if not rows:
                ready = db.execute(
                    text(f"SELECT EXISTS(SELECT 1 FROM {table})")
                ).scalar()
                if not ready:
                    raise ValueError(
                        f"Reference layer {var_name} is not ready: import it first."
                    )
            region_ids: list[str] = []
            region_geom_list: list = []
            for rid, geom_wkb in rows:
                if not geom_wkb or rid is None:
                    raise ValueError(
                        f"Missing reference identity or geometry in {var_name}."
                    )
                g = shapely.from_wkb(bytes(geom_wkb))
                if not g.is_valid:
                    g = make_valid(g)
                region_ids.append(str(rid))
                region_geom_list.append(polygonal(g))
            if len(set(region_ids)) != len(region_ids):
                raise ValueError(f"Duplicate reference identities in {var_name}.")

            region_geoms_arr = np.asarray(region_geom_list, dtype=object)
            digest = hashlib.sha256()
            for rid, geom in zip(region_ids, region_geom_list, strict=True):
                digest.update(rid.encode())
                digest.update(geom.wkb)
            reference_digests[var_name] = digest.hexdigest()
            var_regions[var_name] = (region_ids, region_geoms_arr)

            logger.info(
                "[SPLIT] Loaded %d regions for %s from %s in %.2fs",
                len(region_ids),
                var_name,
                table,
                _time.perf_counter() - t0,
            )
    finally:
        db.close()

    # Bound candidate work to one piece while retaining all prior stratum keys.
    curr_orig_idxs: np.ndarray = np.arange(len(gdf))
    curr_geoms: np.ndarray = np.asarray(
        [polygonal(geom) for geom in gdf.geometry], dtype=object
    )
    processed_vals: dict[str, np.ndarray] = {}

    for var_name in polygon_vars:
        _var_t0 = _time.perf_counter()
        region_ids_list, region_geoms_arr = var_regions[var_name]
        n_curr = len(curr_geoms)

        if not region_ids_list:
            processed_vals[var_name] = np.full(n_curr, None, dtype=object)
            continue

        piece_idxs, new_geoms, new_var_ids = partition_by_regions(
            curr_geoms, region_ids_list, region_geoms_arr, var_name
        )
        curr_orig_idxs = curr_orig_idxs[piece_idxs]
        curr_geoms = new_geoms
        processed_vals = {
            name: values[piece_idxs] for name, values in processed_vals.items()
        }
        processed_vals[var_name] = new_var_ids

        logger.info(
            "[SPLIT] %s: variable done in %.2fs → %d output pieces total",
            var_name,
            _time.perf_counter() - _var_t0,
            len(curr_geoms),
        )

    site_id_arr = gdf["site_id"].to_numpy()[curr_orig_idxs]

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

    piece_areas_ha = equal_area_ha(curr_geoms)
    orig_site_area_ha = equal_area_ha(gdf.geometry)
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
    if "display_site_id" in gdf.columns:
        out_data["display_site_id"] = gdf["display_site_id"].to_numpy()[curr_orig_idxs]
    for var_name in polygon_vars:
        out_data[var_name] = processed_vals[var_name]

    split_gdf = gpd.GeoDataFrame(out_data, crs=gdf.crs, geometry="geometry")
    split_gdf.attrs["reference_versions"] = reference_digests

    # ── Step 5: build group mapping ───────────────────────────────────────────
    group_key_to_id: dict = {}
    next_group_id = 1
    group_mapping: dict[int, list[tuple[str, int]]] = {}

    for i in range(len(split_gdf)):
        key_values = tuple(processed_vals[var_name][i] for var_name in polygon_vars)
        if any(v is None for v in key_values):
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
