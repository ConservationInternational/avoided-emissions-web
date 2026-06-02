#!/usr/bin/env python3
"""Step 0: Prepare spatial inputs for the avoided-emissions pipeline.

Downloads site polygons and reference-layer GeoParquets from S3, computes
the matching extent (union of reference features that intersect the site
bounding box) and the sites exclusion buffer, then writes
``prep_summary.json`` so the subsequent extract step can consume them
without accessing PostGIS at all.

This step runs as a short-lived AWS Batch job *before* the extract array
job, keeping all PostGIS work out of the Batch environment.

Input
-----
Config keys consumed (via ``--config``):

    sites_parquet_s3_uri   S3 URI of the sites GeoParquet
    reference_layer_uris   {layer_name: s3_uri} for admin/ecoregion layers
    exact_match_vars       list of covariate names used for exact matching
    min_control_distance_km  exclusion buffer radius (km); 0 disables buffer
    intermediate_s3_uri    S3 prefix where prep_summary.json is uploaded
                           (main.py handles the upload; this script only
                           writes to ``output_dir``)

Output
------
    {output_dir}/prep_summary.json
        ``matching_extent``      GeoJSON geometry dict (or null)
        ``sites_exclusion_buffer``  GeoJSON geometry dict (or null)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile

import boto3
import geopandas as gpd
from shapely.geometry import box, mapping
from shapely.ops import unary_union
from shapely.validation import make_valid

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("prep")

# Ensure /app/scripts is on the path so py_utils / logging_utils are importable
_scripts_dir = os.path.dirname(os.path.abspath(__file__))
if _scripts_dir not in sys.path:
    sys.path.insert(0, _scripts_dir)

try:
    from logging_utils import configure_third_party_logging

    configure_third_party_logging()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Reference-layer name → which exact_match_vars trigger its download
# ---------------------------------------------------------------------------
# The keys here match the keys used in ``reference_layer_uris`` (same as the
# ``layer_name`` column in the ``reference_layer_exports`` table, which itself
# matches the keys of ``_EXTENT_TABLE_MAP`` in webapp/services.py).
_POLYGON_VARS = {"admin0", "admin1", "admin2", "ecoregion"}


# ---------------------------------------------------------------------------
# Config parsing
# ---------------------------------------------------------------------------


def parse_config(argv: list[str] | None = None) -> dict:
    """Read ``--config`` JSON file and return the config dict."""
    parser = argparse.ArgumentParser(description="Spatial prep step (step 0)")
    parser.add_argument("--config", required=True, help="Path to config JSON")
    args = parser.parse_args(argv)

    with open(args.config) as fh:
        config: dict = json.load(fh)

    return config


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split ``s3://bucket/key`` into ``(bucket, key)``."""
    if uri.startswith("s3://"):
        uri = uri[5:]
    parts = uri.split("/", 1)
    return parts[0], parts[1] if len(parts) > 1 else ""


def _download_s3(s3_uri: str, local_path: str) -> None:
    """Download a single S3 object to *local_path*."""
    bucket, key = _parse_s3_uri(s3_uri)
    log.info("Downloading s3://%s/%s → %s", bucket, key, local_path)
    boto3.client("s3").download_file(bucket, key, local_path)


# ---------------------------------------------------------------------------
# Matching-extent computation
# ---------------------------------------------------------------------------


def _load_reference_layer(
    s3_uri: str, tmp_dir: str, layer_name: str
) -> gpd.GeoDataFrame:
    """Download a reference GeoParquet from S3 and return a GeoDataFrame."""
    local_path = os.path.join(tmp_dir, f"{layer_name}.parquet")
    _download_s3(s3_uri, local_path)
    gdf = gpd.read_parquet(local_path)
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")
    return gdf


def compute_matching_extent(
    sites_gdf: gpd.GeoDataFrame,
    exact_match_vars: list[str],
    reference_layer_uris: dict[str, str],
    tmp_dir: str,
) -> dict | None:
    """Compute the matching extent as the intersection of reference-layer unions.

    For each polygon-type exact-match variable (admin0/1/2/ecoregion) that
    has a corresponding entry in *reference_layer_uris*, this function:

    1. Downloads the GeoParquet from S3.
    2. Filters it to features intersecting the site-set bounding box.
    3. Unions the retained features.

    The final matching extent is the geometric intersection of all per-layer
    unions (i.e. the region that belongs to *at least one* valid region in
    every requested layer).  If no polygon exact-match vars are present the
    function returns ``None``.
    """
    polygon_vars = [
        v for v in exact_match_vars if v in _POLYGON_VARS and v in reference_layer_uris
    ]
    if not polygon_vars:
        log.info(
            "No polygon exact-match vars with reference layers — skipping matching_extent"
        )
        return None

    # Bounding box of all site geometries (with a small buffer to ensure
    # edge-straddling features are included).
    minx, miny, maxx, maxy = sites_gdf.total_bounds
    pad = 0.1  # ~11 km padding
    sites_bbox = box(minx - pad, miny - pad, maxx + pad, maxy + pad)

    layer_unions: list = []
    for var_name in polygon_vars:
        uri = reference_layer_uris[var_name]
        log.info("Loading reference layer '%s' from %s", var_name, uri)
        ref_gdf = _load_reference_layer(uri, tmp_dir, var_name)

        # Spatial filter: keep only features intersecting the sites bbox.
        mask = ref_gdf.intersects(sites_bbox)
        filtered = ref_gdf[mask].copy()
        log.info(
            "Layer '%s': %d / %d features intersect sites bbox",
            var_name,
            len(filtered),
            len(ref_gdf),
        )

        if filtered.empty:
            log.warning(
                "No features from layer '%s' intersect the sites bbox — "
                "matching_extent will be None",
                var_name,
            )
            return None

        # Validate and union.
        geoms = [
            make_valid(g) if not g.is_valid else g
            for g in filtered.geometry
            if g is not None and not g.is_empty
        ]
        layer_union = unary_union(geoms)
        if not layer_union.is_valid:
            layer_union = make_valid(layer_union)

        layer_unions.append(layer_union)
        log.info("Layer '%s': union complete", var_name)

    if not layer_unions:
        return None

    # Intersect all per-layer unions.
    result = layer_unions[0]
    for union in layer_unions[1:]:
        result = result.intersection(union)
        if result.is_empty:
            log.warning("Intersection of reference layers is empty — returning None")
            return None
        if not result.is_valid:
            result = make_valid(result)

    log.info("matching_extent computed (type=%s)", result.geom_type)
    return mapping(result)


# ---------------------------------------------------------------------------
# Sites exclusion buffer computation
# ---------------------------------------------------------------------------


def compute_sites_exclusion_buffer(
    sites_gdf: gpd.GeoDataFrame,
    distance_km: float,
) -> dict | None:
    """Buffer the union of all site geometries by *distance_km*.

    Uses the Lambert Equal-Area Cylindrical projection (EPSG:9804) so the
    buffer distance is in metres and is reasonably accurate at all latitudes.
    Returns a GeoJSON geometry dict, or ``None`` if *distance_km* <= 0.
    """
    if distance_km <= 0:
        log.info("min_control_distance_km <= 0 — skipping exclusion buffer")
        return None

    distance_m = distance_km * 1000

    # Validate geometries before unioning.
    valid_geoms = [
        make_valid(g) if not g.is_valid else g
        for g in sites_gdf.geometry
        if g is not None and not g.is_empty
    ]
    if not valid_geoms:
        log.warning("No valid site geometries — skipping exclusion buffer")
        return None

    sites_union = unary_union(valid_geoms)
    if not sites_union.is_valid:
        sites_union = make_valid(sites_union)

    # Reproject to an equal-area cylindrical CRS for accurate metre-based
    # buffering, then reproject the result back to EPSG:4326.
    sites_series = gpd.GeoSeries([sites_union], crs="EPSG:4326")
    buffered = (
        sites_series.to_crs("+proj=cea +datum=WGS84 +units=m")
        .buffer(distance_m)
        .to_crs("EPSG:4326")
    )
    result = buffered.iloc[0]
    if result is None or result.is_empty:
        log.warning("Exclusion buffer is empty after reprojection — returning None")
        return None

    log.info(
        "sites_exclusion_buffer computed (%.1f km buffer, type=%s)",
        distance_km,
        result.geom_type,
    )
    return mapping(result)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    config = parse_config(argv)

    output_dir = config.get("output_dir", tempfile.mkdtemp(prefix="ae_prep_"))
    os.makedirs(output_dir, exist_ok=True)

    sites_uri = config.get("sites_parquet_s3_uri") or config.get("sites_s3_uri")
    if not sites_uri:
        raise KeyError(
            "Missing required config key: sites_parquet_s3_uri or sites_s3_uri"
        )

    reference_layer_uris: dict[str, str] = config.get("reference_layer_uris") or {}
    exact_match_vars: list[str] = config.get("exact_match_vars") or []
    min_control_distance_km: float = float(config.get("min_control_distance_km", 10.0))

    log.info(
        "Prep step starting — exact_match_vars=%s, min_control_distance_km=%.1f",
        exact_match_vars,
        min_control_distance_km,
    )

    with tempfile.TemporaryDirectory(prefix="ae_prep_ref_") as tmp_dir:
        # Download sites parquet.
        sites_extension = ".parquet" if sites_uri.endswith(".parquet") else ".geojson"
        sites_local = os.path.join(tmp_dir, f"sites{sites_extension}")
        _download_s3(sites_uri, sites_local)

        log.info("Loading sites from %s", sites_local)
        if sites_local.endswith(".parquet"):
            sites_gdf = gpd.read_parquet(sites_local)
        else:
            sites_gdf = gpd.read_file(sites_local)

        if sites_gdf.crs is None:
            sites_gdf = sites_gdf.set_crs("EPSG:4326")
        elif sites_gdf.crs.to_epsg() != 4326:
            sites_gdf = sites_gdf.to_crs("EPSG:4326")

        log.info("Loaded %d sites", len(sites_gdf))

        matching_extent = compute_matching_extent(
            sites_gdf,
            exact_match_vars,
            reference_layer_uris,
            tmp_dir,
        )

    sites_exclusion_buffer = compute_sites_exclusion_buffer(
        sites_gdf,
        min_control_distance_km,
    )

    summary = {
        "matching_extent": matching_extent,
        "sites_exclusion_buffer": sites_exclusion_buffer,
    }

    summary_path = os.path.join(output_dir, "prep_summary.json")
    with open(summary_path, "w") as fh:
        json.dump(summary, fh)

    log.info(
        "prep_summary.json written to %s "
        "(matching_extent=%s, sites_exclusion_buffer=%s)",
        summary_path,
        "set" if matching_extent else "null",
        "set" if sites_exclusion_buffer else "null",
    )


if __name__ == "__main__":
    main()
