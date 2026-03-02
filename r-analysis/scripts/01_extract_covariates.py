#!/usr/bin/env python3
"""Step 1: Extract covariate values for treatment sites and control regions.

Loads covariate COGs from S3 via GDAL virtual filesystems, loads site
polygons, identifies treatment pixels (within sites) and control pixels
(same GADM region), and saves the extracted values for the matching step.

This is the Python rewrite of 01_extract_covariates.R, optimised for
speed using GDAL and xarray/rioxarray for Cloud-Optimised GeoTIFF access.

Input:
    - Task config JSON (--config)
    - Site polygons (GeoJSON or GeoPackage)
    - Covariate COGs on S3

Output:
    - {output_dir}/sites_processed.parquet
    - {output_dir}/treatment_cell_key.parquet
    - {output_dir}/treatments_and_controls.parquet
    - {output_dir}/formula.json
    - {output_dir}/site_id_key.csv
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import warnings

import geopandas as gpd
import numpy as np
import pandas as pd
import rioxarray  # noqa: F401 – registers the rio accessor
import xarray as xr
from osgeo import gdal
from rasterio.features import rasterize
from shapely.geometry import box, mapping, shape

# Silence GDAL/rasterio deprecation noise
warnings.filterwarnings("ignore", category=DeprecationWarning)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("extract_covariates")

# ---------------------------------------------------------------------------
# GDAL / S3 tuning for COG range-request performance
# ---------------------------------------------------------------------------
_GDAL_OPTS = {
    "GDAL_HTTP_MULTIPLEX": "YES",
    "GDAL_HTTP_MERGE_CONSECUTIVE_RANGES": "YES",
    "GDAL_HTTP_MAX_RETRY": "5",
    "GDAL_HTTP_RETRY_DELAY": "2",
    "VSI_CACHE": "TRUE",
    "VSI_CACHE_SIZE": "50000000",  # 50 MB per file
    "AWS_NO_SIGN_REQUEST": "NO",
    "GDAL_CACHEMAX": "1024",  # 1 GB block cache
    "GDAL_NUM_THREADS": "ALL_CPUS",
}


def _configure_gdal() -> None:
    """Set GDAL config options for fast COG access over S3."""
    for key, val in _GDAL_OPTS.items():
        gdal.SetConfigOption(key, val)
        os.environ.setdefault(key, val)
    gdal.UseExceptions()


# ---------------------------------------------------------------------------
# Rollbar integration
# ---------------------------------------------------------------------------

# Ensure /app/scripts is on the path so py_utils is importable both when
# running inside the Docker container and during local development.
_scripts_dir = os.path.dirname(os.path.abspath(__file__))
if _scripts_dir not in sys.path:
    sys.path.insert(0, _scripts_dir)

from py_utils import rollbar_init, with_rollbar  # noqa: E402


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def parse_config(argv: list[str] | None = None) -> dict:
    """Parse ``--config``, ``--site-id``, ``--data-dir`` from *argv*."""
    parser = argparse.ArgumentParser(description="Extract covariates (step 1)")
    parser.add_argument("--config", required=True, help="Path to config JSON")
    parser.add_argument("--site-id", default=None)
    parser.add_argument("--data-dir", default=None)
    args = parser.parse_args(argv)

    with open(args.config) as f:
        config: dict = json.load(f)

    if args.data_dir:
        config["data_dir"] = args.data_dir
    if args.site_id:
        config["site_id"] = args.site_id

    # Defaults
    config.setdefault("max_treatment_pixels", 1000)
    config.setdefault("control_multiplier", 50)
    config.setdefault("min_site_area_ha", 100)
    config.setdefault("min_glm_treatment_pixels", 15)

    config["input_dir"] = os.path.join(config["data_dir"], "input")
    config["output_dir"] = os.path.join(config["data_dir"], "output")
    config["matches_dir"] = os.path.join(config["output_dir"], "matches")
    os.makedirs(config["output_dir"], exist_ok=True)
    os.makedirs(config["matches_dir"], exist_ok=True)

    return config


# ---------------------------------------------------------------------------
# Site loading
# ---------------------------------------------------------------------------


def load_sites(sites_path: str, min_area_ha: float) -> gpd.GeoDataFrame:
    """Load sites from GeoJSON / GeoPackage, filter by area, add metadata."""
    sites = gpd.read_file(sites_path)

    required = {"site_id", "site_name", "start_date"}
    missing = required - set(sites.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Ensure EPSG:4326
    sites = sites.to_crs("EPSG:4326")

    # Parse dates
    sites["start_date"] = pd.to_datetime(sites["start_date"])
    if "end_date" in sites.columns:
        sites["end_date"] = pd.to_datetime(sites["end_date"])
    else:
        sites["end_date"] = pd.NaT

    sites["start_year"] = sites["start_date"].dt.year.astype("Int64")
    sites["end_year"] = sites["end_date"].dt.year.fillna(2099).astype(int)

    # Area in hectares (equal-area projection)
    sites_cea = sites.to_crs("+proj=cea")
    sites["area_ha"] = sites_cea.geometry.area / 10_000

    # Numeric IDs (1-based)
    sites["id_numeric"] = range(1, len(sites) + 1)

    # Filter
    n_before = len(sites)
    sites = sites[sites["area_ha"] >= min_area_ha].copy()
    log.info(
        "Sites: %d loaded, %d after area filter (>= %.0f ha)",
        n_before,
        len(sites),
        min_area_ha,
    )
    if len(sites) == 0:
        raise RuntimeError("No sites remaining after area filter.")

    return sites.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Covariate loading via rioxarray (reads COGs lazily over S3)
# ---------------------------------------------------------------------------


def _s3_path(bucket: str, prefix: str, name: str) -> str:
    """Build a ``/vsis3/`` GDAL virtual path."""
    return f"/vsis3/{bucket}/{prefix}/{name}.tif"


def load_covariates_lazy(
    cog_bucket: str,
    cog_prefix: str,
    covariate_names: list[str],
) -> xr.Dataset:
    """Open each covariate COG as a lazy DataArray and merge into a Dataset.

    Uses rioxarray which reads Cloud-Optimised GeoTIFFs through GDAL's
    ``/vsis3/`` virtual filesystem.  Only metadata is fetched at this stage;
    actual pixel data is pulled on-demand when ``.values`` is accessed or
    ``.load()`` is called.
    """
    arrays: dict[str, xr.DataArray] = {}
    ref_crs = None
    ref_res = None

    for name in covariate_names:
        uri = _s3_path(cog_bucket, cog_prefix, name)
        log.info("  Opening: %s", uri)
        try:
            da = rioxarray.open_rasterio(uri, chunks="auto")
        except Exception as exc:
            raise RuntimeError(f"Failed to open covariate '{name}' at {uri}") from exc

        # Squeeze band dimension if single-band
        if "band" in da.dims and da.sizes["band"] == 1:
            da = da.squeeze("band", drop=True)

        # Validate that all COGs share the same CRS and resolution
        layer_crs = da.rio.crs
        layer_res = da.rio.resolution()
        if ref_crs is None:
            ref_crs = layer_crs
            ref_res = layer_res
        else:
            if layer_crs != ref_crs:
                raise RuntimeError(
                    f"CRS mismatch: '{name}' has {layer_crs}, expected {ref_crs}"
                )
            if layer_res != ref_res:
                raise RuntimeError(
                    f"Resolution mismatch: '{name}' has {layer_res}, expected {ref_res}"
                )

        arrays[name] = da

    ds = xr.Dataset(arrays)
    log.info("  Covariate dataset: %s", dict(ds.sizes))
    return ds


# ---------------------------------------------------------------------------
# Pixel area on WGS-84 ellipsoid
# ---------------------------------------------------------------------------


def calc_pixel_area_ha(
    y: np.ndarray,
    yres: float,
    xres: float,
) -> np.ndarray:
    """Compute the area (ha) of raster cells on the WGS-84 ellipsoid."""
    a = 6_378_137.0  # semi-major axis (m)
    b = 6_356_752.314_2  # semi-minor axis (m)
    e = math.sqrt(1 - (b / a) ** 2)

    y = np.asarray(y, dtype=np.float64)
    ymin_rad = np.deg2rad(y - yres / 2)
    ymax_rad = np.deg2rad(y + yres / 2)

    def _slice_area(phi: np.ndarray) -> np.ndarray:
        sin_phi = np.sin(phi)
        zp = 1 + e * sin_phi
        zm = 1 - e * sin_phi
        return math.pi * b**2 * (np.arctanh(e * sin_phi) / e + sin_phi / (zp * zm))

    area_m2 = (_slice_area(ymax_rad) - _slice_area(ymin_rad)) * (xres / 360)
    return area_m2 / 10_000


# ---------------------------------------------------------------------------
# Rasterize sites to identify treatment pixels
# ---------------------------------------------------------------------------


def _rasterize_sites(
    sites: gpd.GeoDataFrame,
    transform,
    width: int,
    height: int,
) -> np.ndarray:
    """Burn ``id_numeric`` into a raster matching the covariate grid.

    Returns a 2-D int32 array where 0 = no site.
    """
    shapes = [
        (mapping(geom), int(val))
        for geom, val in zip(sites.geometry, sites["id_numeric"])
    ]
    return rasterize(
        shapes,
        out_shape=(height, width),
        transform=transform,
        fill=0,
        dtype="int32",
        all_touched=False,
    )


# ---------------------------------------------------------------------------
# Core extraction logic (GDAL + xarray)
# ---------------------------------------------------------------------------


def extract_covariates(config: dict, sites: gpd.GeoDataFrame) -> None:
    """Extract treatment & control pixel values from COG layers.

    Strategy:
    1. Open all COGs lazily via rioxarray  (only metadata is fetched).
    2. Determine the spatial clip window from the pre-computed
       ``matching_extent`` polygon (the intersection of all polygon-type
       exact-match layers that overlap the sites, computed by the webapp
       via PostGIS).
    3. Clip the lazy dataset to that window  (triggers range-request reads
       for only the needed tiles).
    4. Rasterize sites onto the covariate grid to label treatment pixels.
    5. Rasterize the matching_extent to identify candidate control pixels.
    6. Build treatment_cell_key and the full pixel DataFrame in-memory.
    """
    cog_bucket = config["cog_bucket"]
    cog_prefix = config["cog_prefix"]

    # Layer names to load
    all_layers = (
        config["covariates"]
        + config["exact_match_vars"]
        + [f"fc_{y}" for y in config["fc_years"]]
    )
    log.info("Loading %d covariate layers from S3", len(all_layers))

    # --- 1. open lazily ---
    ds = load_covariates_lazy(cog_bucket, cog_prefix, all_layers)

    # --- 2. spatial window from matching extent ---
    matching_extent_geojson = config.get("matching_extent")
    if not matching_extent_geojson:
        raise RuntimeError(
            "Config must include 'matching_extent' — the intersection of "
            "exact-match layers that overlap the sites, "
            "computed by the webapp via PostGIS."
        )
    extent_geom = shape(matching_extent_geojson)
    ext_bounds = extent_geom.bounds  # (minx, miny, maxx, maxy)
    # Small buffer to avoid edge clipping artefacts
    buffer_deg = 0.1
    clip_box = box(
        ext_bounds[0] - buffer_deg,
        ext_bounds[1] - buffer_deg,
        ext_bounds[2] + buffer_deg,
        ext_bounds[3] + buffer_deg,
    )
    log.info("Clipping covariates to matching extent bbox: %s", clip_box.bounds)

    ds = ds.rio.clip_box(*clip_box.bounds)

    # --- 3. materialise into memory ---
    log.info("Fetching pixel data from S3 (this may take a while)...")
    ds = ds.load()  # pulls tiles over HTTP range requests

    # Resolution and grid dimensions from the raster's affine transform
    # (always available from GeoTIFF metadata, even with a single pixel)
    transform = ds[all_layers[0]].rio.transform()
    xres = abs(transform.a)
    yres = abs(transform.e)
    ys = ds.coords["y"].values
    height, width = len(ys), len(ds.coords["x"].values)

    log.info("Grid: %d x %d (xres=%.6f, yres=%.6f)", width, height, xres, yres)

    # --- 4. rasterize sites ---
    site_mask = _rasterize_sites(sites, transform, width, height)

    # --- 5. build treatment cell key ---
    log.info("Building treatment cell key...")
    site_ids_flat = site_mask.ravel()
    treatment_mask = site_ids_flat > 0
    treatment_indices = np.nonzero(treatment_mask)[0]

    # Map flat index → row (for latitude-dependent area calculation)
    rows_t = treatment_indices // width

    # Pixel areas
    y_coords_treatment = ys[rows_t]
    pixel_areas = calc_pixel_area_ha(y_coords_treatment, yres, xres)

    treatment_key = pd.DataFrame(
        {
            "cell": treatment_indices,
            "id_numeric": site_ids_flat[treatment_indices],
            "area_ha": pixel_areas,
        }
    )

    # Attach site_id
    id_to_site = dict(zip(sites["id_numeric"], sites["site_id"]))
    treatment_key["site_id"] = treatment_key["id_numeric"].map(id_to_site)
    log.info("Treatment cells: %d", len(treatment_key))

    # --- 6. determine candidate control pixels ---
    # Rasterize the matching extent polygon to identify the area
    # where valid controls can exist (the intersection of all
    # polygon-type exact-match layers that overlap the sites).
    extent_mask_2d = rasterize(
        [(mapping(extent_geom), 1)],
        out_shape=(height, width),
        transform=transform,
        fill=0,
        dtype="uint8",
        all_touched=True,
    )
    in_extent = extent_mask_2d.ravel().astype(bool)
    candidate_mask = in_extent | treatment_mask

    candidate_indices = np.nonzero(candidate_mask)[0]

    log.info(
        "Extracting covariate values for %d candidate pixels...", len(candidate_indices)
    )

    # Build DataFrame column-by-column (fast NumPy slicing)
    data: dict[str, np.ndarray] = {"cell": candidate_indices}
    for layer_name in all_layers:
        arr = ds[layer_name].values
        if arr.ndim == 3:
            arr = arr[0]
        data[layer_name] = arr.ravel()[candidate_indices]

    covariate_df = pd.DataFrame(data)

    # Deduplicate (should already be unique, but safety)
    covariate_df = covariate_df.drop_duplicates(subset=["cell"])

    log.info("Total covariate values extracted: %d pixels", len(covariate_df))

    # --- 8. save outputs ---
    output_dir = config["output_dir"]

    treatment_key.to_parquet(
        os.path.join(output_dir, "treatment_cell_key.parquet"), index=False
    )
    covariate_df.to_parquet(
        os.path.join(output_dir, "treatments_and_controls.parquet"), index=False
    )

    log.info("Saved treatment_cell_key.parquet and treatments_and_controls.parquet")


# ---------------------------------------------------------------------------
# Formula builder
# ---------------------------------------------------------------------------


def build_matching_formula(covariates: list[str]) -> dict:
    """Build a formula dict: {"lhs": "treatment", "rhs": ["cov1", ...]}."""
    return {
        "lhs": "treatment",
        "rhs": list(covariates),
        "formula_str": "treatment ~ " + " + ".join(covariates),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    rollbar_init()
    _configure_gdal()

    with with_rollbar("01_extract_covariates"):
        config = parse_config(argv)
        log.info("Step 1: Extracting covariates")
        log.info("  Config: %s", json.dumps(config, default=str))

        # Load & filter sites
        sites = load_sites(config["sites_file"], config["min_site_area_ha"])

        # Save site ID key (CSV for interop)
        key_cols = [
            "site_id",
            "id_numeric",
            "site_name",
            "start_year",
            "end_year",
            "area_ha",
        ]
        sites[key_cols].to_csv(
            os.path.join(config["output_dir"], "site_id_key.csv"), index=False
        )

        # Save processed sites (Parquet for downstream R steps).
        # Convert geometry to WKT text so R's arrow::read_parquet() +
        # sf::st_as_sf(wkt=) can reconstruct it without needing sfarrow.
        sites_out = sites.copy()
        sites_out["geometry"] = sites_out.geometry.to_wkt()
        pd.DataFrame(sites_out).to_parquet(
            os.path.join(config["output_dir"], "sites_processed.parquet"),
            index=False,
        )

        # Build and save matching formula
        formula = build_matching_formula(config["covariates"])
        with open(os.path.join(config["output_dir"], "formula.json"), "w") as f:
            json.dump(formula, f)
        log.info("  Formula: %s", formula["formula_str"])

        # Extract covariates
        extract_covariates(config, sites)

        log.info("Step 1 complete.")


if __name__ == "__main__":
    main()
