#!/usr/bin/env python3
"""Step 1: Extract covariate values for treatment sites and control regions.

Loads covariate COGs from S3 via GDAL virtual filesystems, loads site
polygons, identifies treatment pixels (within sites) and control pixels
(same GADM region), and saves the extracted values for the matching step.

This is the Python rewrite of 01_extract_covariates.R, optimised for
speed using GDAL and xarray/rioxarray for Cloud-Optimised GeoTIFF access.

Input:
    - Task config JSON (--config)
    - Site polygons (GeoJSON, GeoPackage, or GeoParquet)
    - Covariate COGs on S3

Output:
    - {output_dir}/sites_processed.parquet
    - {output_dir}/treatment_cell_key.parquet
    - {output_dir}/treatment_pixels.parquet
    - {output_dir}/control_pixels/  (hive-partitioned by primary exact-match var)
    - {output_dir}/formula.json
    - {output_dir}/site_id_key.csv
"""

from __future__ import annotations

import argparse
import gc
import json
import logging
import math
import os
import random
import sys
import tempfile
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import rioxarray
import xarray as xr
from logging_utils import configure_third_party_logging
from osgeo import gdal
from rasterio.features import rasterize
from shapely.geometry import box, mapping, shape
from shapely.ops import unary_union
from shapely.validation import make_valid

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
configure_third_party_logging()

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

from py_utils import rollbar_init, with_rollbar

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

    # Validate required parameters (no silent defaults)
    required = [
        "max_treatment_pixels",
        "control_multiplier",
        "min_site_area_ha",
        "min_glm_treatment_pixels",
    ]
    missing = [k for k in required if k not in config]
    if missing:
        raise KeyError(f"Missing required config parameters: {', '.join(missing)}")

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
    """Load sites from GeoJSON, GeoPackage, or GeoParquet."""
    if sites_path.lower().endswith(".parquet"):
        sites = gpd.read_parquet(sites_path)
    else:
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


def _open_single_cog(uri: str, name: str) -> tuple[str, xr.DataArray]:
    """Open a single COG lazily (metadata only).  Thread-pool friendly."""
    da = rioxarray.open_rasterio(uri, chunks="auto")
    if "band" in da.dims and da.sizes["band"] == 1:
        da = da.squeeze("band", drop=True)
    return name, da


def _is_transient_raster_error(exc: Exception) -> bool:
    """Return True when *exc* looks like a transient network/COG read error.

    Walks the full exception chain (``__cause__`` / ``__context__``) so that
    wrapped errors like ``RasterioIOError: Read failed`` whose *cause* is a
    transient ``CPLE_AppDefinedError: ZIPDecode:...`` are still recognised.
    """
    transient_markers = (
        "503",
        "502",
        "504",
        "response_code",
        "zipdecode",
        "decoding error",
        "i/o error",
        "connection",
        "timeout",
        "read failed",
    )
    current: BaseException | None = exc
    seen: set[int] = set()  # guard against hypothetical cycles
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        msg = f"{type(current).__name__}: {current}".lower()
        if any(marker in msg for marker in transient_markers):
            return True
        current = current.__cause__ or current.__context__
    return False


def _load_layer_fresh(
    uri: str,
    layer_name: str,
    clip_bounds: tuple[float, float, float, float],
    rows: np.ndarray,
    cols: np.ndarray,
    max_attempts: int = 5,
    base_delay_seconds: float = 1.5,
) -> np.ndarray:
    """Open a COG from scratch and extract candidate pixel values.

    Thread-safe: does not use any shared xr.Dataset.  The full-grid array
    is freed before the function returns, so each caller only retains the
    small candidate-pixel array.
    """
    delay = base_delay_seconds
    for attempt in range(1, max_attempts + 1):
        try:
            _, da = _open_single_cog(uri, layer_name)
            da = da.rio.clip_box(*clip_bounds)
            arr = da.values
            del da  # free DataArray before full-grid array
            if arr.ndim == 3:
                arr = arr[0]
            vals = arr[rows, cols]
            del arr  # free full-grid array; only candidate values survive
            if vals.dtype == np.float64:
                vals = vals.astype(np.float32)
            return vals
        except Exception as exc:
            if attempt == max_attempts or not _is_transient_raster_error(exc):
                raise
            sleep_for = delay + random.uniform(0, 0.5)
            log.warning(
                "Transient read error for '%s' (attempt %d/%d): %s. Retrying in %.1fs",
                layer_name,
                attempt,
                max_attempts,
                exc,
                sleep_for,
            )
            gdal.ErrorReset()
            time.sleep(sleep_for)
            delay = min(delay * 2, 20.0)
    raise RuntimeError(f"Exhausted retries while reading layer '{layer_name}'.")


def open_covariate_dataset(
    cog_bucket: str,
    cog_prefix: str,
    covariate_names: list[str],
) -> xr.Dataset:
    """Open all covariate COGs and return a validated, coordinate-snapped Dataset.

    Opens each COG in parallel via rioxarray / GDAL's ``/vsis3/`` virtual
    filesystem (metadata only — no pixel data is fetched).  Validates that
    all layers share the same CRS and resolution, and snaps their coordinate
    arrays to a common reference grid.

    The returned Dataset is used exclusively for grid metadata (transform,
    shape, CRS) and spatial clipping.  Pixel data is loaded separately via
    ``_load_layer_fresh`` to enable parallel, memory-efficient reads.
    """
    # -- Phase 1: open all COGs in parallel (metadata-only reads) ----------
    uris = {name: _s3_path(cog_bucket, cog_prefix, name) for name in covariate_names}
    opened: dict[str, xr.DataArray] = {}
    n_workers = min(8, len(covariate_names))
    log.info(
        "  Opening %d COGs in parallel (%d workers)...", len(covariate_names), n_workers
    )

    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {
            pool.submit(_open_single_cog, uri, name): name for name, uri in uris.items()
        }
        for future in as_completed(futures):
            fname = futures[future]
            try:
                _, da = future.result()
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to open covariate '{fname}' at {uris[fname]}"
                ) from exc
            opened[fname] = da
            log.info("  Opened: %s", uris[fname])

    # -- Phase 2: validate CRS / resolution and snap coordinates -----------
    arrays: dict[str, xr.DataArray] = {}
    ref_crs = None
    ref_res = None
    ref_x = None
    ref_y = None

    for name in covariate_names:
        da = opened[name]

        # Validate that all COGs share the same CRS and resolution
        layer_crs = da.rio.crs
        layer_res = da.rio.resolution()
        if ref_crs is None:
            ref_crs = layer_crs
            ref_res = layer_res
            ref_x = da.coords["x"].values
            ref_y = da.coords["y"].values
        else:
            if layer_crs != ref_crs:
                raise RuntimeError(
                    f"CRS mismatch: '{name}' has {layer_crs}, expected {ref_crs}"
                )
            # Allow tiny floating-point differences in pixel size
            # (e.g. 1/120 vs GEE's 30 arc-second grid constant).
            if layer_res != ref_res and not all(
                abs(a - b) < 1e-6 for a, b in zip(layer_res, ref_res)
            ):
                raise RuntimeError(
                    f"Resolution mismatch: '{name}' has {layer_res}, expected {ref_res}"
                )

            # Snap coordinates to the reference grid so that all layers
            # share identical x/y arrays.  Without this, layers whose
            # resolution differs by a tiny amount (e.g. 1/120 vs GEE's
            # 30 arc-second constant) end up with divergent coordinate
            # values, causing rioxarray's clip_box to fail with
            # "Bounds and transform are inconsistent".
            if da.sizes["x"] == len(ref_x) and da.sizes["y"] == len(ref_y):
                if not np.array_equal(da.coords["x"].values, ref_x):
                    log.info("    Snapping '%s' coords to reference grid", name)
                    da = da.assign_coords(x=ref_x, y=ref_y)
            else:
                log.warning(
                    "Layer '%s' has different grid shape (%d×%d vs %d×%d) "
                    "— reindexing to reference grid",
                    name,
                    da.sizes["x"],
                    da.sizes["y"],
                    len(ref_x),
                    len(ref_y),
                )
                da = da.reindex(x=ref_x, y=ref_y, method="nearest", tolerance=0.01)

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
    1. Open all COGs to validate CRS/resolution and get grid metadata
       (transform, shape).  No pixel data is fetched at this stage.
    2. Determine the spatial clip window from the ``matching_extent``
       polygon (intersection of exact-match reference layers within
       the sites bbox).
    3. Clip the dataset to that window; extract grid metadata.
    4. Rasterize sites and compute candidate pixel indices.
    5. Load the baseline forest-cover layer to prune non-forest/water
       pixels from the candidate set, reducing memory for global extents.
    6. Load all remaining layers in parallel — each thread opens its COG
       fresh, extracts candidate values, and frees the full grid before
       returning.  Peak memory is O(N_parallel × grid + N_candidates × N_layers).
    7. Build treatment_cell_key and the full pixel DataFrame in-memory.
    """
    cog_bucket = config["cog_bucket"]
    cog_prefix = config["cog_prefix"]

    # Nominal resolution from task config (default 1 km for backwards compat)
    resolution_m = config.get("resolution_m", 1000)
    _expected_pixel_sizes = {1000: 1 / 120, 250: 1 / 480}
    expected_pix = _expected_pixel_sizes.get(resolution_m)
    log.info("Extraction resolution: %d m (cog_prefix=%s)", resolution_m, cog_prefix)

    # Layer names to load
    all_layers = list(
        dict.fromkeys(
            config["covariates"]
            + ["total_biomass_2025"]
            + config["exact_match_vars"]
            + [f"fc_{y}" for y in config["fc_years"]]
        )
    )
    layer_uris = {name: _s3_path(cog_bucket, cog_prefix, name) for name in all_layers}
    log.info("Loading %d covariate layers from S3", len(all_layers))

    # --- 1. open lazily ---
    ds = open_covariate_dataset(cog_bucket, cog_prefix, all_layers)

    # --- 2. spatial window from matching extent ---
    matching_extent_geojson = config.get("matching_extent")
    if not matching_extent_geojson:
        raise RuntimeError(
            "Config must include 'matching_extent' — computed from the "
            "intersection of exact-match reference layers within the sites bbox."
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
    clip_bounds = clip_box.bounds
    log.info("Clipping covariates to matching extent bbox: %s", clip_box.bounds)

    ds = ds.rio.clip_box(*clip_bounds)

    # --- 3. grid metadata from the *unloaded* dataset ---
    # rioxarray reads CRS, transform, and shape from COG headers;
    # no pixel data has been fetched yet.
    first_var = ds[all_layers[0]]
    transform = first_var.rio.transform()
    xres = abs(transform.a)
    yres = abs(transform.e)
    ys = ds.coords["y"].values
    height, width = len(ys), len(ds.coords["x"].values)

    log.info("Grid: %d x %d (xres=%.6f, yres=%.6f)", width, height, xres, yres)

    # Validate that the actual COG pixel size matches the requested resolution.
    if expected_pix is not None and abs(xres - expected_pix) > 1e-6:
        raise RuntimeError(
            f"COG pixel size ({xres:.8f}°) does not match the requested "
            f"resolution_m={resolution_m} (expected {expected_pix:.8f}°). "
            f"Check that cog_prefix '{cog_prefix}' points to the correct "
            f"resolution COGs."
        )

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

    # Free the full-grid site mask — values are captured in treatment_key
    del site_mask, site_ids_flat

    # --- 6. determine candidate control pixels ---
    # Rasterize the matching extent polygon to identify the area
    # where valid controls can exist (the intersection of all
    # polygon-type exact-match layers that overlap the sites,
    # computed by the webapp via PostGIS).
    extent_mask_2d = rasterize(
        [(mapping(extent_geom), 1)],
        out_shape=(height, width),
        transform=transform,
        fill=0,
        dtype="uint8",
        all_touched=True,
    )
    in_extent = extent_mask_2d.ravel().astype(bool)
    del extent_mask_2d  # free full-grid raster
    candidate_mask = in_extent | treatment_mask
    del in_extent, treatment_mask  # free boolean arrays

    candidate_indices = np.nonzero(candidate_mask)[0]
    del candidate_mask

    log.info(
        "Extracting covariate values for %d candidate pixels...", len(candidate_indices)
    )

    # Pre-compute 2-D row/col indices once — avoids creating a full
    # H×W temporary via .ravel() for every layer.
    rows, cols = np.unravel_index(candidate_indices, (height, width))

    # Pixel area (ha) for all candidate pixels (treatment + control).
    # This is required by step 3 to convert forest cover fractions to
    # absolute area and to estimate emissions.
    y_coords_candidates = ys[rows]
    candidate_pixel_areas = calc_pixel_area_ha(y_coords_candidates, yres, xres)

    # --- 7. layer-by-layer loading ---
    # Load each layer individually from S3, extract the candidate pixel
    # values, then discard the full grid.  Peak memory stays at
    # O(grid + N_layers × N_candidates) instead of
    # O(N_layers × grid + N_layers × N_candidates).
    # Downcast float64 → float32 to halve memory and Parquet size;
    # covariates (elevation, slope, forest cover, etc.) don't need
    # float64 precision.

    # Before loading all layers, use the earliest forest cover year as a mask
    # to prune non-forest and water pixels from the candidate set.  The avoided
    # emissions analysis only applies to forest pixels; any pixel with zero tree
    # cover in the baseline year cannot deforest, so excluding it here
    # dramatically reduces the number of candidate pixels for globally
    # distributed site sets.
    # Hansen GFC exports water/non-land pixels as 0 (GEE masks → 0 on export),
    # so the filter ref_vals > 0 correctly removes both ocean and non-forest.
    # Treatment pixels (inside PA boundaries) are always kept unconditionally.
    _fc_years = config.get("fc_years") or []
    _PRUNE_LAYER = f"fc_{min(_fc_years)}" if _fc_years else None
    if _PRUNE_LAYER and _PRUNE_LAYER in all_layers:
        log.info(
            "Pre-loading '%s' (baseline forest cover) to prune non-forest "
            "candidate pixels (%d candidates before pruning)...",
            _PRUNE_LAYER,
            len(candidate_indices),
        )
        _ref_vals = _load_layer_fresh(
            layer_uris[_PRUNE_LAYER],
            _PRUNE_LAYER,
            clip_bounds,
            rows,
            cols,
        )
        # Keep pixels that either belong to a treatment site (unconditionally)
        # or had some tree cover in the baseline year.  Hansen GFC encodes
        # water/non-land as 0, so ref_vals > 0 is correct.
        _is_treatment = np.isin(candidate_indices, treatment_key["cell"].values)
        _keep = _is_treatment | (_ref_vals > 0)
        del _is_treatment
        n_before = len(candidate_indices)
        candidate_indices = candidate_indices[_keep]
        rows = rows[_keep]
        cols = cols[_keep]
        candidate_pixel_areas = candidate_pixel_areas[_keep]
        _ref_vals = _ref_vals[_keep]
        del _keep
        log.info(
            "Non-forest pruning: %d → %d candidates (%d non-forest/water pixels removed)",
            n_before,
            len(candidate_indices),
            n_before - len(candidate_indices),
        )
        data: dict[str, np.ndarray] = {
            "cell": candidate_indices,
            "area_ha": candidate_pixel_areas,
            _PRUNE_LAYER: _ref_vals,
        }
        del _ref_vals
        _preloaded: set[str] = {_PRUNE_LAYER}
    else:
        data = {
            "cell": candidate_indices,
            "area_ha": candidate_pixel_areas,
        }
        _preloaded = set()
        if not _PRUNE_LAYER:
            log.warning("No fc_years in config — skipping non-forest pruning")
        else:
            log.warning(
                "'%s' not in layer list — skipping non-forest pruning", _PRUNE_LAYER
            )

    # Free the lazy Dataset — it is no longer needed for pixel reads.
    # Subsequent reads open each COG fresh, bypassing the shared Dataset
    # entirely and making the reads safe to issue from multiple threads.
    del ds
    gc.collect()

    # Load remaining layers in parallel.  Each thread opens its COG from
    # scratch, reads the clipped grid, extracts candidate-pixel values,
    # then frees the full-grid array before returning — so only the small
    # candidate arrays accumulate.  N_PARALLEL=4 gives ~4× wall-time
    # speedup over sequential reads with ~15 GB extra peak memory.
    _N_PARALLEL = 4
    layers_to_load = [name for name in all_layers if name not in _preloaded]
    layer_futures: dict = {}
    with ThreadPoolExecutor(max_workers=_N_PARALLEL) as pool:
        for layer_name in layers_to_load:
            layer_futures[layer_name] = pool.submit(
                _load_layer_fresh,
                layer_uris[layer_name],
                layer_name,
                clip_bounds,
                rows,
                cols,
            )
        # Collect in all_layers order to keep log messages sequential and
        # preserve column order in the output DataFrame.
        for i, layer_name in enumerate(all_layers, 1):
            if layer_name in _preloaded:
                log.info(
                    "  [%d/%d] Already loaded %s (pre-pruning)",
                    i,
                    len(all_layers),
                    layer_name,
                )
                continue
            log.info("  [%d/%d] Awaiting %s", i, len(all_layers), layer_name)
            data[layer_name] = layer_futures[layer_name].result()
            log.info("  [%d/%d] Done    %s", i, len(all_layers), layer_name)

    log.info("Total covariate values extracted: %d pixels", len(data["cell"]))

    # --- 8. save outputs ---
    output_dir = config["output_dir"]

    treatment_key.to_parquet(
        os.path.join(output_dir, "treatment_cell_key.parquet"),
        index=False,
        compression="zstd",
    )

    # --- Split into treatment_pixels + control_pixels ---
    # The match step downloads the small treatment_pixels.parquet locally and
    # streams control_pixels/ directly from S3 via Arrow's native S3 filesystem,
    # reading only the hive partitions whose exact-match variable values appear
    # in its site's treatment pixels.  This avoids downloading the full control
    # pool for every one of the thousands of match workers.
    #
    # MEMORY STRATEGY (critical):
    #   The full pixel table is ~26 GB (148M rows x ~42 columns x 4 bytes) and
    #   that already dominates the container's available memory.  Anything that
    #   copies the table (pa.Table.filter), holds a second copy, or buffers one
    #   open Parquet writer per partition (pad.write_dataset over unsorted data)
    #   pushes peak memory over the limit and triggers the OOM killer.
    #
    #   So we never build a combined Arrow table and never sort/copy the whole
    #   dataset.  We operate directly on the numpy `data` dict and write each
    #   output by streaming row-blocks: for any set of rows we gather at most
    #   _WRITE_BLOCK rows across all columns at a time (~700 MB), append them to
    #   a single ParquetWriter, and free the block.  Peak stays at
    #   floor (~26 GB) + one block (~0.7 GB).
    #
    # The combined treatments_and_controls.parquet is no longer written: neither
    # the match step nor the summarize step reads it.

    _all_cols = list(data.keys())
    _WRITE_BLOCK = 4_000_000  # rows gathered per parquet write block (~700 MB)

    # Determine the hive partition variable (first exact-match var present).
    _exact_match_vars: list[str] = config.get("exact_match_vars") or []
    _partition_var = next((v for v in _exact_match_vars if v in data), None)

    # Cast the partition column to int32 in-place so BOTH treatment_pixels and
    # the control hive directory names use identical integer keys (otherwise the
    # match step would compare float32 treatment values against int32 control
    # values parsed from the directory paths).  NaN/Inf -> 0; pixels in
    # partition 0 are dropped by R's filter_groups() because no treatment pixel
    # ever lands there.
    if _partition_var:
        _pv = data[_partition_var]
        data[_partition_var] = (
            np.nan_to_num(_pv, nan=0.0, posinf=0.0, neginf=0.0).round().astype(np.int32)
        )
        del _pv
        gc.collect()
        log.info("Cast '%s' to int32 for hive partition keys", _partition_var)

    def _write_rows_blocked(rows, path, idx, columns):
        """Stream the given row indices of ``rows`` to one Parquet file.

        Gathers at most ``_WRITE_BLOCK`` rows across ``columns`` at a time so
        peak extra memory is one block, regardless of how many rows ``idx``
        selects.  Returns the number of rows written.
        """
        writer = None
        written = 0
        for start in range(0, idx.size, _WRITE_BLOCK):
            bidx = idx[start : start + _WRITE_BLOCK]
            block = pa.table({c: rows[c][bidx] for c in columns})
            if writer is None:
                writer = pq.ParquetWriter(path, block.schema, compression="zstd")
            writer.write_table(block)
            written += block.num_rows
            del block
        if writer is not None:
            writer.close()
        return written

    # Treatment membership mask (numpy bool, ~N bytes).
    _treatment_mask = np.isin(data["cell"], treatment_key["cell"].to_numpy())

    # 1. treatment_pixels.parquet -- all columns, all treatment rows.
    _treatment_idx = np.flatnonzero(_treatment_mask)
    _n_treatment = _write_rows_blocked(
        data,
        os.path.join(output_dir, "treatment_pixels.parquet"),
        _treatment_idx,
        _all_cols,
    )
    del _treatment_idx
    gc.collect()
    log.info("Written treatment_pixels.parquet: %d rows", _n_treatment)

    # 2. control_pixels/ -- every pixel that is not a treatment pixel.
    _control_mask = ~_treatment_mask
    del _treatment_mask
    _control_count = int(_control_mask.sum())
    _control_pixels_dir = os.path.join(output_dir, "control_pixels")
    os.makedirs(_control_pixels_dir, exist_ok=True)

    if _partition_var:
        # One hive directory + one file per distinct partition value.  For each
        # value we build a boolean mask over the (cheap, single-column) int32
        # partition array, collect that partition's control row indices, and
        # stream them in blocks.  Peak extra memory = one block + the per-value
        # masks (~150 MB), never a second copy of the table.
        _part_col = data[_partition_var]
        _value_cols = [c for c in _all_cols if c != _partition_var]
        _uniq = np.unique(_part_col[_control_mask])
        log.info(
            "Writing control_pixels/ for %d partition value(s) of '%s' ...",
            len(_uniq),
            _partition_var,
        )
        for _v in _uniq:
            _idx = np.flatnonzero(_control_mask & (_part_col == _v))
            if _idx.size == 0:
                continue
            _pdir = os.path.join(_control_pixels_dir, f"{_partition_var}={int(_v)}")
            os.makedirs(_pdir, exist_ok=True)
            _write_rows_blocked(
                data,
                os.path.join(_pdir, "part-0.parquet"),
                _idx,
                _value_cols,
            )
            del _idx
        log.info(
            "Written control_pixels/ (%d rows across %d partitions of '%s')",
            _control_count,
            len(_uniq),
            _partition_var,
        )
    else:
        # No exact-match vars: a single flat file, still streamed in blocks.
        _idx = np.flatnonzero(_control_mask)
        _write_rows_blocked(
            data,
            os.path.join(_control_pixels_dir, "part-0.parquet"),
            _idx,
            _all_cols,
        )
        del _idx
        log.info(
            "Written control_pixels/part-0.parquet (%d rows, no partition var)",
            _control_count,
        )

    del _control_mask
    del data
    gc.collect()

    # Save grid metadata so downstream steps can convert cell indices
    # back to geographic coordinates (lon/lat).
    grid_meta = {
        "width": width,
        "height": height,
        "pixel_size_deg": float(xres),
        "resolution_m": config.get("resolution_m"),
        "transform": [
            float(transform.a),
            float(transform.b),
            float(transform.c),
            float(transform.d),
            float(transform.e),
            float(transform.f),
        ],
    }
    with open(os.path.join(output_dir, "grid_metadata.json"), "w") as gf:
        json.dump(grid_meta, gf)

    log.info(
        "Saved treatment_cell_key.parquet, treatment_pixels.parquet, "
        "and control_pixels/"
    )


# ---------------------------------------------------------------------------
# Formula builder
# ---------------------------------------------------------------------------


# Covariates that are categorical integer IDs and must be wrapped with
# factor() in the R formula so that GLM treats them as discrete levels
# rather than continuous numeric values.
_CATEGORICAL_COVARIATES = {"ecoregion", "admin0", "admin1", "admin2"}


def build_matching_formula(covariates: list[str]) -> dict:
    """Build a formula dict: {"lhs": "treatment", "rhs": ["cov1", ...]}."""
    terms = []
    for cov in covariates:
        if cov in _CATEGORICAL_COVARIATES:
            terms.append(f"factor({cov})")
        else:
            terms.append(cov)
    return {
        "lhs": "treatment",
        "rhs": list(covariates),
        "formula_str": "treatment ~ " + " + ".join(terms),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Spatial prep helpers
# ---------------------------------------------------------------------------

_POLYGON_VARS = {"admin0", "admin1", "admin2", "ecoregion"}


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    uri = uri.removeprefix("s3://")
    parts = uri.split("/", 1)
    return parts[0], parts[1] if len(parts) > 1 else ""


def _download_ref_parquet(s3_uri: str, local_path: str) -> None:
    bucket, key = _parse_s3_uri(s3_uri)
    log.info("Downloading %s → %s", s3_uri, local_path)
    boto3.client("s3").download_file(bucket, key, local_path)


def _load_reference_layer(
    s3_uri: str, tmp_dir: str, layer_name: str
) -> gpd.GeoDataFrame:
    local_path = os.path.join(tmp_dir, f"{layer_name}.parquet")
    _download_ref_parquet(s3_uri, local_path)
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
    """Return the intersection of reference-layer unions within the sites bbox."""
    polygon_vars = [
        v for v in exact_match_vars if v in _POLYGON_VARS and v in reference_layer_uris
    ]
    if not polygon_vars:
        log.info("No polygon exact-match vars — skipping matching_extent computation")
        return None

    minx, miny, maxx, maxy = sites_gdf.total_bounds
    pad = 0.1
    sites_bbox = box(minx - pad, miny - pad, maxx + pad, maxy + pad)

    # Download all reference layers in parallel — each is an independent
    # S3 read that can proceed concurrently with the others.
    def _fetch_and_union(var_name: str):
        ref_gdf = _load_reference_layer(
            reference_layer_uris[var_name], tmp_dir, var_name
        )
        filtered = ref_gdf[ref_gdf.intersects(sites_bbox)].copy()
        log.info(
            "Reference layer '%s': %d/%d features intersect sites bbox",
            var_name,
            len(filtered),
            len(ref_gdf),
        )
        if filtered.empty:
            log.warning(
                "No features from '%s' intersect bbox — matching_extent=None",
                var_name,
            )
            return None
        geoms = [
            make_valid(g) if not g.is_valid else g
            for g in filtered.geometry
            if g is not None and not g.is_empty
        ]
        layer_union = unary_union(geoms)
        if not layer_union.is_valid:
            layer_union = make_valid(layer_union)
        log.info("Reference layer '%s': union complete", var_name)
        return layer_union

    layer_unions: list = []
    with ThreadPoolExecutor(max_workers=len(polygon_vars)) as pool:
        futures = {pool.submit(_fetch_and_union, v): v for v in polygon_vars}
        for future in as_completed(futures):
            result = future.result()
            if result is None:
                return None
            layer_unions.append(result)

    if not layer_unions:
        return None

    result = layer_unions[0]
    for union in layer_unions[1:]:
        result = result.intersection(union)
        if result.is_empty:
            log.warning(
                "Intersection of reference layers is empty — matching_extent=None"
            )
            return None
        if not result.is_valid:
            result = make_valid(result)

    log.info("matching_extent computed (type=%s)", result.geom_type)
    return mapping(result)


def compute_sites_exclusion_buffer(
    sites_gdf: gpd.GeoDataFrame,
    distance_km: float,
) -> dict | None:
    """Buffer the simplified union of all site geometries by *distance_km*.

    Pre-simplifies each geometry before unioning to dramatically reduce
    vertex count and computation time for large site sets.
    """
    if distance_km <= 0:
        log.info("min_control_distance_km <= 0 — skipping exclusion buffer")
        return None

    distance_m = distance_km * 1000
    valid_geoms = [
        make_valid(g) if not g.is_valid else g
        for g in sites_gdf.geometry
        if g is not None and not g.is_empty
    ]
    if not valid_geoms:
        log.warning("No valid site geometries — skipping exclusion buffer")
        return None

    # Simplify before union to reduce vertex count (major performance win for
    # large globally-distributed site sets).
    simplified = [g.simplify(0.001, preserve_topology=True) for g in valid_geoms]
    sites_union = unary_union(simplified)
    if not sites_union.is_valid:
        sites_union = make_valid(sites_union)

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
        sites_out = pd.DataFrame(sites.copy())
        sites_out["geometry"] = sites.geometry.to_wkt()
        sites_out.to_parquet(
            os.path.join(config["output_dir"], "sites_processed.parquet"),
            index=False,
            compression="zstd",
        )

        # Build and save matching formula
        formula = build_matching_formula(config["covariates"])
        with open(os.path.join(config["output_dir"], "formula.json"), "w") as f:
            json.dump(formula, f)
        log.info("  Formula: %s", formula["formula_str"])

        # Save exact-match group mapping if cross-site grouping is enabled
        if config.get("group_by_exact_matches") and config.get(
            "exact_match_group_mapping"
        ):
            group_mapping = config["exact_match_group_mapping"]
            # Convert tuple keys to strings for JSON serialization
            # group_mapping is {group_id: [(site_id, sub_site_index), ...]}
            group_mapping_json = {int(k): v for k, v in group_mapping.items()}
            with open(
                os.path.join(config["output_dir"], "exact_match_groups.json"), "w"
            ) as f:
                json.dump(group_mapping_json, f)
            log.info("  Saved exact-match group mapping: %d groups", len(group_mapping))

        # Compute matching_extent and sites_exclusion_buffer when not already
        # provided in config.  Results are written to prep_summary.json so
        # downstream match/summarize steps can load them from S3.
        _reference_layer_uris: dict = config.get("reference_layer_uris") or {}
        _exact_match_vars: list = config.get("exact_match_vars") or []
        _min_dist_km: float = float(config.get("min_control_distance_km", 10.0))

        if not config.get("matching_extent"):
            _t0 = time.time()
            with tempfile.TemporaryDirectory(prefix="ae_ref_") as _tmp_ref:
                _me = compute_matching_extent(
                    sites, _exact_match_vars, _reference_layer_uris, _tmp_ref
                )
            log.info("matching_extent computed in %.1fs", time.time() - _t0)
            config["matching_extent"] = _me

        if not config.get("sites_exclusion_buffer") and _min_dist_km > 0:
            _t0 = time.time()
            _seb = compute_sites_exclusion_buffer(sites, _min_dist_km)
            log.info("sites_exclusion_buffer computed in %.1fs", time.time() - _t0)
            config["sites_exclusion_buffer"] = _seb

        # Write prep_summary.json so downstream match/summarize steps can
        # override their config with the correct spatial params.
        with open(os.path.join(config["output_dir"], "prep_summary.json"), "w") as _fh:
            json.dump(
                {
                    "matching_extent": config.get("matching_extent"),
                    "sites_exclusion_buffer": config.get("sites_exclusion_buffer"),
                },
                _fh,
            )
        log.info("prep_summary.json written for downstream pipeline steps")

        # Extract covariates
        extract_covariates(config, sites)

        log.info("Step 1 complete.")


if __name__ == "__main__":
    main()
