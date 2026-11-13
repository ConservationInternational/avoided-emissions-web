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
from pathlib import Path

import boto3
import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
import rioxarray
import xarray as xr
from logging_utils import configure_third_party_logging
from rasterio.features import rasterize
from rasterio.windows import Window
from rasterio.windows import bounds as window_bounds
from shapely.geometry import box, mapping, shape
from shapely.ops import unary_union
from shapely.prepared import prep
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
    "GDAL_CACHEMAX": 1024**3,  # Rasterio expects bytes, not MB.
    "GDAL_NUM_THREADS": "ALL_CPUS",
}


def _configure_gdal() -> rasterio.Env:
    """Configure Rasterio's GDAL and propagate options to worker threads."""
    for key, val in _GDAL_OPTS.items():
        os.environ.setdefault(key, str(val))
    return rasterio.Env(**_GDAL_OPTS)


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


def load_sites(sites_path: str) -> gpd.GeoDataFrame:
    """Load sites from GeoJSON, GeoPackage, or GeoParquet.

    The minimum-area filter is applied to whole sites by the webapp before
    they are split across exact-match regions, so it is not repeated here.
    """
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

    # Keep this precision budget aligned with services.polygon_partition.
    area_parts = []
    for start in range(0, len(sites), 128):
        dense = sites.geometry.iloc[start : start + 128].segmentize(0.005)
        area_parts.append(dense.to_crs("ESRI:54009").area.to_numpy() / 10000)
    computed_area = np.concatenate(area_parts)
    if "area_ha" in sites.columns:
        supplied_area = pd.to_numeric(sites["area_ha"], errors="coerce")
        if supplied_area.isna().any() or not np.allclose(
            supplied_area, computed_area, rtol=1e-8, atol=1e-6
        ):
            raise ValueError("Prepared site areas disagree with polygon geometry.")
    sites["area_ha"] = computed_area
    if "sub_site_index" not in sites.columns:
        sites["sub_site_index"] = 0
    if sites.duplicated(["site_id", "sub_site_index"]).any():
        raise ValueError("Duplicate immutable site/fragment identities.")

    # Numeric IDs (1-based)
    sites["id_numeric"] = range(1, len(sites) + 1)
    if len(sites) == 0:
        raise RuntimeError("No sites to process.")
    log.info("Sites: %d loaded", len(sites))

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
    _clip_bounds: tuple[float, float, float, float],
    grid: tuple,
    rows: np.ndarray,
    cols: np.ndarray,
    max_attempts: int = 5,
    base_delay_seconds: float = 1.5,
) -> np.ndarray:
    """Open a COG from scratch and extract candidate pixel values.

    Thread-safe: does not use any shared xr.Dataset. Only the reference
    tile is read. ``grid`` is its ``(transform, shape)``; a layer on a
    different grid raises instead of being silently misaligned.
    """
    delay = base_delay_seconds
    for attempt in range(1, max_attempts + 1):
        try:
            _, opened = _open_single_cog(uri, layer_name)
            try:
                # Integer windows avoid clip_box rounding across tile edges and
                # support final tiles that are only one pixel wide or high.
                ref_transform, (height, width) = grid
                col, row = ~opened.rio.transform() * (ref_transform.c, ref_transform.f)
                left, top = round(col), round(row)
                if left < 0 or top < 0:
                    raise GridMismatchError(
                        f"Layer '{layer_name}' does not cover the reference tile."
                    )
                da = opened.isel(
                    y=slice(top, top + height), x=slice(left, left + width)
                )
                _check_grid(layer_name, da, grid)
                nodata = da.rio.nodata
                arr = da.values
            finally:
                opened.close()
            del da
            if arr.ndim == 3:
                arr = arr[0]
            vals = arr[rows, cols]
            del arr  # free full-grid array; only candidate values survive
            if vals.dtype == np.float64:
                vals = vals.astype(np.float32)
            if nodata is not None and nodata != 0:
                vals = np.where(vals == nodata, np.nan, vals).astype(np.float32)
            return vals
        except GridMismatchError:
            raise
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
            time.sleep(sleep_for)
            delay = min(delay * 2, 20.0)
    raise RuntimeError(f"Exhausted retries while reading layer '{layer_name}'.")


class GridMismatchError(RuntimeError):
    """A covariate layer is not on the reference pixel grid."""


def _check_grid(layer_name: str, da: xr.DataArray, grid: tuple) -> None:
    """Raise unless *da* has the reference transform and shape.

    All COGs are exported with the same ``crsTransform``, so valid layers
    match exactly apart from floating-point noise in the pixel size.
    """
    ref_transform, ref_shape = grid
    transform = da.rio.transform()
    shape_ = (da.sizes["y"], da.sizes["x"])
    tol = abs(ref_transform.a) * 1e-3
    same = shape_ == ref_shape and all(
        abs(a - b) <= tol for a, b in zip(transform[:6], ref_transform[:6])
    )
    if not same:
        raise GridMismatchError(
            f"Layer '{layer_name}' is not on the reference grid: "
            f"transform {tuple(transform)[:6]}, shape {shape_}; expected "
            f"{tuple(ref_transform)[:6]}, shape {ref_shape}"
        )


def open_reference_grid(
    cog_bucket: str,
    cog_prefix: str,
    covariate_names: list[str],
) -> xr.DataArray:
    """Open all covariate COGs (metadata only) and return the first one.

    Fails early if any layer has a different CRS or pixel size.  Exact
    alignment of the clipped window is checked for every layer when its
    pixels are read (``_load_layer_fresh``).
    """
    uris = {name: _s3_path(cog_bucket, cog_prefix, name) for name in covariate_names}
    n_workers = min(8, len(covariate_names))
    log.info(
        "  Opening %d COGs in parallel (%d workers)...", len(covariate_names), n_workers
    )
    opened: dict[str, xr.DataArray] = {}
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {
            pool.submit(_open_single_cog, uri, name): name for name, uri in uris.items()
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                opened[name] = future.result()[1]
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to open covariate '{name}' at {uris[name]}"
                ) from exc

    ref = opened[covariate_names[0]]
    ref_res = ref.rio.resolution()
    for name in covariate_names[1:]:
        da = opened[name]
        if da.rio.crs != ref.rio.crs:
            raise GridMismatchError(
                f"CRS mismatch: '{name}' has {da.rio.crs}, expected {ref.rio.crs}"
            )
        if any(abs(a - b) > 1e-9 for a, b in zip(da.rio.resolution(), ref_res)):
            raise GridMismatchError(
                f"Resolution mismatch: '{name}' has {da.rio.resolution()}, "
                f"expected {ref_res}"
            )
    return ref


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


def _treatment_memberships(sites, transform, width, height, ys, xres, yres):
    """Build sparse cell membership without overwriting overlapping sites."""
    from rasterio.windows import Window, from_bounds
    from rasterio.windows import transform as window_transform

    frames = []
    # Window each polygon part separately: a MultiPolygon with parts on both
    # sides of the antimeridian would otherwise scan the full grid width.
    parts = [
        (part, row)
        for row in sites.itertuples()
        for part in getattr(row.geometry, "geoms", [row.geometry])
        if part is not None and not part.is_empty
    ]
    for part, row in parts:
        window = from_bounds(*part.bounds, transform=transform)
        left = max(0, int(np.floor(window.col_off)))
        top = max(0, int(np.floor(window.row_off)))
        right = min(width, int(np.ceil(window.col_off + window.width)))
        bottom = min(height, int(np.ceil(window.row_off + window.height)))
        if left >= right or top >= bottom:
            continue
        for start in range(top, bottom, 512):
            stop = min(start + 512, bottom)
            # Horizontal tiles also bound allocations for very wide polygons.
            for col in range(left, right, 512):
                end = min(col + 512, right)
                tile = Window(col, start, end - col, stop - start)
                mask = rasterize(
                    [(part, 1)],
                    out_shape=(stop - start, end - col),
                    transform=window_transform(tile, transform),
                    fill=0,
                    dtype="uint8",
                )
                rr, cc = np.nonzero(mask)
                if len(rr):
                    global_rows = rr + start
                    frames.append(
                        pd.DataFrame(
                            {
                                "cell": global_rows * width + cc + col,
                                "id_numeric": row.id_numeric,
                                "site_id": row.site_id,
                                "area_ha": calc_pixel_area_ha(
                                    ys[global_rows], yres, xres
                                ),
                            }
                        )
                    )
    if not frames:
        return pd.DataFrame(
            {
                "cell": pd.Series(dtype="int64"),
                "id_numeric": pd.Series(dtype="int64"),
                "site_id": pd.Series(dtype="str"),
                "area_ha": pd.Series(dtype="float64"),
            }
        )
    return pd.concat(frames, ignore_index=True).drop_duplicates(
        ["cell", "id_numeric"], ignore_index=True
    )


# ---------------------------------------------------------------------------
# Core extraction logic (GDAL + xarray)
# ---------------------------------------------------------------------------


def _mapped_fragment_ids(config: dict, sites: gpd.GeoDataFrame) -> np.ndarray:
    """``id_numeric`` of fragments in the exact-match group mapping.

    Fragments outside the mapping (no exact-match region) are never matched,
    so their pixels are not extracted as treatment.  Without a mapping every
    fragment is matched.
    """
    mapping_ = config.get("exact_match_group_mapping")
    if not mapping_:
        return sites["id_numeric"].to_numpy()
    units = mapping_.values() if isinstance(mapping_, dict) else mapping_
    keys = {(str(s), int(i)) for unit in units for s, i in unit}
    fragment_keys = zip(
        sites["site_id"].astype(str), sites["sub_site_index"].astype(int)
    )
    mapped = np.fromiter(
        (k in keys for k in fragment_keys), dtype=bool, count=len(sites)
    )
    return sites["id_numeric"].to_numpy()[mapped]


_EXTRACTION_TILE_SIZE = 1024


def extract_covariates(config: dict, sites: gpd.GeoDataFrame) -> None:
    """Extract bounded spatial tiles, preserving global cell IDs and schemas."""
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
            + ["total_biomass_2000", "fc_2000"]
            + config["exact_match_vars"]
            + [f"fc_{y}" for y in config["fc_years"]]
        )
    )
    layer_uris = {name: _s3_path(cog_bucket, cog_prefix, name) for name in all_layers}
    log.info("Loading %d covariate layers from S3", len(all_layers))

    # --- 1. open lazily ---
    ref = open_reference_grid(cog_bucket, cog_prefix, all_layers)

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

    ref = ref.rio.clip_box(*clip_bounds)
    transform = ref.rio.transform()
    height, width = ref.sizes["y"], ref.sizes["x"]
    if expected_pix is not None and abs(abs(transform.a) - expected_pix) > 1e-6:
        raise RuntimeError(
            f"COG pixel size ({abs(transform.a):.8f}°) does not match the requested "
            f"resolution_m={resolution_m} (expected {expected_pix:.8f}°). "
            f"Check that cog_prefix '{cog_prefix}' points to the correct "
            f"resolution COGs."
        )
    log.info(
        "Grid: %d x %d; extracting tiles of at most %d x %d cells",
        width,
        height,
        _EXTRACTION_TILE_SIZE,
        _EXTRACTION_TILE_SIZE,
    )
    output = Path(config["output_dir"])
    controls_dir = output / "control_pixels"
    controls_dir.mkdir(exist_ok=True)
    extent = prep(extent_geom)
    site_index = sites.sindex
    key_schema = pa.schema(
        [
            ("cell", pa.int64()),
            ("id_numeric", pa.int64()),
            ("site_id", pa.string()),
            ("area_ha", pa.float64()),
        ]
    )
    empty_control_schema = None
    control_files = 0
    tile_count = 0
    # One tile's temporary files exist at a time. Writers for the two single-file
    # outputs retain only Parquet metadata, never previous tiles' pixel arrays.
    with (
        tempfile.TemporaryDirectory(prefix="extract-", dir=output) as temp,
        pq.ParquetWriter(
            output / "treatment_cell_key.parquet", key_schema, compression="zstd"
        ) as key_writer,
    ):
        treatment_writer = None
        try:
            for top in range(0, height, _EXTRACTION_TILE_SIZE):
                for left in range(0, width, _EXTRACTION_TILE_SIZE):
                    window = Window(
                        left,
                        top,
                        min(_EXTRACTION_TILE_SIZE, width - left),
                        min(_EXTRACTION_TILE_SIZE, height - top),
                    )
                    bounds = window_bounds(window, transform)
                    tile_geom = box(*bounds)
                    positions = site_index.query(tile_geom, predicate="intersects")
                    if not len(positions) and not extent.intersects(tile_geom):
                        continue
                    tile = tempfile.TemporaryDirectory(dir=temp)
                    tile_dir = Path(tile.name)
                    _extract_tile(
                        {**config, "output_dir": str(tile_dir)},
                        sites.iloc[positions],
                        ref.isel(
                            y=slice(top, top + int(window.height)),
                            x=slice(left, left + int(window.width)),
                        ),
                        bounds,
                        all_layers,
                        layer_uris,
                        top,
                        left,
                        width,
                    )
                    key_path = tile_dir / "treatment_cell_key.parquet"
                    with pq.ParquetFile(key_path) as source:
                        for batch in source.iter_batches(batch_size=65536):
                            key_writer.write_table(
                                pa.Table.from_batches([batch]).cast(key_schema)
                            )
                    key_path.unlink()
                    treatment_path = tile_dir / "treatment_pixels.parquet"
                    with pq.ParquetFile(treatment_path) as source:
                        if treatment_writer is None:
                            treatment_writer = pq.ParquetWriter(
                                output / "treatment_pixels.parquet",
                                source.schema_arrow,
                                compression="zstd",
                            )
                        for batch in source.iter_batches(batch_size=65536):
                            treatment_writer.write_batch(batch)
                    treatment_path.unlink()
                    for path in (tile_dir / "control_pixels").rglob("*.parquet"):
                        with pq.ParquetFile(path) as source:
                            if source.metadata.num_rows == 0:
                                empty_control_schema = source.schema_arrow
                                path.unlink()
                                continue
                        relative = path.relative_to(tile_dir / "control_pixels")
                        destination = controls_dir / relative.parent
                        destination.mkdir(parents=True, exist_ok=True)
                        path.replace(destination / f"part-{tile_count}.parquet")
                        control_files += 1
                    tile.cleanup()
                    tile_count += 1
                    log.info(
                        "Finished extraction tile %d (row=%d, col=%d)",
                        tile_count,
                        top,
                        left,
                    )
        finally:
            if treatment_writer is not None:
                treatment_writer.close()
    if treatment_writer is None:
        raise RuntimeError("No extraction tiles intersect the reference grid.")
    if not control_files:
        if empty_control_schema is None:
            raise RuntimeError("Extraction did not produce a control pixel schema.")
        pq.write_table(
            pa.Table.from_batches([], schema=empty_control_schema),
            controls_dir / "part-0.parquet",
            compression="zstd",
        )
    _write_grid_metadata(config, transform, width, height)
    ref.close()
    log.info("Extraction complete: %d bounded tiles", tile_count)


def _extract_tile(
    config, sites, ref, clip_bounds, all_layers, layer_uris, top, left, global_width
):
    """Apply the pixel-selection rules to one tile and write its outputs."""
    extent_geom = shape(config["matching_extent"])

    # Tile metadata only; no pixel data has been fetched yet.
    transform = ref.rio.transform()
    xres = abs(transform.a)
    yres = abs(transform.e)
    ys = ref.coords["y"].values
    height, width = len(ys), len(ref.coords["x"].values)
    grid = (transform, (height, width))
    del ref

    log.info("Grid: %d x %d (xres=%.6f, yres=%.6f)", width, height, xres, yres)

    log.info("Building sparse treatment cell membership...")
    # The key keeps every fragment so the summary can count pixels of
    # fragments that are not matched (e.g. no exact-match region).
    treatment_key = _treatment_memberships(
        sites, transform, width, height, ys, xres, yres
    )
    site_mask = np.zeros(height * width, dtype=bool)
    site_mask[treatment_key["cell"].to_numpy(dtype=np.int64)] = True
    mapped_ids = _mapped_fragment_ids(config, sites)
    matched_key = treatment_key[treatment_key["id_numeric"].isin(mapped_ids)]
    treatment_indices = np.unique(matched_key["cell"].to_numpy(dtype=np.int64))
    del matched_key
    treatment_mask = np.zeros(height * width, dtype=bool)
    treatment_mask[treatment_indices] = True
    log.info(
        "Treatment cells: %d memberships; %d cells in %d matched fragment(s) "
        "(%d of %d fragments are not matched)",
        len(treatment_key),
        len(treatment_indices),
        len(mapped_ids),
        len(sites) - len(mapped_ids),
        len(sites),
    )

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

    # Exclude candidate controls whose pixel centre lies within
    # min_control_distance_km of any site (done once here rather than in every
    # match worker).  Treatment pixels are added back below.
    exclusion_buffer = config.get("sites_exclusion_buffer")
    if exclusion_buffer:
        near_sites = rasterize(
            [(exclusion_buffer, 1)],
            out_shape=(height, width),
            transform=transform,
            fill=0,
            dtype="uint8",
            all_touched=False,
        ).ravel()
        log.info(
            "Excluding %d candidate pixels within the site exclusion buffer",
            int(np.count_nonzero(in_extent & (near_sites > 0))),
        )
        in_extent &= near_sites == 0
        del near_sites
    # No pixel inside any site, matched or not, may serve as a control.
    in_extent &= ~site_mask
    del site_mask
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
    # Read each layer's tile and retain only this tile's candidate values.
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
            grid,
            rows,
            cols,
        )
        # Keep pixels that either belong to a treatment site (unconditionally)
        # or had some tree cover in the baseline year.  Hansen GFC encodes
        # water/non-land as 0, so ref_vals > 0 is correct.
        _is_treatment = np.isin(candidate_indices, treatment_indices)
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

    # Only this tile's candidate arrays accumulate, regardless of job extent.
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
                grid,
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

    # Selection above uses tile-local cells; downstream consumers require the
    # same IDs as an untiled read of the complete clipped reference grid.
    def global_cells(cells):
        return (cells // width + top) * global_width + cells % width + left

    treatment_key["cell"] = global_cells(treatment_key["cell"].to_numpy())
    _treatment_mask = np.isin(data["cell"], treatment_indices)
    data["cell"] = global_cells(data["cell"])
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
    # The combined treatments_and_controls.parquet is no longer written: neither
    # the match step nor the summarize step reads it.

    _all_cols = list(data.keys())
    _WRITE_BLOCK = 65536

    # Determine the hive partition variable (first exact-match var present).
    _exact_match_vars: list[str] = config.get("exact_match_vars") or []
    _partition_var = next((v for v in _exact_match_vars if v in data), None)

    # Cast the partition column to int32 in-place so BOTH treatment_pixels and
    # the control hive directory names use identical integer keys (otherwise the
    # match step would compare float32 treatment values against int32 control
    # values parsed from the directory paths).  Missing values (NaN/Inf) become
    # -1, which the match step treats as missing, so they never merge with a
    # real value such as pa = 0.
    if _partition_var:
        _pv = data[_partition_var]
        data[_partition_var] = (
            np.nan_to_num(_pv, nan=-1.0, posinf=-1.0, neginf=-1.0)
            .round()
            .astype(np.int32)
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
        else:
            pq.write_table(
                pa.table({c: rows[c][:0] for c in columns}),
                path,
                compression="zstd",
            )
        return written

    # Treatment pixels without forest in any year can never be eligible for
    # matching, whatever the site's baseline year, so they are not written.
    _ever_forest = np.zeros(len(data["cell"]), dtype=bool)
    for _col in [c for c in _all_cols if c.startswith("fc_")]:
        _ever_forest |= np.nan_to_num(data[_col], nan=0.0) > 0

    # 1. treatment_pixels.parquet -- all columns, treatment rows with forest.
    _treatment_idx = np.flatnonzero(_treatment_mask & _ever_forest)
    log.info(
        "Dropping %d never-forested treatment pixels",
        int(np.count_nonzero(_treatment_mask & ~_ever_forest)),
    )
    del _ever_forest
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

    if _partition_var and _control_count > 0:
        # One hive directory + one file per distinct partition value.  For each
        # value we build a boolean mask over the (cheap, single-column) int32
        # partition array, collect that partition's control row indices, and
        # stream them in blocks without copying the complete tile table.
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
            "Written control_pixels/part-0.parquet (%d rows, unpartitioned)",
            _control_count,
        )

    del _control_mask
    del data
    gc.collect()

    log.info(
        "Saved treatment_cell_key.parquet, treatment_pixels.parquet, "
        "and control_pixels/"
    )


def _write_grid_metadata(config, transform, width, height):
    grid_meta = {
        "width": width,
        "height": height,
        "pixel_size_deg": float(abs(transform.a)),
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
    with open(os.path.join(config["output_dir"], "grid_metadata.json"), "w") as gf:
        json.dump(grid_meta, gf)


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
        "formula_str": "treatment ~ " + (" + ".join(terms) or "1"),
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


def _used_region_ids(sites_gdf: gpd.GeoDataFrame, polygon_vars: list[str]):
    """Region IDs of fragments assigned to every polygon var, or None."""
    if not all(v in sites_gdf.columns for v in polygon_vars):
        return None
    mapped = sites_gdf[polygon_vars].notna().all(axis=1)
    if not mapped.any():
        return None
    return {v: set(sites_gdf.loc[mapped, v].astype(str).unique()) for v in polygon_vars}


def _regions_for_extent(ref_gdf, sites_bbox, used_ids):
    """Reference features that can hold controls for the used strata.

    With known region IDs this is the used regions plus their neighbours:
    raster stratum values of fragment edge pixels can fall in an adjacent
    region.  Otherwise every feature in the sites bbox is used.
    """
    if used_ids is None or "region_id" not in ref_gdf.columns:
        return ref_gdf[ref_gdf.intersects(sites_bbox)]
    used = ref_gdf[ref_gdf["region_id"].astype(str).isin(used_ids)]
    if used.empty:
        return used
    idx = ref_gdf.sindex.query(
        used.geometry.make_valid().to_numpy(), predicate="intersects"
    )[1]
    return ref_gdf.iloc[np.unique(idx)]


def compute_matching_extent(
    sites_gdf: gpd.GeoDataFrame,
    exact_match_vars: list[str],
    reference_layer_uris: dict[str, str],
    tmp_dir: str,
) -> dict | None:
    """Return the intersection of reference-layer unions for the analysis.

    Each layer contributes the regions used by mapped fragments and their
    neighbours when fragments carry region IDs, else all features within the
    sites bbox.
    """
    polygon_vars = [
        v for v in exact_match_vars if v in _POLYGON_VARS and v in reference_layer_uris
    ]
    if not polygon_vars:
        log.info("No polygon exact-match vars — skipping matching_extent computation")
        return None

    minx, miny, maxx, maxy = sites_gdf.total_bounds
    pad = 0.1
    sites_bbox = box(minx - pad, miny - pad, maxx + pad, maxy + pad)
    used_region_ids = _used_region_ids(sites_gdf, polygon_vars)

    # Download all reference layers in parallel — each is an independent
    # S3 read that can proceed concurrently with the others.
    def _fetch_and_union(var_name: str):
        ref_gdf = _load_reference_layer(
            reference_layer_uris[var_name], tmp_dir, var_name
        )
        filtered = _regions_for_extent(
            ref_gdf,
            sites_bbox,
            None if used_region_ids is None else used_region_ids[var_name],
        )
        log.info(
            "Reference layer '%s': %d/%d features selected (%s)",
            var_name,
            len(filtered),
            len(ref_gdf),
            "used regions + neighbours" if used_region_ids else "sites bbox",
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


def _split_at_antimeridian(geom) -> list:
    """Return pieces of a lon-wrapped geometry translated into [-180, 180]."""
    from shapely.affinity import translate

    if not geom.is_valid:
        geom = make_valid(geom)
    pieces = []
    for offset in (-360.0, 0.0, 360.0):
        window = box(-180.0 - offset, -90.0, 180.0 - offset, 90.0)
        if not geom.intersects(window):
            continue
        piece = geom.intersection(window)
        if piece.is_empty or piece.area == 0:
            continue
        pieces.append(translate(piece, xoff=offset) if offset else piece)
    return pieces


def _check_buffer_width(buffered, source, distance_km: float) -> None:
    """Fail loudly if a buffer is far wider than its source plus the buffer.

    A buffer torn across the antimeridian turns into a band around the globe,
    which would silently exclude every control at that latitude.
    """
    if buffered.is_empty:
        raise ValueError("Exclusion buffer for a site is empty")
    minx, miny, maxx, maxy = buffered.bounds
    max_abs_lat = min(max(abs(miny), abs(maxy)), 89.0)
    if max_abs_lat > 85.0:
        return  # Near the poles a legitimate buffer can span all longitudes.
    km_per_deg = 111.32 * math.cos(math.radians(max_abs_lat))
    allowed = (source.bounds[2] - source.bounds[0]) + 2 * distance_km / km_per_deg
    if (maxx - minx) > allowed + 1.0:
        raise ValueError(
            f"Exclusion buffer spans {maxx - minx:.2f} degrees of longitude but "
            f"its site spans {source.bounds[2] - source.bounds[0]:.2f}; refusing a "
            "buffer that would wrap around the globe (site bounds "
            f"{tuple(round(v, 4) for v in source.bounds)})"
        )


def compute_sites_exclusion_buffer(
    sites_gdf: gpd.GeoDataFrame,
    distance_km: float,
) -> dict | None:
    """Buffer sites in local distance-preserving projections, then union."""
    if distance_km <= 0:
        log.info("min_control_distance_km <= 0 — skipping exclusion buffer")
        return None

    distance_m = distance_km * 1000
    valid_geoms = [
        make_valid(g) if not g.is_valid else g
        for g in sites_gdf.explode(index_parts=False).geometry
        if g is not None and not g.is_empty
    ]
    if not valid_geoms:
        raise ValueError(
            "Cannot compute exclusion buffer without valid site geometries"
        )

    # Equal-area projections do not preserve distances: at 60 degrees a
    # nominal 10 km CEA buffer extends roughly 20 km north/south.
    local_groups: dict[tuple[float, float], list] = {}
    for geom in valid_geoms:
        centre = geom.centroid
        key = (math.floor(centre.x / 5) * 5 + 2.5, math.floor(centre.y / 5) * 5 + 2.5)
        local_groups.setdefault(key, []).append(geom)

    # Share projection setup for nearby sites instead of doing it per feature.
    buffered = []
    for (lon, lat), geoms in local_groups.items():
        local_crs = f"+proj=aeqd +lat_0={lat} +lon_0={lon} +datum=WGS84 +units=m"
        # Longitudes continuous around the group centre, so buffers that cross
        # the antimeridian are not torn into 360-degree-wide invalid polygons.
        wrapped_crs = f"+proj=longlat +datum=WGS84 +lon_wrap={lon} +no_defs"
        source = gpd.GeoSeries(geoms, crs="EPSG:4326")
        projected = source.to_crs(local_crs)
        wrapped = (
            projected.simplify(min(100.0, distance_m / 100), preserve_topology=True)
            .buffer(distance_m)
            .to_crs(wrapped_crs)
        )
        source_wrapped = source.to_crs(wrapped_crs)
        for geom, src in zip(wrapped, source_wrapped):
            _check_buffer_width(geom, src, distance_km)
            buffered.extend(_split_at_antimeridian(geom))
    result = make_valid(unary_union(buffered))
    if result is None or result.is_empty:
        raise ValueError("Exclusion buffer is empty after reprojection")

    log.info(
        "sites_exclusion_buffer computed (%.1f km buffer, type=%s)",
        distance_km,
        result.geom_type,
    )
    return mapping(result)


# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    rollbar_init()
    with _configure_gdal(), with_rollbar("01_extract_covariates"):
        config = parse_config(argv)
        log.info("Step 1: Extracting covariates")
        log.info("  Config: %s", json.dumps(config, default=str))

        # Load & filter sites
        sites = load_sites(config["sites_file"])

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
