"""GEE batch task management for covariate exports.

Provides a function to start a GEE Export.image.toCloudStorage task for a
single covariate, returning the task object for status tracking.
"""

import ee

from config import (
    COVARIATES,
    EXPORT_CRS,
    EXPORT_CRS_TRANSFORM,
    MAX_PIXELS_PER_TASK,
)
from derived_layers import get_derived_image


def _load_simple_image(covariate_name, cfg):
    """Load an ee.Image from a simple asset reference.

    Handles both ImageCollection (with optional year filter) and single
    Image assets.
    """
    asset = cfg["asset"]
    bands = cfg.get("select", [])

    if cfg.get("filter_year"):
        # ImageCollection filtered to a specific year
        col = ee.ImageCollection(asset).filter(ee.Filter.eq("year", cfg["filter_year"]))
        image = col.mosaic()
    else:
        # Try as Image first; if it's a FeatureCollection, handle separately
        try:
            image = ee.Image(asset)
        except Exception:
            image = ee.ImageCollection(asset).mosaic()

    if bands:
        image = image.select(bands)

    # Rename to match the covariate name
    if image.bandNames().size().getInfo() == 1:
        image = image.rename(covariate_name)

    return image


def get_covariate_image(covariate_name):
    """Build or load the ee.Image for a named covariate.

    Args:
        covariate_name: Key from config.COVARIATES.

    Returns:
        An ee.Image with a single band named after the covariate.
    """
    cfg = COVARIATES[covariate_name]

    if cfg.get("derived"):
        return get_derived_image(covariate_name, cfg)
    else:
        return _load_simple_image(covariate_name, cfg)


def _apply_resampling(image, covariate_name, crs, crs_transform):
    """Apply appropriate spatial resampling for a covariate.

    Uses reduceResolution with the reducer specified in the covariate's
    'resample' config (mean, sum, or mode) to properly aggregate
    finer-resolution pixels to the target ~1km export scale.

    Args:
        image: The ee.Image to resample.
        covariate_name: Key from COVARIATES.
        crs: Target CRS string.
        crs_transform: 6-element affine transform list for the target grid.

    Returns:
        An ee.Image resampled to the target resolution.
    """
    cfg = COVARIATES[covariate_name]
    method = cfg.get("resample", "mean")

    reducers = {
        "mean": ee.Reducer.mean(),
        "sum": ee.Reducer.sum(),
        "mode": ee.Reducer.mode(),
    }
    reducer = reducers.get(method, ee.Reducer.mean())

    return image.reduceResolution(reducer=reducer, maxPixels=65536).reproject(
        crs=crs, crsTransform=crs_transform
    )


def start_export_task(
    covariate_name, bucket, prefix, region=None, description_prefix="ae_cov"
):
    """Start a GEE batch export task for a single covariate.

    Exports the covariate as a Cloud-Optimized GeoTIFF to GCS.
    Applies appropriate resampling (mean, sum, or mode) based on
    the covariate's configuration before exporting at ~1km resolution.

    All exports use an explicit ``crsTransform`` (rather than ``scale``)
    so that every covariate lands on exactly the same pixel grid.

    Args:
        covariate_name: Key from config.COVARIATES.
        bucket: GCS bucket name.
        prefix: GCS path prefix (no trailing slash).
        region: ee.Geometry for the export region; defaults to global.
        description_prefix: Prefix for the GEE task description.

    Returns:
        The started ee.batch.Task object.
    """
    image = get_covariate_image(covariate_name)
    export_region = region or ee.Geometry.Rectangle(
        [-180, -90, 180, 90], proj=None, geodesic=False
    )

    # Apply appropriate resampling for the target resolution
    image = _apply_resampling(image, covariate_name, EXPORT_CRS, EXPORT_CRS_TRANSFORM)

    file_prefix = f"{prefix}/{covariate_name}".strip("/")
    task_description = f"{description_prefix}_{covariate_name}"

    task = ee.batch.Export.image.toCloudStorage(
        image=image.toFloat(),
        description=task_description,
        bucket=bucket,
        fileNamePrefix=file_prefix,
        region=export_region,
        crs=EXPORT_CRS,
        crsTransform=EXPORT_CRS_TRANSFORM,
        maxPixels=MAX_PIXELS_PER_TASK,
        fileFormat="GeoTIFF",
        formatOptions={"cloudOptimized": True},
    )
    task.start()
    return task


def check_task_status(task):
    """Return the current status dict for a GEE task.

    Args:
        task: An ee.batch.Task object.

    Returns:
        A dict with keys: id, state, description, and (if failed) error_message.
    """
    status = task.status()
    result = {
        "id": status.get("id"),
        "state": status.get("state"),
        "description": status.get("description"),
    }
    if status.get("state") == "FAILED":
        result["error_message"] = status.get("error_message", "Unknown error")
    return result
