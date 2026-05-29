"""Ingest pre-computed SDG 15.3.1 COG from Trends.Earth public storage.

The source is a single multi-band Cloud-Optimized GeoTIFF hosted on
Google Cloud Storage.  This module extracts specific bands (SDG baseline,
status years), resamples them to the project grid, and uploads the
resulting single-band COGs to S3.

Because the source is a Cloud-Optimized GeoTIFF, GDAL can read it
directly via ``/vsicurl/`` — only the byte ranges for the requested
bands are fetched over HTTP, avoiding a full 7 GB download.
"""

import logging
import os
import shutil
import subprocess
import tempfile

import boto3

from config import Config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source COG location (accessed via /vsicurl/ — no local download needed)
# ---------------------------------------------------------------------------
SDG_COG_URL = (
    "https://storage.googleapis.com/trendsearth-public/"
    "unccd_reporting/2016-2023/"
    "TrendsEarth_SDG15.3.1_2000-2023_Trends.Earth.tif"
)
SDG_COG_VSICURL = f"/vsicurl/{SDG_COG_URL}"

# ---------------------------------------------------------------------------
# Band → covariate mapping
#
# Band numbers are 1-based (GDAL convention).
# - Band 1:  SDG 15.3.1 baseline indicator (2000–2015), values: -1, 0, 1
# - Band 9:  SDG 15.3.1 status (2019), values: 1–7
# - Band 14: SDG 15.3.1 status (2023), values: 1–7
# NoData for all bands: -32768
# ---------------------------------------------------------------------------
SDG_LAYERS = {
    "sdg_baseline": {
        "band": 1,
        "description": "SDG 15.3.1 baseline indicator (2000-2015)",
    },
    "sdg_status_2019": {
        "band": 9,
        "description": "SDG 15.3.1 status (2019)",
    },
    "sdg_status_2023": {
        "band": 14,
        "description": "SDG 15.3.1 status (2023)",
    },
}

# Grid specification — must match gee-export/config.py and rasterize_vectors.py
XMIN, YMIN, XMAX, YMAX = -180, -90, 180, 90
SRS = "EPSG:4326"

# Resolution presets (same as rasterize_vectors.py)
RESOLUTIONS = {
    1000: {"pixel_size_deg": 1 / 120, "cog_suffix": "_1km"},
    250: {"pixel_size_deg": 1 / 480, "cog_suffix": "_250m"},
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _run_cmd(cmd: list[str], env_override: dict | None = None) -> None:
    """Run a shell command, raising on failure.

    Parameters
    ----------
    cmd : list[str]
        Command and arguments to run.
    env_override : dict | None
        Additional environment variables to set for this command.
    """
    logger.info("Running: %s", " ".join(cmd))
    env = os.environ.copy()
    if env_override:
        env.update(env_override)
    result = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)
    if result.returncode != 0:
        logger.error("STDOUT: %s", result.stdout)
        logger.error("STDERR: %s", result.stderr)
        raise RuntimeError(
            f"Command failed (exit {result.returncode}): {' '.join(cmd)}\n"
            f"{result.stderr}"
        )


def _upload_to_s3(
    local_path: str, bucket: str, key: str, content_type: str = "image/tiff"
) -> str:
    """Upload a local file to S3 and return the HTTPS URL."""
    file_size = os.path.getsize(local_path)
    logger.info(
        "Uploading %s (%.1f MB) -> s3://%s/%s",
        local_path,
        file_size / (1024 * 1024),
        bucket,
        key,
    )
    s3 = boto3.client("s3", region_name=Config.AWS_REGION)
    extra_args = {
        "ContentType": content_type,
        "Tagging": "Project=avoided-emissions",
    }
    s3.upload_file(local_path, bucket, key, ExtraArgs=extra_args)
    url = f"https://{bucket}.s3.amazonaws.com/{key}"
    logger.info("Upload complete: %s", url)
    return url


# ---------------------------------------------------------------------------
# Band extraction + resampling
# ---------------------------------------------------------------------------


def extract_and_resample(
    source_path: str,
    band: int,
    output_name: str,
    workdir: str,
    pixel_size_deg: float,
) -> str:
    """Extract a single band from the source COG and resample to grid.

    Parameters
    ----------
    source_path : str
        Path or GDAL virtual path (e.g. ``/vsicurl/https://…``) to the
        multi-band source COG.  When using ``/vsicurl/``, GDAL fetches
        only the byte ranges it needs.
    band : int
        1-based band number to extract.
    output_name : str
        Base name for the output file (e.g. ``sdg_baseline``).
    workdir : str
        Temporary directory for intermediate files.
    pixel_size_deg : float
        Target pixel size in degrees.

    Returns
    -------
    str
        Path to the output COG file.
    """
    band_tif = os.path.join(workdir, f"{output_name}_band.tif")
    warped_tif = os.path.join(workdir, f"{output_name}_warped.tif")
    cog_tif = os.path.join(workdir, f"{output_name}.tif")

    # GDAL memory constraints to prevent OOM on global 250m rasters
    # merge-worker container has 4GB limit, so keep GDAL under 1.5GB total
    # to leave headroom for Python, OS, and intermediate file buffers
    gdal_env = {
        "GDAL_CACHEMAX": "1024",  # 1GB cache maximum
        "GDAL_NUM_THREADS": "ALL_CPUS",
        "GDAL_SWATH_SIZE": "256000000",  # 256MB swath for warp
    }

    # Step 1: Extract single band
    _run_cmd(
        [
            "gdal_translate",
            "-b",
            str(band),
            "-co",
            "COMPRESS=DEFLATE",
            "-co",
            "TILED=YES",
            "-co",
            "BIGTIFF=YES",
            source_path,
            band_tif,
        ],
        env_override=gdal_env,
    )

    # Step 2: Resample to target grid (mode for categorical data)
    # Use -wm (working memory) and -multi for chunked processing
    # Keep working memory low to stay within container's 4GB limit
    _run_cmd(
        [
            "gdalwarp",
            "-wm",
            "256",  # 256MB working memory (conservative for 4GB container)
            "-multi",  # Multi-threaded processing
            "-wo",
            "NUM_THREADS=ALL_CPUS",
            "-t_srs",
            SRS,
            "-te",
            str(XMIN),
            str(YMIN),
            str(XMAX),
            str(YMAX),
            "-tr",
            str(pixel_size_deg),
            str(pixel_size_deg),
            "-r",
            "mode",
            "-srcnodata",
            "-32768",
            "-dstnodata",
            "-32768",
            "-co",
            "COMPRESS=DEFLATE",
            "-co",
            "TILED=YES",
            "-co",
            "BIGTIFF=YES",
            "-overwrite",
            band_tif,
            warped_tif,
        ],
        env_override=gdal_env,
    )

    # Step 3: Convert to Cloud-Optimized GeoTIFF
    _run_cmd(
        [
            "gdal_translate",
            "-of",
            "COG",
            "-co",
            "COMPRESS=DEFLATE",
            "-co",
            "NUM_THREADS=ALL_CPUS",
            "-co",
            "BIGTIFF=IF_SAFER",
            warped_tif,
            cog_tif,
        ],
        env_override=gdal_env,
    )

    # Clean up intermediates immediately to free memory
    for tmp in (band_tif, warped_tif):
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
                logger.debug("Removed intermediate file: %s", tmp)
            except OSError as e:
                logger.warning("Failed to remove %s: %s", tmp, e)

    size_mb = os.path.getsize(cog_tif) / (1024 * 1024)
    logger.info("Created COG: %s (%.1f MB)", cog_tif, size_mb)
    return cog_tif


# ---------------------------------------------------------------------------
# End-to-end pipeline
# ---------------------------------------------------------------------------


def ingest_sdg_layers() -> dict[str, dict]:
    """Extract bands from the remote SDG COG, resample, and upload to S3.

    GDAL reads the source COG via ``/vsicurl/``, fetching only the byte
    ranges for the requested bands instead of downloading the full file.

    Returns
    -------
    dict
        Keyed by ``"{name}{cog_suffix}"`` with values like
        ``{"cog_url": str, "size_bytes": int, "skipped": bool}``.
    """
    bucket = Config.S3_BUCKET
    if not bucket:
        raise RuntimeError("S3_BUCKET is not configured")

    results: dict[str, dict] = {}
    workdir = tempfile.mkdtemp(prefix="sdg_ingest_")

    try:
        from botocore.exceptions import ClientError

        s3 = boto3.client("s3", region_name=Config.AWS_REGION)

        # Process resolutions sequentially to avoid memory spikes
        # (processing all layers at 250m resolution requires ~30GB+ memory)
        for resolution_m, res_cfg in RESOLUTIONS.items():
            cog_suffix = res_cfg["cog_suffix"]
            pixel_size_deg = res_cfg["pixel_size_deg"]
            prefix = f"{Config.S3_PREFIX}/cog{cog_suffix}".strip("/")

            logger.info(
                "Processing SDG layers at %dm resolution (pixel size: %.8f deg)",
                resolution_m,
                pixel_size_deg,
            )

            for name, layer_info in SDG_LAYERS.items():
                result_key = f"{name}{cog_suffix}"
                s3_key = f"{prefix}/{name}.tif"

                # Check if this specific layer already exists on S3
                try:
                    resp = s3.head_object(Bucket=bucket, Key=s3_key)
                    results[result_key] = {
                        "cog_url": f"https://{bucket}.s3.amazonaws.com/{s3_key}",
                        "size_bytes": resp.get("ContentLength", 0),
                        "skipped": True,
                    }
                    logger.info(
                        "SDG layer %s (%dm) already on S3 — skipping",
                        name,
                        resolution_m,
                    )
                    continue
                except ClientError:
                    pass  # not on S3 yet — proceed with extraction

                logger.info(
                    "Processing SDG layer: %s band %d (%dm)",
                    name,
                    layer_info["band"],
                    resolution_m,
                )

                try:
                    cog_path = extract_and_resample(
                        source_path=SDG_COG_VSICURL,
                        band=layer_info["band"],
                        output_name=f"{name}{cog_suffix}",
                        workdir=workdir,
                        pixel_size_deg=pixel_size_deg,
                    )
                    size_bytes = os.path.getsize(cog_path)
                    cog_url = _upload_to_s3(cog_path, bucket, s3_key)
                    results[result_key] = {
                        "cog_url": cog_url,
                        "size_bytes": size_bytes,
                        "skipped": False,
                    }
                    logger.info(
                        "Uploaded SDG layer %s (%dm) -> %s",
                        name,
                        resolution_m,
                        cog_url,
                    )
                    # Clean up COG file immediately after upload
                    try:
                        os.remove(cog_path)
                        logger.debug("Removed uploaded COG: %s", cog_path)
                    except OSError as e:
                        logger.warning("Failed to remove %s: %s", cog_path, e)
                except Exception as exc:
                    logger.exception(
                        "Failed to process SDG layer %s (%dm)",
                        name,
                        resolution_m,
                    )
                    results[result_key] = {
                        "error": str(exc)[:500],
                        "skipped": False,
                    }
                    # Continue processing other layers even if one fails
                    continue
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
        logger.info("Cleaned up temporary directory: %s", workdir)

    return results
