"""Merge GEE-exported covariate tiles into single Cloud-Optimized GeoTIFFs.

GEE splits global exports into multiple tiles when the image exceeds the
32,768-pixel dimension limit.  This module downloads all tiles for a given
covariate from GCS, merges them into one COG with lossless compression using
GDAL, and uploads the result to S3.

Requirements (already installed in webapp Dockerfile):
    * gdal-bin  — provides ``gdalbuildvrt`` and ``gdal_translate``
    * requests  — for downloading tiles from public GCS URLs
    * boto3     — for uploading merged COGs to S3

Usage from the web application is through the service layer
(``services.start_cog_merge``), which creates a database record and calls
``merge_covariate_tiles()`` in a background thread.
"""

import logging
import os
import shutil
import subprocess
import tempfile

import boto3
import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# GCS helpers
# ---------------------------------------------------------------------------

def list_gcs_tiles(bucket: str, prefix: str, covariate_name: str) -> list[str]:
    """Return public GCS URLs for all ``.tif`` tiles of a covariate.

    Uses the public GCS JSON API (no credentials needed for public buckets).
    """
    obj_prefix = f"{prefix}/{covariate_name}".strip("/")
    api_url = (
        f"https://storage.googleapis.com/storage/v1/b/{bucket}/o"
        f"?prefix={obj_prefix}&maxResults=1000"
    )
    resp = requests.get(api_url, timeout=30)
    resp.raise_for_status()
    items = []
    page_token = None
    while True:
        url = api_url
        if page_token:
            url += f"&pageToken={page_token}"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        items.extend(data.get("items", []))
        page_token = data.get("nextPageToken")
        if not page_token:
            break

    urls = [
        f"https://storage.googleapis.com/{bucket}/{item['name']}"
        for item in items
        if item["name"].endswith(".tif")
    ]
    return sorted(urls)


def list_all_gcs_tiles(
    bucket: str, prefix: str, known_covariates: list[str]
) -> dict[str, int]:
    """Scan all tiles on GCS and return tile counts grouped by covariate.

    Makes paginated API calls listing every ``.tif`` object under *prefix*,
    then matches each filename to the longest known covariate name.

    Parameters
    ----------
    bucket : str
        GCS bucket name (public, no credentials needed).
    prefix : str
        Object prefix (e.g. ``avoided-emissions/covariates``).
    known_covariates : list[str]
        Covariate names from config to match filenames against.

    Returns
    -------
    dict[str, int]
        Mapping of covariate name → number of tiles found on GCS.
    """
    norm_prefix = prefix.strip("/") + "/"
    base_url = (
        f"https://storage.googleapis.com/storage/v1/b/{bucket}/o"
        f"?prefix={norm_prefix}&maxResults=1000"
    )

    all_items: list[dict] = []
    page_token = None
    while True:
        url = base_url
        if page_token:
            url += f"&pageToken={page_token}"
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        all_items.extend(data.get("items", []))
        page_token = data.get("nextPageToken")
        if not page_token:
            break

    # Extract filenames (strip the prefix)
    filenames = [
        item["name"][len(norm_prefix):]
        for item in all_items
        if item["name"].endswith(".tif")
    ]

    if not filenames:
        return {}

    # Sort known names longest-first so e.g. "fc_2000" matches before "fc_"
    sorted_names = sorted(known_covariates, key=len, reverse=True)

    counts: dict[str, int] = {}
    for fname in filenames:
        for cov_name in sorted_names:
            if fname.startswith(cov_name) and (
                fname == cov_name + ".tif"
                or (len(fname) > len(cov_name) and fname[len(cov_name)].isdigit())
            ):
                counts[cov_name] = counts.get(cov_name, 0) + 1
                break

    return counts


def list_gcs_cog_objects(bucket: str, prefix: str) -> list[dict]:
    """List all ``.tif`` objects under a COG prefix on GCS.

    Returns a list of dicts with keys:
        * ``name``  – full object name (e.g. ``avoided-emissions/cog/elev.tif``)
        * ``url``   – public HTTPS URL
        * ``size``  – file size in bytes (int)
        * ``covariate`` – inferred covariate name (filename without extension)
    """
    api_url = (
        f"https://storage.googleapis.com/storage/v1/b/{bucket}/o"
        f"?prefix={prefix.strip('/')}/&maxResults=1000"
    )
    resp = requests.get(api_url, timeout=30)
    resp.raise_for_status()
    items = resp.json().get("items", [])

    results = []
    page_token = None
    while True:
        url = api_url
        if page_token:
            url += f"&pageToken={page_token}"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
        
        for item in items:
            obj_name = item["name"]
            if not obj_name.endswith(".tif"):
                continue
            # Extract covariate name from filename:  prefix/elev.tif -> elev
            filename = obj_name.rsplit("/", 1)[-1]
            covariate = filename.removesuffix(".tif")
            results.append({
                "name": obj_name,
                "url": f"https://storage.googleapis.com/{bucket}/{obj_name}",
                "size": int(item.get("size", 0)),
                "covariate": covariate,
            })
        
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return results


def list_s3_cog_objects(bucket: str, prefix: str,
                        region: str = "us-east-1") -> list[dict]:
    """List all ``.tif`` objects under a prefix on S3.

    Returns a list of dicts with keys:
        * ``key``       – S3 object key
        * ``url``       – public HTTPS URL
        * ``size``      – file size in bytes (int)
        * ``covariate`` – inferred covariate name (filename without extension)
    """
    s3 = boto3.client("s3", region_name=region)
    paginator = s3.get_paginator("list_objects_v2")
    norm_prefix = prefix.strip("/") + "/"

    results = []
    for page in paginator.paginate(Bucket=bucket, Prefix=norm_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".tif"):
                continue
            filename = key.rsplit("/", 1)[-1]
            covariate = filename.removesuffix(".tif")
            results.append({
                "key": key,
                "url": f"https://{bucket}.s3.amazonaws.com/{key}",
                "size": obj["Size"],
                "covariate": covariate,
            })
    return results


def delete_s3_cog(bucket: str, prefix: str, covariate_name: str,
                  region: str = "us-east-1") -> bool:
    """Delete a merged COG from S3.

    Returns True if the object was deleted, False if it didn't exist.
    """
    key = f"{prefix.strip('/')}/{covariate_name}.tif"
    s3 = boto3.client("s3", region_name=region)
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except s3.exceptions.ClientError:
        logger.info("S3 COG not found: s3://%s/%s", bucket, key)
        return False
    s3.delete_object(Bucket=bucket, Key=key)
    logger.info("Deleted S3 COG: s3://%s/%s", bucket, key)
    return True


def delete_gcs_tiles(bucket: str, prefix: str,
                     covariate_name: str) -> int:
    """Delete all GCS tiles for a covariate.

    Uses the GCS JSON API with an OAuth2 token from the default
    application credentials (``GOOGLE_APPLICATION_CREDENTIALS`` or
    service account).  Returns the number of objects deleted.

    Falls back to doing nothing if no credentials are available
    (GCS public buckets don't support unauthenticated deletes).
    """
    import google.auth
    import google.auth.transport.requests

    # List all tile objects for this covariate
    tile_urls = list_gcs_tiles(bucket, prefix, covariate_name)
    if not tile_urls:
        return 0

    # Get authenticated credentials
    try:
        credentials, _project = google.auth.default(
            scopes=["https://www.googleapis.com/auth/devstorage.full_control"]
        )
        auth_req = google.auth.transport.requests.Request()
        credentials.refresh(auth_req)
    except Exception:
        logger.warning(
            "No GCS credentials available — cannot delete tiles for %s",
            covariate_name,
        )
        return 0

    deleted = 0
    for url in tile_urls:
        # Extract object name from URL
        # URL: https://storage.googleapis.com/{bucket}/{object_name}
        obj_name = url.split(f"/{bucket}/", 1)[-1]
        import urllib.parse
        encoded_name = urllib.parse.quote(obj_name, safe="")
        delete_url = (
            f"https://storage.googleapis.com/storage/v1/b/{bucket}"
            f"/o/{encoded_name}"
        )
        resp = requests.delete(
            delete_url,
            headers={"Authorization": f"Bearer {credentials.token}"},
            timeout=30,
        )
        if resp.status_code in (200, 204):
            deleted += 1
        elif resp.status_code == 404:
            logger.debug("GCS tile already gone: %s", obj_name)
        else:
            logger.warning(
                "Failed to delete GCS tile %s: %s %s",
                obj_name, resp.status_code, resp.text[:200],
            )
    logger.info("Deleted %d/%d GCS tiles for %s", deleted, len(tile_urls),
                covariate_name)
    return deleted


def _download_tile(url: str, dest_dir: str) -> str:
    """Download a single tile to *dest_dir*, returning the local path."""
    filename = url.rsplit("/", 1)[-1]
    local_path = os.path.join(dest_dir, filename)
    logger.info("Downloading tile: %s", url)
    with requests.get(url, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        with open(local_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                fh.write(chunk)
    size_mb = os.path.getsize(local_path) / (1024 * 1024)
    logger.info("  -> %s (%.1f MB)", filename, size_mb)
    return local_path


def _upload_to_s3(local_path: str, bucket: str, key: str,
                  region: str = "us-east-1") -> str:
    """Upload a file to S3.

    Uses the default boto3 credential chain (env vars, instance profile,
    etc.).

    Returns
    -------
    str
        The HTTPS URL of the uploaded object.
    """
    file_size = os.path.getsize(local_path)
    logger.info("Uploading %s (%.1f MB) -> s3://%s/%s",
                local_path, file_size / (1024 * 1024), bucket, key)

    s3 = boto3.client("s3", region_name=region)
    s3.upload_file(
        local_path, bucket, key,
        ExtraArgs={"ContentType": "image/tiff"},
    )
    url = f"https://{bucket}.s3.amazonaws.com/{key}"
    logger.info("Upload complete: %s", url)
    return url


# ---------------------------------------------------------------------------
# GDAL merge pipeline
# ---------------------------------------------------------------------------

def _run_cmd(cmd: list[str]) -> None:
    """Run a shell command, raising on failure."""
    logger.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        logger.error("STDOUT: %s", result.stdout)
        logger.error("STDERR: %s", result.stderr)
        raise RuntimeError(
            f"Command failed (exit {result.returncode}): {' '.join(cmd)}\n"
            f"{result.stderr}"
        )


def merge_tiles_to_cog(tile_paths: list[str], output_path: str) -> str:
    """Merge multiple GeoTIFF tiles into a single COG with DEFLATE compression.

    Pipeline:
        1. ``gdalbuildvrt`` — create a virtual mosaic of all input tiles
        2. ``gdal_translate`` — materialise the mosaic as a single
           Cloud-Optimized GeoTIFF with DEFLATE (lossless) compression

    Parameters
    ----------
    tile_paths : list[str]
        Local file paths to the input GeoTIFF tiles.
    output_path : str
        Desired output file path for the merged COG.

    Returns
    -------
    str
        The *output_path* on success.
    """
    if not tile_paths:
        raise ValueError("No tiles provided for merging")

    vrt_path = output_path + ".vrt"

    # Step 1: Build VRT mosaic
    _run_cmd(["gdalbuildvrt", vrt_path] + tile_paths)

    # Step 2: Translate VRT -> COG with lossless DEFLATE compression
    _run_cmd([
        "gdal_translate",
        "-of", "COG",
        "-co", "COMPRESS=DEFLATE",
        "-co", "PREDICTOR=2",         # horizontal differencing (good for int)
        "-co", "NUM_THREADS=ALL_CPUS",
        "-co", "BIGTIFF=IF_SAFER",
        vrt_path,
        output_path,
    ])

    # Clean up the intermediate VRT
    if os.path.exists(vrt_path):
        os.remove(vrt_path)

    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    logger.info("Merged COG created: %s (%.1f MB)", output_path, size_mb)
    return output_path


# ---------------------------------------------------------------------------
# End-to-end pipeline
# ---------------------------------------------------------------------------

def merge_covariate_tiles(
    covariate_name: str,
    source_bucket: str,
    source_prefix: str,
    output_bucket: str,
    output_prefix: str = "avoided-emissions/cog",
    aws_region: str = "us-east-1",
) -> dict:
    """Download tiles from GCS, merge into COG, upload to S3.

    Parameters
    ----------
    covariate_name : str
        Covariate key from config.COVARIATES.
    source_bucket : str
        GCS bucket containing exported tiles.
    source_prefix : str
        GCS prefix under which tiles are stored.
    output_bucket : str
        S3 bucket for the merged COG.
    output_prefix : str
        S3 key prefix for the merged COG.
    aws_region : str
        AWS region for S3.

    Returns
    -------
    dict
        ``{"url": str, "size_bytes": int, "n_tiles": int}``

    Raises
    ------
    RuntimeError
        If no tiles are found, or GDAL commands fail.
    """
    # 1. List tiles on GCS (source)
    tile_urls = list_gcs_tiles(source_bucket, source_prefix, covariate_name)
    if not tile_urls:
        raise RuntimeError(
            f"No tiles found for covariate '{covariate_name}' in "
            f"gs://{source_bucket}/{source_prefix}/"
        )
    n_tiles = len(tile_urls)
    logger.info(
        "Found %d tile(s) for '%s' in gs://%s/%s",
        n_tiles, covariate_name, source_bucket, source_prefix,
    )

    # If there's only 1 tile, it's already a COG — just re-upload with
    # DEFLATE compression applied.
    workdir = tempfile.mkdtemp(prefix=f"cog_{covariate_name}_")
    try:
        # 2. Download all tiles
        local_tiles = [_download_tile(url, workdir) for url in tile_urls]

        # 3. Merge into a single COG
        output_filename = f"{covariate_name}.tif"
        output_path = os.path.join(workdir, output_filename)
        merge_tiles_to_cog(local_tiles, output_path)

        merged_size = os.path.getsize(output_path)

        # 4. Upload merged COG to S3
        s3_key = f"{output_prefix}/{output_filename}".strip("/")
        s3_url = _upload_to_s3(
            output_path, output_bucket, s3_key, aws_region
        )

        return {
            "url": s3_url,
            "size_bytes": merged_size,
            "n_tiles": n_tiles,
        }
    finally:
        # Clean up temp directory
        shutil.rmtree(workdir, ignore_errors=True)
