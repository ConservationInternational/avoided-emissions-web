"""Rasterize vector reference layers from PostGIS to Cloud-Optimized GeoTIFFs.

Produces rasters for admin boundaries (region), ecoregions, biome, and
protected areas that are pixel-aligned with the GEE-exported covariates.
Each raster is accompanied by a CSV key that maps raster values back to the
source polygon attributes in the database.

Grid specification (must match gee-export/config.py):
    CRS:         EPSG:4326
    Pixel size:  1/120° (30 arc-seconds ≈ 927 m at equator)
    Origin:      0°E, 0°N (pixel edges at exact multiples of 1/120°)
    Extent:      -180 to 180 (x), -90 to 90 (y)

Uses ``gdal_rasterize`` via subprocess so we can burn PostGIS query results
directly (``PG:`` connection string) without dumping to an intermediate
shapefile.  The result is written as a Cloud-Optimized GeoTIFF with DEFLATE
compression, then uploaded to S3.
"""

import csv
import logging
import os
import shutil
import subprocess
import tempfile

import boto3
from sqlalchemy import create_engine, text

from config import Config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Grid constants — must stay in sync with gee-export/config.py
# ---------------------------------------------------------------------------
PIXEL_SIZE_DEG = 1 / 120  # 30 arc-seconds
XMIN, YMIN, XMAX, YMAX = -180, -90, 180, 90
SRS = "EPSG:4326"

# ---------------------------------------------------------------------------
# Layer definitions
#
# Each layer describes:
#   table        – PostGIS table to rasterize
#   burn_col     – column whose integer value is burned into the raster
#   output_name  – base filename (without extension) for the COG and CSV
#   key_columns  – columns included in the CSV key (plus the burn column)
#   description  – human-readable label
# ---------------------------------------------------------------------------
VECTOR_LAYERS = [
    {
        "table": "geoboundaries_adm0",
        "burn_col": "id",
        "output_name": "admin0",
        "key_columns": ["id", "shape_group", "shape_name", "shape_type"],
        "description": "geoBoundaries ADM0 country-level boundary ID",
    },
    {
        "table": "geoboundaries_adm1",
        "burn_col": "id",
        "output_name": "admin1",
        "key_columns": ["id", "shape_group", "shape_name", "shape_id", "shape_type"],
        "description": "geoBoundaries ADM1 administrative region ID",
    },
    {
        "table": "geoboundaries_adm2",
        "burn_col": "id",
        "output_name": "admin2",
        "key_columns": ["id", "shape_group", "shape_name", "shape_id", "shape_type"],
        "description": "geoBoundaries ADM2 district-level boundary ID",
    },
    {
        "table": "ecoregions",
        "burn_col": "eco_id",
        "output_name": "ecoregion",
        "key_columns": ["eco_id", "eco_name", "biome_num", "biome_name", "realm"],
        "description": "RESOLVE ecoregion ID",
    },
    {
        "table": "wdpa",
        "burn_col": None,
        "burn_value": 1,
        "output_name": "pa",
        "key_columns": None,
        # Only include IUCN categories that represent strict protection.
        "where": "iucn_cat IN ('Ia', 'Ib', 'II', 'III', 'IV')",
        "description": "Binary protected area mask (1 = strictly protected, 0 = not)",
    },
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _pg_connection_string() -> str:
    """Build a GDAL-compatible PG connection string from DATABASE_URL."""
    from urllib.parse import urlparse

    parsed = urlparse(Config.DATABASE_URL)
    parts = [
        f"host={parsed.hostname}",
        f"port={parsed.port or 5432}",
        f"dbname={parsed.path.lstrip('/')}",
        f"user={parsed.username}",
    ]
    if parsed.password:
        parts.append(f"password={parsed.password}")
    return "PG:" + " ".join(parts)


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
# Core rasterization
# ---------------------------------------------------------------------------


def rasterize_layer(layer_def: dict, workdir: str) -> str:
    """Rasterize a single vector layer to a COG aligned with the GEE grid.

    Parameters
    ----------
    layer_def : dict
        One of the entries from :data:`VECTOR_LAYERS`.
    workdir : str
        Temporary directory for intermediate files.

    Returns
    -------
    str
        Path to the output COG file.
    """
    pg_conn = _pg_connection_string()
    output_name = layer_def["output_name"]
    table = layer_def["table"]
    burn_col = layer_def.get("burn_col")
    fixed_burn = layer_def.get("burn_value")
    where_clause = layer_def.get("where")

    raw_tif = os.path.join(workdir, f"{output_name}_raw.tif")
    cog_tif = os.path.join(workdir, f"{output_name}.tif")

    # Build the gdal_rasterize command.  Two modes:
    #   • burn_col set     → burn the column value from a SQL query (-a)
    #   • burn_value set   → burn a fixed constant for every polygon (-burn)
    # An optional "where" key adds a WHERE filter to the SQL query.
    base_cmd = [
        "gdal_rasterize",
        "-te",
        str(XMIN),
        str(YMIN),
        str(XMAX),
        str(YMAX),
        "-tr",
        str(PIXEL_SIZE_DEG),
        str(PIXEL_SIZE_DEG),
        "-a_srs",
        SRS,
        "-a_nodata",
        "0",
        "-co",
        "COMPRESS=DEFLATE",
        "-co",
        "BIGTIFF=YES",
    ]

    where_sql = f" WHERE {where_clause}" if where_clause else ""

    if burn_col:
        sql = f"SELECT {burn_col}::integer AS burn_value, geom FROM {table}{where_sql}"
        base_cmd += ["-sql", sql, "-a", "burn_value", "-ot", "Int32"]
    elif fixed_burn is not None:
        sql = f"SELECT geom FROM {table}{where_sql}"
        base_cmd += ["-sql", sql, "-burn", str(fixed_burn), "-ot", "Byte"]
    else:
        raise ValueError(f"Layer {output_name}: set burn_col or burn_value")

    base_cmd += [pg_conn, raw_tif]
    _run_cmd(base_cmd)

    # Step 2: Convert to Cloud-Optimized GeoTIFF
    _run_cmd(
        [
            "gdal_translate",
            "-of",
            "COG",
            "-co",
            "COMPRESS=DEFLATE",
            "-co",
            "PREDICTOR=2",
            "-co",
            "NUM_THREADS=ALL_CPUS",
            "-co",
            "BIGTIFF=IF_SAFER",
            raw_tif,
            cog_tif,
        ]
    )

    # Clean up intermediate file
    if os.path.exists(raw_tif):
        os.remove(raw_tif)

    size_mb = os.path.getsize(cog_tif) / (1024 * 1024)
    logger.info("Created COG: %s (%.1f MB)", cog_tif, size_mb)
    return cog_tif


# ---------------------------------------------------------------------------
# CSV key generation
# ---------------------------------------------------------------------------


def generate_csv_key(layer_def: dict, workdir: str) -> str:
    """Query PostGIS for the distinct key values and write a CSV.

    Parameters
    ----------
    layer_def : dict
        One of the entries from :data:`VECTOR_LAYERS`.
    workdir : str
        Temporary directory for the CSV file.

    Returns
    -------
    str
        Path to the output CSV file.
    """
    output_name = layer_def["output_name"]
    table = layer_def["table"]
    key_columns = layer_def["key_columns"]

    csv_path = os.path.join(workdir, f"{output_name}_key.csv")

    col_list = ", ".join(key_columns)
    sql = f"SELECT DISTINCT {col_list} FROM {table} ORDER BY {key_columns[0]}"

    engine = create_engine(Config.DATABASE_URL)
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        rows = result.mappings().all()

    with open(csv_path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=key_columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row[col] for col in key_columns})

    logger.info("Wrote CSV key: %s (%d rows)", csv_path, len(rows))
    return csv_path


# ---------------------------------------------------------------------------
# End-to-end pipeline
# ---------------------------------------------------------------------------


def rasterize_and_upload(
    layer_def: dict,
    s3_bucket: str | None = None,
    s3_prefix: str | None = None,
) -> dict:
    """Rasterize a vector layer, generate its CSV key, and upload both to S3.

    Parameters
    ----------
    layer_def : dict
        One of the entries from :data:`VECTOR_LAYERS`.
    s3_bucket : str, optional
        Target S3 bucket.  Defaults to ``Config.S3_BUCKET``.
    s3_prefix : str, optional
        Target S3 prefix.  Defaults to ``<Config.S3_PREFIX>/cog``.

    Returns
    -------
    dict
        ``{"cog_url": str, "size_bytes": int}`` and optionally
        ``"csv_url": str`` when the layer has ``key_columns``.
    """
    bucket = s3_bucket or Config.S3_BUCKET
    prefix = (s3_prefix or f"{Config.S3_PREFIX}/cog").strip("/")
    output_name = layer_def["output_name"]

    if not bucket:
        raise RuntimeError("S3_BUCKET is not configured")

    workdir = tempfile.mkdtemp(prefix=f"rasterize_{output_name}_")
    try:
        # 1. Rasterize to COG
        cog_path = rasterize_layer(layer_def, workdir)
        size_bytes = os.path.getsize(cog_path)

        # 2. Upload COG
        cog_key = f"{prefix}/{output_name}.tif"
        cog_url = _upload_to_s3(cog_path, bucket, cog_key, "image/tiff")

        result = {
            "cog_url": cog_url,
            "size_bytes": size_bytes,
        }

        # 3. Generate and upload CSV key (only for layers with key_columns)
        if layer_def.get("key_columns"):
            csv_path = generate_csv_key(layer_def, workdir)
            csv_key = f"{prefix}/{output_name}_key.csv"
            csv_url = _upload_to_s3(csv_path, bucket, csv_key, "text/csv")
            result["csv_url"] = csv_url

        return result
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def rasterize_all_layers(
    s3_bucket: str | None = None,
    s3_prefix: str | None = None,
) -> dict[str, dict]:
    """Rasterize all vector layers and upload to S3.

    Returns
    -------
    dict[str, dict]
        Mapping of layer output_name → upload result dict.
    """
    results = {}
    for layer_def in VECTOR_LAYERS:
        name = layer_def["output_name"]
        logger.info("=" * 60)
        logger.info("Rasterizing %s (%s)", name, layer_def["description"])
        logger.info("=" * 60)
        try:
            results[name] = rasterize_and_upload(
                layer_def, s3_bucket=s3_bucket, s3_prefix=s3_prefix
            )
        except Exception:
            logger.exception("Failed to rasterize %s", name)
            results[name] = {"error": True}
    return results
