#!/usr/bin/env python3
"""Merge GEE-exported covariate tiles locally and print SQL to register the result.

Replicates the tile-download + GDAL pipeline that the Celery ``run_cog_merge``
task performs inside Docker, but runs entirely on your local machine.  After
the merged COG is uploaded to S3 the script prints a SQL statement that you
can run on the server to update the ``covariates`` table so the web app picks
up the new layer on its next 30-second refresh (no restart needed).

Prerequisites
-------------
* GDAL CLI tools on PATH (``gdalbuildvrt``, ``gdal_translate``).
  macOS:   ``brew install gdal``
  Ubuntu:  ``sudo apt-get install gdal-bin``
* Python dependencies installed (``pip install -r webapp/requirements.txt``
  in your local venv, or just run via the venv already in the repo root).
* AWS credentials available — either ``AWS_ACCESS_KEY_ID`` /
  ``AWS_SECRET_ACCESS_KEY`` env vars, or a configured AWS CLI profile.
* The ``.env`` file at the repo root (copied from ``deploy/.env.example``
  and filled in).

Usage (from the repo root)
--------------------------
Run a single covariate at the default 1 km resolution::

    python webapp/scripts/run_local_merge.py --covariate slope

Run multiple covariates::

    python webapp/scripts/run_local_merge.py --covariate slope elevation

Run at 250 m resolution::

    python webapp/scripts/run_local_merge.py --covariate slope --resolution 250

Point at a specific .env file::

    python webapp/scripts/run_local_merge.py --covariate slope --env path/to/.env

Dry-run (lists tiles found on GCS but does not merge or upload)::

    python webapp/scripts/run_local_merge.py --covariate slope --dry-run
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Bootstrap: add webapp/ to sys.path so all webapp modules are importable,
# and load the .env file before Config is imported.
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_WEBAPP_DIR = _REPO_ROOT / "webapp"
# webapp/ first so its modules take priority, then repo root so that
# `gee_export` (which lives at the repo root, mirroring the Dockerfile layout)
# is importable as a package.
for _p in (str(_WEBAPP_DIR), str(_REPO_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)


def _load_env(env_path: Path) -> None:
    """Load environment variables from *env_path* without overriding existing ones."""
    if not env_path.exists():
        print(f"[warn] .env file not found at {env_path} — relying on environment")
        return
    with open(env_path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
    print(f"[info] Loaded environment from {env_path}")


# ---------------------------------------------------------------------------
# CLI argument parsing (must happen before imports that read Config)
# ---------------------------------------------------------------------------

parser = argparse.ArgumentParser(
    description="Merge GEE tiles locally and register the result in the DB.",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog=__doc__,
)
parser.add_argument(
    "--covariate",
    nargs="+",
    required=True,
    metavar="NAME",
    help="One or more covariate names (e.g. slope elevation fc_2020).",
)
parser.add_argument(
    "--resolution",
    type=int,
    default=1000,
    choices=[1000, 250],
    metavar="{1000,250}",
    help="Resolution in metres (default: 1000).",
)
parser.add_argument(
    "--env",
    default=str(_REPO_ROOT / ".env"),
    metavar="PATH",
    help="Path to the .env file (default: <repo-root>/.env).",
)
parser.add_argument(
    "--gcs-bucket",
    default=None,
    metavar="BUCKET",
    help="Override GCS bucket (default: read from GCS_BUCKET env var).",
)
parser.add_argument(
    "--gcs-prefix",
    default=None,
    metavar="PREFIX",
    help="Override GCS prefix (default: read from GCS_PREFIX env var).",
)
parser.add_argument(
    "--dry-run",
    action="store_true",
    help="List GCS tiles and exit without merging or uploading.",
)
parser.add_argument(
    "--gdal-cache-mb",
    type=int,
    default=2048,
    metavar="MB",
    help="GDAL block-cache size in MiB passed as GDAL_CACHEMAX (default: 2048).",
)
parser.add_argument(
    "--gdal-bin",
    default="C:\\OSGeo4W\\bin",
    metavar="DIR",
    help="Directory containing gdalbuildvrt / gdal_translate (prepended to PATH).",
)
parser.add_argument(
    "--verbose",
    "-v",
    action="store_true",
    help="Enable DEBUG-level logging.",
)

args = parser.parse_args()

# Prepend user-supplied GDAL bin dir to PATH before any shutil.which checks.
if args.gdal_bin:
    os.environ["PATH"] = args.gdal_bin + os.pathsep + os.environ.get("PATH", "")

# Load .env before any webapp import so Config picks up the values.
_load_env(Path(args.env))

# Now it is safe to import webapp modules.
logging.basicConfig(
    level=logging.DEBUG if args.verbose else logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("run_local_merge")

# Silence noisy third-party loggers unless --verbose.
if not args.verbose:
    for _noisy in ("botocore", "urllib3", "boto3", "s3transfer"):
        logging.getLogger(_noisy).setLevel(logging.WARNING)

from cog_merge import list_gcs_tiles, merge_covariate_tiles
from config import Config

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_bucket_prefix() -> tuple[str, str, str, str]:
    """Return (gcs_bucket, gcs_prefix, s3_bucket, s3_prefix) for a merge."""
    gcs_bucket = args.gcs_bucket or Config.GCS_BUCKET
    cog_suffix = "_1km" if args.resolution == 1000 else "_250m"
    # If the user explicitly supplies --gcs-prefix use it as-is; otherwise
    # derive the GCS prefix from the resolution (replace the trailing resolution
    # suffix so that 1km → covariates_1km and 250m → covariates_250m).
    if args.gcs_prefix:
        gcs_prefix = args.gcs_prefix
    else:
        base = Config.GCS_PREFIX.rsplit("_", 1)[0]  # strip existing suffix
        gcs_prefix = f"{base}{cog_suffix}"
    s3_bucket = Config.S3_BUCKET
    s3_prefix = f"{Config.S3_PREFIX}/cog{cog_suffix}"
    return gcs_bucket, gcs_prefix, s3_bucket, s3_prefix


def _print_sql(
    covariate_name: str,
    result: dict,
    gcs_bucket: str,
    gcs_prefix: str,
    s3_bucket: str,
    s3_prefix: str,
) -> None:
    """Print the SQL to run on the server to register the merged COG in the DB.

    Updates the most recent existing row for this covariate.  A missing row
    means the covariate was never submitted via the admin tool (wrong name or
    resolution), so no INSERT fallback is provided — UPDATE ... RETURNING will
    return 0 rows and DO UPDATE will raise an error.
    """

    def _esc(val: str) -> str:
        """Escape single quotes for use inside a SQL string literal."""
        return val.replace("'", "''")

    merged_url = _esc(result["url"])
    size_bytes = result.get("size_bytes") or 0
    n_tiles = result.get("n_tiles") or 0
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00")

    sql = f"""\
-- Run this on the server to register the merged COG in the covariates table.
-- Updates the most recent existing row for this covariate/resolution pair.
-- If 0 rows are updated the covariate name or resolution does not match any
-- record — double-check both before re-running.
UPDATE covariates SET
    status        = 'merged',
    merged_url    = '{merged_url}',
    size_bytes    = {size_bytes},
    n_tiles       = {n_tiles},
    output_bucket = '{_esc(s3_bucket)}',
    output_prefix = '{_esc(s3_prefix)}',
    gcs_bucket    = '{_esc(gcs_bucket)}',
    gcs_prefix    = '{_esc(gcs_prefix)}',
    completed_at  = '{now}',
    metadata      = '{{"source": "local_merge_script"}}'::jsonb
WHERE id = (
    SELECT id
    FROM   covariates
    WHERE  covariate_name = '{_esc(covariate_name)}'
      AND  resolution_m   = {args.resolution}
    ORDER  BY started_at DESC
    LIMIT  1
)
RETURNING id, covariate_name, status;
-- Expect exactly 1 row returned. 0 rows means no matching record exists."""

    print("\n" + "=" * 60)
    print("  SQL to register the merge — run on the server:")
    print("=" * 60)
    print(sql)


# ---------------------------------------------------------------------------
# Main merge loop
# ---------------------------------------------------------------------------


def run_merge(covariate_name: str) -> None:
    gcs_bucket, gcs_prefix, s3_bucket, s3_prefix = _resolve_bucket_prefix()

    if not gcs_bucket:
        logger.error(
            "GCS_BUCKET is not set. Pass --gcs-bucket or set GCS_BUCKET in .env."
        )
        sys.exit(1)

    # --- Preflight: verify GDAL CLI tools are available before downloading ---
    import shutil

    missing = [t for t in ("gdalbuildvrt", "gdal_translate") if not shutil.which(t)]
    if missing and not args.dry_run:
        logger.error(
            "GDAL CLI tool(s) not found on PATH: %s",
            ", ".join(missing),
        )
        sys.exit(1)

    # --- Dry run: just list tiles and exit ---
    if args.dry_run:
        logger.info(
            "DRY RUN — listing tiles for '%s' in gs://%s/%s",
            covariate_name,
            gcs_bucket,
            gcs_prefix,
        )
        tiles = list_gcs_tiles(gcs_bucket, gcs_prefix, covariate_name)
        if tiles:
            print(f"\n{len(tiles)} tile(s) found for '{covariate_name}':")
            for t in tiles:
                print(f"  {t}")
        else:
            print(f"\nNo tiles found for '{covariate_name}'.")
        return

    # Set GDAL block-cache before invoking GDAL CLI tools. Using MB units
    # (plain integer) is the most portable form; GDAL also accepts a % suffix
    # but that requires GDAL ≥ 2.x to parse reliably.
    os.environ["GDAL_CACHEMAX"] = str(args.gdal_cache_mb)
    logger.info("GDAL_CACHEMAX set to %d MiB", args.gdal_cache_mb)

    logger.info(
        "Merging '%s' from gs://%s/%s -> s3://%s/%s",
        covariate_name,
        gcs_bucket,
        gcs_prefix,
        s3_bucket,
        s3_prefix,
    )

    result = merge_covariate_tiles(
        covariate_name=covariate_name,
        source_bucket=gcs_bucket,
        source_prefix=gcs_prefix,
        output_bucket=s3_bucket,
        output_prefix=s3_prefix,
        aws_region=Config.AWS_REGION,
        layer_id=None,
    )

    size_mb = (result["size_bytes"] or 0) / (1024 * 1024)
    logger.info(
        "Done: '%s' merged (%d tile(s), %.1f MB) -> %s",
        covariate_name,
        result.get("n_tiles", 0),
        size_mb,
        result["url"],
    )

    _print_sql(covariate_name, result, gcs_bucket, gcs_prefix, s3_bucket, s3_prefix)


if __name__ == "__main__":
    if not Config.S3_BUCKET:
        logger.error("S3_BUCKET is not set. Check your .env file.")
        sys.exit(1)

    failed = []
    for name in args.covariate:
        print(f"\n{'=' * 60}")
        print(f"  Covariate: {name}  (resolution: {args.resolution} m)")
        print(f"{'=' * 60}")
        try:
            run_merge(name)
        except Exception:
            logger.exception("Failed to merge '%s'", name)
            failed.append(name)

    if failed:
        print(f"\n[error] The following covariates failed: {', '.join(failed)}")
        sys.exit(1)

    print("\n[done] All covariates processed successfully.")
