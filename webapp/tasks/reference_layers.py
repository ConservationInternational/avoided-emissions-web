"""Celery tasks: export reference layers to S3 and ingest SDG COG."""

import logging
from datetime import datetime, timezone

from celery_app import celery_app
from config import report_exception

logger = logging.getLogger(__name__)


@celery_app.task(
    name="tasks.export_reference_layers",
    bind=True,
    max_retries=2,
    acks_late=True,
    reject_on_worker_lost=True,
    soft_time_limit=7200,
    time_limit=7500,
)
def export_reference_layers_task(self) -> dict:
    """Export PostGIS reference layers (admin boundaries, ecoregions) to S3.

    Writes each reference layer as a GeoParquet file so that the AWS Batch
    ``prep`` step can compute matching extents and exclusion buffers without
    accessing PostGIS at all.

    Skips tables that are not yet populated (e.g., before the first vector
    import completes).  Safe to call repeatedly — each run overwrites the
    previous artifact at the same S3 key.

    Typically dispatched after :func:`import_vector_data_task` completes
    and also run on a monthly beat schedule to pick up any re-imports.

    Returns
    -------
    dict
        ``{"status": "complete", "exported": {layer_name: s3_uri, ...}}``
    """
    try:
        from services import export_reference_layers_to_s3

        exported = export_reference_layers_to_s3()
        return {"status": "complete", "exported": exported}
    except Exception as exc:
        logger.exception("Reference layer export failed")
        report_exception()
        raise self.retry(exc=exc, countdown=120)


@celery_app.task(
    name="tasks.ingest_sdg_cog",
    bind=True,
    max_retries=0,  # No retries - task is idempotent and memory-intensive
    acks_late=True,
    reject_on_worker_lost=True,
    soft_time_limit=10800,  # 3 hours (250m global rasters are very slow)
    time_limit=11100,
)
def ingest_sdg_cog_task(self) -> dict:
    """Download the Trends.Earth SDG 15.3.1 COG, extract bands, and upload to S3.

    The source is a pre-computed multi-band COG on Google Cloud Storage.
    Individual bands are extracted, resampled to each resolution grid,
    and uploaded as single-band COGs.  Layers already present on S3 are
    skipped.

    Typically dispatched after :func:`rasterize_vectors_task` completes.

    **Memory constraints**: Processing 250m global rasters requires significant
    memory (~10-15GB). GDAL memory limits are configured to prevent OOM.

    **Idempotency**: Task checks S3 before processing each layer. Safe to
    dispatch multiple times — already-uploaded layers are skipped.

    Returns
    -------
    dict
        ``{"status": "complete", "layers": {...}}`` on success.
    """

    from config import Config
    from ingest_sdg_cog import RESOLUTIONS, SDG_LAYERS, ingest_sdg_layers
    from models import Covariate, get_db

    # Idempotency guard: check if ALL layers already exist on S3
    logger.info("Checking if SDG ingestion has already completed...")
    try:
        import boto3
        from botocore.exceptions import ClientError

        s3 = boto3.client("s3", region_name=Config.AWS_REGION)
        all_exist = True
        for resolution_m, res_cfg in RESOLUTIONS.items():
            cog_suffix = res_cfg["cog_suffix"]
            prefix = f"{Config.S3_PREFIX}/cog{cog_suffix}".strip("/")
            for name in SDG_LAYERS.keys():
                s3_key = f"{prefix}/{name}.tif"
                try:
                    s3.head_object(Bucket=Config.S3_BUCKET, Key=s3_key)
                except ClientError:
                    all_exist = False
                    break
            if not all_exist:
                break

        if all_exist:
            logger.info("All SDG layers already exist on S3 — skipping ingestion")
            return {"status": "skipped", "message": "All layers already exist"}
    except Exception as exc:
        logger.warning("Failed to check S3 for existing layers: %s", exc)
        # Continue with ingestion if check fails

    try:
        logger.info("Starting SDG COG ingestion (this may take 1-3 hours for 250m)...")
        upload_results = ingest_sdg_layers()

        # Create or update Covariate DB records for each uploaded layer
        db = get_db()
        try:
            for resolution_m, res_cfg in RESOLUTIONS.items():
                cog_suffix = res_cfg["cog_suffix"]
                bucket = Config.S3_BUCKET
                prefix = f"{Config.S3_PREFIX}/cog{cog_suffix}".strip("/")

                for name, layer_info in SDG_LAYERS.items():
                    result_key = f"{name}{cog_suffix}"
                    result = upload_results.get(result_key)
                    if not result:
                        continue

                    existing = (
                        db.query(Covariate)
                        .filter(
                            Covariate.covariate_name == name,
                            Covariate.resolution_m == resolution_m,
                        )
                        .order_by(Covariate.started_at.desc())
                        .first()
                    )

                    if (
                        existing
                        and existing.status == "merged"
                        and result.get("skipped")
                    ):
                        logger.info(
                            "SDG layer %s (%dm) already tracked — skipping DB update",
                            name,
                            resolution_m,
                        )
                        continue

                    if existing:
                        layer = existing
                    else:
                        layer = Covariate(
                            covariate_name=name,
                            resolution_m=resolution_m,
                            output_bucket=bucket,
                            output_prefix=prefix,
                            started_at=datetime.now(timezone.utc),
                        )
                        db.add(layer)
                        db.flush()

                    layer.status = "merged"
                    layer.merged_url = result["cog_url"]
                    layer.size_bytes = result["size_bytes"]
                    layer.completed_at = datetime.now(timezone.utc)
                    meta = dict(layer.extra_metadata or {})
                    meta["source"] = "sdg_cog_ingest"
                    meta["band"] = layer_info["band"]
                    meta["description"] = layer_info["description"]
                    if result.get("skipped"):
                        meta["adopted_from_s3"] = True
                    layer.extra_metadata = meta
                    db.commit()
                    logger.info(
                        "SDG layer %s (%dm) -> %s",
                        name,
                        resolution_m,
                        result["cog_url"],
                    )
        finally:
            db.close()

        return {"status": "complete", "layers": upload_results}

    except Exception as exc:
        logger.exception("SDG COG ingestion failed")
        report_exception()
        raise self.retry(exc=exc, countdown=120)


class _MergeSuperseded(Exception):
    """Raised when a merge is aborted because the covariate was re-exported."""
