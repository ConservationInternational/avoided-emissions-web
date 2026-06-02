"""Celery tasks: import vector reference data and rasterize PostGIS vectors."""

import logging
from datetime import datetime, timezone

import boto3

from celery_app import celery_app
from config import report_exception
from tasks.reference_layers import export_reference_layers_task

logger = logging.getLogger(__name__)


@celery_app.task(
    name="tasks.import_vector_data",
    bind=True,
    max_retries=2,
    soft_time_limit=7200,
    time_limit=7500,
)
def import_vector_data_task(self) -> dict:
    """Import vector reference data (geoboundaries, ecoregions, wdpa).

    Dispatched once per deploy by the one-shot ``migrate`` service.
    Only imports tables that are empty, making it safe to retry or
    call repeatedly.

    On successful import, automatically dispatches
    :func:`rasterize_vectors_task` to produce grid-aligned COGs from
    the freshly-imported vector data.
    """
    try:
        from import_vector_data import run_import

        imported = run_import(check_only=False)

        # Chain rasterization of the imported vector layers.
        rasterize_vectors_task.delay()
        logger.info("Dispatched rasterize_vectors_task after successful import")

        # Only export reference layers when fresh data was actually imported.
        # If the tables were already populated nothing changed, so there is no
        # need to re-export — the monthly beat schedule covers routine refreshes.
        if imported:
            export_reference_layers_task.delay()
            logger.info(
                "Dispatched export_reference_layers_task after successful import"
            )
        else:
            logger.info(
                "Skipping export_reference_layers dispatch — "
                "all vector tables were already populated"
            )

        return {"status": "complete"}
    except Exception as exc:
        logger.exception("Vector data import failed")
        report_exception()
        raise self.retry(exc=exc, countdown=60)


@celery_app.task(
    name="tasks.rasterize_vectors",
    bind=True,
    max_retries=1,
    acks_late=True,
    reject_on_worker_lost=True,
    soft_time_limit=7200,
    time_limit=7500,
)
def rasterize_vectors_task(self) -> dict:
    """Rasterize vector reference layers to COGs aligned with the GEE grid.

    Converts PostGIS vector tables (admin boundaries, ecoregions,
    protected areas) into Cloud-Optimized GeoTIFFs sharing the same grid
    as the GEE-exported covariates.  Also uploads a CSV key for each
    layer that maps raster values to source polygon attributes.

    Layers whose COGs already exist on S3 are skipped, making this safe
    to call on every webapp startup without redundant work.

    Typically dispatched automatically after :func:`import_vector_data_task`
    completes.

    Returns
    -------
    dict
        ``{"status": "complete", "layers": {name: {cog_url, csv_url, ...}}}``
        on success, or ``{"status": "failed", "error": ...}`` on failure.
    """

    from config import Config
    from models import Covariate, get_db
    from rasterize_vectors import RESOLUTIONS, VECTOR_LAYERS, rasterize_and_upload

    all_results = {}

    try:
        for resolution_m, res_cfg in RESOLUTIONS.items():
            cog_suffix = res_cfg["cog_suffix"]
            pixel_size_deg = res_cfg["pixel_size_deg"]
            bucket = Config.S3_BUCKET
            prefix = f"{Config.S3_PREFIX}/cog{cog_suffix}".strip("/")

            logger.info(
                "Rasterizing vectors at %dm (pixel_size=%.6f°, prefix=%s)",
                resolution_m,
                pixel_size_deg,
                prefix,
            )

            # Build map of layer names whose COG already exists on S3,
            # storing the S3 URL and file size for each.
            existing_on_s3: dict[str, dict] = {}
            if bucket:
                try:
                    from botocore.exceptions import ClientError

                    s3 = boto3.client("s3", region_name=Config.AWS_REGION)
                    for layer_def in VECTOR_LAYERS:
                        name = layer_def["output_name"]
                        key = f"{prefix}/{name}.tif"
                        try:
                            resp = s3.head_object(Bucket=bucket, Key=key)
                            existing_on_s3[name] = {
                                "url": (f"https://{bucket}.s3.amazonaws.com/{key}"),
                                "size_bytes": resp.get("ContentLength", 0),
                            }
                        except ClientError:
                            pass
                except Exception:
                    logger.warning(
                        "Failed to check S3 for existing rasterized layers — "
                        "will rasterize all layers.",
                        exc_info=True,
                    )

            db = get_db()
            try:
                results = {}
                for layer_def in VECTOR_LAYERS:
                    name = layer_def["output_name"]
                    result_key = f"{name}{cog_suffix}"

                    # Skip layers that already have a COG on S3.  If the
                    # DB record is missing or not in "merged" state,
                    # adopt the existing S3 object so it becomes visible
                    # to downstream code (analysis UI, R extraction).
                    if name in existing_on_s3:
                        s3_info = existing_on_s3[name]
                        existing = (
                            db.query(Covariate)
                            .filter(
                                Covariate.covariate_name == name,
                                Covariate.resolution_m == resolution_m,
                            )
                            .order_by(Covariate.started_at.desc())
                            .first()
                        )
                        if existing and existing.status == "merged":
                            logger.info(
                                "Skipping %s (%dm) — already on S3 (%s)",
                                name,
                                resolution_m,
                                existing.merged_url,
                            )
                            results[result_key] = {
                                "cog_url": existing.merged_url,
                                "skipped": True,
                            }
                            continue

                        # COG exists on S3 but DB is out of sync —
                        # adopt the existing S3 object.
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
                        layer.merged_url = s3_info["url"]
                        layer.size_bytes = s3_info["size_bytes"]
                        layer.completed_at = datetime.now(timezone.utc)
                        meta = dict(layer.extra_metadata or {})
                        meta["source"] = "postgis_rasterize"
                        meta["adopted_from_s3"] = True
                        layer.extra_metadata = meta
                        db.commit()
                        logger.info(
                            "Adopted existing S3 COG for %s (%dm) — %s",
                            name,
                            resolution_m,
                            s3_info["url"],
                        )
                        results[result_key] = {
                            "cog_url": s3_info["url"],
                            "skipped": True,
                            "adopted": True,
                        }
                        continue

                    logger.info(
                        "Rasterizing vector layer: %s (%dm)",
                        name,
                        resolution_m,
                    )

                    # Create or update a Covariate record for this
                    # layer+resolution combination.
                    existing = (
                        db.query(Covariate)
                        .filter(
                            Covariate.covariate_name == name,
                            Covariate.resolution_m == resolution_m,
                        )
                        .order_by(Covariate.started_at.desc())
                        .first()
                    )
                    if existing:
                        layer = existing
                    else:
                        layer = Covariate(
                            covariate_name=name,
                            resolution_m=resolution_m,
                            output_bucket=Config.S3_BUCKET,
                            output_prefix=prefix,
                            started_at=datetime.now(timezone.utc),
                        )
                        db.add(layer)
                        db.flush()
                    layer.status = "rasterizing"
                    db.commit()

                    try:
                        result = rasterize_and_upload(
                            layer_def,
                            s3_prefix=prefix,
                            pixel_size_deg=pixel_size_deg,
                        )
                        layer.status = "merged"
                        layer.merged_url = result["cog_url"]
                        layer.size_bytes = result["size_bytes"]
                        layer.completed_at = datetime.now(timezone.utc)
                        meta = dict(layer.extra_metadata or {})
                        if result.get("csv_url"):
                            meta["csv_key_url"] = result["csv_url"]
                        meta["source"] = "postgis_rasterize"
                        layer.extra_metadata = meta
                        db.commit()
                        results[result_key] = result
                        logger.info(
                            "Rasterized %s (%dm) -> %s",
                            name,
                            resolution_m,
                            result["cog_url"],
                        )
                    except Exception as exc:
                        logger.exception(
                            "Failed to rasterize %s (%dm)",
                            name,
                            resolution_m,
                        )
                        layer.status = "failed"
                        layer.error_message = str(exc)[:2000]
                        layer.completed_at = datetime.now(timezone.utc)
                        db.commit()
                        results[result_key] = {"error": str(exc)[:500]}

                all_results.update(results)
            finally:
                db.close()

        # SDG ingestion is now dispatched by auto_merge_unmerged after
        # all COG merges complete, not immediately after rasterization.
        logger.info(
            "Vector rasterization complete. SDG ingestion will be dispatched "
            "by auto_merge_unmerged once all COG merges finish."
        )

        return {"status": "complete", "layers": all_results}

    except Exception as exc:
        logger.exception("Vector rasterization failed")
        report_exception()
        raise self.retry(exc=exc, countdown=120)
