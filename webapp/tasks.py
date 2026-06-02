"""Celery tasks for background processing.

All long-running or I/O-heavy work is defined here and executed by the
Celery worker process.  The web application dispatches work by calling
``task.delay(…)`` or ``task.apply_async(…)``.
"""

import logging
from pathlib import Path
from datetime import datetime, timezone

import boto3

from celery_app import celery_app
from config import report_exception, report_message
from gee_export import gee_config

logger = logging.getLogger(__name__)


@celery_app.task(
    name="tasks.import_user_site_upload",
    bind=True,
    max_retries=0,
    acks_late=True,
    reject_on_worker_lost=True,
    soft_time_limit=7200,
    time_limit=7500,
)
def import_user_site_upload_task(
    self, upload_id, user_id, upload_token, column_mapping=None
) -> dict:
    """Persist a staged site upload asynchronously.

    Parameters
    ----------
    self : celery.Task
        Bound Celery task instance.
    upload_id : str
        Upload-job UUID stored in ``user_site_uploads``.
    user_id : str
        Owning user UUID.
    upload_token : str
        Token pointing at the staged upload payload.
    column_mapping : dict | None
        Canonical site-field to source-column mapping selected in the UI.

    Returns
    -------
    dict
        Completion payload with job status plus imported site-set identifiers.
    """
    import uuid

    from services import (
        UserSiteUpload,
        get_db,
        save_user_site_set_from_staged,
        update_user_site_upload_status,
    )

    upload_uuid = uuid.UUID(str(upload_id))
    user_uuid = uuid.UUID(str(user_id))

    db = get_db()
    try:
        upload = (
            db.query(UserSiteUpload)
            .filter(
                UserSiteUpload.id == upload_uuid, UserSiteUpload.user_id == user_uuid
            )
            .first()
        )
        if upload and upload.status == "cancelled":
            return {
                "status": "cancelled",
                "site_set_id": None,
                "site_set_name": None,
                "n_sites": 0,
            }
    finally:
        db.close()

    update_user_site_upload_status(
        upload_uuid,
        status="running",
        started_at=datetime.now(timezone.utc),
        n_sites_imported=0,
        error_message=None,
    )

    try:
        detail = save_user_site_set_from_staged(
            user_uuid,
            upload_token,
            column_mapping=column_mapping,
            upload_id=upload_uuid,
        )
        update_user_site_upload_status(
            upload_uuid,
            status="completed",
            completed_at=datetime.now(timezone.utc),
            site_set_id=uuid.UUID(str(detail["id"])),
            site_set_name=detail["name"],
            n_sites_imported=detail["n_sites"],
            ingest_stats=detail.get("ingest_stats"),
            error_message=None,
        )
        return {
            "status": "completed",
            "site_set_id": detail["id"],
            "site_set_name": detail["name"],
            "n_sites": detail["n_sites"],
        }
    except Exception as exc:
        logger.exception("Asynchronous user site upload failed")
        report_exception()
        update_user_site_upload_status(
            upload_uuid,
            status="failed",
            completed_at=datetime.now(timezone.utc),
            n_sites_imported=0,
            error_message=str(exc),
        )
        raise


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

        run_import(check_only=False)

        # Chain rasterization of the imported vector layers.
        rasterize_vectors_task.delay()
        logger.info("Dispatched rasterize_vectors_task after successful import")

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
    from datetime import datetime, timezone

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
    from datetime import datetime, timezone

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


@celery_app.task(
    name="tasks.run_cog_merge",
    bind=True,
    max_retries=1,
    soft_time_limit=7200,  # 2 h — raises SoftTimeLimitExceeded
    time_limit=7500,  # 2 h 5 m — SIGKILL fallback
)
def run_cog_merge(self, layer_id: str) -> dict:
    """Merge GCS tiles into a single COG and upload to S3.

    Also records tile-level provenance and merge metrics in
    :class:`~models.GeeExportMetadata` so that a merged COG can be
    reliably traced back to the exact set of GEE export tiles.

    Parameters
    ----------
    layer_id : str
        UUID of the :class:`~models.Covariate` database row.

    Returns
    -------
    dict
        ``{"status": "merged", "url": …, "size_bytes": …}`` on success,
        or ``{"status": "failed", "error": …}`` on failure.
    """
    from datetime import datetime, timezone

    from cog_merge import (
        compute_tile_etag_hash,
        list_gcs_tile_details,
        merge_covariate_tiles,
    )
    from config import Config
    from models import Covariate, GeeExportMetadata, get_db

    db = get_db()
    try:
        layer = db.query(Covariate).filter(Covariate.id == layer_id).first()
        if not layer:
            logger.warning(
                "Covariate %s not found — likely superseded by a re-export",
                layer_id,
            )
            db.close()
            return {"status": "superseded", "error": "record deleted"}

        # Guard against duplicate merges.
        if layer.status == "merging":
            logger.info(
                "Covariate %s (%s) is already being merged by another "
                "task — skipping duplicate",
                layer.covariate_name,
                layer_id,
            )
            db.close()
            return {"status": "skipped", "reason": "already merging"}

        # If another duplicate task already completed the merge, skip.
        # The auto_merge scheduler will dispatch a fresh task if tiles
        # change again.
        if layer.status == "merged":
            logger.info(
                "Covariate %s (%s) is already merged — skipping duplicate merge task",
                layer.covariate_name,
                layer_id,
            )
            db.close()
            return {"status": "skipped", "reason": "already merged"}

        # Look for an existing metadata snapshot (created by auto_merge
        # or poll_gee_exports).
        meta = (
            db.query(GeeExportMetadata)
            .filter(
                GeeExportMetadata.covariate_id == layer_id,
                GeeExportMetadata.status.in_(["pending_merge", "detected"]),
            )
            .order_by(GeeExportMetadata.created_at.desc())
            .first()
        )

        # Fetch full tile details from GCS (ETags, sizes, md5 hashes)
        source_bucket = layer.gcs_bucket or Config.GCS_BUCKET
        source_prefix = layer.gcs_prefix or Config.GCS_PREFIX
        tile_details: list[dict] = []
        tile_urls: list[str] | None = None
        try:
            tile_details = list_gcs_tile_details(
                source_bucket, source_prefix, layer.covariate_name
            )
            tile_urls = [
                f"https://storage.googleapis.com/{source_bucket}/{t['name']}"
                for t in tile_details
            ]
        except Exception:
            logger.warning(
                "Failed to fetch tile details for %s — "
                "merge will still proceed but metadata will be incomplete",
                layer.covariate_name,
            )

        tile_hash = compute_tile_etag_hash(tile_details) if tile_details else None
        merge_start = datetime.now(timezone.utc)

        # Create a metadata snapshot if one doesn't exist yet
        if not meta:
            meta = GeeExportMetadata(
                covariate_id=layer.id,
                covariate_name=layer.covariate_name,
                gcs_bucket=source_bucket,
                gcs_prefix=source_prefix,
                gee_task_id=layer.gee_task_id,
                tiles_detected_at=merge_start,
                status="pending_merge",
                created_at=merge_start,
            )
            db.add(meta)
            db.flush()

        # Populate tile details on the snapshot
        if tile_details:
            meta.tile_count = len(tile_details)
            meta.tile_total_bytes = sum(t["size_bytes"] for t in tile_details)
            meta.tile_details = tile_details
            meta.tile_etag_hash = tile_hash

        # Transition to 'merging'
        layer.status = "merging"
        layer.started_at = merge_start
        meta.status = "merging"
        meta.merge_started_at = merge_start
        db.commit()

        # Determine resolution-specific path if output_prefix not set
        if not layer.output_prefix:
            cog_suffix = "_1km" if layer.resolution_m == 1000 else "_250m"
            fallback_prefix = f"{Config.S3_PREFIX}/cog{cog_suffix}"
        else:
            fallback_prefix = layer.output_prefix

        result = merge_covariate_tiles(
            covariate_name=layer.covariate_name,
            source_bucket=source_bucket,
            source_prefix=source_prefix,
            output_bucket=layer.output_bucket,
            output_prefix=fallback_prefix,
            aws_region=Config.AWS_REGION,
            layer_id=layer_id,
            tile_urls=tile_urls,
        )

        # Re-check the record still exists after the (slow) merge
        merge_end = datetime.now(timezone.utc)
        db.expire_all()
        layer = db.query(Covariate).filter(Covariate.id == layer_id).first()
        if not layer:
            logger.warning(
                "Covariate %s deleted during merge — discarding result",
                layer_id,
            )
            db.close()
            return {"status": "superseded"}

        if layer.status == "failed":
            logger.info(
                "Covariate %s was reset to 'failed' during merge "
                "(stale detection race) — overwriting with merge result",
                layer_id,
            )

        layer.status = "merged"
        layer.error_message = None
        layer.merged_url = result["url"]
        layer.size_bytes = result["size_bytes"]
        layer.n_tiles = result["n_tiles"]
        layer.completed_at = merge_end

        # Store the tile fingerprint directly on the Covariate record
        # so that auto_merge_unmerged can compare hashes without any
        # cross-table lookup (which has historically been fragile).
        if tile_hash:
            md = dict(layer.extra_metadata or {})
            md["tile_etag_hash"] = tile_hash
            md.pop("merge_retry_count", None)  # reset on success
            layer.extra_metadata = md
            # Ensure SQLAlchemy detects the JSON column change even if
            # the new dict happens to compare equal to the old one.
            from sqlalchemy.orm.attributes import flag_modified

            flag_modified(layer, "extra_metadata")
            logger.info(
                "run_cog_merge: wrote tile_etag_hash=%s on Covariate "
                "extra_metadata for '%s'",
                tile_hash,
                layer.covariate_name,
            )

        # Update metadata snapshot with merge results.  The stale-
        # detection code in auto_merge_unmerged may have flipped the
        # snapshot to 'failed' while the merge was running, so also
        # accept that status when looking for the snapshot to update.
        meta = (
            db.query(GeeExportMetadata)
            .filter(
                GeeExportMetadata.covariate_id == layer_id,
                GeeExportMetadata.status.in_(["merging", "failed"]),
            )
            .order_by(GeeExportMetadata.created_at.desc())
            .first()
        )
        if meta:
            meta.status = "merged"
            meta.merge_completed_at = merge_end
            meta.merge_duration_seconds = (
                merge_end - (meta.merge_started_at or merge_start)
            ).total_seconds()
            meta.merged_cog_key = result.get("s3_key")
            meta.merged_cog_url = result["url"]
            meta.merged_cog_bytes = result["size_bytes"]
            meta.merged_cog_etag = result.get("s3_etag")
            logger.info(
                "run_cog_merge: stored hash=%s gcs_prefix=%s for '%s'",
                meta.tile_etag_hash,
                meta.gcs_prefix,
                layer.covariate_name,
            )
        else:
            logger.warning(
                "run_cog_merge: no metadata snapshot found for '%s' "
                "(covariate_id=%s) — hash will not be recorded!",
                layer.covariate_name,
                layer_id,
            )

        db.commit()
        logger.info("COG merge completed for '%s'", layer.covariate_name)
        db.close()
        return {
            "status": "merged",
            "url": result["url"],
            "size_bytes": result["size_bytes"],
        }

    except _MergeSuperseded:
        logger.info("Merge for %s superseded by re-export — aborting", layer_id)
        db.close()
        return {"status": "superseded"}

    except Exception as exc:
        logger.exception("COG merge failed for layer %s", layer_id)
        report_exception(layer_id=layer_id)
        # The original session may have a broken connection, so use a
        # fresh one to persist the failure status.
        db.close()
        db = get_db()
        try:
            layer = db.query(Covariate).filter(Covariate.id == layer_id).first()
            if layer:
                layer.status = "failed"
                layer.error_message = str(exc)[:2000]
                layer.completed_at = datetime.now(timezone.utc)
                # Track retry count so auto_merge can retry up to 3 times
                md = dict(layer.extra_metadata or {})
                md["merge_retry_count"] = md.get("merge_retry_count", 0) + 1
                layer.extra_metadata = md
                from sqlalchemy.orm.attributes import flag_modified

                flag_modified(layer, "extra_metadata")
                logger.info(
                    "run_cog_merge: failure #%d for '%s' (max 3 retries)",
                    md["merge_retry_count"],
                    layer.covariate_name,
                )
            # Also mark the metadata snapshot as failed
            meta = (
                db.query(GeeExportMetadata)
                .filter(
                    GeeExportMetadata.covariate_id == layer_id,
                    GeeExportMetadata.status.in_(["pending_merge", "merging"]),
                )
                .order_by(GeeExportMetadata.created_at.desc())
                .first()
            )
            if meta:
                meta.status = "failed"
                meta.error_message = str(exc)[:2000]
                meta.merge_completed_at = datetime.now(timezone.utc)
            db.commit()
        except Exception:
            db.rollback()
        finally:
            db.close()
        return {"status": "failed", "error": str(exc)[:500]}


@celery_app.task(name="tasks.poll_gee_exports")
def poll_gee_exports() -> dict:
    """Poll GEE for active export task statuses and update the database.

    This is called periodically by Celery Beat (every 60 s) so the webapp
    no longer needs to poll inline during page refreshes.

    Returns
    -------
    dict
        ``{"checked": N, "updated": N}``
    """
    import json
    import os
    from datetime import datetime, timezone

    from config import Config
    from models import Covariate, GeeExportMetadata, get_db

    db = get_db()
    try:
        active = (
            db.query(Covariate)
            .filter(Covariate.status.in_(["pending_export", "exporting"]))
            .all()
        )
        if not active:
            return {"checked": 0, "updated": 0}

        _auto_merge_ids: list[str] = []  # collect exports to auto-merge

        import base64

        import ee

        # Initialize EE
        project = Config.GEE_PROJECT_ID or None
        opt_url = Config.GEE_ENDPOINT or None
        ee_sa_json = os.environ.get("EE_SERVICE_ACCOUNT_JSON", "")
        if ee_sa_json:
            try:
                key_data = base64.b64decode(ee_sa_json).decode("utf-8")
            except Exception:
                key_data = ee_sa_json
            sa_info = json.loads(key_data)
            credentials = ee.ServiceAccountCredentials(
                sa_info["client_email"], key_data=json.dumps(sa_info)
            )
            ee.Initialize(credentials=credentials, project=project, opt_url=opt_url)
        else:
            ee.Initialize(project=project, opt_url=opt_url)

        state_map = {
            "PENDING": "pending_export",
            "RUNNING": "exporting",
            "SUCCEEDED": "exported",
            "FAILED": "failed",
            "CANCELLED": "cancelled",
            "CANCELLING": "exporting",
        }

        updated = 0
        for export in active:
            if not export.gee_task_id:
                continue
            try:
                op_name = f"projects/{project}/operations/{export.gee_task_id}"
                op = ee.data.getOperation(op_name)
                metadata = op.get("metadata", {})
                gee_state = metadata.get("state", op.get("done") and "SUCCEEDED")
                new_status = state_map.get(gee_state, export.status)

                if new_status != export.status:
                    export.status = new_status
                    updated += 1
                    if new_status in ("exported", "failed", "cancelled"):
                        export.completed_at = datetime.now(timezone.utc)
                    if new_status == "exported":
                        from services import list_export_tiles

                        tile_urls = list_export_tiles(
                            export.gcs_bucket,
                            export.gcs_prefix,
                            export.covariate_name,
                        )
                        extra = dict(export.extra_metadata or {})
                        extra["tile_urls"] = tile_urls
                        export.extra_metadata = extra

                        # Auto-trigger COG merge now that tiles are ready
                        export.status = "pending_merge"
                        export.output_bucket = Config.S3_BUCKET
                        if not export.output_prefix:
                            _cog_suffixes = {1000: "_1km", 250: "_250m"}
                            _cog_suffix = _cog_suffixes.get(export.resolution_m, "_1km")
                            export.output_prefix = (
                                f"{Config.S3_PREFIX}/cog{_cog_suffix}"
                            )
                        _auto_merge_ids.append(str(export.id))

                        # Create a GeeExportMetadata record that links
                        # the GEE task to the upcoming merge.  Full tile
                        # details (ETags, sizes) will be populated by
                        # run_cog_merge when it fetches tiles from GCS.
                        gee_meta = GeeExportMetadata(
                            covariate_id=export.id,
                            covariate_name=export.covariate_name,
                            gcs_bucket=export.gcs_bucket,
                            gcs_prefix=export.gcs_prefix,
                            tile_count=len(tile_urls) if tile_urls else None,
                            gee_task_id=export.gee_task_id,
                            gee_completed_at=export.completed_at,
                            tiles_detected_at=datetime.now(timezone.utc),
                            status="pending_merge",
                        )
                        db.add(gee_meta)

                error = op.get("error")
                if error:
                    export.error_message = error.get("message", str(error))
                    if export.status == "exporting":
                        export.status = "failed"
                        export.completed_at = datetime.now(timezone.utc)
                        updated += 1

            except Exception as exc:
                logger.warning(
                    "Failed to poll GEE status for task %s: %s",
                    export.gee_task_id,
                    exc,
                )
                report_exception(gee_task_id=export.gee_task_id)

        db.commit()

        # Dispatch COG merges for any exports that just completed
        for layer_id in _auto_merge_ids:
            run_cog_merge.delay(layer_id)
            logger.info("Auto-dispatched COG merge for covariate %s", layer_id)

        return {
            "checked": len(active),
            "updated": updated,
            "merges_dispatched": len(_auto_merge_ids),
        }
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


@celery_app.task(name="tasks.auto_merge_unmerged")
def auto_merge_unmerged() -> dict:
    """Find covariates with GCS tiles but no up-to-date merge, and dispatch.

    Scans GCS for tiles with full metadata (ETags, sizes), computes a
    fingerprint hash for each covariate, and compares against the most
    recent :class:`~models.GeeExportMetadata` snapshot.  Merges are
    dispatched only when the tile fingerprint has changed or no snapshot
    exists.

    Also creates :class:`~models.Covariate` DB records for merged COGs
    that already exist on S3 but are not yet tracked in the database
    (the *fresh-database* scenario).

    Called periodically by Celery Beat (every 120 s).

    A Redis lock prevents concurrent invocations (caused by beat
    firing while a previous scan is still running) from piling up
    duplicate merge tasks in the queue.

    Returns
    -------
    dict
        ``{"scanned": N, "dispatched": N, "discovered": N}``
    """
    import redis as _redis

    from config import Config

    if not Config.GCS_BUCKET:
        return {"scanned": 0, "dispatched": 0, "discovered": 0}

    # Acquire a non-blocking Redis lock so only one invocation runs at
    # a time.  The 10-minute timeout is a safety net in case the task
    # crashes without releasing the lock.
    _rconn = _redis.from_url(Config.CELERY_BROKER_URL)
    _lock = _rconn.lock("auto_merge_unmerged_lock", timeout=600, blocking=False)
    if not _lock.acquire(blocking=False):
        logger.info("auto_merge: another instance is already running — skipping")
        return {"skipped": True}

    try:
        return _auto_merge_unmerged_inner()
    finally:
        try:
            _lock.release()
        except Exception:
            pass  # lock may have expired; safe to ignore


def _auto_merge_unmerged_inner() -> dict:
    """Actual logic for auto_merge_unmerged, called under a Redis lock."""
    from datetime import datetime, timedelta, timezone

    import sqlalchemy as sa

    from config import Config
    from models import Covariate, GeeExportMetadata, get_db

    if not Config.GCS_BUCKET:
        return {"scanned": 0, "dispatched": 0, "discovered": 0}

    # Load covariate names from GEE export config

    gee_export_dir = Path(__file__).parent / "gee_export"
    if not gee_export_dir.exists():
        logger.warning("GEE export directory not found at %s", gee_export_dir)
        return {"scanned": 0, "dispatched": 0, "discovered": 0}

    # gee_config already imported at module level
    known_covariates = list(gee_config.COVARIATES.keys())
    resolutions = gee_config.RESOLUTIONS  # {1000: {...}, 250: {...}}

    # Scan GCS for tile details (ETags, sizes, md5 hashes)
    from cog_merge import (
        compute_tile_etag_hash,
        list_s3_cog_objects,
        scan_gcs_tile_details,
    )

    # Scan all resolution-specific GCS prefixes for tiles.
    # Each entry is keyed by (covariate_name, resolution_m).
    gcs_details: dict[tuple[str, int], list[dict]] = {}
    for res_m, res_cfg in resolutions.items():
        gcs_prefix = gee_config.get_gcs_prefix(Config.GCS_PREFIX, res_m)
        try:
            for name, tiles in scan_gcs_tile_details(
                Config.GCS_BUCKET,
                gcs_prefix,
                known_covariates,
            ).items():
                gcs_details[(name, res_m)] = tiles
        except Exception:
            logger.warning(
                "Failed to scan GCS tiles at prefix %s",
                gcs_prefix,
                exc_info=True,
            )

    if not gcs_details:
        return {"scanned": len(known_covariates), "dispatched": 0, "discovered": 0}

    with_tiles = {key for key, tiles in gcs_details.items() if tiles}
    if not with_tiles:
        return {"scanned": len(known_covariates), "dispatched": 0, "discovered": 0}

    db = get_db()
    dispatched_ids: list[str] = []
    discovered = 0
    now = datetime.now(timezone.utc)

    try:
        # ---- Discover pre-existing S3 COGs without DB records ----
        # Handles the fresh-database scenario: COGs from a prior
        # deployment are already on S3 but the new DB has no rows.
        # Scan each resolution's S3 prefix.
        _cog_suffixes = {1000: "_1km", 250: "_250m"}
        s3_cog_map: dict[tuple[str, int], dict] = {}
        try:
            if Config.S3_BUCKET:
                for res_m, cog_suffix in _cog_suffixes.items():
                    cog_prefix = f"{Config.S3_PREFIX}/cog{cog_suffix}"
                    for obj in list_s3_cog_objects(
                        Config.S3_BUCKET, cog_prefix, Config.AWS_REGION
                    ):
                        s3_cog_map[(obj["covariate"], res_m)] = obj
        except Exception:
            logger.warning("Failed to scan S3 for existing COGs")
            report_exception()

        existing_covariate_keys = {
            (row.covariate_name, row.resolution_m)
            for row in db.query(Covariate.covariate_name, Covariate.resolution_m)
            .filter(Covariate.status.in_(["pending_merge", "merging", "merged"]))
            .all()
        }

        for (cov_name, res_m), s3_obj in s3_cog_map.items():
            if (cov_name, res_m) in existing_covariate_keys:
                continue

            cog_suffix = _cog_suffixes.get(res_m, "_1km")
            gcs_prefix = gee_config.get_gcs_prefix(Config.GCS_PREFIX, res_m)

            # Create a Covariate record so the UI shows the COG
            layer = Covariate(
                covariate_name=cov_name,
                resolution_m=res_m,
                status="merged",
                gcs_bucket=Config.GCS_BUCKET,
                gcs_prefix=gcs_prefix,
                output_bucket=Config.S3_BUCKET,
                output_prefix=f"{Config.S3_PREFIX}/cog{cog_suffix}",
                merged_url=s3_obj["url"],
                size_bytes=s3_obj["size"],
                completed_at=now,
            )
            db.add(layer)
            db.flush()
            existing_covariate_keys.add((cov_name, res_m))

            # Store tile hash on Covariate for hash comparison
            tiles = gcs_details.get((cov_name, res_m), [])
            tile_hash = compute_tile_etag_hash(tiles) if tiles else None
            if tile_hash:
                md = dict(layer.extra_metadata or {})
                md["tile_etag_hash"] = tile_hash
                layer.extra_metadata = md
                from sqlalchemy.orm.attributes import flag_modified

                flag_modified(layer, "extra_metadata")

            # Create an export metadata snapshot for the discovered COG
            meta = GeeExportMetadata(
                covariate_id=layer.id,
                covariate_name=cov_name,
                gcs_bucket=Config.GCS_BUCKET,
                gcs_prefix=gcs_prefix,
                tile_count=len(tiles),
                tile_total_bytes=sum(t["size_bytes"] for t in tiles),
                tile_details=tiles or None,
                tile_etag_hash=tile_hash,
                tiles_detected_at=now,
                merged_cog_key=s3_obj.get("key"),
                merged_cog_url=s3_obj["url"],
                merged_cog_bytes=s3_obj["size"],
                merged_cog_etag=s3_obj.get("etag"),
                status="skipped_existing",
                created_at=now,
            )
            db.add(meta)
            discovered += 1
            logger.info(
                "Discovered pre-existing COG for %s on S3 — "
                "created Covariate + GeeExportMetadata records",
                cov_name,
            )

        db.flush()

        # ---- Reconcile orphaned "merged" records without S3 files ----
        # If a DB record claims status='merged' but the S3 scan shows
        # the file is missing, reset it to 'failed' with an error
        # message. This can happen if:
        #   - S3 file was manually deleted
        #   - Merge task set status='merged' but S3 upload failed
        #   - Historical data from before proper validation
        orphaned_count = 0
        orphaned_merged = (
            db.query(Covariate)
            .filter(
                Covariate.status == "merged",
                Covariate.merged_url.isnot(None),
            )
            .all()
        )
        for rec in orphaned_merged:
            key = (rec.covariate_name, rec.resolution_m)
            if key not in s3_cog_map:
                # DB says merged, but S3 file is missing
                logger.warning(
                    "Orphaned merged record for %s (id=%s) — "
                    "S3 file missing, resetting to failed",
                    rec.covariate_name,
                    rec.id,
                )
                rec.status = "failed"
                rec.error_message = (
                    "Reset by auto_merge: DB claimed 'merged' but S3 file not found. "
                    "File may have been deleted or upload may have failed."
                )
                rec.completed_at = now
                orphaned_count += 1

                # Also update any metadata snapshots
                orphaned_metas = (
                    db.query(GeeExportMetadata)
                    .filter(
                        GeeExportMetadata.covariate_id == rec.id,
                        GeeExportMetadata.status == "merged",
                    )
                    .all()
                )
                for om in orphaned_metas:
                    om.status = "failed"
                    om.error_message = "S3 file missing (orphaned record)"

        if orphaned_count > 0:
            db.flush()
            logger.info(
                "Reset %d orphaned 'merged' records without S3 files", orphaned_count
            )

        # ---- Reset stale merges FIRST ----
        # Must run before the snapshot query so that stale records
        # show as "failed" (not "merging") in the snapshot.
        #
        # Use different timeouts for queue-waiting vs actively-merging:
        #  - pending_merge: task is sitting in the Redis queue — use a
        #    long timeout (6 h) because the single-concurrency merge
        #    worker may have a deep backlog.
        #  - merging: worker has started processing — 2 h is generous
        #    even for the largest 250 m COGs (~25 min typical).
        stale_pending_cutoff = now - timedelta(hours=6)
        stale_merging_cutoff = now - timedelta(hours=2)
        stale_merging = (
            db.query(Covariate)
            .filter(
                sa.or_(
                    sa.and_(
                        Covariate.status == "pending_merge",
                        Covariate.started_at.isnot(None),
                        Covariate.started_at < stale_pending_cutoff,
                    ),
                    sa.and_(
                        Covariate.status == "merging",
                        Covariate.started_at.isnot(None),
                        Covariate.started_at < stale_merging_cutoff,
                    ),
                    # Catch orphaned pending_merge records whose Redis
                    # task was lost before the worker picked them up
                    # (started_at is still NULL).  Apply the same long
                    # timeout via completed_at as a fallback timestamp.
                    sa.and_(
                        Covariate.status == "pending_merge",
                        Covariate.started_at.is_(None),
                        sa.or_(
                            sa.and_(
                                Covariate.completed_at.isnot(None),
                                Covariate.completed_at < stale_pending_cutoff,
                            ),
                            Covariate.completed_at.is_(None),
                        ),
                    ),
                ),
            )
            .all()
        )
        for stale in stale_merging:
            was_merging = stale.status == "merging"
            logger.warning(
                "Resetting stale covariate %s (%s) from '%s' to 'failed' "
                "(stuck since %s)",
                stale.covariate_name,
                stale.id,
                stale.status,
                stale.started_at,
            )
            stale.status = "failed"
            if was_merging:
                stale.error_message = "Reset by auto_merge: stuck in merging for >2 h"
            else:
                stale.error_message = (
                    "Reset by auto_merge: stuck in pending_merge for >6 h "
                    "(Redis task likely lost)"
                )
            stale.completed_at = now
            # Count ALL stale resets (both pending_merge and merging)
            # against the retry limit.  If the merge worker is down,
            # pending_merge records would otherwise loop indefinitely:
            #   pending_merge → (6 h) → failed → retry → pending_merge → …
            md = dict(stale.extra_metadata or {})
            md["merge_retry_count"] = md.get("merge_retry_count", 0) + 1
            stale.extra_metadata = md
            from sqlalchemy.orm.attributes import flag_modified

            flag_modified(stale, "extra_metadata")
            stale_metas = (
                db.query(GeeExportMetadata)
                .filter(
                    GeeExportMetadata.covariate_id == stale.id,
                    GeeExportMetadata.status.in_(["pending_merge", "merging"]),
                )
                .all()
            )
            for sm in stale_metas:
                sm.status = "failed"
                sm.error_message = "Reset: worker killed during merge"
                sm.merge_completed_at = now
        if stale_merging:
            db.flush()

        # ---- Determine which covariates need a (re-)merge ----
        # CRITICAL: derive both `latest_hashes` and `in_progress`
        # from a SINGLE query to avoid a TOCTOU race.
        #
        # Previously, hashes were fetched with one SELECT (status =
        # "merged") and the in-progress set with a second SELECT
        # (status IN ("pending_merge", "merging")).  Under PostgreSQL
        # READ COMMITTED, each statement sees the latest committed
        # data at the instant it runs.  If the merge worker committed
        # a status change from "merging" → "merged" between the two
        # queries, the record was excluded from BOTH checks:
        #   • hash lookup: missed it (status was still "merging")
        #   • in_progress: missed it (status was now "merged")
        # → covariate fell through and was re-dispatched every cycle.
        #
        # The fix: one query fetches all Covariate records, and Python
        # derives both sets from the same snapshot.
        all_covariates = (
            db.query(
                Covariate.id,
                Covariate.covariate_name,
                Covariate.resolution_m,
                Covariate.status,
                Covariate.extra_metadata,
                Covariate.completed_at,
            )
            .order_by(Covariate.completed_at.desc().nulls_last())
            .all()
        )

        latest_hashes: dict[tuple[str, int], str] = {}
        in_progress: set[tuple[str, int]] = set()
        # Track the most recent failed covariate per (name, res_m) that
        # is eligible for an automatic retry (merge_retry_count < 3).
        _max_merge_retries = 3
        retryable_failed: dict[tuple[str, int], str] = {}  # key → covariate id
        # Track covariates that have exhausted all retry attempts so
        # the need_merge loop can skip them when there is no stored
        # hash (i.e. never successfully merged).  Without this, the
        # need_merge path (stored_hash=None != current_hash) bypasses
        # the retry limit and redispatches every 6 h stale cycle.
        exhausted_retries: set[tuple[str, int]] = set()
        for row in all_covariates:
            key = (row.covariate_name, row.resolution_m)
            if row.status in ("pending_merge", "merging"):
                in_progress.add(key)
            elif row.status == "merged" and key not in latest_hashes:
                tile_hash = (row.extra_metadata or {}).get("tile_etag_hash")
                if tile_hash:
                    latest_hashes[key] = tile_hash
            elif (
                row.status == "failed"
                and key not in retryable_failed
                and key not in in_progress
                and key not in latest_hashes
                and key in with_tiles
            ):
                retry_count = (row.extra_metadata or {}).get("merge_retry_count", 0)
                if retry_count < _max_merge_retries:
                    retryable_failed[key] = str(row.id)
                elif key not in exhausted_retries:
                    exhausted_retries.add(key)

        # Prune retryable_failed of keys that also have an active
        # pending_merge/merging record.  Due to query ordering
        # (completed_at DESC NULLS LAST) a failed record can be
        # iterated *before* a pending_merge record for the same key,
        # causing the key to appear in both sets.  Dispatching a retry
        # for a covariate that already has a pending task would create
        # duplicate merge jobs.
        for key in in_progress:
            retryable_failed.pop(key, None)

        if in_progress:
            in_progress_names = sorted(
                f"{name}@{res_m}m" for name, res_m in in_progress
            )
            logger.info(
                "auto_merge: in-progress covariates: %s",
                ", ".join(in_progress_names),
            )
        logger.info(
            "auto_merge: %d covariate(s) with tiles, %d in-progress, "
            "%d latest hashes loaded, %d retryable failures, "
            "%d exhausted retries",
            len(with_tiles),
            len(in_progress),
            len(latest_hashes),
            len(retryable_failed),
            len(exhausted_retries),
        )

        need_merge: list[tuple[str, int]] = []

        # Sort by resolution first (1000m before 250m), then by total
        # tile size (smallest first within each resolution).  This ensures:
        # - Coarse (1km) COGs are available quickly for preview
        # - Small merges complete first, reducing queue depth
        # - Large 250m COGs are deferred until last, reducing OOM risk
        def get_sort_key(key: tuple[str, int]) -> tuple[int, int]:
            """Return (negative_resolution, total_size) for sorting."""
            name, res_m = key
            tiles = gcs_details.get(key, [])
            total_size = sum(t.get("size_bytes", 0) for t in tiles)
            # Negative resolution to sort 1000m before 250m (descending)
            return (-res_m, total_size)

        for name, res_m in sorted(with_tiles, key=get_sort_key):
            if (name, res_m) in in_progress:
                continue
            current_hash = compute_tile_etag_hash(gcs_details[(name, res_m)])
            stored_hash = latest_hashes.get((name, res_m))
            if stored_hash == current_hash:
                continue  # tiles unchanged since last merge
            # When there is no stored hash (covariate was never
            # successfully merged), respect the retry limit.  Without
            # this guard the need_merge path bypasses
            # merge_retry_count and redispatches the covariate on
            # every 6 h stale-reset cycle, creating an infinite loop
            # when the merge-worker is down or the merge keeps
            # failing.  If stored_hash is not None the tiles have
            # genuinely changed since the last successful merge, so a
            # fresh attempt is always warranted.
            if stored_hash is None and (name, res_m) in exhausted_retries:
                logger.info(
                    "auto_merge: %s@%dm needs merge but retry limit "
                    "(%d) exhausted — skipping",
                    name,
                    res_m,
                    _max_merge_retries,
                )
                continue
            need_merge.append((name, res_m))
            logger.info(
                "auto_merge: %s@%dm needs merge — stored_hash=%s current_hash=%s",
                name,
                res_m,
                stored_hash,
                current_hash,
            )

        if not need_merge and not retryable_failed:
            db.commit()
            return {
                "scanned": len(known_covariates),
                "dispatched": 0,
                "discovered": discovered,
            }

        # Remove retryable keys that will already be handled by need_merge
        dispatched_keys = set(need_merge)
        for key in dispatched_keys:
            retryable_failed.pop(key, None)

        for name, res_m in need_merge:
            tiles = gcs_details[(name, res_m)]
            tile_hash = compute_tile_etag_hash(tiles)

            cog_suffix = _cog_suffixes.get(res_m, "_1km")
            gcs_prefix = gee_config.get_gcs_prefix(Config.GCS_PREFIX, res_m)
            s3_output_prefix = f"{Config.S3_PREFIX}/cog{cog_suffix}"

            # Create or update Covariate record
            existing = (
                db.query(Covariate)
                .filter(
                    Covariate.covariate_name == name,
                    Covariate.resolution_m == res_m,
                    Covariate.status.in_(["exported", "merged", "failed"]),
                )
                .order_by(Covariate.started_at.desc())
                .first()
            )
            if existing:
                existing.status = "pending_merge"
                existing.started_at = now
                existing.error_message = None
                existing.output_bucket = Config.S3_BUCKET
                existing.output_prefix = s3_output_prefix
                existing.gcs_prefix = gcs_prefix
                cov_id = existing.id
                dispatched_ids.append(str(existing.id))
            else:
                layer = Covariate(
                    covariate_name=name,
                    resolution_m=res_m,
                    status="pending_merge",
                    gcs_bucket=Config.GCS_BUCKET,
                    gcs_prefix=gcs_prefix,
                    output_bucket=Config.S3_BUCKET,
                    output_prefix=s3_output_prefix,
                    started_at=now,
                )
                db.add(layer)
                db.flush()
                cov_id = layer.id
                dispatched_ids.append(str(layer.id))

            # Create metadata snapshot with full tile details
            meta = GeeExportMetadata(
                covariate_id=cov_id,
                covariate_name=name,
                gcs_bucket=Config.GCS_BUCKET,
                gcs_prefix=gcs_prefix,
                tile_count=len(tiles),
                tile_total_bytes=sum(t["size_bytes"] for t in tiles),
                tile_details=tiles,
                tile_etag_hash=tile_hash,
                tiles_detected_at=now,
                status="pending_merge",
                created_at=now,
            )
            db.add(meta)

        # ---- Auto-retry failed merges (up to 3 attempts) ----
        # Only retry covariates that failed, still have GCS tiles, and
        # haven't already been picked up by the need_merge loop above
        # (which covers tile-hash changes).  Overlapping keys were
        # already pruned above.
        retry_ids: list[str] = []
        for key, cov_id in retryable_failed.items():
            cov = db.query(Covariate).filter(Covariate.id == cov_id).first()
            if not cov or cov.status != "failed":
                continue
            retry_count = (cov.extra_metadata or {}).get("merge_retry_count", 0)
            logger.info(
                "auto_merge: retrying failed covariate '%s'@%dm (attempt %d/%d, id=%s)",
                cov.covariate_name,
                cov.resolution_m,
                retry_count + 1,
                _max_merge_retries,
                cov_id,
            )
            cov.status = "pending_merge"
            cov.started_at = now
            cov.error_message = None
            retry_ids.append(cov_id)

        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    # Dispatch merge tasks (runs on the merge queue)
    for layer_id in dispatched_ids:
        run_cog_merge.delay(layer_id)
        logger.info("Auto-merge dispatched for covariate %s", layer_id)

    for layer_id in retry_ids:
        run_cog_merge.delay(layer_id)
        logger.info("Auto-merge retry dispatched for covariate %s", layer_id)

    # Check if all merges are complete and dispatch SDG ingestion if needed
    sdg_dispatched = False
    if not dispatched_ids and not retry_ids and not in_progress:
        # Check if there are any covariates with tiles on GCS that haven't
        # been merged to S3 yet. Only dispatch SDG when all GCS tiles have
        # been successfully merged.
        unmerged = with_tiles - set(latest_hashes.keys())
        if unmerged:
            logger.info(
                "Not dispatching SDG: %d covariates with GCS tiles not yet merged: %s",
                len(unmerged),
                sorted(f"{name}@{res}m" for name, res in unmerged),
            )
        else:
            # All covariates with GCS tiles have been merged
            # Check if SDG ingestion is already queued or running
            try:
                from celery_app import celery_app as app

                inspector = app.control.inspect()
                active_tasks = inspector.active() or {}
                scheduled_tasks = inspector.scheduled() or {}

                task_name = "tasks.ingest_sdg_cog"
                already_running = False

                for worker_tasks in active_tasks.values():
                    if any(t.get("name") == task_name for t in worker_tasks):
                        already_running = True
                        break

                if not already_running:
                    for worker_tasks in scheduled_tasks.values():
                        if any(t.get("name") == task_name for t in worker_tasks):
                            already_running = True
                            break

                if not already_running:
                    ingest_sdg_cog_task.delay()
                    logger.info(
                        "All COG merges complete — dispatched SDG ingestion task"
                    )
                    sdg_dispatched = True
                else:
                    logger.info(
                        "All COG merges complete but SDG ingestion already queued/running"
                    )
            except Exception:
                logger.warning("Failed to check/dispatch SDG ingestion", exc_info=True)

    return {
        "scanned": len(known_covariates),
        "dispatched": len(dispatched_ids) + len(retry_ids),
        "retried": len(retry_ids),
        "discovered": discovered,
        "sdg_dispatched": sdg_dispatched,
    }


@celery_app.task(name="tasks.poll_batch_tasks")
def poll_batch_tasks() -> dict:
    """Poll for active analysis task statuses and update the DB.

    Checks API-routed tasks (extract_job_id starts with ``api:``)
    by querying the trends.earth API for execution status.  Also
    discovers API executions that have no local tracking record and
    adopts them automatically.  Called periodically by Celery Beat
    (every 30 s).

    Returns
    -------
    dict
        ``{"checked": N, "updated": N, "adopted": N}``
    """
    from datetime import datetime, timezone

    from models import AnalysisTask, get_db

    db = get_db()
    try:
        active = (
            db.query(AnalysisTask)
            .filter(AnalysisTask.status.in_(["submitted", "running"]))
            .all()
        )

        now = datetime.now(timezone.utc)
        updated = 0
        client = None  # may be shared between polling and discovery

        # ---- Poll API-routed tasks ----
        # Background polling uses the system-level service credentials
        # (TRENDSEARTH_CLIENT_ID / TRENDSEARTH_CLIENT_SECRET) because
        # Celery workers have no per-user context.  User credentials are
        # only used at submission time.
        api_tasks = [t for t in active if (t.extract_job_id or "").startswith("api:")]
        if api_tasks:
            from config import Config
            from trendsearth_client import TrendsEarthClient

            if not Config.TRENDSEARTH_CLIENT_ID or not Config.TRENDSEARTH_CLIENT_SECRET:
                msg = (
                    "Skipping API task polling: TRENDSEARTH_CLIENT_ID and "
                    "TRENDSEARTH_CLIENT_SECRET must be set in the environment "
                    "for background status polling to work. "
                    f"{len(api_tasks)} task(s) will not be polled until "
                    "these are configured."
                )
                logger.warning(msg)
                report_message(msg, level="error", pending_tasks=len(api_tasks))
            else:
                client = TrendsEarthClient(
                    api_url=Config.TRENDSEARTH_API_URL,
                    client_id=Config.TRENDSEARTH_CLIENT_ID,
                    client_secret=Config.TRENDSEARTH_CLIENT_SECRET,
                )
                for task in api_tasks:
                    # Capture the task ID as a plain string now, while the
                    # session is healthy, so exception handlers can reference
                    # it even if the session later enters PendingRollbackError.
                    task_id_str = str(task.id)
                    try:
                        exec_id = task.extract_job_id[4:]  # strip "api:"
                        execution = client.get_execution(exec_id)
                        # The API returns {"data": {"status": ...}}
                        exec_data = execution.get("data", {})
                        api_status = exec_data.get("status", "").upper()
                        old_status = task.status

                        logger.info(
                            "Polling API task %s (exec %s): api_status=%s, local_status=%s",
                            task.id,
                            exec_id,
                            api_status,
                            old_status,
                        )

                        # Capture batch job IDs from API results
                        # into extra_metadata for display purposes.
                        api_results = exec_data.get("results") or {}
                        api_batch_jobs = api_results.get("batch_jobs")
                        if api_batch_jobs and isinstance(api_batch_jobs, dict):
                            meta = dict(task.extra_metadata or {})
                            meta["batch_jobs"] = api_batch_jobs
                            task.extra_metadata = meta

                        if api_status == "FINISHED":
                            task.status = "succeeded"
                            task.completed_at = now
                            # Fetch and import results into the local DB
                            try:
                                results_payload = client.get_execution_results(exec_id)
                                if results_payload:
                                    from services import import_execution_results

                                    import_execution_results(
                                        str(task.id), results_payload, db=db
                                    )
                                else:
                                    logger.warning(
                                        "Task %s finished but no results "
                                        "returned by API",
                                        task.id,
                                    )
                            except Exception as results_exc:
                                # Roll back the session FIRST.  If import_execution_results
                                # raised a SQLAlchemy exception (e.g. IntegrityError from
                                # an autoflush) the session is in PendingRollbackError state
                                # and any further ORM attribute access — including task.id —
                                # will raise another exception before we can recover.
                                db.rollback()
                                logger.warning(
                                    "Task %s finished but failed to import results: %s",
                                    task_id_str,
                                    results_exc,
                                )
                                report_exception(task_id=task_id_str)
                                # Re-apply the status after the rollback (which expires
                                # all ORM-tracked attributes).
                                task.status = "succeeded"
                                task.completed_at = now
                        elif api_status == "FAILED":
                            task.status = "failed"
                            task.error_message = exec_data.get("results", {}).get(
                                "error", "Execution failed on API"
                            )
                            task.completed_at = now
                        elif api_status == "CANCELLED":
                            task.status = "cancelled"
                            task.completed_at = now
                        elif api_status in ("RUNNING", "READY"):
                            task.status = "running"
                            if not task.started_at:
                                task.started_at = now
                        # PENDING / SUBMITTED → keep as "submitted"

                        if task.status != old_status:
                            updated += 1
                    except Exception as exc:
                        logger.warning(
                            "Failed to poll API status for task %s: %s",
                            task_id_str,
                            exc,
                        )
                        report_exception(task_id=task_id_str)

        db.commit()

        # ---- Discover untracked API executions ----
        # After handling locally-known tasks, query the trends.earth API
        # for *all* executions of the avoided-emissions script and adopt
        # any that don't already have a local AnalysisTask record.
        # Disabled by default outside development — set
        # ENABLE_TASK_ADOPTION=true to opt in.
        adopted = 0
        try:
            from config import Config
            from trendsearth_client import TrendsEarthClient

            from models import User

            if not Config.ENABLE_TASK_ADOPTION:
                pass  # skip discovery entirely
            elif not (Config.TRENDSEARTH_SCRIPT_ID and Config.TRENDSEARTH_CLIENT_ID):
                pass  # not configured
            else:
                script_id = Config.TRENDSEARTH_SCRIPT_ID

                # Pre-check: skip discovery entirely when no users exist.
                # adopt_api_execution needs a user to assign ownership to.
                has_users = db.query(User.id).first() is not None
                if not has_users:
                    logger.info(
                        "Skipping API execution discovery: no users in the "
                        "database yet. Create a user to enable adoption of "
                        "API executions."
                    )
                else:
                    # Re-use the client created during polling if available
                    if client is None:
                        client = TrendsEarthClient(
                            api_url=Config.TRENDSEARTH_API_URL,
                            client_id=Config.TRENDSEARTH_CLIENT_ID,
                            client_secret=Config.TRENDSEARTH_CLIENT_SECRET,
                        )

                    resp = client.list_executions(script_id=script_id) or {}
                    api_executions = resp.get("data", [])
                    if not isinstance(api_executions, list):
                        api_executions = []

                    # Build set of API exec IDs we already track locally
                    known_exec_ids = set()
                    all_tasks = (
                        db.query(AnalysisTask.extract_job_id)
                        .filter(AnalysisTask.extract_job_id.isnot(None))
                        .all()
                    )
                    for (job_id,) in all_tasks:
                        if job_id.startswith("api:"):
                            known_exec_ids.add(job_id[4:])

                    for exec_data in api_executions:
                        eid = exec_data.get("id", "")
                        if eid and eid not in known_exec_ids:
                            try:
                                from services import (
                                    adopt_api_execution,
                                    import_execution_results,
                                )

                                task_obj = adopt_api_execution(exec_data, db)
                                if task_obj:
                                    # If finished, also import results
                                    api_status = exec_data.get("status", "").upper()
                                    if api_status == "FINISHED":
                                        results_payload = client.get_execution_results(
                                            eid
                                        )
                                        if results_payload:
                                            import_execution_results(
                                                str(task_obj.id),
                                                results_payload,
                                                db=db,
                                            )
                                    adopted += 1
                            except Exception as adopt_exc:
                                db.rollback()
                                logger.warning(
                                    "Failed to adopt API execution %s: %s",
                                    eid,
                                    adopt_exc,
                                )
                                report_exception(extra_data={"exec_id": eid})

                if adopted:
                    db.commit()
                    logger.info("Discovery: adopted %d new API execution(s)", adopted)
        except Exception as disc_exc:
            logger.warning("API execution discovery failed: %s", disc_exc)
            db.rollback()

        return {"checked": len(active), "updated": updated, "adopted": adopted}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


@celery_app.task(
    name="tasks.submit_analysis_task_worker",
    bind=True,
    # Limit retries: if the worker is OOM-killed the message is nacked and
    # re-queued (acks_late + reject_on_worker_lost), but we cap at 2 retries
    # so a persistently memory-constrained submission doesn't loop forever.
    max_retries=2,
    acks_late=True,
    reject_on_worker_lost=True,
    soft_time_limit=1800,  # 30 minutes
    time_limit=1900,
)
def submit_analysis_task_worker(self, task_id: str, user_id: str) -> None:
    """Complete the async submission of an analysis task.

    Called by :func:`services.queue_analysis_task` after it has created
    a local ``AnalysisTask`` record with ``status='submitting'``.  This
    task handles all of the slow, I/O-heavy work:

    * PostGIS geometry computations (matching extent, exclusion buffer)
    * Optional site splitting across exact-match boundaries
    * ``TaskSite`` row creation
    * S3 site upload
    * trends.earth API call (``create_execution``)

    On success the task record is updated to ``status='submitted'``.
    On failure it is updated to ``status='failed'`` and the exception
    is re-raised so Rollbar is notified.

    Parameters
    ----------
    task_id:
        UUID of the ``AnalysisTask`` record to complete.
    user_id:
        UUID of the submitting user (used to retrieve OAuth2 credentials).
    """
    from services import _complete_analysis_task_submission

    logger.info(
        "submit_analysis_task_worker: starting for task %s (user=%s)",
        task_id,
        user_id,
    )
    try:
        _complete_analysis_task_submission(task_id, user_id)
        logger.info("submit_analysis_task_worker: completed for task %s", task_id)
    except Exception as exc:
        logger.error(
            "submit_analysis_task_worker: failed for task %s: %s",
            task_id,
            exc,
            exc_info=True,
        )
        report_exception()
        raise


@celery_app.task(
    name="tasks.generate_match_quality_summary",
    soft_time_limit=3600,
    time_limit=3900,
)
def generate_match_quality_summary_task(
    task_id: str, results_s3_uri: str | None = None
) -> dict:
    """Generate the pre-computed match quality summary JSON for a task.

    This is the *backfill* path for tasks that completed before the R
    summarize script started producing ``results_match_quality_summary.json``.
    It downloads the raw pixel-level CSVs to temporary files and processes
    them with chunked reads to keep memory usage low, then uploads the
    summary JSON to S3.

    Routed to the ``merge`` queue (higher memory limit) via
    ``celery_app.conf.task_routes``.
    """
    from services import generate_match_quality_summary

    summary = generate_match_quality_summary(task_id, results_s3_uri)
    return {"task_id": task_id, "success": summary is not None}
