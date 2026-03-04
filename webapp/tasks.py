"""Celery tasks for background processing.

All long-running or I/O-heavy work is defined here and executed by the
Celery worker process.  The web application dispatches work by calling
``task.delay(…)`` or ``task.apply_async(…)``.
"""

import logging

import boto3

from celery_app import celery_app
from config import report_exception, report_message

logger = logging.getLogger(__name__)


@celery_app.task(name="tasks.import_vector_data", bind=True, max_retries=2)
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


@celery_app.task(name="tasks.rasterize_vectors", bind=True, max_retries=1)
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
    from rasterize_vectors import VECTOR_LAYERS, rasterize_and_upload

    bucket = Config.S3_BUCKET
    prefix = f"{Config.S3_PREFIX}/cog".strip("/")

    # Build set of layer names whose COG already exists on S3.
    existing_on_s3: set[str] = set()
    if bucket:
        try:
            from botocore.exceptions import ClientError

            s3 = boto3.client("s3", region_name=Config.AWS_REGION)
            for layer_def in VECTOR_LAYERS:
                name = layer_def["output_name"]
                key = f"{prefix}/{name}.tif"
                try:
                    s3.head_object(Bucket=bucket, Key=key)
                    existing_on_s3.add(name)
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

            # Skip layers that already have a COG on S3 and a
            # corresponding "merged" Covariate record in the DB.
            if name in existing_on_s3:
                existing = (
                    db.query(Covariate)
                    .filter(
                        Covariate.covariate_name == name,
                        Covariate.status == "merged",
                    )
                    .first()
                )
                if existing:
                    logger.info(
                        "Skipping %s — COG already exists on S3 (%s)",
                        name,
                        existing.merged_url,
                    )
                    results[name] = {"cog_url": existing.merged_url, "skipped": True}
                    continue

            logger.info("Rasterizing vector layer: %s", name)

            # Create (or update) a Covariate record so the layer shows
            # up in the dashboard alongside GEE-exported covariates.
            existing = (
                db.query(Covariate)
                .filter(Covariate.covariate_name == name)
                .order_by(Covariate.started_at.desc())
                .first()
            )
            if existing:
                layer = existing
            else:
                layer = Covariate(
                    covariate_name=name,
                    output_bucket=Config.S3_BUCKET,
                    output_prefix=f"{Config.S3_PREFIX}/cog",
                    started_at=datetime.now(timezone.utc),
                )
                db.add(layer)
                db.flush()

            layer.status = "rasterizing"
            db.commit()

            try:
                result = rasterize_and_upload(layer_def)
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
                results[name] = result
                logger.info("Rasterized %s -> %s", name, result["cog_url"])
            except Exception as exc:
                logger.exception("Failed to rasterize %s", name)
                layer.status = "failed"
                layer.error_message = str(exc)[:2000]
                layer.completed_at = datetime.now(timezone.utc)
                db.commit()
                results[name] = {"error": str(exc)[:500]}

        return {"status": "complete", "layers": results}

    except Exception as exc:
        logger.exception("Vector rasterization failed")
        report_exception()
        db.rollback()
        raise self.retry(exc=exc, countdown=120)
    finally:
        db.close()


class _MergeSuperseded(Exception):
    """Raised when a merge is aborted because the covariate was re-exported."""


@celery_app.task(name="tasks.run_cog_merge", bind=True, max_retries=1)
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

        result = merge_covariate_tiles(
            covariate_name=layer.covariate_name,
            source_bucket=source_bucket,
            source_prefix=source_prefix,
            output_bucket=layer.output_bucket,
            output_prefix=layer.output_prefix or f"{Config.S3_PREFIX}/cog",
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
                        export.output_prefix = f"{Config.S3_PREFIX}/cog"
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

    Returns
    -------
    dict
        ``{"scanned": N, "dispatched": N, "discovered": N}``
    """
    import importlib.util
    import os
    from datetime import datetime, timedelta, timezone

    from config import Config
    from models import Covariate, GeeExportMetadata, get_db

    if not Config.GCS_BUCKET:
        return {"scanned": 0, "dispatched": 0, "discovered": 0}

    # Load covariate names from GEE export config
    gee_config_path = os.path.join(os.path.dirname(__file__), "gee-export", "config.py")
    if not os.path.exists(gee_config_path):
        logger.warning("GEE config not found at %s", gee_config_path)
        return {"scanned": 0, "dispatched": 0, "discovered": 0}

    spec = importlib.util.spec_from_file_location("gee_export_config", gee_config_path)
    gee_config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gee_config)
    known_covariates = list(gee_config.COVARIATES.keys())

    # Scan GCS for tile details (ETags, sizes, md5 hashes)
    from cog_merge import (
        compute_tile_etag_hash,
        list_s3_cog_objects,
        scan_gcs_tile_details,
    )

    try:
        gcs_details = scan_gcs_tile_details(
            Config.GCS_BUCKET,
            Config.GCS_PREFIX,
            known_covariates,
        )
    except Exception:
        logger.exception("Failed to scan GCS tiles for auto-merge")
        report_exception()
        return {"scanned": 0, "dispatched": 0, "discovered": 0}

    with_tiles = {name for name, tiles in gcs_details.items() if tiles}
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
        s3_cog_map: dict[str, dict] = {}
        try:
            if Config.S3_BUCKET:
                cog_prefix = f"{Config.S3_PREFIX}/cog"
                for obj in list_s3_cog_objects(
                    Config.S3_BUCKET, cog_prefix, Config.AWS_REGION
                ):
                    s3_cog_map[obj["covariate"]] = obj
        except Exception:
            logger.warning("Failed to scan S3 for existing COGs")
            report_exception()

        existing_covariate_names = {
            row.covariate_name
            for row in db.query(Covariate.covariate_name)
            .filter(Covariate.status.in_(["pending_merge", "merging", "merged"]))
            .all()
        }

        for cov_name, s3_obj in s3_cog_map.items():
            if cov_name in existing_covariate_names:
                continue

            # Create a Covariate record so the UI shows the COG
            layer = Covariate(
                covariate_name=cov_name,
                status="merged",
                gcs_bucket=Config.GCS_BUCKET,
                gcs_prefix=Config.GCS_PREFIX,
                output_bucket=Config.S3_BUCKET,
                output_prefix=f"{Config.S3_PREFIX}/cog",
                merged_url=s3_obj["url"],
                size_bytes=s3_obj["size"],
                completed_at=now,
            )
            db.add(layer)
            db.flush()
            existing_covariate_names.add(cov_name)

            # Create an export metadata snapshot for the discovered COG
            tiles = gcs_details.get(cov_name, [])
            tile_hash = compute_tile_etag_hash(tiles) if tiles else None
            meta = GeeExportMetadata(
                covariate_id=layer.id,
                covariate_name=cov_name,
                gcs_bucket=Config.GCS_BUCKET,
                gcs_prefix=Config.GCS_PREFIX,
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

        # ---- Determine which covariates need a (re-)merge ----
        # Compare the current tile fingerprint against the most recent
        # successfully merged snapshot.
        from sqlalchemy import func

        latest_hashes: dict[str, str] = {}
        subq = (
            db.query(
                GeeExportMetadata.covariate_name,
                func.max(GeeExportMetadata.created_at).label("max_created"),
            )
            .filter(
                GeeExportMetadata.status.in_(["merged", "skipped_existing"]),
                GeeExportMetadata.tile_etag_hash.isnot(None),
            )
            .group_by(GeeExportMetadata.covariate_name)
            .subquery()
        )
        for row in (
            db.query(GeeExportMetadata)
            .join(
                subq,
                (GeeExportMetadata.covariate_name == subq.c.covariate_name)
                & (GeeExportMetadata.created_at == subq.c.max_created),
            )
            .all()
        ):
            if row.tile_etag_hash:
                latest_hashes[row.covariate_name] = row.tile_etag_hash

        # Reset covariates stuck in "merging" for more than 15 minutes.
        # This happens when a worker is killed mid-merge (e.g. during a
        # Docker Swarm rolling update) and the task message is lost.
        stale_cutoff = now - timedelta(minutes=15)
        stale_merging = (
            db.query(Covariate)
            .filter(
                Covariate.status.in_(["pending_merge", "merging"]),
                Covariate.started_at < stale_cutoff,
            )
            .all()
        )
        for stale in stale_merging:
            logger.warning(
                "Resetting stale covariate %s (%s) from '%s' to 'failed' "
                "(stuck since %s)",
                stale.covariate_name,
                stale.id,
                stale.status,
                stale.started_at,
            )
            stale.status = "failed"
            stale.error_message = (
                "Reset by auto_merge: stuck in merge for >15 min "
                "(likely killed during deploy)"
            )
            stale.completed_at = now
            # Also mark any associated metadata snapshots as failed
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

        # Skip covariates with an in-progress merge (fresh ones only)
        in_progress = {
            row.covariate_name
            for row in db.query(Covariate.covariate_name)
            .filter(Covariate.status.in_(["pending_merge", "merging"]))
            .all()
        }

        need_merge: list[str] = []
        for name in sorted(with_tiles):
            if name in in_progress:
                continue
            current_hash = compute_tile_etag_hash(gcs_details[name])
            if latest_hashes.get(name) == current_hash:
                continue  # tiles unchanged since last merge
            need_merge.append(name)

        if not need_merge:
            db.commit()
            return {
                "scanned": len(known_covariates),
                "dispatched": 0,
                "discovered": discovered,
            }

        for name in need_merge:
            tiles = gcs_details[name]
            tile_hash = compute_tile_etag_hash(tiles)

            # Create or update Covariate record
            existing = (
                db.query(Covariate)
                .filter(
                    Covariate.covariate_name == name,
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
                existing.output_prefix = f"{Config.S3_PREFIX}/cog"
                cov_id = existing.id
                dispatched_ids.append(str(existing.id))
            else:
                layer = Covariate(
                    covariate_name=name,
                    status="pending_merge",
                    gcs_bucket=Config.GCS_BUCKET,
                    gcs_prefix=Config.GCS_PREFIX,
                    output_bucket=Config.S3_BUCKET,
                    output_prefix=f"{Config.S3_PREFIX}/cog",
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
                gcs_prefix=Config.GCS_PREFIX,
                tile_count=len(tiles),
                tile_total_bytes=sum(t["size_bytes"] for t in tiles),
                tile_details=tiles,
                tile_etag_hash=tile_hash,
                tiles_detected_at=now,
                status="pending_merge",
                created_at=now,
            )
            db.add(meta)

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

    return {
        "scanned": len(known_covariates),
        "dispatched": len(dispatched_ids),
        "discovered": discovered,
    }


@celery_app.task(name="tasks.poll_batch_tasks")
def poll_batch_tasks() -> dict:
    """Poll for active analysis task statuses and update the DB.

    Checks API-routed tasks (extract_job_id starts with ``api:``)
    by querying the trends.earth API for execution status.  Called
    periodically by Celery Beat (every 30 s).

    Returns
    -------
    dict
        ``{"checked": N, "updated": N}``
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
        if not active:
            return {"checked": 0, "updated": 0}

        now = datetime.now(timezone.utc)
        updated = 0

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

                        if api_status == "FINISHED":
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
                            task.id,
                            exc,
                        )
                        report_exception(task_id=str(task.id))

        db.commit()
        return {"checked": len(active), "updated": updated}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
