"""Celery tasks: COG merge, GEE export polling, and auto-merge."""

import logging
from datetime import datetime, timezone
from pathlib import Path

from celery_app import celery_app
from config import report_exception

from gee_export import gee_config
from tasks.reference_layers import _MergeSuperseded, ingest_sdg_cog_task

logger = logging.getLogger(__name__)


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
        except Exception:  # noqa: BLE001
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
        except Exception:  # noqa: BLE001
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
            except Exception:  # noqa: BLE001
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

            except Exception as exc:  # noqa: BLE001
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
        except Exception:  # noqa: BLE001, S110
            pass  # lock may have expired; safe to ignore


def _auto_merge_unmerged_inner() -> dict:
    """Actual logic for auto_merge_unmerged, called under a Redis lock."""
    from datetime import timedelta

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
    for res_m in resolutions:
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
        except Exception:  # noqa: BLE001
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
            _name, res_m = key
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
