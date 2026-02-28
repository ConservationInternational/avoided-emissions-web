"""Celery tasks for background processing.

All long-running or I/O-heavy work is defined here and executed by the
Celery worker process.  The web application dispatches work by calling
``task.delay(…)`` or ``task.apply_async(…)``.
"""

import logging

from celery_app import celery_app
from config import report_exception

logger = logging.getLogger(__name__)


@celery_app.task(name="tasks.run_cog_merge", bind=True, max_retries=1)
def run_cog_merge(self, layer_id: str) -> dict:
    """Merge GCS tiles into a single COG and upload to S3.

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

    from cog_merge import merge_covariate_tiles
    from config import Config
    from models import Covariate, get_db

    db = get_db()
    try:
        layer = db.query(Covariate).filter(Covariate.id == layer_id).first()
        if not layer:
            logger.error("Covariate %s not found", layer_id)
            return {"status": "failed", "error": "record not found"}

        # Transition to 'merging'
        layer.status = "merging"
        db.commit()

        result = merge_covariate_tiles(
            covariate_name=layer.covariate_name,
            source_bucket=layer.gcs_bucket or Config.GCS_BUCKET,
            source_prefix=layer.gcs_prefix or Config.GCS_PREFIX,
            output_bucket=layer.output_bucket,
            output_prefix=layer.output_prefix or f"{Config.S3_PREFIX}/cog",
            aws_region=Config.AWS_REGION,
        )

        layer.status = "merged"
        layer.merged_url = result["url"]
        layer.size_bytes = result["size_bytes"]
        layer.n_tiles = result["n_tiles"]
        layer.completed_at = datetime.now(timezone.utc)
        db.commit()
        logger.info("COG merge completed for '%s'", layer.covariate_name)
        return {
            "status": "merged",
            "url": result["url"],
            "size_bytes": result["size_bytes"],
        }

    except Exception as exc:
        logger.exception("COG merge failed for layer %s", layer_id)
        report_exception(layer_id=layer_id)
        try:
            layer = db.query(Covariate).filter(Covariate.id == layer_id).first()
            if layer:
                layer.status = "failed"
                layer.error_message = str(exc)[:2000]
                layer.completed_at = datetime.now(timezone.utc)
                db.commit()
        except Exception:
            db.rollback()
        return {"status": "failed", "error": str(exc)[:500]}
    finally:
        db.close()


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
    from models import Covariate, get_db

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
                gee_state = metadata.get(
                    "state", op.get("done") and "SUCCEEDED"
                )
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
                        meta = dict(export.extra_metadata or {})
                        meta["tile_urls"] = tile_urls
                        export.extra_metadata = meta

                        # Auto-trigger COG merge now that tiles are ready
                        export.status = "pending_merge"
                        export.output_bucket = Config.S3_BUCKET
                        export.output_prefix = f"{Config.S3_PREFIX}/cog"
                        _auto_merge_ids.append(str(export.id))

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
    """Find covariates with GCS tiles but no merge, and dispatch merges.

    Scans GCS for tiles, checks which covariates are already merged or
    in progress, and auto-dispatches :func:`run_cog_merge` for any
    covariates that have tiles but haven't been merged yet.

    This covers covariates exported outside the app (e.g. manually via
    GEE), or tiles that were already on GCS before the app was set up.

    Called periodically by Celery Beat.

    Returns
    -------
    dict
        ``{"scanned": N, "dispatched": N}``
    """
    import importlib.util
    from datetime import datetime, timezone

    from config import Config
    from models import Covariate, get_db

    if not Config.GCS_BUCKET:
        return {"scanned": 0, "dispatched": 0}

    # Load covariate names from GEE export config
    import os

    gee_config_path = os.path.join(
        os.path.dirname(__file__), "gee-export", "config.py"
    )
    if not os.path.exists(gee_config_path):
        logger.warning("GEE config not found at %s", gee_config_path)
        return {"scanned": 0, "dispatched": 0}

    spec = importlib.util.spec_from_file_location(
        "gee_export_config", gee_config_path
    )
    gee_config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gee_config)
    known_covariates = list(gee_config.COVARIATES.keys())

    # Scan GCS for tile counts
    from cog_merge import list_all_gcs_tiles

    try:
        gcs_counts = list_all_gcs_tiles(
            Config.GCS_BUCKET,
            Config.GCS_PREFIX,
            known_covariates,
        )
    except Exception:
        logger.exception("Failed to scan GCS tiles for auto-merge")
        report_exception()
        return {"scanned": 0, "dispatched": 0}

    # Covariates that have tiles on GCS
    with_tiles = {name for name, count in gcs_counts.items() if count > 0}
    if not with_tiles:
        return {"scanned": len(known_covariates), "dispatched": 0}

    # Check DB for covariates already merged or in progress
    db = get_db()
    dispatched_ids: list[str] = []
    try:
        skip_statuses = ["pending_merge", "merging", "merged"]
        already_handled = {
            row.covariate_name
            for row in db.query(Covariate.covariate_name)
            .filter(Covariate.status.in_(skip_statuses))
            .all()
        }

        # Also skip covariates that already have a COG on S3
        from cog_merge import list_s3_cog_objects

        try:
            if Config.S3_BUCKET:
                cog_prefix = f"{Config.S3_PREFIX}/cog"
                for obj in list_s3_cog_objects(
                    Config.S3_BUCKET, cog_prefix, Config.AWS_REGION
                ):
                    already_handled.add(obj["covariate"])
        except Exception:
            logger.warning("Failed to scan S3 for existing COGs")
            report_exception()

        need_merge = with_tiles - already_handled
        if not need_merge:
            return {"scanned": len(known_covariates), "dispatched": 0}

        for name in sorted(need_merge):
            # Check if there's an existing exported row to update
            existing = (
                db.query(Covariate)
                .filter(
                    Covariate.covariate_name == name,
                    Covariate.status == "exported",
                )
                .order_by(Covariate.started_at.desc())
                .first()
            )
            if existing:
                existing.status = "pending_merge"
                existing.output_bucket = Config.S3_BUCKET
                existing.output_prefix = f"{Config.S3_PREFIX}/cog"
                dispatched_ids.append(str(existing.id))
            else:
                # Create a new record for pre-existing GCS tiles
                layer = Covariate(
                    covariate_name=name,
                    status="pending_merge",
                    gcs_bucket=Config.GCS_BUCKET,
                    gcs_prefix=Config.GCS_PREFIX,
                    output_bucket=Config.S3_BUCKET,
                    output_prefix=f"{Config.S3_PREFIX}/cog",
                    started_at=datetime.now(timezone.utc),
                )
                db.add(layer)
                db.flush()
                dispatched_ids.append(str(layer.id))

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

    return {"scanned": len(known_covariates), "dispatched": len(dispatched_ids)}


@celery_app.task(name="tasks.poll_batch_tasks")
def poll_batch_tasks() -> dict:
    """Poll AWS Batch for active analysis task statuses and update the DB.

    Finds all ``AnalysisTask`` rows with status *submitted* or *running*
    and checks their Batch job states.  Called periodically by Celery
    Beat (every 30 s).

    Returns
    -------
    dict
        ``{"checked": N, "updated": N}``
    """
    import importlib
    import os
    import sys
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

        # Lazy-import the batch_jobs helper
        r_analysis_dir = os.path.join(
            os.path.dirname(__file__), "r-analysis"
        )
        if r_analysis_dir not in sys.path:
            sys.path.insert(0, r_analysis_dir)
        batch = importlib.import_module("batch_jobs")

        now = datetime.now(timezone.utc)
        updated = 0

        for task in active:
            try:
                old_status = task.status

                # Check the summarize job first (last step)
                if task.summarize_job_id:
                    status = batch.get_job_status(task.summarize_job_id)
                    if status["status"] == "SUCCEEDED":
                        task.status = "succeeded"
                        task.completed_at = now
                        updated += 1
                        continue
                    elif status["status"] == "FAILED":
                        task.status = "failed"
                        task.error_message = status.get(
                            "reason", "Summarize job failed"
                        )
                        task.completed_at = now
                        updated += 1
                        continue

                # Check matching job
                if task.match_job_id:
                    status = batch.get_job_status(task.match_job_id)
                    if status["status"] == "RUNNING":
                        task.status = "running"
                        if not task.started_at:
                            task.started_at = now
                    elif status["status"] == "FAILED":
                        task.status = "failed"
                        task.error_message = status.get(
                            "reason", "Match job failed"
                        )
                        task.completed_at = now

                # Check extract job
                if task.extract_job_id:
                    status = batch.get_job_status(task.extract_job_id)
                    if (
                        status["status"] == "RUNNING"
                        and task.status == "submitted"
                    ):
                        task.status = "running"
                        if not task.started_at:
                            task.started_at = now
                    elif status["status"] == "FAILED":
                        task.status = "failed"
                        task.error_message = status.get(
                            "reason", "Extract job failed"
                        )
                        task.completed_at = now

                if task.status != old_status:
                    updated += 1

            except Exception as exc:
                logger.warning(
                    "Failed to poll Batch status for task %s: %s",
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
