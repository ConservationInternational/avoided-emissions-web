"""Celery tasks for background processing.

All long-running or I/O-heavy work is defined here and executed by the
Celery worker process.  The web application dispatches work by calling
``task.delay(…)`` or ``task.apply_async(…)``.
"""

import logging

from celery_app import celery_app

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

        db.commit()
        return {"checked": len(active), "updated": updated}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


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
            os.path.dirname(__file__), "..", "r-analysis"
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

        db.commit()
        return {"checked": len(active), "updated": updated}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
