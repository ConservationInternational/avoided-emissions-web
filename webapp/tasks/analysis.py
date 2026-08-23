"""Celery tasks: Batch polling, analysis task submission, and match quality."""

import logging
from datetime import datetime, timedelta, timezone

from celery_app import celery_app
from config import report_exception, report_message

logger = logging.getLogger(__name__)


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
                            except Exception as results_exc:  # noqa: BLE001
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
                    except Exception as exc:  # noqa: BLE001
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
            from models import User
            from trendsearth_client import TrendsEarthClient

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
                                # Commit each adoption individually so that
                                # rows added by import_execution_results are
                                # flushed and committed before the next
                                # iteration's queries trigger an autoflush.
                                # Batching all adoptions in a single commit
                                # leaves rows as "pending" in the session,
                                # which causes Query-invoked autoflush to
                                # attempt duplicate INSERTs when the next
                                # adoption fires a SELECT.
                                db.commit()
                                adopted += 1
                            except Exception as adopt_exc:  # noqa: BLE001
                                db.rollback()
                                logger.warning(
                                    "Failed to adopt API execution %s: %s",
                                    eid,
                                    adopt_exc,
                                )
                                report_exception(extra_data={"exec_id": eid})

                if adopted:
                    logger.info("Discovery: adopted %d new API execution(s)", adopted)
        except Exception as disc_exc:  # noqa: BLE001
            logger.warning("API execution discovery failed: %s", disc_exc)
            db.rollback()

        return {"checked": len(active), "updated": updated, "adopted": adopted}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


@celery_app.task(name="tasks.expire_stale_submitting_tasks")
def expire_stale_submitting_tasks() -> dict:
    """Mark analysis tasks stuck in ``submitting`` as ``failed``.

    A task enters ``submitting`` when the user presses Submit and a Celery
    worker is dispatched to complete the slow submission work.  Normally the
    worker transitions the task to ``submitted`` (success) or ``failed``
    (error) within a few minutes.

    When a worker is killed mid-task (e.g. during a rolling deploy) and the
    Redis broker has already popped the message, the task is permanently lost
    and the DB record is left stuck in ``submitting``.  This periodic cleanup
    detects such records and marks them ``failed`` so users can resubmit.

    Two expiry thresholds are used:

    * **5 minutes + no active worker** — for tasks where the worker never set
      a ``worker_started_at`` heartbeat AND no other submission is currently
      running.  The submission worker writes the heartbeat within seconds of
      picking up the message; if it is absent after 5 min *and* no other
      worker is active, the Celery message was lost (e.g. worker killed during
      a rolling deploy).  If another submission *is* actively running, the
      task is legitimately queued behind it and must not be killed.
    * **70 minutes** — for tasks where a worker did start but never completed.
      Safely above the per-attempt ``soft_time_limit`` (20 min) ×
      ``max_retries + 1`` (3 attempts = 60 min total), so only genuinely
      orphaned tasks are touched.

    Returns
    -------
    dict
        ``{"expired": N}`` — number of tasks marked failed.
    """
    from models import AnalysisTask, get_db

    now = datetime.now(timezone.utc)
    lost_cutoff = now - timedelta(minutes=5)
    stuck_cutoff = now - timedelta(minutes=70)

    db = get_db()
    expired = 0
    try:
        # Load ALL currently-submitting tasks (not just old ones) so we can
        # determine whether the submission worker is actively processing one.
        all_submitting = (
            db.query(AnalysisTask).filter(AnalysisTask.status == "submitting").all()
        )

        # A worker that has started sets worker_started_at within seconds of
        # picking up the message.  If ANY task has this heartbeat, the
        # submission-worker is busy — tasks without a heartbeat are
        # legitimately queued, not lost.
        any_worker_active = any(
            (t.config or {}).get("worker_started_at") is not None
            for t in all_submitting
        )

        for task in all_submitting:
            if task.created_at >= lost_cutoff:
                # Too young to act on — still within normal dispatch latency.
                continue

            worker_started_at = (task.config or {}).get("worker_started_at")
            if worker_started_at is None:
                if any_worker_active:
                    # Another submission is actively running — this task is
                    # legitimately waiting in the submission queue.
                    continue
                # No worker is active and no heartbeat after 5 min:
                # Celery message was lost before pickup.
                error_msg = (
                    "Celery task message was lost before a worker could start "
                    "(likely killed during a rolling deploy). Please resubmit."
                )
            elif task.created_at < stuck_cutoff:
                # Worker started but never completed within the allowed time.
                error_msg = (
                    "Submission worker started but did not complete within the "
                    "expected time (soft_time_limit exceeded or worker OOM). "
                    "Please resubmit."
                )
            else:
                # Worker is still within its soft_time_limit — leave it alone.
                continue
            task.status = "failed"
            task.error_message = error_msg
            expired += 1
            logger.warning(
                "expire_stale_submitting_tasks: marked task %s as failed "
                "(stuck in submitting since %s, worker_started_at=%s)",
                task.id,
                task.created_at,
                worker_started_at,
            )
        if expired:
            db.commit()
            report_message(
                f"Expired {expired} stale submitting task(s)",
                level="warning",
                expired_count=expired,
            )
        return {"expired": expired}
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
    soft_time_limit=1200,  # 20 minutes
    time_limit=1320,
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
        logger.exception(
            "submit_analysis_task_worker: failed for task %s: %s",
            task_id,
            exc,  # noqa: TRY401
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
