"""Celery application factory.

Configures Celery with Redis as broker/backend using settings from
:class:`config.Config`.  Import the ``celery_app`` instance from here
when defining tasks or when the worker process starts::

    from celery_app import celery_app
"""

import logging
import sys

import rollbar
from celery import Celery
from celery.signals import task_failure
from config import Config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Rollbar — initialise at module level so every worker process inherits it.
# Follows https://github.com/rollbar/rollbar-celery-example
# ---------------------------------------------------------------------------
_rollbar_kwargs = dict(
    access_token=Config.ROLLBAR_ACCESS_TOKEN,
    environment=Config.ROLLBAR_ENVIRONMENT,
    root=__name__,
    allow_logging_basic_config=False,
)
if Config.GIT_REVISION:
    _rollbar_kwargs["code_version"] = Config.GIT_REVISION

if Config.ROLLBAR_ACCESS_TOKEN:
    rollbar.init(**_rollbar_kwargs)

    def _celery_base_data_hook(request, data):
        data["framework"] = "celery"

    rollbar.BASE_DATA_HOOK = _celery_base_data_hook
    logger.info(
        "Rollbar initialized for Celery (environment=%s)", Config.ROLLBAR_ENVIRONMENT
    )
else:
    logger.warning("ROLLBAR_ACCESS_TOKEN not set — Celery error tracking disabled")

celery_app = Celery(
    "avoided_emissions",
    broker=Config.CELERY_BROKER_URL,
    backend=Config.CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    # Serialisation
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    # Timezone
    timezone="UTC",
    enable_utc=True,
    # Reliability
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    # Result expiry (24 h)
    result_expires=86400,
    # Autodiscover tasks in the 'tasks' module
    imports=["tasks"],
    # Route CPU/IO-heavy tasks to the heavy queue (higher memory limit)
    # so they never starve the lightweight polling tasks on the default queue.
    task_routes={
        "tasks.run_cog_merge": {"queue": "heavy"},
        "tasks.rasterize_vectors": {"queue": "heavy"},
        "tasks.import_vector_data": {"queue": "heavy"},
        "tasks.import_user_site_upload": {"queue": "heavy"},
        "tasks.ingest_sdg_cog": {"queue": "heavy"},
        "tasks.generate_match_quality_summary": {"queue": "heavy"},
        # submit_analysis_task_worker has its own dedicated queue so that
        # long-running site uploads on the heavy worker cannot delay or
        # starve pending analysis submissions.
        "tasks.submit_analysis_task_worker": {"queue": "submission"},
        # export_reference_layers streams PostGIS → GeoParquet → S3 and may
        # use several hundred MB for large admin-boundary tables.
        "tasks.export_reference_layers": {"queue": "heavy"},
    },
)

# ---------------------------------------------------------------------------
# Celery Beat schedule — periodic background jobs
# ---------------------------------------------------------------------------
celery_app.conf.beat_schedule = {
    "poll-gee-export-status": {
        "task": "tasks.poll_gee_exports",
        "schedule": 60.0,  # every 60 seconds
    },
    "poll-batch-task-status": {
        "task": "tasks.poll_batch_tasks",
        "schedule": 30.0,  # every 30 seconds
    },
    "auto-merge-unmerged": {
        "task": "tasks.auto_merge_unmerged",
        "schedule": 120.0,  # every 2 minutes
    },
    # Re-export reference layers monthly so that any re-imports (e.g. after a
    # geoboundaries update) are reflected in S3 without manual intervention.
    # Layers are also exported immediately after each vector import, so a
    # monthly beat is sufficient when they are already present on S3.
    "export-reference-layers": {
        "task": "tasks.export_reference_layers",
        "schedule": 2592000.0,  # every 30 days
    },
    # Expire tasks stuck in 'submitting' due to worker death (e.g. rolling
    # deploy or OOM kill).  Threshold is 40 min — see task docstring.
    "expire-stale-submitting-tasks": {
        "task": "tasks.expire_stale_submitting_tasks",
        "schedule": 300.0,  # every 5 minutes
    },
}


# ---------------------------------------------------------------------------
# Rollbar integration — report task failures from worker processes.
# Follows https://github.com/rollbar/rollbar-celery-example
# ---------------------------------------------------------------------------
@task_failure.connect
def handle_task_failure(sender=None, task_id=None, exception=None, einfo=None, **kw):
    """Send every unhandled task exception to Rollbar.

    Uses ``sys.exc_info()`` when available (i.e. inside the failing
    worker process) and falls back to the exception/einfo provided by
    the signal for maximum reliability.

    Also marks ``submitting`` analysis tasks as ``failed`` when the
    ``submit_analysis_task_worker`` task fails so they are never left
    stuck in the ``submitting`` state (e.g. after an OOM SIGKILL).
    """
    if Config.ROLLBAR_ACCESS_TOKEN:
        exc_info = sys.exc_info()
        # If sys.exc_info() returns (None, None, None) we are outside the
        # original exception context — reconstruct from signal kwargs.
        if exc_info[0] is None and exception is not None:
            exc_info = (type(exception), exception, getattr(einfo, "tb", None))
        extra = {
            "task_name": sender.name if sender else kw.get("sender"),
            "task_id": task_id,
        }
        rollbar.report_exc_info(exc_info, extra_data=extra)

    # When submit_analysis_task_worker fails after all retries are exhausted
    # (or is killed by the OOM killer), mark the analysis task as failed so
    # it is not left stuck in 'submitting'.  The kwargs passed to the signal
    # contain the Celery task args.
    task_name = sender.name if sender else None
    if task_name == "tasks.submit_analysis_task_worker":
        try:
            args = kw.get("args", ())
            analysis_task_id = args[0] if args else None
            if analysis_task_id:
                from models import AnalysisTask, get_db

                db = get_db()
                try:
                    record = (
                        db.query(AnalysisTask)
                        .filter(
                            AnalysisTask.id == analysis_task_id,
                            AnalysisTask.status == "submitting",
                        )
                        .first()
                    )
                    if record:
                        record.status = "failed"
                        record.error_message = (
                            f"Worker killed before submission completed "
                            f"({type(exception).__name__}: {exception})"
                        )
                        db.commit()
                        logger.error(
                            "submit_analysis_task_worker: marked task %s as "
                            "failed after terminal worker failure",
                            analysis_task_id,
                        )
                except Exception as db_exc:
                    db.rollback()
                    logger.error(
                        "handle_task_failure: could not mark task %s failed: %s",
                        analysis_task_id,
                        db_exc,
                    )
                finally:
                    db.close()
        except Exception as signal_exc:
            logger.error(
                "handle_task_failure cleanup raised: %s", signal_exc, exc_info=True
            )
