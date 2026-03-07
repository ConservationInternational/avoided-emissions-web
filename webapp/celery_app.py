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
    # Route CPU/IO-heavy tasks to the merge queue (higher memory limit)
    # so they never starve the lightweight polling tasks on the default queue.
    task_routes={
        "tasks.run_cog_merge": {"queue": "merge"},
        "tasks.rasterize_vectors": {"queue": "merge"},
        "tasks.import_vector_data": {"queue": "merge"},
        "tasks.generate_match_quality_summary": {"queue": "merge"},
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
    """
    if not Config.ROLLBAR_ACCESS_TOKEN:
        return
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
