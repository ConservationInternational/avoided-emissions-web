"""Celery application factory.

Configures Celery with Redis as broker/backend using settings from
:class:`config.Config`.  Import the ``celery_app`` instance from here
when defining tasks or when the worker process starts::

    from celery_app import celery_app
"""

import logging

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
    logger.info("Rollbar initialized for Celery (environment=%s)", Config.ROLLBAR_ENVIRONMENT)
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
    # Route CPU/IO-heavy merge tasks to a dedicated queue so they
    # never starve the lightweight polling tasks on the default queue.
    task_routes={
        "tasks.run_cog_merge": {"queue": "merge"},
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
# Rollbar integration — report task failures from worker processes
# ---------------------------------------------------------------------------
@task_failure.connect
def handle_task_failure(**kw):
    """Send every unhandled task exception to Rollbar."""
    if Config.ROLLBAR_ACCESS_TOKEN:
        rollbar.report_exc_info(extra_data=kw)
