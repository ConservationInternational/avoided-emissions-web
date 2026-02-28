"""Celery application factory.

Configures Celery with Redis as broker/backend using settings from
:class:`config.Config`.  Import the ``celery_app`` instance from here
when defining tasks or when the worker process starts::

    from celery_app import celery_app
"""

from celery import Celery

from config import Config

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
