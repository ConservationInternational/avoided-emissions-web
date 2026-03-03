#!/bin/sh
# Entrypoint for all containers built from the webapp image.
#
# Commands:
#   migrate  — run Alembic migrations, dispatch vector-data import, then exit.
#   celery   — start a Celery worker/beat (no migrations).
#   *        — start the webapp (gunicorn / dev server).

set -e

# Ensure celerybeat schedule directory exists (volume mounts may shadow it)
mkdir -p /app/celerybeat

case "$1" in
    migrate)
        echo "Running database migrations..."
        alembic upgrade head
        echo "Migrations complete."

        echo "Dispatching vector data import to background worker..."
        python -c "from tasks import import_vector_data_task; import_vector_data_task.delay(); print('Vector data import task queued.')"
        exit 0
        ;;
    celery)
        # Workers/beat skip migrations — the migrate service handles them.
        ;;
    *)
        # Webapp — just start serving.
        ;;
esac

exec "$@"
