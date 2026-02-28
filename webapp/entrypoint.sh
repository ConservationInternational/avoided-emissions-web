#!/bin/sh
# Run Alembic migrations (webapp only), then exec the container CMD.
# Workers and beat schedulers skip migrations since only one process
# should run them.

set -e

# Only run migrations for the webapp process, not for celery workers/beat
case "$1" in
    celery)
        # Skip migrations for Celery processes
        ;;
    *)
        if [ -n "$DATABASE_URL" ]; then
            echo "Running database migrations..."
            alembic upgrade head
            echo "Migrations complete."

            echo "Dispatching vector data import to background worker..."
            python -c "from tasks import import_vector_data_task; import_vector_data_task.delay(); print('Vector data import task queued.')"
        fi
        ;;
esac

exec "$@"
