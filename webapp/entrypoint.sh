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

# Wait for Postgres to be reachable.  In Docker Swarm the overlay network
# may not resolve service DNS immediately, and first-time data-directory
# initialisation on a bind mount can take several minutes.
wait_for_postgres() {
    echo "Waiting for Postgres to become reachable..."
    attempts=0
    max_attempts=90
    while [ $attempts -lt $max_attempts ]; do
        # Use Python+psycopg2 (already installed) for a lightweight probe.
        if python -c "
import os, sys
try:
    import psycopg2
    psycopg2.connect(os.environ['DATABASE_URL'], connect_timeout=2).close()
    sys.exit(0)
except Exception:
    sys.exit(1)
" 2>/dev/null; then
            echo "Postgres is ready."
            return 0
        fi
        attempts=$((attempts + 1))
        echo "  Postgres not ready (attempt $attempts/$max_attempts) — retrying in 2 s..."
        sleep 2
    done
    echo "ERROR: Postgres did not become reachable after $max_attempts attempts."
    return 1
}

case "$1" in
    migrate)
        wait_for_postgres
        echo "Running database migrations..."
        alembic upgrade head
        echo "Migrations complete."

        echo "Dispatching vector data import to background worker..."
        python -c "from tasks import import_vector_data_task; import_vector_data_task.delay(); print('Vector data import task queued.')"
        exit 0
        ;;
    celery)
        # Workers/beat skip migrations — the migrate service handles them.
        # But they still need Postgres to be reachable before starting.
        wait_for_postgres
        ;;
    *)
        # Webapp — just start serving.
        ;;
esac

exec "$@"
