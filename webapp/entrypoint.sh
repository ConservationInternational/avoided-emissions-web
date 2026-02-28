#!/bin/sh
# Run Alembic migrations, then exec the container CMD.
# If DATABASE_URL is not set, skip migrations (allows running the
# container for one-off commands that don't need a database).

set -e

if [ -n "$DATABASE_URL" ]; then
    echo "Running database migrations..."
    alembic upgrade head
    echo "Migrations complete."
fi

exec "$@"
