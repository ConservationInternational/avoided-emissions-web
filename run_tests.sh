#!/usr/bin/env bash
# run_tests.sh — run the webapp test suite inside the Docker Compose webapp container.
#
# Usage:
#   ./run_tests.sh                         # run all unit tests
#   ./run_tests.sh tests/unit/test_auth.py # run a single file
#   ./run_tests.sh -m integration          # run integration tests (need DB)
#   ./run_tests.sh -v --tb=long            # pass extra pytest flags
#
# Requirements:
#   - Docker and Docker Compose must be available on the host.
#   - The webapp service does NOT need to be running; this script uses
#     `docker compose run` to start a fresh one-shot container.
#
# The --entrypoint override bypasses entrypoint.sh (which waits for Postgres
# and runs Alembic) so unit tests run immediately without any live services.

set -euo pipefail

COMPOSE_FILE="deploy/docker-compose.develop.yml"
SERVICE="webapp"

# Build the image if it is not already present (fast no-op when up to date).
docker compose -f "$COMPOSE_FILE" build --quiet "$SERVICE"

# Run pytest in a fresh, disposable container.  --entrypoint bash bypasses
# entrypoint.sh; --user root allows pip to write to the system site-packages.
# All positional args and flags are forwarded to pytest.
# Default: run unit tests only.  Pass -m integration to run integration tests.
if [ $# -eq 0 ]; then
    docker compose -f "$COMPOSE_FILE" run --rm \
        --entrypoint bash \
        --user root \
        "$SERVICE" \
        -c "pip install -q -r requirements-dev.txt && python -m pytest tests/unit/ -v"
else
    docker compose -f "$COMPOSE_FILE" run --rm \
        --entrypoint bash \
        --user root \
        "$SERVICE" \
        -c "pip install -q -r requirements-dev.txt && python -m pytest $*"
fi
