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
#   - The webapp service must be running (or will be started implicitly by exec).
#     Start with:  docker compose -f deploy/docker-compose.develop.yml up -d
#
# The script installs dev dependencies on first run (adds pytest, pytest-mock,
# etc. into the already-running container without rebuilding the image).

set -euo pipefail

COMPOSE_FILE="deploy/docker-compose.develop.yml"
SERVICE="webapp"

# Install dev requirements inside the container if not already present.
# This is fast (pip no-ops if everything is installed) and avoids a rebuild.
docker compose -f "$COMPOSE_FILE" exec "$SERVICE" \
    pip install -q -r requirements-dev.txt

# Run pytest.  All positional args and flags are forwarded to pytest.
# Default: run unit tests only.  Pass -m integration to run integration tests.
if [ $# -eq 0 ]; then
    docker compose -f "$COMPOSE_FILE" exec "$SERVICE" \
        python -m pytest tests/unit/ -v
else
    docker compose -f "$COMPOSE_FILE" exec "$SERVICE" \
        python -m pytest "$@"
fi
