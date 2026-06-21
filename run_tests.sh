#!/usr/bin/env bash
# run_tests.sh — run the webapp test suite locally using the host Python.
#
# Usage:
#   ./run_tests.sh                                  # run all unit tests
#   ./run_tests.sh tests/unit/test_auth.py          # run a single file
#   ./run_tests.sh -m integration                   # run integration tests (need DB)
#   ./run_tests.sh -v --tb=long                     # pass extra pytest flags
#
# Requirements:
#   - Python 3.13 with webapp/requirements.txt and webapp/requirements-dev.txt
#     installed (pip install -r webapp/requirements.txt -r webapp/requirements-dev.txt).
#   - For integration tests (-m integration), a PostgreSQL + PostGIS instance
#     must be reachable (e.g. start with:
#       docker compose -f deploy/docker-compose.develop.yml up postgres).
#
# pytest is configured via pytest.ini at the repo root, which adds webapp/ to
# sys.path so all app imports resolve without any PYTHONPATH manipulation.

set -euo pipefail

# Default: run unit tests only.  Pass -m integration to run integration tests.
if [ $# -eq 0 ]; then
    python -m pytest tests/unit/ -v
else
    python -m pytest "$@"
fi
