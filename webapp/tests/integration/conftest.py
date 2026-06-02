"""Integration test fixtures.

Integration tests require a running PostgreSQL + PostGIS instance and are
intended to run inside the webapp Docker container where all services are
available.

Run with:
    docker compose -f deploy/docker-compose.develop.yml exec webapp \
        python -m pytest tests/integration/ -v -m integration
"""

import pytest
from sqlalchemy import text

from models import get_db


@pytest.fixture(scope="session")
def db_session():
    """Yield a database session connected to the test database.

    Caller is responsible for closing the session.  Integration tests
    that need a fresh state should truncate relevant tables in a
    function-scoped fixture.
    """
    db = get_db()
    try:
        # Verify PostGIS is available
        db.execute(text("SELECT PostGIS_Version()"))
        yield db
    finally:
        db.close()


@pytest.fixture
def clean_db(db_session):
    """Truncate test-relevant tables before each integration test."""
    yield db_session
    db_session.execute(
        text(
            "TRUNCATE task_results, task_result_totals, analysis_tasks RESTART IDENTITY CASCADE"
        )
    )
    db_session.commit()
