"""Integration test fixtures.

Integration tests require a running PostgreSQL + PostGIS instance.

Run locally (from the repo root):
    pytest tests/integration/ -v -m integration
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
    db_session.rollback()  # clear any aborted transaction from a failed test
    db_session.execute(
        text(
            "TRUNCATE users, task_results, task_results_total, analysis_tasks RESTART IDENTITY CASCADE"
        )
    )
    db_session.commit()
