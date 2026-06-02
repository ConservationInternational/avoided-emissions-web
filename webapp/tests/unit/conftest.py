"""Shared fixtures for unit tests."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

FIXTURE_DIR = Path(__file__).parent.parent / "fixtures"

# Stable IDs reused across tests for predictable assertions.
SAMPLE_TASK_ID = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
SAMPLE_USER_ID = "11223344-5566-7788-99aa-bbccddeeff00"


@pytest.fixture
def sample_task_id():
    return SAMPLE_TASK_ID


@pytest.fixture
def sample_user_id():
    return SAMPLE_USER_ID


@pytest.fixture
def sample_results_payload():
    with open(FIXTURE_DIR / "results_payload.json") as f:
        return json.load(f)


@pytest.fixture
def mock_db():
    """A MagicMock that mimics a SQLAlchemy Session.

    Query chains return consistent MagicMock objects so callers can add
    ``return_value`` overrides without the mock hierarchy collapsing.
    """
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = None
    db.query.return_value.filter.return_value.all.return_value = []
    db.query.return_value.filter.return_value.delete.return_value = 0
    db.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
    db.execute.return_value.fetchall.return_value = []
    return db
