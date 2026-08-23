"""Unit tests for queue_analysis_task (services/analysis_task.py).

All external I/O is mocked:
  - services.analysis_task.get_db          → MagicMock session
  - services.analysis_task.get_ready_covariate_names → list of names
  - credential_store.get_decrypted_secret  → (client_id, client_secret)
  - tasks.submit_analysis_task_worker      → MagicMock Celery task

No database connection, Redis broker, or trends.earth API call is made.
"""

import uuid
from unittest.mock import MagicMock

import pytest
from services.analysis_task import queue_analysis_task

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_KWARGS = {
    "task_name": "Unit Test Task",
    "description": "Created by unit tests",
    "site_set_id": str(uuid.uuid4()),
    "covariates": ["elev", "precip"],
    "exact_match_vars": ["admin0"],
}


def _setup_happy_path(mocker, creds=("cid", "csecret")):
    """Patch all external dependencies so queue_analysis_task succeeds."""
    mock_db = MagicMock()
    mocker.patch("services.analysis_task.get_db", return_value=mock_db)
    mocker.patch(
        "services.analysis_task.get_ready_covariate_names",
        return_value=["elev", "precip", "temp"],
    )
    mocker.patch("credential_store.get_decrypted_secret", return_value=creds)
    mock_submit = mocker.patch("tasks.submit_analysis_task_worker")
    return mock_db, mock_submit


# ---------------------------------------------------------------------------
# Happy-path tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestQueueAnalysisTaskSuccess:
    def test_returns_valid_uuid_string(self, mocker):
        _setup_happy_path(mocker)
        task_id = queue_analysis_task(user_id=str(uuid.uuid4()), **_BASE_KWARGS)
        # raises ValueError if not a valid UUID
        uuid.UUID(task_id)

    def test_creates_analysis_task_in_db(self, mocker):
        from models import AnalysisTask

        mock_db, _ = _setup_happy_path(mocker)
        queue_analysis_task(user_id=str(uuid.uuid4()), **_BASE_KWARGS)

        mock_db.add.assert_called_once()
        added = mock_db.add.call_args.args[0]
        assert isinstance(added, AnalysisTask)

    def test_task_created_with_submitting_status(self, mocker):
        mock_db, _ = _setup_happy_path(mocker)
        queue_analysis_task(user_id=str(uuid.uuid4()), **_BASE_KWARGS)

        added = mock_db.add.call_args.args[0]
        assert added.status == "submitting"

    def test_task_config_stores_exact_match_vars(self, mocker):
        mock_db, _ = _setup_happy_path(mocker)
        queue_analysis_task(user_id=str(uuid.uuid4()), **_BASE_KWARGS)

        added = mock_db.add.call_args.args[0]
        assert added.config["exact_match_vars"] == ["admin0"]

    def test_task_config_stores_n_replicates(self, mocker):
        mock_db, _ = _setup_happy_path(mocker)
        queue_analysis_task(user_id=str(uuid.uuid4()), n_replicates=5, **_BASE_KWARGS)

        added = mock_db.add.call_args.args[0]
        assert added.config["n_replicates"] == 5

    def test_dispatches_celery_task_with_correct_args(self, mocker):
        _mock_db, mock_submit = _setup_happy_path(mocker)
        user_id = str(uuid.uuid4())
        task_id = queue_analysis_task(user_id=user_id, **_BASE_KWARGS)

        mock_submit.delay.assert_called_once_with(task_id, user_id)

    def test_commits_db_before_celery_dispatch(self, mocker):
        """DB commit must succeed before Celery task is enqueued."""
        mock_db, _ = _setup_happy_path(mocker)
        queue_analysis_task(user_id=str(uuid.uuid4()), **_BASE_KWARGS)

        mock_db.commit.assert_called_once()

    def test_closes_db_session(self, mocker):
        mock_db, _ = _setup_happy_path(mocker)
        queue_analysis_task(user_id=str(uuid.uuid4()), **_BASE_KWARGS)

        mock_db.close.assert_called_once()


# ---------------------------------------------------------------------------
# Validation error tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestQueueAnalysisTaskValidation:
    def test_raises_if_empty_exact_match_vars(self, mocker):
        _setup_happy_path(mocker)
        with pytest.raises(ValueError, match="exact match variable"):
            queue_analysis_task(
                user_id=str(uuid.uuid4()),
                task_name="Test",
                description="",
                site_set_id=str(uuid.uuid4()),
                covariates=[],
                exact_match_vars=[],
            )

    def test_raises_if_covariate_overlaps_with_exact_match(self, mocker):
        _setup_happy_path(mocker)
        with pytest.raises(ValueError, match="both covariates and exact matches"):
            queue_analysis_task(
                user_id=str(uuid.uuid4()),
                task_name="Test",
                description="",
                site_set_id=str(uuid.uuid4()),
                covariates=["elev"],
                exact_match_vars=["admin0", "elev"],  # "elev" in both
            )

    def test_raises_if_covariate_not_ready(self, mocker):
        mock_db = MagicMock()
        mocker.patch("services.analysis_task.get_db", return_value=mock_db)
        mocker.patch(
            "services.analysis_task.get_ready_covariate_names",
            return_value=["elev"],  # "precip" is not ready
        )
        mocker.patch("credential_store.get_decrypted_secret", return_value=("a", "b"))
        mocker.patch("tasks.submit_analysis_task_worker")

        with pytest.raises(ValueError, match="not fully processed"):
            queue_analysis_task(
                user_id=str(uuid.uuid4()),
                task_name="Test",
                description="",
                site_set_id=str(uuid.uuid4()),
                covariates=["elev", "precip"],
                exact_match_vars=["admin0"],
            )

    def test_raises_if_no_credentials(self, mocker):
        mock_db = MagicMock()
        mocker.patch("services.analysis_task.get_db", return_value=mock_db)
        mocker.patch(
            "services.analysis_task.get_ready_covariate_names",
            return_value=["elev"],
        )
        mocker.patch("credential_store.get_decrypted_secret", return_value=None)

        with pytest.raises(ValueError, match="trends.earth account"):
            queue_analysis_task(
                user_id=str(uuid.uuid4()),
                task_name="Test",
                description="",
                site_set_id=str(uuid.uuid4()),
                covariates=["elev"],
                exact_match_vars=["admin0"],
            )

    def test_raises_if_n_replicates_exceeds_maximum(self, mocker):
        _setup_happy_path(mocker)
        with pytest.raises(ValueError, match="n_replicates"):
            queue_analysis_task(
                user_id=str(uuid.uuid4()),
                n_replicates=9999,
                **_BASE_KWARGS,
            )

    def test_raises_if_n_replicates_below_minimum(self, mocker):
        _setup_happy_path(mocker)
        with pytest.raises(ValueError, match="n_replicates"):
            queue_analysis_task(
                user_id=str(uuid.uuid4()),
                n_replicates=0,
                **_BASE_KWARGS,
            )

    def test_raises_if_max_treatment_pixels_below_one(self, mocker):
        _setup_happy_path(mocker)
        with pytest.raises(ValueError, match="max_treatment_pixels"):
            queue_analysis_task(
                user_id=str(uuid.uuid4()),
                max_treatment_pixels=0,
                **_BASE_KWARGS,
            )

    def test_db_not_written_on_validation_failure(self, mocker):
        """Validation errors must be raised before any DB work is done."""
        mock_db, _ = _setup_happy_path(mocker)
        with pytest.raises(ValueError):
            queue_analysis_task(
                user_id=str(uuid.uuid4()),
                task_name="Test",
                description="",
                site_set_id=str(uuid.uuid4()),
                covariates=[],
                exact_match_vars=[],
            )
        mock_db.add.assert_not_called()
        mock_db.commit.assert_not_called()
