"""Integration tests for the task submission flow.

These tests verify the complete path from queue_analysis_task through to
the DB record using a real PostgreSQL + PostGIS database.

Run inside the webapp Docker container:
    python -m pytest tests/integration/test_submission_flow.py -v -m integration
"""

import uuid

import pytest


pytestmark = pytest.mark.integration


@pytest.mark.skip(reason="requires Docker PostgreSQL — run with: pytest -m integration")
class TestSubmissionFlowIntegration:
    """End-to-end: queue_analysis_task creates a real DB record."""

    def test_task_record_persisted_in_db(self, clean_db, mocker):
        """queue_analysis_task must write a row to the analysis_tasks table."""
        from models import AnalysisTask
        from services.analysis_task import queue_analysis_task

        mocker.patch(
            "services.analysis_task.get_ready_covariate_names",
            return_value=["elev", "precip"],
        )
        mocker.patch(
            "credential_store.get_decrypted_secret", return_value=("cid", "csecret")
        )
        mocker.patch("tasks.submit_analysis_task_worker")

        user_id = str(uuid.uuid4())
        task_id = queue_analysis_task(
            task_name="Integration Test Task",
            description="Automated integration test",
            user_id=user_id,
            site_set_id=None,
            covariates=["elev"],
            exact_match_vars=["admin0"],
        )

        row = clean_db.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
        assert row is not None
        assert row.status == "submitting"
        assert row.name == "Integration Test Task"

    def test_task_config_round_trips_through_db(self, clean_db, mocker):
        """Config dict written to JSONB must be readable back as a Python dict."""
        from models import AnalysisTask
        from services.analysis_task import queue_analysis_task

        mocker.patch(
            "services.analysis_task.get_ready_covariate_names",
            return_value=["elev"],
        )
        mocker.patch(
            "credential_store.get_decrypted_secret", return_value=("cid", "csecret")
        )
        mocker.patch("tasks.submit_analysis_task_worker")

        task_id = queue_analysis_task(
            task_name="Config Test",
            description="",
            user_id=str(uuid.uuid4()),
            site_set_id=None,
            covariates=["elev"],
            exact_match_vars=["admin0"],
            n_replicates=7,
        )

        row = clean_db.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
        assert isinstance(row.config, dict)
        assert row.config["n_replicates"] == 7
        assert row.config["exact_match_vars"] == ["admin0"]
