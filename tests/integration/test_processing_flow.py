"""Integration tests for the task processing flow.

These tests verify import_execution_results and the poll_batch_tasks Celery
task using a real PostgreSQL + PostGIS database.

Run locally (from the repo root):
    python -m pytest tests/integration/test_processing_flow.py -v -m integration
"""

import uuid

import pytest

pytestmark = pytest.mark.integration


class TestProcessingFlowIntegration:
    """End-to-end: import_execution_results writes real DB rows."""

    def _insert_stub_task(self, db, task_id, user_id=None):
        """Insert a minimal AnalysisTask row so foreign keys are satisfied."""
        from models import AnalysisTask

        if user_id is None:
            user_id = str(uuid.uuid4())

        task = AnalysisTask(
            id=task_id,
            name="Integration Test",
            description="",
            submitted_by=user_id,
            status="succeeded",
            config={},
            covariates=[],
        )
        db.add(task)
        db.commit()
        return task

    def test_task_results_written_to_db(self, clean_db, sample_results_payload):
        """import_execution_results must persist TaskResult rows."""
        from models import TaskResult
        from services.analysis_task import import_execution_results

        task_id = str(uuid.uuid4())
        self._insert_stub_task(clean_db, task_id)

        import_execution_results(task_id, sample_results_payload, db=clean_db)
        clean_db.commit()

        rows = clean_db.query(TaskResult).filter(TaskResult.task_id == task_id).all()
        assert len(rows) == 10  # 2 sites × 5 years

    def test_task_result_totals_written_to_db(self, clean_db, sample_results_payload):
        """import_execution_results must persist TaskResultTotal rows."""
        from models import TaskResultTotal
        from services.analysis_task import import_execution_results

        task_id = str(uuid.uuid4())
        self._insert_stub_task(clean_db, task_id)

        import_execution_results(task_id, sample_results_payload, db=clean_db)
        clean_db.commit()

        rows = (
            clean_db.query(TaskResultTotal)
            .filter(TaskResultTotal.task_id == task_id)
            .all()
        )
        assert len(rows) == 2

    def test_import_is_idempotent(self, clean_db, sample_results_payload):
        """Calling import_execution_results twice must not double the rows."""
        from models import TaskResult
        from services.analysis_task import import_execution_results

        task_id = str(uuid.uuid4())
        self._insert_stub_task(clean_db, task_id)

        import_execution_results(task_id, sample_results_payload, db=clean_db)
        clean_db.commit()
        import_execution_results(task_id, sample_results_payload, db=clean_db)
        clean_db.commit()

        rows = clean_db.query(TaskResult).filter(TaskResult.task_id == task_id).all()
        assert len(rows) == 10  # still 10, not 20

    def test_failed_import_does_not_undo_other_tasks(
        self, clean_db, sample_results_payload, mocker
    ):
        """A database error importing one task must not roll back another."""
        import copy

        from models import AnalysisTask, TaskResult
        from tasks.analysis import poll_batch_tasks

        ok_id, bad_id = uuid.uuid4(), uuid.uuid4()
        owner = self._insert_stub_task(clean_db, ok_id).submitted_by
        self._insert_stub_task(clean_db, bad_id, user_id=str(owner))
        for task_id in (ok_id, bad_id):
            task = clean_db.get(AnalysisTask, task_id)
            task.status = "running"
            task.extract_job_id = f"api:{task_id}"
        clean_db.commit()

        bad_payload = copy.deepcopy(sample_results_payload)
        bad_payload["records"][0]["entity_id"] = "x" * 200  # exceeds String(100)
        client = mocker.MagicMock()
        client.get_execution.return_value = {"data": {"status": "FINISHED"}}
        client.get_execution_results.side_effect = lambda exec_id: (
            sample_results_payload if exec_id == str(ok_id) else bad_payload
        )
        mocker.patch("trendsearth_client.TrendsEarthClient", return_value=client)
        mocker.patch("config.Config.TRENDSEARTH_CLIENT_ID", "id")
        mocker.patch("config.Config.TRENDSEARTH_CLIENT_SECRET", "secret")

        poll_batch_tasks()

        clean_db.expire_all()
        assert clean_db.get(AnalysisTask, ok_id).status == "succeeded"
        assert (
            clean_db.query(TaskResult).filter(TaskResult.task_id == ok_id).count() == 10
        )
        # The failed import is retried on the next poll.
        assert clean_db.get(AnalysisTask, bad_id).status == "running"
