"""Integration tests for the task processing flow.

These tests verify import_execution_results and the poll_batch_tasks Celery
task using a real PostgreSQL + PostGIS database.

Run inside the webapp Docker container:
    python -m pytest tests/integration/test_processing_flow.py -v -m integration
"""

import uuid

import pytest


pytestmark = pytest.mark.integration


class TestProcessingFlowIntegration:
    """End-to-end: import_execution_results writes real DB rows."""

    def _insert_stub_task(self, db, task_id, user_id=None):
        """Insert a minimal AnalysisTask row so foreign keys are satisfied."""
        from models import AnalysisTask, User

        if user_id is None:
            user = User(
                email=f"stub-{task_id}@test.example",
                password_hash="placeholder",
                name="Stub User",
                role="user",
                is_approved=True,
            )
            db.add(user)
            db.flush()
            user_id = str(user.id)

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
