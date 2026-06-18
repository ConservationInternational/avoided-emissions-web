"""Integration tests for the results viewing flow.

These tests verify get_task_detail and get_task_site_results using a real
PostgreSQL + PostGIS database with actual rows inserted.

Run inside the webapp Docker container:
    python -m pytest tests/integration/test_results_flow.py -v -m integration
"""

import uuid

import pytest


pytestmark = pytest.mark.integration


class TestResultsFlowIntegration:
    """End-to-end: get_task_detail and get_task_site_results read real DB rows."""

    def _seed_task_with_results(self, db, payload):
        """Insert a task and import results; return the task_id."""
        from models import AnalysisTask, User
        from services.analysis_task import import_execution_results

        task_id = str(uuid.uuid4())
        user = User(
            email=f"results-{task_id}@test.example",
            password_hash="placeholder",
            name="Results User",
            role="user",
            is_approved=True,
        )
        db.add(user)
        db.flush()
        task = AnalysisTask(
            id=task_id,
            name="Results Test",
            description="",
            submitted_by=str(user.id),
            status="succeeded",
            n_sites=2,
            config={},
            covariates=[],
        )
        db.add(task)
        db.commit()

        import_execution_results(task_id, payload, db=db)
        db.commit()
        return task_id

    def test_get_task_detail_returns_task(self, clean_db, sample_results_payload):
        from services.analysis_task import get_task_detail

        task_id = self._seed_task_with_results(clean_db, sample_results_payload)
        detail = get_task_detail(task_id)

        assert detail is not None
        assert str(detail["task"].id) == task_id

    def test_get_task_detail_loads_totals(self, clean_db, sample_results_payload):
        from services.analysis_task import get_task_detail

        task_id = self._seed_task_with_results(clean_db, sample_results_payload)
        detail = get_task_detail(task_id)

        assert len(detail["totals"]) == 2

    def test_get_task_detail_loads_results_for_small_task(
        self, clean_db, sample_results_payload
    ):
        from services.analysis_task import get_task_detail

        task_id = self._seed_task_with_results(clean_db, sample_results_payload)
        detail = get_task_detail(task_id)

        # n_sites=2 → small task → results list should be populated
        assert detail["is_large"] is False
        assert detail["results"] is not None
        assert len(detail["results"]) == 10  # 2 sites × 5 years

    def test_get_task_site_results_returns_correct_rows(
        self, clean_db, sample_results_payload
    ):
        from services.analysis_task import get_task_site_results

        task_id = self._seed_task_with_results(clean_db, sample_results_payload)
        results = get_task_site_results(task_id, "site-001")

        assert len(results) == 5  # 5 years for site-001

    def test_get_task_site_results_sorted_by_year(
        self, clean_db, sample_results_payload
    ):
        from services.analysis_task import get_task_site_results

        task_id = self._seed_task_with_results(clean_db, sample_results_payload)
        results = get_task_site_results(task_id, "site-001")

        years = [r["year"] for r in results]
        assert years == sorted(years)

    def test_get_task_site_results_values_match_fixture(
        self, clean_db, sample_results_payload
    ):
        from services.analysis_task import get_task_site_results

        task_id = self._seed_task_with_results(clean_db, sample_results_payload)
        results = get_task_site_results(task_id, "site-001")

        year_2019 = next(r for r in results if r["year"] == 2019)
        assert year_2019["emissions_avoided_mgco2e"] == pytest.approx(4.8)
        assert year_2019["is_pre_intervention"] is True

    def test_get_task_detail_returns_none_for_unknown_task(self, clean_db):
        from services.analysis_task import get_task_detail

        result = get_task_detail(str(uuid.uuid4()))
        assert result is None

    def test_get_task_site_results_empty_for_unknown_task(self, clean_db):
        from services.analysis_task import get_task_site_results

        results = get_task_site_results(str(uuid.uuid4()), "nonexistent-site")
        assert results == []
