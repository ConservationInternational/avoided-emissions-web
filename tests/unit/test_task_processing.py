"""Unit tests for import_execution_results (services/analysis_task.py).

A mock DB session is injected via the optional ``db=`` parameter so no real
database connection is needed.  The fixture payload (tests/fixtures/
results_payload.json) provides a representative 2-site × 5-year dataset.
"""

from unittest.mock import MagicMock

import pytest
from models import TaskResult, TaskResultTotal
from services.analysis_task import import_execution_results

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_db_with_task(task_obj=None):
    """Return a MagicMock session pre-wired with a query that returns *task_obj*."""
    mock_db = MagicMock()
    if task_obj is None:
        task_obj = MagicMock()
        task_obj.extra_metadata = {}

    # All models use the same query chain in this function, so we set a
    # single return value for .filter().first() — only AnalysisTask is
    # retrieved via .first(); TaskResult/TaskResultTotal use .delete().
    mock_db.query.return_value.filter.return_value.first.return_value = task_obj
    mock_db.query.return_value.filter.return_value.delete.return_value = 0
    return mock_db, task_obj


def _get_adds_by_type(mock_db, model_cls):
    """Extract all db.add() calls where the added object is an instance of *model_cls*."""
    return [
        c.args[0]
        for c in mock_db.add.call_args_list
        if isinstance(c.args[0], model_cls)
    ]


# ---------------------------------------------------------------------------
# Row counts
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestImportExecutionResultsRowCounts:
    def test_creates_correct_number_of_task_result_rows(
        self, sample_results_payload, sample_task_id
    ):
        mock_db, _ = _make_mock_db_with_task()
        import_execution_results(sample_task_id, sample_results_payload, db=mock_db)

        task_results = _get_adds_by_type(mock_db, TaskResult)
        # 2 sites × 5 years = 10 time-series rows
        assert len(task_results) == 10

    def test_creates_correct_number_of_task_result_total_rows(
        self, sample_results_payload, sample_task_id
    ):
        mock_db, _ = _make_mock_db_with_task()
        import_execution_results(sample_task_id, sample_results_payload, db=mock_db)

        totals = _get_adds_by_type(mock_db, TaskResultTotal)
        # 2 records (one per site)
        assert len(totals) == 2


# ---------------------------------------------------------------------------
# Value correctness
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestImportExecutionResultsValues:
    def test_task_result_emissions_match_payload(
        self, sample_results_payload, sample_task_id
    ):
        mock_db, _ = _make_mock_db_with_task()
        import_execution_results(sample_task_id, sample_results_payload, db=mock_db)

        task_results = _get_adds_by_type(mock_db, TaskResult)
        # Results are added in payload order; first entry is site-001 / 2019
        first = task_results[0]
        assert first.site_id == "site-001"
        assert first.year == 2019
        assert first.extrapolated_emissions_avoided_mgco2e == pytest.approx(4.8)

    def test_task_result_total_emissions_match_payload(
        self, sample_results_payload, sample_task_id
    ):
        mock_db, _ = _make_mock_db_with_task()
        import_execution_results(sample_task_id, sample_results_payload, db=mock_db)

        totals = _get_adds_by_type(mock_db, TaskResultTotal)
        site001_total = next(t for t in totals if t.site_id == "site-001")
        assert site001_total.extrapolated_emissions_avoided_mgco2e == pytest.approx(
            24.0
        )

    def test_task_result_total_site_name_stored(
        self, sample_results_payload, sample_task_id
    ):
        mock_db, _ = _make_mock_db_with_task()
        import_execution_results(sample_task_id, sample_results_payload, db=mock_db)

        totals = _get_adds_by_type(mock_db, TaskResultTotal)
        site001_total = next(t for t in totals if t.site_id == "site-001")
        assert site001_total.site_name == "Protected Area Alpha"

    def test_ci_bounds_stored_on_task_result(
        self, sample_results_payload, sample_task_id
    ):
        mock_db, _ = _make_mock_db_with_task()
        import_execution_results(sample_task_id, sample_results_payload, db=mock_db)

        task_results = _get_adds_by_type(mock_db, TaskResult)
        first = task_results[0]  # site-001 / 2019
        assert first.extrapolated_emissions_avoided_mgco2e_ci_lower == pytest.approx(
            3.9
        )
        assert first.extrapolated_emissions_avoided_mgco2e_ci_upper == pytest.approx(
            5.7
        )

    def test_pre_intervention_flag_set_for_year_2019(
        self, sample_results_payload, sample_task_id
    ):
        mock_db, _ = _make_mock_db_with_task()
        import_execution_results(sample_task_id, sample_results_payload, db=mock_db)

        task_results = _get_adds_by_type(mock_db, TaskResult)
        first = task_results[0]  # site-001 / 2019
        assert first.is_pre_intervention is True
        assert first.is_post_intervention is False

    def test_post_intervention_flag_set_for_year_2022(
        self, sample_results_payload, sample_task_id
    ):
        mock_db, _ = _make_mock_db_with_task()
        import_execution_results(sample_task_id, sample_results_payload, db=mock_db)

        task_results = _get_adds_by_type(mock_db, TaskResult)
        year_2022 = next(
            r for r in task_results if r.site_id == "site-001" and r.year == 2022
        )
        assert year_2022.is_post_intervention is True
        assert year_2022.is_pre_intervention is False

    def test_sub_site_index_stored_from_metadata(
        self, sample_results_payload, sample_task_id
    ):
        mock_db, _ = _make_mock_db_with_task()
        import_execution_results(sample_task_id, sample_results_payload, db=mock_db)

        task_results = _get_adds_by_type(mock_db, TaskResult)
        assert task_results[0].sub_site_index == 0

    def test_period_years_stored_on_total(self, sample_results_payload, sample_task_id):
        mock_db, _ = _make_mock_db_with_task()
        import_execution_results(sample_task_id, sample_results_payload, db=mock_db)

        totals = _get_adds_by_type(mock_db, TaskResultTotal)
        site001 = next(t for t in totals if t.site_id == "site-001")
        assert site001.first_year == 2019
        assert site001.last_year == 2023


# ---------------------------------------------------------------------------
# Summary metadata
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestImportExecutionResultsSummary:
    def test_summary_stored_in_task_extra_metadata(
        self, sample_results_payload, sample_task_id
    ):
        mock_db, task_obj = _make_mock_db_with_task()
        import_execution_results(sample_task_id, sample_results_payload, db=mock_db)

        assert task_obj.extra_metadata["n_sites"] == 2
        assert task_obj.extra_metadata["n_replicates"] == 3
        assert task_obj.extra_metadata["failed_sites"] == ["site-bad"]
        assert task_obj.extra_metadata["n_failed_sites"] == 1

    def test_git_sha_stored_in_task_extra_metadata(
        self, sample_results_payload, sample_task_id
    ):
        mock_db, task_obj = _make_mock_db_with_task()
        import_execution_results(sample_task_id, sample_results_payload, db=mock_db)

        assert task_obj.extra_metadata["r_analysis_git_sha"] == "abc123def456"


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestImportExecutionResultsIdempotency:
    def test_deletes_existing_results_before_inserting(
        self, sample_results_payload, sample_task_id
    ):
        """Existing TaskResult and TaskResultTotal rows must be deleted first."""
        mock_db, _ = _make_mock_db_with_task()
        import_execution_results(sample_task_id, sample_results_payload, db=mock_db)

        delete_mock = mock_db.query.return_value.filter.return_value.delete
        # One delete() call per model (TaskResult + TaskResultTotal)
        assert delete_mock.call_count >= 2


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestImportExecutionResultsEdgeCases:
    def test_empty_payload_returns_without_error(self, sample_task_id):
        mock_db, _ = _make_mock_db_with_task()
        # Should log a warning and return without adding anything
        import_execution_results(sample_task_id, {}, db=mock_db)
        mock_db.add.assert_not_called()

    def test_none_payload_returns_without_error(self, sample_task_id):
        mock_db, _ = _make_mock_db_with_task()
        import_execution_results(sample_task_id, None, db=mock_db)
        mock_db.add.assert_not_called()

    def test_payload_without_time_series_creates_no_task_results(self, sample_task_id):
        payload = {
            "summary": {"n_sites": 0, "n_replicates": 1},
            "records": [],
        }
        mock_db, _ = _make_mock_db_with_task()
        import_execution_results(sample_task_id, payload, db=mock_db)

        task_results = _get_adds_by_type(mock_db, TaskResult)
        assert len(task_results) == 0

    def test_does_not_commit_when_external_session_provided(
        self, sample_results_payload, sample_task_id
    ):
        """Caller-managed sessions must not be committed inside the function."""
        mock_db, _ = _make_mock_db_with_task()
        import_execution_results(sample_task_id, sample_results_payload, db=mock_db)

        mock_db.commit.assert_not_called()
