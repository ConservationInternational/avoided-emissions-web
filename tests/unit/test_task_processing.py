"""Unit tests for import_execution_results (services/analysis_task.py).

A mock DB session is injected via the optional ``db=`` parameter so no real
database connection is needed.  The fixture payload (tests/fixtures/
results_payload.json) provides a representative 2-site × 5-year dataset with
three replicates.
"""

import copy
from unittest.mock import MagicMock

import pytest
from models import TaskResult, TaskResultTotal
from services.analysis_task import ResultImportError, import_execution_results

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_db_with_task():
    """Return a MagicMock session whose AnalysisTask query returns a mock task."""
    mock_db = MagicMock()
    task_obj = MagicMock()
    task_obj.extra_metadata = {}
    mock_db.query.return_value.filter.return_value.first.return_value = task_obj
    return mock_db, task_obj


def _inserted_rows(mock_db, model_cls):
    """Rows passed to bulk ``db.execute(model.__table__.insert(), rows)`` calls."""
    rows = []
    for c in mock_db.execute.call_args_list:
        if getattr(c.args[0], "table", None) is model_cls.__table__:
            rows.extend(c.args[1])
    return rows


def _import(task_id, payload):
    mock_db, task_obj = _make_mock_db_with_task()
    import_execution_results(task_id, payload, db=mock_db)
    return mock_db, task_obj


# ---------------------------------------------------------------------------
# Rows and values
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestImportExecutionResultsRows:
    def test_row_counts(self, sample_results_payload, sample_task_id):
        mock_db, _ = _import(sample_task_id, sample_results_payload)
        assert len(_inserted_rows(mock_db, TaskResult)) == 10  # 2 sites × 5 years
        assert len(_inserted_rows(mock_db, TaskResultTotal)) == 2

    def test_site_year_values_and_bounds(self, sample_results_payload, sample_task_id):
        mock_db, _ = _import(sample_task_id, sample_results_payload)
        first = _inserted_rows(mock_db, TaskResult)[0]
        assert (first["site_id"], first["year"]) == ("site-001", 2019)
        assert first["extrapolated_emissions_avoided_mgco2e"] == pytest.approx(4.8)
        assert first["extrapolated_emissions_avoided_mgco2e_ci_lower"] == pytest.approx(
            3.9
        )
        assert first["is_pre_intervention"] is True
        assert first["is_post_intervention"] is False
        assert "sub_site_index" not in first

    def test_site_total_values(self, sample_results_payload, sample_task_id):
        mock_db, _ = _import(sample_task_id, sample_results_payload)
        total = next(
            r
            for r in _inserted_rows(mock_db, TaskResultTotal)
            if r["site_id"] == "site-001"
        )
        assert total["site_name"] == "Protected Area Alpha"
        assert total["extrapolated_emissions_avoided_mgco2e"] == pytest.approx(24.0)
        assert total["n_eligible_pixels"] == 4000
        assert (total["first_year"], total["last_year"]) == (2019, 2023)

    def test_summary_stored_in_task_metadata(
        self, sample_results_payload, sample_task_id
    ):
        _, task_obj = _import(sample_task_id, sample_results_payload)
        meta = task_obj.extra_metadata
        assert meta["n_replicates"] == 3
        assert meta["n_failed_sites"] == 1
        assert meta["failed_sites"][0]["site_id"] == "site-bad"
        assert meta["bounds_label"] == "Range across matching replicates"
        assert len(meta["portfolio"]["yearly"]) == 5
        assert meta["r_analysis_git_sha"] == "abc123def456"

    def test_replaces_existing_rows_without_committing(
        self, sample_results_payload, sample_task_id
    ):
        mock_db, _ = _import(sample_task_id, sample_results_payload)
        assert mock_db.query.return_value.filter.return_value.delete.call_count == 2
        mock_db.commit.assert_not_called()

    def test_single_replicate_needs_no_bounds(
        self, sample_results_payload, sample_task_id
    ):
        payload = copy.deepcopy(sample_results_payload)
        payload["summary"]["n_replicates"] = 1
        for item in payload["time_series"] + payload["records"]:
            item["values"] = {
                k: v for k, v in item["values"].items() if "_ci_" not in k
            }
        mock_db, _ = _import(sample_task_id, payload)
        first = _inserted_rows(mock_db, TaskResult)[0]
        assert not any("_ci_" in k for k in first)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestImportExecutionResultsValidation:
    @pytest.mark.parametrize("payload", [None, {}, {"summary": {}}])
    def test_rejects_missing_payload(self, payload, sample_task_id):
        mock_db, _ = _make_mock_db_with_task()
        with pytest.raises(ResultImportError):
            import_execution_results(sample_task_id, payload, db=mock_db)
        mock_db.execute.assert_not_called()

    def test_rejects_missing_metric(self, sample_results_payload, sample_task_id):
        payload = copy.deepcopy(sample_results_payload)
        del payload["time_series"][0]["values"]["extrapolated_control_defor_ha"]
        mock_db, _ = _make_mock_db_with_task()
        with pytest.raises(ResultImportError):
            import_execution_results(sample_task_id, payload, db=mock_db)
        mock_db.execute.assert_not_called()

    def test_rejects_missing_bounds_with_replicates(
        self, sample_results_payload, sample_task_id
    ):
        payload = copy.deepcopy(sample_results_payload)
        del payload["records"][0]["values"][
            "extrapolated_emissions_avoided_mgco2e_ci_lower"
        ]
        with pytest.raises(ResultImportError):
            import_execution_results(
                sample_task_id, payload, db=_make_mock_db_with_task()[0]
            )

    def test_rejects_duplicate_site_years(self, sample_results_payload, sample_task_id):
        payload = copy.deepcopy(sample_results_payload)
        payload["time_series"].append(payload["time_series"][0])
        with pytest.raises(ResultImportError, match="duplicate"):
            import_execution_results(
                sample_task_id, payload, db=_make_mock_db_with_task()[0]
            )
