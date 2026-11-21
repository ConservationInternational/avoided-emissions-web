"""Unit tests for get_task_detail and get_task_site_results
(services/analysis_task.py).

External I/O is mocked via:
  - services.analysis_task.get_db                      → MagicMock session
  - services.analysis_task.get_user_site_set_geojson    → returns None

No DB connection or S3 access is made during these tests.
"""

from typing import ClassVar
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from models import AnalysisTask, TaskResult, TaskResultTotal, TaskSite
from services.analysis_task import (
    LARGE_TASK_THRESHOLD,
    get_task_detail,
    get_task_site_results,
    get_task_top_site_yearly,
)
from sqlalchemy import Column, MetaData, Table, create_engine
from sqlalchemy.orm import Session

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_GET_DB = "services.analysis_task.get_db"
_GET_GEOJSON = "services.analysis_task.get_user_site_set_geojson"


@pytest.mark.unit
def test_top_site_query_uses_selected_year_for_leaders_and_remainders(mocker):
    task_id = UUID("00000000-0000-0000-0000-000000000001")
    metadata = MetaData()
    table = Table(
        "task_results", metadata,
        *(Column(name, TaskResult.__table__.c[name].type) for name in (
            "task_id", "site_id", "year", "extrapolated_emissions_avoided_mgco2e",
            "extrapolated_forest_loss_avoided_ha",
        )),
    )
    engine = create_engine("sqlite://")
    metadata.create_all(engine)
    rows = [
        {
            "task_id": task_id, "site_id": f"site-{index}", "year": year,
            **{
                metric: (index if year == 2025 else 9 - index)
                * (1 if index % 2 else -1)
                for metric in (
                    "extrapolated_emissions_avoided_mgco2e",
                    "extrapolated_forest_loss_avoided_ha",
                )
            },
        }
        for index in range(1, 9) for year in (2024, 2025)
    ]
    with engine.begin() as connection:
        connection.execute(table.insert(), rows)
    session = Session(engine)
    mocker.patch(_GET_DB, return_value=session)
    allowed = [f"site-{index}" for index in range(1, 9)]
    try:
        original = get_task_top_site_yearly(task_id, allowed)
        changed = get_task_top_site_yearly(task_id, allowed, 2024)
        assert {row["site_id"] for row in original["site_yearly"]} == {
            f"site-{index}" for index in (3, 4, 5, 6, 7, 8)
        }
        assert {row["site_id"] for row in changed["site_yearly"]} == {
            f"site-{index}" for index in (1, 2, 3, 4, 5, 6)
        }
        assert changed["signed_yearly"]["forest_loss_avoided_ha"] == [
            {"year": 2024, "positive": 2, "negative": -1},
            {"year": 2025, "positive": 7, "negative": -8},
        ]
    finally:
        session.close()
        engine.dispose()


@pytest.mark.unit
@pytest.mark.parametrize("allowed_count", [0, 3, 12, 24])
@pytest.mark.parametrize("has_2025", [False, True])
def test_top_site_yearly_query_ranks_2025_and_preserves_signed_remainders(
    mocker, allowed_count, has_2025
):
    task_id = UUID("00000000-0000-0000-0000-000000000001")
    other_task = UUID("00000000-0000-0000-0000-000000000002")
    columns = (
        "task_id",
        "site_id",
        "year",
        "extrapolated_emissions_avoided_mgco2e",
        "extrapolated_forest_loss_avoided_ha",
    )
    metadata = MetaData()
    table = Table(
        "task_results",
        metadata,
        *(Column(name, TaskResult.__table__.c[name].type) for name in columns),
    )
    engine = create_engine("sqlite://")
    metadata.create_all(engine)
    rows = [
        {
            "task_id": owner,
            "site_id": f"site-{index:02d}",
            "year": year,
            "extrapolated_emissions_avoided_mgco2e": index
            * (1 if index % 2 else -1)
            * multiplier
            * (1 if owner == task_id else 1000),
            "extrapolated_forest_loss_avoided_ha": (25 - index)
            * (-1 if index % 2 else 1)
            * multiplier
            * (1 if owner == task_id else 1000),
        }
        for owner in (task_id, other_task)
        for index in range(1, 25)
        for year, multiplier in ((2024, -2), (2025, 1), (2026, -0.5))
        if has_2025 or year != 2025
    ]
    with engine.begin() as connection:
        connection.execute(table.insert(), rows)
    session = Session(engine)
    mocker.patch(_GET_DB, return_value=session)
    close = mocker.spy(session, "close")
    allowed = [f"site-{index:02d}" for index in range(1, allowed_count + 1)]
    try:
        result = get_task_top_site_yearly(task_id, allowed)
        emissions_top = (
            {
                0: [],
                3: [1, 2, 3],
                12: [7, 9, 11, 8, 10, 12],
                24: [19, 21, 23, 20, 22, 24],
            }[allowed_count]
            if has_2025
            else []
        )
        forest_top = (
            {
                0: [],
                3: [1, 2, 3],
                12: [1, 3, 5, 2, 4, 6],
                24: [1, 3, 5, 2, 4, 6],
            }[allowed_count]
            if has_2025
            else []
        )
        expected = {f"site-{index:02d}" for index in emissions_top + forest_top}
        assert {row["site_id"] for row in result["site_yearly"]} == expected
        assert len(result["site_yearly"]) == len(expected) * (3 if has_2025 else 2)
        assert len(expected) <= 12
        for row in result["site_yearly"]:
            index = int(row["site_id"].split("-")[1])
            multiplier = {2024: -2, 2025: 1, 2026: -0.5}[row["year"]]
            assert (
                row["emissions_avoided_mgco2e"]
                == index * (1 if index % 2 else -1) * multiplier
            )
            assert (
                row["forest_loss_avoided_ha"]
                == (25 - index) * (-1 if index % 2 else 1) * multiplier
            )
        for metric, excluded in (
            ("emissions_avoided_mgco2e", emissions_top),
            ("forest_loss_avoided_ha", forest_top),
        ):
            remaining = [
                row
                for row in rows
                if row["task_id"] == task_id
                and row["site_id"] in allowed
                and int(row["site_id"].split("-")[1]) not in excluded
            ]
            expected_years = sorted({row["year"] for row in remaining})
            assert [
                row["year"] for row in result["signed_yearly"][metric]
            ] == expected_years
            for aggregate in result["signed_yearly"][metric]:
                values = [
                    row[f"extrapolated_{metric}"]
                    for row in remaining
                    if row["year"] == aggregate["year"]
                ]
                assert aggregate["positive"] == sum(
                    value for value in values if value > 0
                )
                assert aggregate["negative"] == sum(
                    value for value in values if value < 0
                )
        close.assert_called_once()
    finally:
        session.close()
        engine.dispose()


def _make_mock_task(n_sites=5, task_id="test-task-id"):
    """Return a MagicMock that looks like an AnalysisTask."""
    t = MagicMock(spec=AnalysisTask)
    t.id = task_id
    t.n_sites = n_sites
    t.site_set_id = None  # skip PostGIS geojson lookup
    t.sites_s3_uri = None  # skip S3 fallback
    t.config = {}
    t.extra_metadata = {"portfolio": {"yearly": [{"year": 2020, "n_sites": n_sites}]}}
    return t


def _build_mock_db(task_obj, totals=None, sites=None, results=None):
    """Return a MagicMock session whose query() routes to the right list."""
    totals = totals or []
    sites = sites or []
    results = results or []

    mock_db = MagicMock()

    def _query(model_cls):
        q = MagicMock()
        if model_cls is AnalysisTask:
            q.filter.return_value.first.return_value = task_obj
        elif model_cls is TaskResultTotal:
            q.filter.return_value.all.return_value = totals
        elif model_cls is TaskSite:
            q.filter.return_value.all.return_value = sites
        elif model_cls is TaskResult:
            q.filter.return_value.order_by.return_value.all.return_value = results
        return q

    mock_db.query.side_effect = _query
    return mock_db


def _make_task_result_row(site_id, year, is_pre=False, is_post=False):
    """Return a MagicMock row with all fields that get_task_site_results reads."""
    r = MagicMock()
    r.site_id = site_id
    r.year = year
    r.is_pre_intervention = is_pre
    r.is_post_intervention = is_post
    r.extrapolated_treatment_defor_ha = 12.3
    r.extrapolated_control_defor_ha = 157.5
    r.extrapolated_emissions_avoided_mgco2e = 4.8
    r.extrapolated_forest_loss_avoided_ha = 145.2
    r.extrapolated_treatment_emissions_mgco2e = 0.41
    r.extrapolated_control_emissions_mgco2e = 5.21
    r.extrapolated_treatment_emissions_mgco2e_ci_lower = 0.3
    r.extrapolated_treatment_emissions_mgco2e_ci_upper = 0.5
    r.extrapolated_control_emissions_mgco2e_ci_lower = 4.8
    r.extrapolated_control_emissions_mgco2e_ci_upper = 5.6
    r.extrapolated_treatment_defor_ha_ci_lower = 10.0
    r.extrapolated_treatment_defor_ha_ci_upper = 14.6
    r.extrapolated_control_defor_ha_ci_lower = 139.0
    r.extrapolated_control_defor_ha_ci_upper = 176.0
    r.extrapolated_forest_loss_avoided_ha_ci_lower = 129.0
    r.extrapolated_forest_loss_avoided_ha_ci_upper = 161.4
    r.extrapolated_emissions_avoided_mgco2e_ci_lower = 3.9
    r.extrapolated_emissions_avoided_mgco2e_ci_upper = 5.7
    r.extrapolated_cumulative_treatment_defor_ha = 12.3
    r.extrapolated_cumulative_control_defor_ha = 157.5
    r.extrapolated_cumulative_treatment_defor_ha_ci_lower = 10.0
    r.extrapolated_cumulative_treatment_defor_ha_ci_upper = 14.6
    r.extrapolated_cumulative_control_defor_ha_ci_lower = 139.0
    r.extrapolated_cumulative_control_defor_ha_ci_upper = 176.0
    return r


# ---------------------------------------------------------------------------
# get_task_detail — structure
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGetTaskDetailStructure:
    def _call(self, mocker, task_obj=None, totals=None, n_sites=5):
        """Invoke get_task_detail with fully mocked dependencies."""
        if task_obj is None:
            task_obj = _make_mock_task(n_sites=n_sites)
        mock_db = _build_mock_db(task_obj, totals=totals)
        mocker.patch(_GET_DB, return_value=mock_db)
        mocker.patch(_GET_GEOJSON, return_value=None)
        return get_task_detail("any-task-id")

    def test_returns_none_for_missing_task(self, mocker):
        mock_db = _build_mock_db(None)
        mocker.patch(_GET_DB, return_value=mock_db)
        mocker.patch(_GET_GEOJSON, return_value=None)

        result = get_task_detail("nonexistent-task-id")

        assert result is None

    def test_returns_dict_with_all_required_keys(self, mocker):
        detail = self._call(mocker)
        expected_keys = {
            "task",
            "sites",
            "results",
            "totals",
            "sites_geojson",
            "is_large",
            "agg_yearly",
        }
        assert expected_keys.issubset(detail.keys())

    def test_agg_yearly_is_the_portfolio_series_from_r(self, mocker):
        detail = self._call(mocker)
        assert detail["agg_yearly"] == [{"year": 2020, "n_sites": 5}]

    def test_closes_db_session_on_success(self, mocker):
        mock_db = _build_mock_db(_make_mock_task())
        mocker.patch(_GET_DB, return_value=mock_db)
        mocker.patch(_GET_GEOJSON, return_value=None)

        get_task_detail("any-task-id")

        mock_db.close.assert_called_once()


# ---------------------------------------------------------------------------
# get_task_detail — small vs large task routing
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGetTaskDetailRouting:
    @pytest.mark.parametrize(
        ("section", "expected_tables"),
        [
            ("overview", [TaskResultTotal, TaskSite]),
            ("results", [TaskResultTotal, TaskSite]),
            ("yearly", [TaskResult]),
            ("plots", [TaskResultTotal, TaskSite, TaskResult]),
            ("map", [TaskResultTotal]),
        ],
    )
    def test_tab_queries_only_its_required_data(self, mocker, section, expected_tables):
        mock_db = _build_mock_db(_make_mock_task())
        mocker.patch(_GET_DB, return_value=mock_db)
        geometry = mocker.patch("services.analysis_task._get_task_geometry")
        get_task_detail("any-task-id", section=section)
        assert [args[0] for args, _ in mock_db.query.call_args_list] == [
            AnalysisTask, *expected_tables
        ]
        assert geometry.call_count == (1 if section == "map" else 0)

    def _call_with_sizes(self, mocker, n_sites):
        task_obj = _make_mock_task(n_sites=n_sites)
        mock_db = _build_mock_db(task_obj)
        mocker.patch(_GET_DB, return_value=mock_db)
        mocker.patch(_GET_GEOJSON, return_value=None)
        return get_task_detail("any-task-id")

    def test_small_task_is_large_false(self, mocker):
        detail = self._call_with_sizes(mocker, n_sites=5)
        assert detail["is_large"] is False

    def test_small_task_results_is_list(self, mocker):
        detail = self._call_with_sizes(mocker, n_sites=5)
        assert isinstance(detail["results"], list)

    def test_at_threshold_is_not_large(self, mocker):
        # n_sites == LARGE_TASK_THRESHOLD is still "small"
        detail = self._call_with_sizes(mocker, n_sites=LARGE_TASK_THRESHOLD)
        assert detail["is_large"] is False

    def test_above_threshold_is_large(self, mocker):
        detail = self._call_with_sizes(mocker, n_sites=LARGE_TASK_THRESHOLD + 1)
        assert detail["is_large"] is True

    def test_large_task_results_is_none(self, mocker):
        detail = self._call_with_sizes(mocker, n_sites=LARGE_TASK_THRESHOLD + 1)
        assert detail["results"] is None

    def test_totals_always_loaded_for_small_task(self, mocker):
        mock_total = MagicMock(spec=TaskResultTotal)
        task_obj = _make_mock_task(n_sites=5)
        mock_db = _build_mock_db(task_obj, totals=[mock_total])
        mocker.patch(_GET_DB, return_value=mock_db)
        mocker.patch(_GET_GEOJSON, return_value=None)

        detail = get_task_detail("any-task-id")

        assert len(detail["totals"]) == 1

    def test_totals_always_loaded_for_large_task(self, mocker):
        mock_total = MagicMock(spec=TaskResultTotal)
        task_obj = _make_mock_task(n_sites=LARGE_TASK_THRESHOLD + 1)
        mock_db = _build_mock_db(task_obj, totals=[mock_total])
        mocker.patch(_GET_DB, return_value=mock_db)
        mocker.patch(_GET_GEOJSON, return_value=None)

        detail = get_task_detail("any-task-id")

        assert len(detail["totals"]) == 1
        assert detail["is_large"] is True


# ---------------------------------------------------------------------------
# get_task_site_results — structure
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGetTaskSiteResults:
    _EXPECTED_KEYS: ClassVar[set[str]] = {
        "site_id",
        "year",
        "treatment_defor_ha",
        "control_defor_ha",
        "emissions_avoided_mgco2e",
        "forest_loss_avoided_ha",
        "treatment_emissions_mgco2e",
        "control_emissions_mgco2e",
        "is_pre_intervention",
        "is_post_intervention",
        "treatment_defor_ha_ci_lower",
        "treatment_defor_ha_ci_upper",
        "control_defor_ha_ci_lower",
        "control_defor_ha_ci_upper",
        "emissions_avoided_mgco2e_ci_lower",
        "emissions_avoided_mgco2e_ci_upper",
        "forest_loss_avoided_ha_ci_lower",
        "forest_loss_avoided_ha_ci_upper",
        "treatment_emissions_mgco2e_ci_lower",
        "treatment_emissions_mgco2e_ci_upper",
        "control_emissions_mgco2e_ci_lower",
        "control_emissions_mgco2e_ci_upper",
        "cumulative_treatment_defor_ha",
        "cumulative_control_defor_ha",
        "cumulative_treatment_defor_ha_ci_lower",
        "cumulative_treatment_defor_ha_ci_upper",
        "cumulative_control_defor_ha_ci_lower",
        "cumulative_control_defor_ha_ci_upper",
    }

    def _call(self, mocker, rows):
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.order_by.return_value.all.return_value = rows
        mocker.patch(_GET_DB, return_value=mock_db)
        return get_task_site_results("task-id", "site-001")

    def test_returns_list(self, mocker):
        result = self._call(mocker, [])
        assert isinstance(result, list)

    def test_empty_list_for_no_results(self, mocker):
        result = self._call(mocker, [])
        assert result == []

    def test_result_dict_has_all_expected_keys(self, mocker):
        row = _make_task_result_row("site-001", 2020)
        result = self._call(mocker, [row])

        assert len(result) == 1
        assert self._EXPECTED_KEYS.issubset(result[0].keys())

    def test_values_mapped_correctly(self, mocker):
        row = _make_task_result_row("site-001", 2021, is_pre=True)
        result = self._call(mocker, [row])

        item = result[0]
        assert item["site_id"] == "site-001"
        assert item["year"] == 2021
        assert item["emissions_avoided_mgco2e"] == pytest.approx(4.8)
        assert item["is_pre_intervention"] is True
        assert item["is_post_intervention"] is False

    def test_ci_bounds_present_in_result(self, mocker):
        row = _make_task_result_row("site-001", 2022)
        result = self._call(mocker, [row])

        item = result[0]
        assert item["emissions_avoided_mgco2e_ci_lower"] == pytest.approx(3.9)
        assert item["emissions_avoided_mgco2e_ci_upper"] == pytest.approx(5.7)
        assert item["cumulative_treatment_defor_ha_ci_lower"] == pytest.approx(10.0)
        assert item["treatment_emissions_mgco2e_ci_upper"] == pytest.approx(0.5)

    def test_results_in_year_order(self, mocker):
        """The ORM .order_by(TaskResult.year) determines sort order."""
        rows = [
            _make_task_result_row("site-001", 2022),
            _make_task_result_row("site-001", 2019),
            _make_task_result_row("site-001", 2021),
        ]
        # Simulate DB returning them pre-ordered (as the real query would)
        ordered_rows = sorted(rows, key=lambda r: r.year)
        result = self._call(mocker, ordered_rows)

        years = [item["year"] for item in result]
        assert years == sorted(years)

    def test_closes_db_session(self, mocker):
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
        mocker.patch(_GET_DB, return_value=mock_db)

        get_task_site_results("task-id", "site-001")

        mock_db.close.assert_called_once()
