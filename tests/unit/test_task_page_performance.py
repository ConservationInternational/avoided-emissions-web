"""Regression coverage for task page instrumentation and cached results."""

from time import perf_counter
from types import SimpleNamespace

import pytest
import redis
from callbacks import _detail_builders, _match_quality, _registration
from dash.exceptions import PreventUpdate
from flask import Flask, Response, g
from services import analysis_task, task_result_cache
from task_timing import add_task_timing_headers, task_timing

pytestmark = pytest.mark.unit


def test_task_timing_is_reported_in_the_response():
    with Flask(__name__).test_request_context():
        g.dash_request_start = perf_counter()
        with task_timing("task_totals_sql"):
            pass
        header = add_task_timing_headers(Response()).headers["Server-Timing"]
    assert "task_totals_sql;dur=" in header
    assert "dash_request;dur=" in header


def test_cached_site_selection_is_order_independent_and_revision_bound(mocker):
    task = SimpleNamespace(
        id="task", status="succeeded", results_s3_uri="s3://bucket/output",
        extra_metadata={"result_revision": "first"},
    )
    storage = {}
    client = mocker.Mock()
    client.get.side_effect = storage.get
    client.set.side_effect = lambda key, value, *, ex: storage.update({key: value})
    mocker.patch.object(task_result_cache, "_client", return_value=client)
    mocker.patch.object(task_result_cache.Config, "TASK_RESULTS_CACHE_URL", "redis://test")
    compute = mocker.Mock(side_effect=[{"value": 1}, {"value": 2}, {"value": 3}])

    def load(ids):
        return task_result_cache.cached_task_result(task, "total", compute, site_ids=ids)

    assert load(["a", "b"]) == load(["b", "a", "a"]) == {"value": 1}
    compute.assert_called_once()
    task.extra_metadata["result_revision"] = "second"
    assert load(["b", "a"]) == {"value": 2}
    client.get.side_effect = redis.ConnectionError("Redis unavailable")
    assert load(["a", "b"]) == {"value": 3}


def test_imported_warnings_do_not_download_the_balance_artifact(mocker):
    download = mocker.patch.object(_match_quality, "download_results_csv")
    task = SimpleNamespace(
        extra_metadata={task_result_cache.QUALITY_WARNINGS_KEY: []}
    )
    assert _match_quality._compute_quality_warnings("task", task, []) == []
    download.assert_not_called()


def test_plot_controls_have_source_filters_before_the_map_loads(mocker):
    from dash import Dash
    from models import AnalysisTask, UserSiteFeature

    attributes = {
        str(i): {"region": "North" if i % 2 else "South", "count": 0}
        for i in range(427)
    }
    task = SimpleNamespace(
        name="Task", status="succeeded", site_set_id="site-set", n_sites=427,
        extra_metadata={"portfolio": {"yearly": [{
            "year": 2020, "treatment_defor_ha": 0, "control_defor_ha": 0,
            "cum_treatment_defor_ha": 0, "cum_control_defor_ha": 0,
            "emissions_avoided_mgco2e": 0, "forest_loss_avoided_ha": 0,
        }]}},
    )
    db = mocker.Mock()
    task_query = mocker.Mock()
    task_query.filter.return_value.first.return_value = task
    attribute_query = mocker.Mock()
    attribute_query.filter.return_value.all.return_value = [
        SimpleNamespace(id=sid, source_attributes=props)
        for sid, props in attributes.items()
    ]
    totals_query = mocker.Mock()
    totals_query.filter.return_value.all.return_value = []

    def query(*columns):
        if columns[0] is AnalysisTask:
            return task_query
        if columns[0] is UserSiteFeature.id:
            assert columns[1] is UserSiteFeature.source_attributes
            return attribute_query
        return totals_query

    db.query.side_effect = query
    mocker.patch.object(analysis_task, "get_db", return_value=db)
    geometry = mocker.patch.object(
        analysis_task, "_get_task_geometry",
        side_effect=AssertionError("Plot controls must not wait for map geometry"),
    )
    app = Dash(__name__, suppress_callback_exceptions=True)
    _registration.register_callbacks(app)
    refresh = next(
        value["callback"].__wrapped__ for key, value in app.callback_map.items()
        if "task-title.children" in key
    )
    mocker.patch.object(_registration, "_authorize_task_access", return_value=True)
    mocker.patch.object(_registration, "_compute_quality_warnings", return_value=[])
    mocker.patch.object(_registration, "get_task_top_site_yearly", return_value=None)
    mocker.patch.object(_registration, "can_resume_task", return_value=False)
    mocker.patch.object(_registration, "get_current_user", return_value=None)
    content = refresh(
        0, "tab-plots", None, "task", None,
        {"field": "region", "value": '"North"'},
    )[4]
    assert content["site-filter-attributes"].data == attributes
    assert content["site-filter-field"].options == [
        {"label": "count", "value": "count"},
        {"label": "region", "value": "region"},
    ]
    assert content["site-filter-value"].options == [
        {"label": "North", "value": '"North"'},
        {"label": "South", "value": '"South"'},
    ]
    assert content["site-filtered-ids"].data == [
        sid for sid in attributes if int(sid) % 2
    ]
    assert content["task-map-content"].children is None
    geometry.assert_not_called()


@pytest.mark.parametrize("site_set_id", ["site-set", None])
def test_deferred_map_preserves_early_filters_and_legacy_attributes(mocker, site_set_id):
    from dash import Dash, html, no_update

    app = Dash(__name__, suppress_callback_exceptions=True)
    _registration.register_callbacks(app)
    load_map = next(
        value["callback"].__wrapped__ for key, value in app.callback_map.items()
        if "task-map-content.children" in key
    )
    mocker.patch.object(_registration, "_authorize_task_access", return_value=True)
    mocker.patch.object(_registration, "get_task_detail", return_value={
        "task": SimpleNamespace(site_set_id=site_set_id, covariates=[], config={}),
        "totals": [],
        "sites_geojson": {"features": [{
            "properties": {"site_id": "site", "source_attributes": {"region": "North"}}
        }]},
    })
    mocker.patch.object(_registration, "_build_map", return_value=html.Div())
    _, attributes = load_map(True, "task", None)
    if site_set_id:
        assert attributes is no_update
    else:
        assert attributes == {"site": {"region": "North"}}


@pytest.mark.parametrize(
    ("name", "request_data"),
    [
        ("load_task_yearly_results", True),
        ("load_task_map", True),
        ("load_task_quality_rows", {"startRow": 0, "endRow": 50}),
        ("load_task_site_status_rows", {"startRow": 0, "endRow": 100}),
    ],
)
def test_deferred_data_is_not_fetched_after_shared_access_is_revoked(
    mocker, name, request_data
):
    callbacks = {}

    def callback(*args, **kwargs):
        def capture(function):
            callbacks[function.__name__] = function
            return function

        return capture

    _registration.register_callbacks(
        SimpleNamespace(callback=callback, clientside_callback=mocker.Mock())
    )
    authorize = mocker.patch.object(
        _registration, "_authorize_task_access", return_value=None
    )
    detail = mocker.patch.object(_registration, "get_task_detail")
    with pytest.raises(PreventUpdate):
        callbacks[name](request_data, "task", "revoked")
    authorize.assert_called_once_with("task", "revoked")
    detail.assert_not_called()


def test_quality_grid_serves_a_partial_final_block_without_aggregate_warnings():
    warnings = [
        {"scope": f"s{i:02}", "level": "warning", "message": f"Issue {i}"}
        for i in range(52)
    ] + [
        {"scope": "s50", "level": "danger", "message": "Critical issue"},
        {"scope": "aggregate", "level": "danger", "message": "Portfolio issue"},
    ]
    totals = [SimpleNamespace(site_id="s50", site_name="Named site")]
    assert _match_quality._site_quality_rows(warnings, totals, 50, 100) == {
        "rowCount": 52,
        "rowData": [
            {
                "site_id": "s50",
                "site": "Named site",
                "severity": "Critical",
                "issues": "Issue 50; Critical issue",
            },
            {
                "site_id": "s51",
                "site": "s51",
                "severity": "Warning",
                "issues": "Issue 51",
            },
        ],
    }


def _site_status_rows():
    def total(site_id, name, matched, area=10.0):
        return SimpleNamespace(
            site_id=site_id, display_site_id=None, site_name=name, area_ha=area,
            n_sample_pixels=matched, n_eligible_pixels=200, sampled_fraction=0.5,
        )

    meta = {
        "failed_sites": [
            {"site_id": "b", "error": "No matches found"},
            {
                "site_id": "b",
                "error": "No overlapping strata",
                "separation_warnings": {"g1": ["perfect separation"]},
            },
            {"site_id": "gone", "site_name": "Gone", "error": "No matches found"},
        ],
        "subsampled_sites": [{"site_id": "c", "sampled_fraction": 0.039}],
    }
    warnings = [
        {"scope": "a", "level": "warning", "message": "Poor balance"},
        {"scope": "a", "level": "danger", "message": "Few matches"},
        {"scope": "aggregate", "level": "danger", "message": "Portfolio"},
    ]
    site_index = [
        {"site_id": "a", "display_site_id": "A-1", "site_name": "Alpha", "area_ha": 5},
        {"site_id": "d", "display_site_id": None, "site_name": "Delta", "area_ha": 1},
    ]
    totals = [total("a", "Alpha", 50), total("b", "Bravo", 100), total("c", "C", 0)]
    return _match_quality.build_site_status_rows(
        meta, totals, site_index, warnings, has_results=True
    )


def test_site_status_rows_merge_quality_fragments_and_subsampling_per_site():
    rows = {row["site_id"]: row for row in _site_status_rows()}

    assert set(rows) == {"a", "b", "c", "d", "gone"}
    assert rows["a"] | {"issues": None} == {
        "site_id": "a", "site": "Alpha (A-1)", "quality": "Critical",
        "area_ha": 10.0, "matched_pixels": 50, "pct_matched": 50.0,
        "issues": None, "sampled_pct": None, "n_excluded_fragments": 0,
        "excluded_fragments": "",
    }
    assert rows["a"]["issues"] == "Poor balance; Few matches"
    assert rows["b"]["quality"] == "OK"
    assert rows["b"]["excluded_fragments"] == (
        "2: No matches found; No overlapping strata (Group g1: perfect separation)"
    )
    assert rows["c"]["sampled_pct"] == 3.9
    assert rows["d"]["quality"] == "No results"
    assert rows["gone"]["site"] == "Gone (gone)"
    assert rows["gone"]["quality"] == "No results"


def test_site_status_block_sorts_filters_and_slices_on_the_server():
    rows = _site_status_rows()
    block = _match_quality.site_status_block
    default = block(rows, {"startRow": 0, "endRow": 100})
    assert [r["site_id"] for r in default["rowData"]] == ["a", "gone", "d", "b", "c"]

    by_sampling = block(
        rows,
        {"startRow": 0, "endRow": 2, "sortModel": [{"colId": "sampled_pct", "sort": "desc"}]},
    )
    assert by_sampling["rowCount"] == 5
    assert by_sampling["rowData"][0]["site_id"] == "c"

    filtered = block(
        rows,
        {
            "startRow": 0,
            "endRow": 100,
            "filterModel": {
                "excluded_fragments": {"filterType": "text", "type": "contains", "filter": "NO MATCHES"},
                "matched_pixels": {"filterType": "number", "type": "greaterThan", "filter": 60},
            },
        },
    )
    assert [r["site_id"] for r in filtered["rowData"]] == ["b"]

    critical = block(
        rows,
        {
            "startRow": 0,
            "endRow": 100,
            "filterModel": {"quality": {"filterType": "text", "type": "is:Critical"}},
        },
    )
    assert critical["rowCount"] > 0
    assert {r["quality"] for r in critical["rowData"]} == {"Critical"}


def test_site_status_card_keeps_task_wide_warnings_without_top_banner():
    task = SimpleNamespace(extra_metadata={})
    totals = [SimpleNamespace(site_id="a")]
    card = _match_quality._build_site_status_card(
        task,
        totals,
        [
            {"scope": "aggregate", "level": "danger", "message": "Overall imbalance"},
            {"scope": "a", "level": "warning", "message": "Site-only issue"},
        ],
    )
    text = str(card)
    assert "Overall imbalance" in text
    assert "Site-only issue" not in text


def test_raw_results_skips_replay_objects_until_the_text_download(mocker):
    task = SimpleNamespace(
        id="task", status="succeeded", results_s3_uri="s3://bucket/output"
    )
    paginator = mocker.Mock()
    paginator.paginate.side_effect = [
        [
            {
                "Contents": [{"Key": "output/results_summary.json"}],
                "CommonPrefixes": [
                    {"Prefix": "output/matching_replay/"},
                    {"Prefix": "output/matches/"},
                ],
            }
        ],
        [{"Contents": [{"Key": "output/matches/batch.parquet"}]}],
        [
            {"Contents": [{"Key": "output/matching_replay/"}]},
            {
                "Contents": [
                    {"Key": f"output/matching_replay/site/{i}.receipt.json"}
                    for i in range(2000)
                ]
            },
        ],
    ]
    s3 = mocker.Mock()
    s3.get_paginator.return_value = paginator
    s3.generate_presigned_url.side_effect = (
        lambda operation, *, Params, ExpiresIn: f"https://example.test/{Params['Key']}"
    )
    mocker.patch.object(analysis_task, "get_s3_client", return_value=s3)

    layout = _detail_builders._build_raw_results(task)
    assert layout["download-replay-links-btn"]
    assert paginator.paginate.call_args_list == [
        mocker.call(Bucket="bucket", Prefix="output/", Delimiter="/"),
        mocker.call(Bucket="bucket", Prefix="output/matches/"),
    ]
    assert s3.generate_presigned_url.call_count == 2

    from dash import Dash

    app = Dash(__name__, suppress_callback_exceptions=True)
    _registration.register_callbacks(app)
    key = next(key for key in app.callback_map if "download-replay-links.data" in key)
    download = app.callback_map[key]["callback"].__wrapped__
    authorize = mocker.patch.object(_registration, "_authorize_task_access")
    authorize.side_effect = [False, True]
    mocker.patch.object(_registration, "get_task_header", return_value=task)
    assert download(1, task.id, "revoked")[0] is _registration.no_update
    assert s3.generate_presigned_url.call_count == 2

    payload, error = download(1, task.id, "valid")
    assert error is None
    assert payload["filename"] == "matching_replay_links.txt"
    assert payload["content"].splitlines() == [
        f"https://example.test/output/matching_replay/site/{i}.receipt.json"
        for i in range(2000)
    ]
    paginator.paginate.assert_called_with(
        Bucket="bucket", Prefix="output/matching_replay/"
    )
