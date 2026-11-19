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


@pytest.mark.parametrize(
    ("name", "request_data"),
    [
        ("load_task_yearly_results", True),
        ("load_task_map", True),
        ("load_task_quality_rows", {"startRow": 0, "endRow": 50}),
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
