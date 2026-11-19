"""Regression coverage for task page instrumentation and cached results."""

from time import perf_counter
from types import SimpleNamespace

import pytest
import redis
from callbacks import _match_quality, _registration
from dash.exceptions import PreventUpdate
from flask import Flask, Response, g
from services import task_result_cache
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


@pytest.mark.parametrize("name", ["load_task_yearly_results", "load_task_map"])
def test_deferred_data_is_not_fetched_after_shared_access_is_revoked(mocker, name):
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
        callbacks[name](True, "task", "revoked")
    authorize.assert_called_once_with("task", "revoked")
    detail.assert_not_called()
