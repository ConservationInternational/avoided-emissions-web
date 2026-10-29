"""Map, catalog, mapping and persisted preparation UX regressions."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import api_routes
import pytest
from callbacks import _registration as registration
from callbacks._detail_builders import (
    _build_preparation_summary,
    _build_results_content,
)
from flask import Flask
from layouts.admin import _site_upload_card

pytestmark = pytest.mark.unit


def test_import_history_exposes_progress_and_readable_failure_diagnostics():
    from layouts.admin import IMPORT_HISTORY_COLUMNS

    fields = {column["field"]: column for column in IMPORT_HISTORY_COLUMNS}
    assert {"phase", "rows_scanned", "rows_accepted", "rows_skipped"} <= fields.keys()
    assert fields["error_message"]["wrapText"]
    assert fields["error_message"]["autoHeight"]


USER_ID = "00000000-0000-0000-0000-000000000001"
SET_ID = "00000000-0000-0000-0000-000000000002"
FEATURE_ID = "00000000-0000-0000-0000-000000000003"


def _walk(component):
    yield component
    children = getattr(component, "children", None)
    if isinstance(children, (list, tuple)):
        for child in children:
            yield from _walk(child)
    elif children is not None:
        yield from _walk(children)


def test_upload_limits_are_visible_before_transfer_and_match_archive_validation():
    from services import site_upload

    text = next(
        component.children
        for component in _walk(_site_upload_card("Import asynchronously"))
        if getattr(component, "id", None) == "site-upload-limits"
    )
    assert "800 MiB including form data" in text
    assert f"{site_upload.MAX_ARCHIVE_FILE_COUNT:,} files" in text
    assert (
        f"{site_upload.MAX_ARCHIVE_MEMBER_UNCOMPRESSED_BYTES // 1024**2} MiB per file"
        in text
    )
    assert (
        f"{site_upload.MAX_ARCHIVE_TOTAL_UNCOMPRESSED_BYTES // 1024**3} GiB expanded"
        in text
    )
    assert f"{int(site_upload.MAX_ARCHIVE_COMPRESSION_RATIO)}:1 compression" in text


@pytest.fixture
def api_client(monkeypatch):
    app = Flask(__name__)
    app.config.update(TESTING=True, LOGIN_DISABLED=True)
    user = SimpleNamespace(id=USER_ID, is_authenticated=True)
    monkeypatch.setattr(api_routes.flask_login, "current_user", user)
    limiter = SimpleNamespace(limit=lambda _rate: lambda function: function)
    app.register_blueprint(api_routes.create_api_blueprint(limiter))
    db = Mock()
    monkeypatch.setattr(api_routes, "get_db", lambda: db)
    return app.test_client(), db


def test_feature_lookup_returns_only_owned_exact_extent(api_client):
    client, db = api_client
    db.execute.return_value.mappings.return_value.first.return_value = {
        "feature_id": FEATURE_ID,
        "display_site_id": "duplicate label",
        "site_name": "A site",
        "area_ha": 123.4,
        "start_date": "2000-01-01",
        "end_date": None,
        "west": 1.0,
        "south": 2.0,
        "east": 3.0,
        "north": 4.0,
    }
    response = client.get(f"/api/site-feature/{SET_ID}/{FEATURE_ID}")
    assert response.status_code == 200
    assert response.json["site_id"] == FEATURE_ID
    assert response.json["display_site_id"] == "duplicate label"
    assert response.json["bounds"] == {
        "west": 1,
        "south": 2,
        "east": 3,
        "north": 4,
    }
    assert "geometry" not in response.json
    filters = db.query.return_value.filter.call_args.args
    assert any("user_site_sets.user_id" in str(item) for item in filters)
    assert db.execute.call_args.args[1] == {
        "feature_id": FEATURE_ID,
        "site_set_id": SET_ID,
    }
    db.close.assert_called_once()


def test_feature_lookup_hides_unowned_sets_and_rejects_bad_uuid(api_client):
    client, db = api_client
    db.query.return_value.filter.return_value.first.return_value = None
    assert client.get(f"/api/site-feature/{SET_ID}/{FEATURE_ID}").status_code == 404
    assert client.get(f"/api/site-feature/{SET_ID}/not-a-uuid").status_code == 400
    db.execute.assert_not_called()


def test_task_feature_lookup_is_scoped_to_owned_task_set(api_client):
    client, db = api_client
    db.query.return_value.filter.return_value.first.return_value = SimpleNamespace(
        site_set_id=SET_ID
    )
    db.execute.return_value.mappings.return_value.first.return_value = None
    response = client.get(f"/api/task-site-feature/{USER_ID}/{FEATURE_ID}")
    assert response.status_code == 404
    assert db.execute.call_args.args[1]["site_set_id"] == SET_ID
    filters = db.query.return_value.filter.call_args.args
    assert any("analysis_tasks.submitted_by" in str(item) for item in filters)


@pytest.mark.parametrize(
    "path",
    [f"/api/sites-tiles/{SET_ID}/7/64/63", f"/api/task-sites-tiles/{USER_ID}/7/64/63"],
)
def test_tiles_use_canonical_identity_and_preserve_display_id(api_client, path):
    client, db = api_client
    db.execute.return_value.fetchone.return_value = SimpleNamespace(mvt=b"tile")
    response = client.get(path)
    assert response.status_code == 200
    sql = str(db.execute.call_args.args[0])
    assert "f.id::text AS site_id" in sql
    assert "f.site_id AS display_site_id" in sql
    assert "ST_AsMVTGeom" in sql
    assert "ST_Transform(f.geom, 3857)" in sql
    assert "ST_Simplify" not in sql
    assert "simplify_tol" not in db.execute.call_args.args[1]
    if "task-sites" in path:
        assert "tr.site_id = f.id::text" in sql


def test_preparation_download_uses_persisted_metadata(api_client, monkeypatch):
    client, db = api_client
    report = {"input_sites": 3, "retained_sites": 2, "fragments": 7}
    db.query.return_value.filter.return_value.first.return_value = SimpleNamespace(
        extra_metadata={"preparation": report},
        config={"sites_parquet_s3_uri": "s3://owned-bucket/task/sites.parquet"},
    )
    response = client.get(f"/api/tasks/{USER_ID}/preparation/report")
    assert response.json == report
    assert "attachment" in response.headers["Content-Disposition"]
    s3 = Mock()
    s3.generate_presigned_url.return_value = "https://download.example.test/prepared"
    monkeypatch.setattr(api_routes.boto3, "client", lambda *args, **kwargs: s3)
    response = client.get(f"/api/tasks/{USER_ID}/preparation/sites")
    assert response.status_code == 302
    assert response.location == "https://download.example.test/prepared"
    params = s3.generate_presigned_url.call_args.kwargs["Params"]
    assert params["Bucket"] == "owned-bucket"
    assert params["Key"] == "task/sites.parquet"


def test_preparation_download_hides_unowned_tasks(api_client):
    client, db = api_client
    db.query.return_value.filter.return_value.first.return_value = None
    assert client.get(f"/api/tasks/{USER_ID}/preparation/report").status_code == 404


@pytest.fixture
def callbacks(monkeypatch):
    functions = {}

    def callback(*args, **kwargs):
        def capture(function):
            functions[function.__name__] = function
            return function

        return capture

    registration.register_callbacks(
        SimpleNamespace(callback=callback, clientside_callback=lambda *a, **k: None)
    )
    monkeypatch.setattr(
        registration, "get_current_user", lambda: SimpleNamespace(id=USER_ID)
    )
    return functions


def test_missing_upload_token_returns_all_thirteen_outputs(callbacks, monkeypatch):
    monkeypatch.setattr(
        registration,
        "callback_context",
        SimpleNamespace(
            triggered=[{"prop_id": "confirm-site-upload-mapping-btn.n_clicks"}]
        ),
    )
    outputs = callbacks["handle_site_upload_flow"](
        None, 1, None, {"filename": "sites.geojson"}, None, None, "date", None
    )
    assert len(outputs) == 13
    assert outputs[0].color == "danger"
    assert "token missing" in outputs[0].children.lower()


@pytest.mark.parametrize(
    "trigger,payload,pending,error,authenticated",
    [
        ("site-upload-stream-payload", "{broken", None, None, True),
        (
            "site-upload-stream-payload",
            '{"status":"error","response":{"errors":["Invalid file"]}}',
            None,
            None,
            True,
        ),
        ("confirm-site-upload-mapping-btn", None, None, None, True),
        (
            "confirm-site-upload-mapping-btn",
            None,
            {"upload_token": "token"},
            ValueError("Invalid column mapping"),
            True,
        ),
        (
            "confirm-site-upload-mapping-btn",
            None,
            {"upload_token": "token"},
            RuntimeError("Broker unavailable"),
            True,
        ),
        ("cancel-site-upload-mapping-btn", None, None, None, True),
        ("confirm-site-upload-mapping-btn", None, None, None, False),
    ],
)
def test_upload_error_and_cancel_paths_have_complete_callback_shape(
    callbacks, monkeypatch, trigger, payload, pending, error, authenticated
):
    monkeypatch.setattr(
        registration,
        "callback_context",
        SimpleNamespace(triggered=[{"prop_id": f"{trigger}.value"}]),
    )
    if not authenticated:
        monkeypatch.setattr(registration, "get_current_user", lambda: None)
    if error:
        monkeypatch.setattr(
            registration, "create_user_site_upload", Mock(side_effect=error)
        )
    monkeypatch.setattr(registration, "report_exception", lambda: None)
    outputs = callbacks["handle_site_upload_flow"](
        payload, 1, 1, pending, None, None, "date", None
    )
    assert len(outputs) == 13


@pytest.mark.parametrize(
    "status,error,color,expected",
    [
        ("pending", None, "success", "Queued background"),
        ("pending", "Broker unavailable", "warning", "Dispatch is delayed"),
        ("failed", "No valid features", "danger", "Import failed"),
        ("cancelled", "Cancelled by user", "warning", "Import cancelled"),
    ],
)
def test_import_confirmation_surfaces_dispatch_delays_and_terminal_states(
    callbacks, monkeypatch, status, error, color, expected
):
    monkeypatch.setattr(
        registration,
        "callback_context",
        SimpleNamespace(
            triggered=[{"prop_id": "confirm-site-upload-mapping-btn.n_clicks"}]
        ),
    )
    monkeypatch.setattr(
        registration,
        "create_user_site_upload",
        Mock(
            return_value={
                "filename": "sites.geojson",
                "status": status,
                "error_message": error,
            }
        ),
    )
    outputs = callbacks["handle_site_upload_flow"](
        None, 1, 0, {"upload_token": "token"}, None, None, "date", None
    )
    assert len(outputs) == 13
    assert outputs[0].color == color
    assert expected in outputs[0].children
    if error:
        assert error in outputs[0].children


def test_mapping_only_requires_start_date_and_samples_are_visible(
    callbacks, monkeypatch
):
    pending = {"available_columns": ["date"], "n_features": 2}
    alert = callbacks["validate_site_upload_mapping_selection"](
        "", None, "date", "", pending
    )
    assert alert.color == "success"
    monkeypatch.setattr(
        registration,
        "callback_context",
        SimpleNamespace(triggered=[{"prop_id": "site-upload-stream-payload.value"}]),
    )
    outputs = callbacks["handle_site_upload_flow"](
        json.dumps(
            {
                "status": "ok",
                "response": {
                    "preview": {
                        "column_info": [
                            {"name": "date", "dtype": "str", "sample": "2000-01-01"}
                        ]
                    }
                },
            }
        ),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    assert len(outputs) == 13
    assert outputs[5][0]["value"] == ""
    assert "2000-01-01" in outputs[5][1]["label"]


def test_catalog_is_independent_of_fifty_import_history_rows():
    site_sets = [
        {"id": str(i), "name": f"Current {i}", "n_sites": i, "is_archived": i == 2}
        for i in range(75)
    ]
    uploads = [
        {"id": str(i), "site_set_id": str(i), "site_set_name": "Outdated"}
        for i in range(50)
    ]
    catalog = registration._site_catalog_rows(site_sets)
    assert len(catalog) == 74
    assert catalog[-1]["site_set_id"] == "74"
    history = registration._site_import_history_rows(uploads, site_sets)
    assert len(history) == 50
    assert history[0]["site_set_name"] == "Current 0"
    assert len(registration._site_catalog_rows(site_sets, True)) == 75


@pytest.mark.parametrize("returned_rows", [[], [{"feature_id": FEATURE_ID}]])
def test_filtered_preview_terminates_on_short_or_empty_block(
    callbacks, monkeypatch, returned_rows
):
    monkeypatch.setattr(
        registration, "get_user_site_set_detail", lambda *args: {"id": SET_ID}
    )
    monkeypatch.setattr(
        registration, "get_user_site_set_preview_rows", lambda *a, **k: returned_rows
    )
    response = callbacks["supply_preview_rows"](
        {
            "startRow": 100,
            "endRow": 200,
            "filterModel": {
                "feature_id": {
                    "filterType": "text",
                    "type": "equals",
                    "filter": FEATURE_ID,
                }
            },
        },
        {"site_set_id": SET_ID, "n_sites": 500},
    )
    assert response["rowCount"] == 100 + len(returned_rows)


def test_preview_does_not_trust_client_store_ownership(callbacks, monkeypatch):
    monkeypatch.setattr(registration, "get_user_site_set_detail", lambda *args: None)
    get_rows = Mock()
    monkeypatch.setattr(registration, "get_user_site_set_preview_rows", get_rows)
    response = callbacks["supply_preview_rows"](
        {"startRow": 0, "endRow": 100}, {"site_set_id": SET_ID, "n_sites": 500}
    )
    assert response == {"rowData": [], "rowCount": 0}
    get_rows.assert_not_called()


def test_upload_status_is_accessible_and_transfer_cancellable():
    components = {
        component.id: component
        for component in _walk(_site_upload_card("Import asynchronously"))
        if getattr(component, "id", None)
    }
    assert components["upload-status"].role == "status"
    assert (
        components["upload-status"].to_plotly_json()["props"]["aria-live"] == "polite"
    )
    assert "upload-sites-stream-cancel-btn" in components


def test_preparation_summary_shows_audit_and_download_links():
    task = SimpleNamespace(
        id=USER_ID,
        config={"sites_parquet_s3_uri": "s3://bucket/sites.parquet"},
        extra_metadata={
            "preparation": {"input_sites": 3, "excluded_sites": 1, "fragments": 5}
        },
    )
    components = list(_walk(_build_preparation_summary(task)))
    assert "Input sites" in components
    assert "Excluded sites" in components
    links = [component.href for component in components if hasattr(component, "href")]
    assert f"/api/tasks/{USER_ID}/preparation/report" in links
    assert f"/api/tasks/{USER_ID}/preparation/sites" in links


def test_results_keep_canonical_row_keys_with_duplicate_display_labels():
    totals = [
        SimpleNamespace(
            site_id=f"canonical-{index}",
            display_site_id="duplicate",
            site_name="Name",
            extrapolated_emissions_avoided_mgco2e=1,
            extrapolated_forest_loss_avoided_ha=2,
            area_ha=3,
            first_year=2000,
            last_year=2001,
            sampled_fraction=1,
        )
        for index in range(2)
    ]
    component = _build_results_content([], totals)
    grid = next(
        component
        for component in _walk(component)
        if getattr(component, "id", None) == "results-totals-table"
    )
    assert [row["site_id"] for row in grid.rowData] == ["canonical-0", "canonical-1"]
    assert all(row["display_site_id"] == "duplicate" for row in grid.rowData)
    assert grid.dashGridOptions["getRowId"]["function"] == "params.data.site_id"
    assert any(column["field"] == "display_site_id" for column in grid.columnDefs)
    ids = {getattr(item, "id", None) for item in _walk(component)}
    assert {"download-site-identity", "download-results"} <= ids


@pytest.mark.parametrize(
    "trigger,result_type,filename",
    [
        ("download-by-year", "by_site_year", "results_by_site_year.csv"),
        ("download-totals", "by_site_total", "results_by_site_total.csv"),
        ("download-site-identity", "site_identity", "site_identity.csv"),
    ],
)
def test_result_downloads_include_site_identity_mapping(
    callbacks, monkeypatch, trigger, result_type, filename
):
    monkeypatch.setattr(
        registration,
        "callback_context",
        SimpleNamespace(triggered=[{"prop_id": f"{trigger}.n_clicks"}]),
    )
    monkeypatch.setattr(
        registration, "_authorize_task_access", lambda task_id, token: True
    )
    requested = []

    def fake_download(task_id, kind):
        requested.append((task_id, kind))
        return "site_id,display_site_id,site_name\n"

    monkeypatch.setattr(registration, "download_results_csv", fake_download)
    result = callbacks["handle_download"](None, None, 1, "task-1", None)
    assert requested == [("task-1", result_type)]
    assert result == {
        "content": "site_id,display_site_id,site_name\n",
        "filename": filename,
    }


def test_site_identity_download_denied_without_task_access(callbacks, monkeypatch):
    from dash.exceptions import PreventUpdate

    monkeypatch.setattr(
        registration,
        "callback_context",
        SimpleNamespace(triggered=[{"prop_id": "download-site-identity.n_clicks"}]),
    )
    monkeypatch.setattr(
        registration, "_authorize_task_access", lambda task_id, token: False
    )
    download = Mock()
    monkeypatch.setattr(registration, "download_results_csv", download)
    with pytest.raises(PreventUpdate):
        callbacks["handle_download"](None, None, 1, "task-1", None)
    download.assert_not_called()


def test_paginated_site_grids_include_page_size_in_selector(monkeypatch):
    from layouts import admin

    monkeypatch.setattr(admin, "_build_category_options", list)
    layout = admin.admin_layout(SimpleNamespace(is_admin=True, name="Admin"))
    grids = {
        component.id: component.dashGridOptions
        for component in _walk(layout)
        if getattr(component, "id", None)
        in {"admin-combined-site-table", "admin-site-import-history"}
    }
    assert set(grids) == {"admin-combined-site-table", "admin-site-import-history"}
    for options in grids.values():
        assert options["pagination"] is True
        assert options["paginationPageSize"] in options["paginationPageSizeSelector"]
