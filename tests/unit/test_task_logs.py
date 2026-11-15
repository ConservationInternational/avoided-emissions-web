"""Task-list logs use the acting user's API access and expose failures."""

import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from callbacks import _registration as registration
from dash import Dash, html
from layouts.dashboard import dashboard_layout
from services.analysis_task import get_task_execution_logs
from trendsearth_client import TrendsEarthAPIError, TrendsEarthClient


def test_log_controls_remain_outside_the_scrollable_body():
    layout = dashboard_layout(None)
    modal = next(
        child
        for child in layout.children
        if getattr(child, "id", None) == "task-logs-modal"
    )
    assert modal.scrollable
    header, body = modal.children
    assert body.id == "task-logs-body"
    assert [button.id for button in header.children[1].children] == [
        "task-logs-refresh",
        "task-logs-close",
    ]


@pytest.mark.parametrize(
    "admin,owner,allowed",
    [(False, True, True), (False, False, False), (True, False, True)],
)
def test_task_logs_enforce_ownership_and_use_acting_token(
    mocker, admin, owner, allowed
):
    user = SimpleNamespace(id=uuid.uuid4(), is_admin=admin, session_id="session")
    task = SimpleNamespace(
        submitted_by=user.id if owner else uuid.uuid4(), extract_job_id="api:execution"
    )
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = task
    mocker.patch("services.analysis_task.get_db", return_value=db)
    token = mocker.patch("session_store.get_access_token", return_value="user-token")
    client = mocker.patch("trendsearth_client.TrendsEarthClient").return_value
    client.get_execution_logs.return_value = [{"text": "started"}]

    if allowed:
        assert get_task_execution_logs(str(uuid.uuid4()), user) == [{"text": "started"}]
        token.assert_called_once_with("session")
        client.get_execution_logs.assert_called_once_with("execution")
    else:
        with pytest.raises(PermissionError):
            get_task_execution_logs(str(uuid.uuid4()), user)
        token.assert_not_called()
        client.get_execution_logs.assert_not_called()
    db.close.assert_called_once()


def test_client_surfaces_api_failures_and_invalid_log_responses(mocker):
    client = TrendsEarthClient(
        api_url="https://te.example/api/v1", access_token="token"
    )
    response = MagicMock(status_code=403, ok=False)
    response.json.return_value = {"detail": "Access denied"}
    get = mocker.patch.object(client._session, "get", return_value=response)
    with pytest.raises(TrendsEarthAPIError, match="Access denied"):
        client.get_execution_logs("execution")
    assert get.call_args.args[0] == "https://te.example/api/v1/execution/execution/log"
    assert get.call_args.kwargs["headers"]["Authorization"] == "Bearer token"

    response.ok = True
    response.json.return_value = {"data": None}
    with pytest.raises(TrendsEarthAPIError, match="Invalid execution logs"):
        client.get_execution_logs("execution")
    response.json.return_value = {"data": [{"text": "message"}]}
    assert client.get_execution_logs("execution", last_id=7) == [{"text": "message"}]
    assert get.call_args.kwargs["params"] == {"last-id": 7}


def test_log_modal_loads_refreshes_and_closes(mocker):
    app = Dash(__name__, suppress_callback_exceptions=True)
    app.layout = html.Div()
    registration.register_callbacks(app)
    user = SimpleNamespace(id=uuid.uuid4(), is_admin=False)
    mocker.patch.object(registration, "get_current_user", return_value=user)
    logs = mocker.patch.object(
        registration,
        "get_task_execution_logs",
        return_value=[
            {"register_date": "2026-01-01", "level": "INFO", "text": "Started"},
            {"register_date": "2026-01-02", "level": "ERROR", "text": "Failed"},
        ],
    )
    key = next(name for name in app.callback_map if "task-logs-modal.is_open" in name)
    outputs = app.callback_map[key]["output"]
    client = app.server.test_client()

    def post(trigger, is_open, task_id):
        response = client.post(
            "/_dash-update-component",
            json={
                "output": key,
                "outputs": [
                    {"id": output.component_id, "property": output.component_property}
                    for output in outputs
                ],
                "inputs": [
                    {
                        "id": "task-list-table",
                        "property": "cellRendererData",
                        "value": {"value": {"action": "logs", "task_id": "task"}},
                    },
                    {"id": "task-logs-refresh", "property": "n_clicks", "value": 1},
                    {"id": "task-logs-interval", "property": "n_intervals", "value": 1},
                    {"id": "task-logs-close", "property": "n_clicks", "value": 1},
                ],
                "state": [
                    {"id": "task-logs-modal", "property": "is_open", "value": is_open},
                    {"id": "task-logs-task-id", "property": "data", "value": task_id},
                ],
                "changedPropIds": [trigger],
            },
        )
        assert response.status_code == 200
        return response.json["response"]

    opened = post("task-list-table.cellRendererData", False, None)
    assert opened["task-logs-modal"]["is_open"]
    assert opened["task-logs-interval"]["disabled"] is False
    assert opened["task-logs-body"]["children"]["props"]["children"].startswith(
        "2026-01-02"
    )
    post("task-logs-refresh.n_clicks", True, "task")
    assert logs.call_count == 2
    logs.side_effect = TrendsEarthAPIError("Access denied")
    failed = post("task-logs-interval.n_intervals", True, "task")
    assert failed["task-logs-body"]["children"]["props"]["children"] == "Access denied"
    closed = post("task-logs-close.n_clicks", True, "task")
    assert not closed["task-logs-modal"]["is_open"]
    assert closed["task-logs-interval"]["disabled"]
    assert logs.call_count == 3
