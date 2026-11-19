"""Resume keeps the original execution, authorization, and checkpoint inputs."""

import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa
from callbacks import _registration as registration
from dash import Dash, html, no_update
from dash.exceptions import PreventUpdate
from layouts.task_detail import task_detail_layout
from services.analysis_task import can_resume_task, get_task_list, resume_task
from sqlalchemy.orm import Session
from trendsearth_client import TrendsEarthAPIError, TrendsEarthClient


@pytest.fixture
def resume_context(mocker):
    user = SimpleNamespace(
        id=uuid.uuid4(), name="Task owner", is_admin=False, session_id="current-session"
    )
    task = SimpleNamespace(
        id=uuid.uuid4(),
        submitted_by=user.id,
        name="Failed analysis",
        status="failed",
        progress=65,
        config={"workflow_backend": "nextflow", "random_seed": 42},
        extra_metadata={
            "batch_jobs": {"nextflow": "previous-driver"},
            "preparation": {"fragments": 5},
        },
        extract_job_id="api:original-execution",
        completed_at="previous-completion",
        error_message="Previous failure",
        submit_session_id="previous-session",
    )
    db = MagicMock()
    db.query.return_value.filter.return_value.with_for_update.return_value.first.return_value = task
    mocker.patch("services.analysis_task.get_db", return_value=db)
    token = mocker.patch("session_store.get_access_token", return_value="user-token")
    client = mocker.patch("trendsearth_client.TrendsEarthClient").return_value
    return user, task, db, client, token


def test_resume_preserves_execution_inputs_and_reactivates_polling(resume_context):
    user, task, db, client, token = resume_context
    resume_task(str(task.id), user)
    client.resume_execution.assert_called_once_with("original-execution")
    client.create_execution.assert_not_called()
    token.assert_called_once_with("current-session")
    assert task.extract_job_id == "api:original-execution"
    assert task.config["random_seed"] == 42
    assert task.extra_metadata == {"preparation": {"fragments": 5}}
    assert (task.status, task.completed_at, task.error_message) == (
        "submitted",
        None,
        None,
    )
    assert task.submit_session_id == "current-session"
    assert task.progress == 0
    db.commit.assert_called_once()
    db.close.assert_called_once()


@pytest.mark.parametrize("admin", [False, True])
def test_resume_authorization_uses_the_acting_users_token(resume_context, admin):
    user, task, db, client, token = resume_context
    task.submitted_by = uuid.uuid4()
    user.is_admin = admin
    if admin:
        resume_task(str(task.id), user)
        token.assert_called_once_with(user.session_id)
        client.resume_execution.assert_called_once()
    else:
        with pytest.raises(PermissionError):
            resume_task(str(task.id), user)
        client.resume_execution.assert_not_called()
        db.commit.assert_not_called()


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "cancelled"},
        {"status": "running"},
        {"status": "succeeded"},
        {"extract_job_id": None},
        {"config": {"workflow_backend": "batch"}},
    ],
)
def test_ineligible_tasks_cannot_be_resumed(resume_context, changes):
    user, task, db, client, _ = resume_context
    for key, value in changes.items():
        setattr(task, key, value)
    assert not can_resume_task(task, user)
    with pytest.raises(ValueError):
        resume_task(str(task.id), user)
    client.resume_execution.assert_not_called()
    db.commit.assert_not_called()


def test_prior_nextflow_job_metadata_supports_existing_tasks(resume_context):
    user, task, _, _, _ = resume_context
    task.config.pop("workflow_backend")
    assert can_resume_task(task, user)
    task.extra_metadata = {}
    assert not can_resume_task(task, user)


def test_task_list_loads_metadata_only_for_failed_legacy_tasks(mocker):
    engine = sa.create_engine("sqlite://")
    owner = uuid.uuid4()
    legacy_id, explicit_id = uuid.uuid4(), uuid.uuid4()
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "CREATE TABLE analysis_tasks (id TEXT PRIMARY KEY, name TEXT, "
                "status TEXT, progress FLOAT, n_sites INTEGER, submitted_by TEXT, "
                "submitter_name TEXT, submitter_email TEXT, config JSON, "
                "covariates TEXT, created_at DATETIME, submitted_at DATETIME, "
                "completed_at DATETIME, error_message TEXT, extract_job_id TEXT, "
                "metadata JSON)"
            )
        )
        connection.execute(
            sa.text(
                "INSERT INTO analysis_tasks "
                "(id, name, status, submitted_by, config, extract_job_id, metadata) "
                "VALUES (:id, 'task', 'failed', :owner, :config, 'api:execution', :metadata)"
            ),
            [
                {
                    "id": legacy_id.hex,
                    "owner": owner.hex,
                    "config": "{}",
                    "metadata": '{"batch_jobs":{"nextflow":"driver"}}',
                },
                {
                    "id": explicit_id.hex,
                    "owner": owner.hex,
                    "config": '{"workflow_backend":"nextflow"}',
                    "metadata": '{"large":"not needed"}',
                },
            ],
        )
    mocker.patch("services.analysis_task.get_db", return_value=Session(engine))
    tasks = {task.id: task for task in get_task_list()}
    user = SimpleNamespace(id=owner, is_admin=False)
    assert can_resume_task(tasks[legacy_id], user)
    assert can_resume_task(tasks[explicit_id], user)
    assert "extra_metadata" not in sa.inspect(tasks[legacy_id]).unloaded
    assert "extra_metadata" in sa.inspect(tasks[explicit_id]).unloaded
    engine.dispose()


def test_cleanup_rejection_keeps_failed_state_and_error(resume_context):
    user, task, db, client, _ = resume_context
    client.resume_execution.side_effect = TrendsEarthAPIError(
        "Nextflow child cleanup is still being verified"
    )
    with pytest.raises(TrendsEarthAPIError, match="cleanup"):
        resume_task(str(task.id), user)
    assert task.status == "failed"
    assert task.progress == 65
    assert task.error_message == "Previous failure"
    assert task.completed_at == "previous-completion"
    assert task.extra_metadata["batch_jobs"] == {"nextflow": "previous-driver"}
    db.commit.assert_not_called()
    db.close.assert_called_once()


@pytest.mark.parametrize("status", [202, 409])
def test_resume_client_posts_to_existing_execution_and_exposes_api_errors(
    mocker, status
):
    client = TrendsEarthClient(
        api_url="https://te.example/api/v1", access_token="signed-in"
    )
    response = MagicMock(status_code=status, ok=status < 400)
    response.json.return_value = {"detail": "Child cleanup is still being verified"}
    post = mocker.patch.object(client._session, "post", return_value=response)
    if status == 409:
        with pytest.raises(TrendsEarthAPIError, match="cleanup"):
            client.resume_execution("original")
    else:
        assert client.resume_execution("original") == response.json.return_value
    assert post.call_args.args == (
        "https://te.example/api/v1/execution/original/resume",
    )
    assert post.call_args.kwargs["headers"]["Authorization"] == "Bearer signed-in"


@pytest.fixture
def callbacks(mocker, resume_context):
    functions = {}

    def callback(*args, **kwargs):
        def capture(function):
            functions[function.__name__] = function
            return function

        return capture

    registration.register_callbacks(
        SimpleNamespace(
            callback=callback, clientside_callback=lambda *args, **kwargs: None
        )
    )
    user, task, _, _, _ = resume_context
    mocker.patch.object(registration, "get_current_user", return_value=user)
    mocker.patch.object(registration, "_authorize_task_access", return_value=True)
    mocker.patch.object(
        registration,
        "get_task_detail",
        return_value={
            "task": task,
            "sites": [],
            "results": [],
            "totals": [],
        },
    )
    mocker.patch.object(registration, "_build_raw_results", return_value=None)
    return functions


def test_resume_action_refreshes_status_and_restarts_the_disabled_interval(
    mocker, callbacks, resume_context
):
    _, task, _, _, _ = resume_context
    refresh = callbacks["refresh_task_detail"]
    before = refresh(0, "tab-raw-results", None, str(task.id), None)
    assert before[-2:] == (True, {})

    def accepted(*args):
        task.status = "submitted"

    mocker.patch.object(registration, "resume_task", side_effect=accepted)
    alert, event = callbacks["handle_resume_task"](1, str(task.id), None)
    assert alert.color == "success"
    assert event
    after = refresh(0, "tab-raw-results", event, str(task.id), None)
    assert after[-2:] == (False, {"display": "none"})
    assert after[1].children == "SUBMITTED"


def test_cleanup_message_is_visible_without_restarting_polling(mocker, callbacks):
    mocker.patch.object(
        registration,
        "resume_task",
        side_effect=TrendsEarthAPIError(
            "Nextflow child cleanup is still being verified"
        ),
    )
    alert, event = callbacks["handle_resume_task"](1, "task", None)
    assert "cleanup" in alert.children
    assert alert.color == "warning"
    assert event is no_update


@pytest.mark.parametrize(
    ("status", "backend", "owner", "admin", "expected"),
    [
        ("failed", "nextflow", True, False, True),
        ("failed", "nextflow", False, False, False),
        ("failed", "nextflow", False, True, True),
        ("failed", "batch", True, False, False),
        ("running", "nextflow", True, False, False),
        ("cancelled", "nextflow", True, False, False),
    ],
)
def test_task_list_resume_eligibility(
    mocker, callbacks, resume_context, status, backend, owner, admin, expected
):
    user, task, _, _, _ = resume_context
    user.is_admin = admin
    task.submitted_by = user.id if owner else uuid.uuid4()
    task.status = status
    task.config["workflow_backend"] = backend
    task.covariates = []
    task.n_sites = 0
    task.created_at = task.submitted_at = task.completed_at = None
    mocker.patch.object(registration, "get_task_list", return_value=[task])

    rows, _, _ = callbacks["refresh_task_list"](0, 0, False, None)

    assert rows[0]["can_resume"] is expected


def test_task_list_resume_refreshes_on_success_and_shows_api_errors(
    mocker, callbacks, resume_context
):
    _, task, _, _, _ = resume_context
    resume = mocker.patch.object(registration, "resume_task")
    event = {"value": {"action": "resume", "task_id": str(task.id)}}
    redirect, alert, refresh, complete = callbacks["handle_task_list_actions"](event)
    resume.assert_called_once_with(str(task.id), resume_context[0])
    assert redirect is no_update
    assert alert.color == "success"
    assert refresh
    assert complete

    resume.side_effect = TrendsEarthAPIError("Child cleanup is still underway")
    redirect, alert, refresh, complete = callbacks["handle_task_list_actions"](event)
    assert redirect is no_update
    assert alert.color == "warning"
    assert "cleanup" in alert.children
    assert refresh is no_update
    assert complete


@pytest.mark.parametrize(
    "error", [None, ValueError("Already cancelled"), RuntimeError()]
)
def test_list_cancel_releases_action_on_success_and_failure(mocker, callbacks, error):
    cancel = mocker.patch.object(registration, "cancel_task", side_effect=error)
    event = {"value": {"action": "cancel", "task_id": "task"}}

    redirect, alert, refresh, complete = callbacks["handle_task_list_actions"](event)

    cancel.assert_called_once()
    assert redirect is no_update
    assert alert.color == (
        "success"
        if error is None
        else "warning"
        if isinstance(error, ValueError)
        else "danger"
    )
    assert (refresh is not no_update) == (error is None)
    assert complete


def test_list_recompute_opens_submission_form_and_releases_action(callbacks):
    redirect, alert, refresh, complete = callbacks["handle_task_list_actions"](
        {"value": {"action": "recompute", "task_id": "task"}}
    )
    assert redirect.href == "/submit?recompute=task"
    assert alert is no_update
    assert refresh is no_update
    assert complete


@pytest.mark.parametrize(
    ("action", "button_id"),
    [("resume", "resume-task-btn"), ("cancel", "cancel-task-btn")],
)
def test_detail_actions_are_triggered_only_by_confirmation(
    callbacks, resume_context, action, button_id
):
    user, task, _, _, _ = resume_context
    app = Dash(__name__, suppress_callback_exceptions=True)
    app.layout = task_detail_layout(user, str(task.id))
    registration.register_callbacks(app)
    confirmation_id = f"confirm-{action}-task"
    key = next(
        name for name in app.callback_map if f"{action}-task-result.children" in name
    )
    assert app.callback_map[key]["inputs"] == [
        {"id": confirmation_id, "property": "submit_n_clicks"}
    ]

    def find(component, component_id):
        if getattr(component, "id", None) == component_id:
            return component
        children = getattr(component, "children", None)
        if children is None:
            return None
        if not isinstance(children, (list, tuple)):
            children = [children]
        for child in children:
            found = find(child, component_id)
            if found is not None:
                return found
        return None

    confirmation = find(app.layout, confirmation_id)
    assert confirmation.message
    assert confirmation.children.id == button_id


def test_registered_list_resume_emits_refresh_event(mocker, callbacks, resume_context):
    _, task, _, _, _ = resume_context
    resume = mocker.patch.object(registration, "resume_task")
    app = Dash(__name__, suppress_callback_exceptions=True)
    app.layout = html.Div()
    registration.register_callbacks(app)
    key = next(
        name for name in app.callback_map if "task-list-action-refresh.data" in name
    )
    outputs = app.callback_map[key]["output"]
    response = app.server.test_client().post(
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
                    "value": {"value": {"action": "resume", "task_id": str(task.id)}},
                }
            ],
            "state": [],
            "changedPropIds": ["task-list-table.cellRendererData"],
        },
    )
    assert response.status_code == 200
    assert response.json["response"]["task-list-action-refresh"]["data"]
    assert response.json["response"]["task-list-action-complete"]["data"]
    resume.assert_called_once()


def test_shared_views_never_offer_or_invoke_resume(mocker, callbacks):
    resume = mocker.patch.object(registration, "resume_task")
    outputs = callbacks["refresh_task_detail"](
        0, "tab-raw-results", None, "task", "share-token"
    )
    assert outputs[-1] == {"display": "none"}
    with pytest.raises(PreventUpdate):
        callbacks["handle_resume_task"](1, "task", "share-token")
    resume.assert_not_called()


def test_registered_dash_callback_restarts_polling_on_resume_event(
    callbacks, resume_context
):
    user, task, _, _, _ = resume_context
    app = Dash(__name__, suppress_callback_exceptions=True)
    app.layout = task_detail_layout(user, str(task.id))
    registration.register_callbacks(app)
    client = app.server.test_client()
    assert client.get("/_dash-layout").status_code == 200
    key = next(
        name
        for name in app.callback_map
        if "task-title.children" in name and "detail-refresh-interval.disabled" in name
    )
    outputs = app.callback_map[key]["output"]
    task.status = "submitted"
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
                    "id": "detail-refresh-interval",
                    "property": "n_intervals",
                    "value": 0,
                },
                {
                    "id": "detail-tabs",
                    "property": "active_tab",
                    "value": "tab-raw-results",
                },
                {
                    "id": "task-resume-refresh",
                    "property": "data",
                    "value": "resume-accepted",
                },
            ],
            "state": [
                {"id": "task-id-store", "property": "data", "value": str(task.id)},
                {"id": "share-token-store", "property": "data", "value": None},
                {"id": "task-plot-selection", "property": "data", "value": {}},
            ],
            "changedPropIds": ["task-resume-refresh.data"],
        },
    )
    assert response.status_code == 200
    updated = response.json["response"]
    assert updated["detail-refresh-interval"]["disabled"] is False
    assert updated["resume-task-control"]["style"] == {"display": "none"}
