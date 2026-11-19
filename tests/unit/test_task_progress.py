"""API progress reaches the task list without changing task lifecycle behavior."""

import runpy
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from callbacks import _registration as registration
from models import AnalysisTask
from services.analysis_task import adopt_api_execution
from tasks.analysis import poll_batch_tasks

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("progress", [0, 42.5, 99, None])
def test_poll_caches_progress_even_when_status_is_unchanged(mocker, progress):
    task = AnalysisTask(
        id=uuid.uuid4(),
        name="Analysis",
        status="running",
        progress=10,
        extract_job_id="api:execution",
    )
    db = MagicMock()
    db.query.return_value.filter.return_value.all.return_value = [task]
    mocker.patch("models.get_db", return_value=db)
    mocker.patch("config.Config.ENABLE_TASK_ADOPTION", False)
    client = mocker.patch("trendsearth_client.TrendsEarthClient").return_value
    data = {"status": "RUNNING"}
    if progress is not None:
        data["progress"] = progress
    client.get_execution.return_value = {"data": data}

    result = poll_batch_tasks.run()

    assert task.progress == progress
    assert task.status == "running"
    assert result["updated"] == 0
    db.commit.assert_called_once()


def test_adoption_keeps_api_progress():
    task = adopt_api_execution(
        {
            "id": "execution",
            "user_id": str(uuid.uuid4()),
            "status": "RUNNING",
            "progress": 40,
        },
        MagicMock(),
    )
    assert task.progress == 40


def test_task_list_passes_cached_percentage_to_renderer(mocker):
    callbacks = {}

    def callback(*args, **kwargs):
        def capture(function):
            callbacks[function.__name__] = function
            return function

        return capture

    registration.register_callbacks(
        SimpleNamespace(
            callback=callback, clientside_callback=lambda *args, **kwargs: None
        )
    )
    user = SimpleNamespace(id=uuid.uuid4(), is_admin=False)
    task = AnalysisTask(
        id=uuid.uuid4(),
        name="Analysis",
        status="running",
        progress=42.5,
        covariates=["precip", "elev"],
        config={
            "max_treatment_pixels": 1000,
            "control_multiplier": 50,
            "caliper_width": 0,
            "max_controls_per_treatment": 1,
            "exact_match_vars": ["admin1"],
            "nested": {"enabled": True},
        },
    )
    mocker.patch.object(registration, "get_current_user", return_value=user)
    get_tasks = mocker.patch.object(registration, "get_task_list", return_value=[task])

    rows, columns, count = callbacks["refresh_task_list"](1, 0, False)

    assert rows[0]["progress"] == 42.5
    assert rows[0]["params"] == {**task.config, "covariates": task.covariates}
    assert not {
        "max_treatment_pixels",
        "control_multiplier",
        "caliper_width",
        "max_controls_per_treatment",
        "covariates_short",
        "exact_matches_short",
    }.intersection(column["field"] for column in columns)
    assert (
        next(col for col in columns if col["field"] == "progress")["cellRenderer"]
        == "TaskProgress"
    )
    assert count == "Total: 1"
    get_tasks.assert_called_once_with(user_id=user.id)


def test_progress_migration_preserves_existing_tasks_and_can_be_reversed():
    path = (
        Path(__file__).resolve().parents[2]
        / "webapp/migrations/versions/0035a1b2c3f6_add_task_progress.py"
    )
    migration = runpy.run_path(str(path))
    engine = sa.create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(
            sa.text("CREATE TABLE analysis_tasks (id INTEGER PRIMARY KEY)")
        )
        connection.execute(sa.text("INSERT INTO analysis_tasks (id) VALUES (1)"))
        with Operations.context(MigrationContext.configure(connection)):
            migration["upgrade"]()
            assert connection.execute(
                sa.text("SELECT id, progress FROM analysis_tasks")
            ).one() == (1, None)
            connection.execute(sa.text("UPDATE analysis_tasks SET progress = 42.5"))
            assert (
                connection.scalar(sa.text("SELECT progress FROM analysis_tasks"))
                == 42.5
            )
            migration["downgrade"]()
            assert connection.scalar(sa.text("SELECT id FROM analysis_tasks")) == 1
            assert "progress" not in {
                column["name"]
                for column in sa.inspect(connection).get_columns("analysis_tasks")
            }
    engine.dispose()
