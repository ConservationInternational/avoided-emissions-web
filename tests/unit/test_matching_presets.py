"""Unified matching preset persistence and form regressions."""

import uuid
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from callbacks import _registration as registration
from dash import Dash, dcc, html, no_update
from layouts.submit import submit_layout
from services import preset

pytestmark = pytest.mark.unit


@pytest.fixture
def callbacks(monkeypatch, sample_user_id):
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
        registration, "get_current_user", lambda: SimpleNamespace(id=sample_user_id)
    )
    return functions


@pytest.fixture
def settings():
    return {
        "max_treatment_pixels": 800,
        "control_multiplier": 30,
        "min_site_area_ha": 50,
        "min_glm_treatment_pixels": 20,
        "caliper_width": 0,
        "max_controls_per_treatment": "3",
        "min_control_distance_km": 0,
        "matching_method": "nearest",
        "separation_fallback_mahalanobis": False,
        "n_replicates": 5,
        "match_memory_gb": 60,
        "matching_job_queue": "ae-spot-gp3",
    }


def test_form_preset_round_trip(monkeypatch, sample_user_id, settings):
    user = SimpleNamespace(id=sample_user_id, name="User", is_admin=False)
    monkeypatch.setattr(registration, "get_current_user", lambda: user)
    app = Dash(__name__, suppress_callback_exceptions=True)
    app.layout = html.Div([dcc.Location(id="url"), submit_layout(user)])
    registration.register_callbacks(app)
    client = app.server.test_client()

    def invoke(function_name, values):
        key, metadata = next(
            (key, metadata)
            for key, metadata in app.callback_map.items()
            if metadata.get("callback")
            and metadata["callback"].__name__ == function_name
        )
        response = client.post(
            "/_dash-update-component",
            json={
                "output": key,
                "outputs": [output.to_dict() for output in metadata["output"]],
                "inputs": [
                    {**field, "value": values[field["id"]]}
                    for field in metadata["inputs"]
                ],
                "state": [
                    {**field, "value": values[field["id"]]}
                    for field in metadata["state"]
                ],
                "changedPropIds": [f"{metadata['inputs'][0]['id']}.n_clicks"],
            },
        )
        assert response.status_code == 200
        return response.json["response"]

    save = Mock()
    monkeypatch.setattr(registration, "save_matching_preset", save)
    data = [
        {
            "id": "preset-1",
            "name": "Standard",
            "covariates": ["elev", "temp"],
            "exact_match_vars": ["admin1", "pa"],
            "settings": settings,
        }
    ]
    monkeypatch.setattr(registration, "get_matching_presets", lambda _id: data)

    result = invoke(
        "save_preset",
        {
            "save-preset-btn": 1,
            "preset-name-input": " Standard ",
            "covariate-selection": ["elev", "temp"],
            "exact-match-selection": ["admin1", "pa"],
            **{name.replace("_", "-"): value for name, value in settings.items()},
        },
    )

    save.assert_called_once_with(
        sample_user_id, "Standard", ["elev", "temp"], ["admin1", "pa"], settings
    )
    assert result["presets-store"]["data"] == data
    assert result["preset-name-input"]["value"] == ""
    loaded = invoke(
        "load_preset",
        {
            "load-preset-btn": 1,
            "preset-selector": "preset-1",
            "presets-store": result["presets-store"]["data"],
        },
    )
    assert loaded["covariate-selection"]["value"] == ["elev", "temp"]
    assert loaded["exact-match-selection"]["value"] == ["admin1", "pa"]
    for name, value in settings.items():
        assert loaded[name.replace("_", "-")]["value"] == value
    assert "random-seed" not in loaded


@pytest.mark.parametrize("preset_id", [None, "missing"])
def test_unselected_or_missing_preset_leaves_all_fields_unchanged(callbacks, preset_id):
    result = callbacks["load_preset"](1, preset_id, [{"id": "other"}])
    assert len(result) == 15
    assert all(value is no_update for value in result[:-1])


@pytest.mark.parametrize("existing", [False, True])
def test_save_and_read_persist_selections_and_settings_together(
    monkeypatch, mock_db, sample_user_id, settings, existing
):
    from models import MatchingPreset

    saved_id = uuid.uuid4()
    old = (
        MatchingPreset(
            id=saved_id,
            user_id=sample_user_id,
            name="Standard",
            covariates=["precip"],
            exact_match_vars=["admin0"],
            settings={"caliper_width": 0.5},
        )
        if existing
        else None
    )
    mock_db.query.return_value.filter.return_value.first.return_value = old
    monkeypatch.setattr(preset, "get_db", lambda: mock_db)
    if not existing:
        mock_db.add.side_effect = lambda row: setattr(row, "id", saved_id)

    result = preset.save_matching_preset(
        sample_user_id, "Standard", ["elev"], ["pa"], settings
    )

    row = old if existing else mock_db.add.call_args.args[0]
    assert result == str(saved_id)
    mock_db.commit.assert_called_once()
    filters = mock_db.query.return_value.filter.call_args.args
    assert {clause.right.value for clause in filters} == {
        sample_user_id,
        "Standard",
    }
    mock_db.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
        row
    ]
    assert preset.get_matching_presets(sample_user_id) == [
        {
            "id": str(saved_id),
            "name": "Standard",
            "covariates": ["elev"],
            "exact_match_vars": ["pa"],
            "settings": settings,
        }
    ]


def test_failed_save_rolls_back_both_selections_and_settings(
    monkeypatch, mock_db, sample_user_id, settings
):
    monkeypatch.setattr(preset, "get_db", lambda: mock_db)
    mock_db.commit.side_effect = RuntimeError("Database unavailable")
    with pytest.raises(RuntimeError, match="Database unavailable"):
        preset.save_matching_preset(
            sample_user_id, "Standard", ["elev"], ["pa"], settings
        )
    mock_db.rollback.assert_called_once()
    mock_db.close.assert_called_once()
