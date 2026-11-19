"""Task parameters are displayed as formatted text without fetching task results."""

import json

from callbacks import _registration as registration
from dash import Dash
from layouts.dashboard import dashboard_layout


def test_params_modal_opens_closes_and_ignores_other_actions():
    app = Dash(__name__, suppress_callback_exceptions=True)
    app.layout = dashboard_layout(None)
    registration.register_callbacks(app)
    key = next(name for name in app.callback_map if "task-params-modal.is_open" in name)
    outputs = app.callback_map[key]["output"]
    client = app.server.test_client()
    params = {
        "caliper_width": 0,
        "covariates": ["precip", "elev"],
        "nested": {"enabled": True, "optional": None},
        "label": "<script>alert('test')</script>",
    }

    def post(action="params", trigger="task-list-table.cellRendererData"):
        return client.post(
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
                        "value": {
                            "value": {
                                "action": action,
                                "task_name": "Test analysis",
                                "params": params,
                            }
                        },
                    },
                    {"id": "task-params-close", "property": "n_clicks", "value": 1},
                ],
                "state": [],
                "changedPropIds": [trigger],
            },
        )

    response = post()
    assert response.status_code == 200
    opened = response.json["response"]
    assert opened["task-params-modal"]["is_open"]
    assert opened["task-params-title"]["children"] == "Params: Test analysis"
    body = opened["task-params-body"]["children"]
    assert body["type"] == "Pre"
    assert body["props"]["children"] == json.dumps(params, indent=2, sort_keys=True)
    assert "\n  " in body["props"]["children"]

    closed = post(trigger="task-params-close.n_clicks")
    assert closed.json["response"]["task-params-modal"]["is_open"] is False
    assert post(action="logs").status_code == 204
