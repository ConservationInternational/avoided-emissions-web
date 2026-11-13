"""Exercise the publication workflow's verification against API responses."""

import json
import runpy
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests
import yaml

pytestmark = pytest.mark.unit

WORKFLOW_PATH = (
    Path(__file__).resolve().parents[2] / ".github/workflows/publish-script.yml"
)


@pytest.fixture
def publication_check(tmp_path, monkeypatch):
    workflow = yaml.safe_load(WORKFLOW_PATH.read_text())
    step = next(
        step
        for step in workflow["jobs"]["publish-script"]["steps"]
        if step["name"] == "Verify publication"
    )
    run = step["run"]
    assert run.startswith("python - <<'PY'\n")
    source = run.split("\n", 1)[1].rsplit("\nPY", 1)[0]
    script = tmp_path / "verify_publication.py"
    script.write_text(source)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    expected = {
        "id": "test-script",
        "compute_type": "batch",
        "batch_image": "avoided-emissions-ranalysis:production-latest",
        "batch_execution_config": {
            "allowed_workflow_backends": ["nextflow"],
            "default_workflow_backend": "nextflow",
        },
    }
    (tmp_path / "r-analysis").mkdir()
    (tmp_path / "r-analysis/configuration.json").write_text(json.dumps(expected))
    (tmp_path / ".tecli.yml").write_text(
        yaml.safe_dump({"url_api": "https://api.example.test", "JWT": "test-token"})
    )
    response = Mock()
    response.json.return_value = {"data": {**expected, "status": "SUCCESS"}}
    get = Mock(return_value=response)
    monkeypatch.setattr(requests, "get", get)
    return script, response, get


def test_publication_verifies_live_policy(publication_check, capsys):
    script, response, get = publication_check

    runpy.run_path(str(script))

    response.raise_for_status.assert_called_once_with()
    get.assert_called_once_with(
        "https://api.example.test/api/v1/script/test-script",
        params={"include": "environment"},
        headers={"Authorization": "Bearer test-token"},
        timeout=60,
    )
    assert "execution policy verified" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("compute_type", "gee"),
        ("batch_image", "avoided-emissions-ranalysis:staging-latest"),
        ("batch_execution_config", {}),
        ("batch_execution_config", None),
    ],
)
def test_publication_rejects_stale_script(publication_check, field, value):
    script, response, _ = publication_check
    response.json.return_value["data"][field] = value

    with pytest.raises(SystemExit, match=f"verification failed for {field}"):
        runpy.run_path(str(script))


def test_publication_rejects_api_error(publication_check):
    script, response, _ = publication_check
    response.raise_for_status.side_effect = requests.HTTPError("403 Forbidden")

    with pytest.raises(requests.HTTPError, match="403 Forbidden"):
        runpy.run_path(str(script))
