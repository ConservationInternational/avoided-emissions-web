"""Exercise the publication workflow's verification against API responses."""

import json
import runpy
from pathlib import Path
import sys
from unittest.mock import Mock

import pytest
import requests
import yaml

pytestmark = pytest.mark.unit

WORKFLOW_PATH = (
    Path(__file__).resolve().parents[2] / ".github/workflows/publish-script.yml"
)
VERIFY_SCRIPT = Path(__file__).resolve().parents[2] / "deploy/publish_nextflow_driver.py"


@pytest.fixture
def publication_check(tmp_path, monkeypatch):
    workflow = yaml.safe_load(WORKFLOW_PATH.read_text())
    step = next(
        step
        for step in workflow["jobs"]["publish-script"]["steps"]
        if step["name"] == "Verify publication"
    )
    assert step["run"] == "python deploy/publish_nextflow_driver.py --verify"
    script = VERIFY_SCRIPT
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
        "batch_job_definition": (
            "arn:aws:batch:us-east-1:123456789012:job-definition/driver:5"
        ),
        "batch_job_queue": "arn:aws:batch:us-east-1:123456789012:job-queue/driver",
    }
    (tmp_path / "r-analysis").mkdir()
    (tmp_path / "r-analysis/configuration.json").write_text(json.dumps(expected))
    (tmp_path / ".tecli.yml").write_text(
        yaml.safe_dump({"url_api": "https://api.example.test", "JWT": "test-token"})
    )
    response = Mock()
    response.json.return_value = {"data": {**expected, "status": "SUCCESS"}}
    response.status_code = 200
    api = Mock()
    api.headers = {}
    api.request.return_value = response
    monkeypatch.setattr(requests, "Session", Mock(return_value=api))
    monkeypatch.setattr(sys, "argv", [str(script), "--verify"])
    return script, response, api


def test_publication_verifies_live_policy(publication_check, capsys):
    script, response, api = publication_check

    runpy.run_path(str(script), run_name="__main__")

    response.raise_for_status.assert_called_once_with()
    api.request.assert_called_once_with(
        "GET",
        "https://api.example.test/api/v1/script/test-script",
        timeout=60,
        allow_redirects=False,
        params={"include": "environment,nextflow_driver"},
    )
    assert api.headers == {"Authorization": "Bearer test-token"}
    assert "execution policy, and pinned driver verified" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("compute_type", "gee"),
        ("batch_image", "avoided-emissions-ranalysis:staging-latest"),
        ("batch_execution_config", {}),
        ("batch_execution_config", None),
        (
            "batch_job_definition",
            "arn:aws:batch:us-east-1:123456789012:job-definition/driver:4",
        ),
        ("batch_job_definition", None),
        ("batch_job_queue", "arn:aws:batch:us-east-1:123456789012:job-queue/old"),
        ("batch_job_queue", None),
    ],
)
def test_publication_rejects_stale_script(publication_check, field, value):
    script, response, _ = publication_check
    response.json.return_value["data"][field] = value

    with pytest.raises(RuntimeError, match=f"verification failed for {field}"):
        runpy.run_path(str(script), run_name="__main__")


@pytest.mark.parametrize("field", ["batch_job_definition", "batch_job_queue"])
def test_publication_rejects_missing_driver_setting(publication_check, field):
    script, response, _ = publication_check
    del response.json.return_value["data"][field]

    with pytest.raises(RuntimeError, match=f"verification failed for {field}"):
        runpy.run_path(str(script), run_name="__main__")


def test_publication_rejects_api_error(publication_check):
    script, response, _ = publication_check
    response.raise_for_status.side_effect = requests.HTTPError("403 Forbidden")

    with pytest.raises(requests.HTTPError, match="403 Forbidden"):
        runpy.run_path(str(script), run_name="__main__")
