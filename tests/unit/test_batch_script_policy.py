"""Keep the published Batch policy aligned with the app's submission contract."""

import json
from pathlib import Path

import pytest
from services.analysis_task import (
    ALLOWED_MATCHING_JOB_QUEUES,
    DEFAULT_MATCHING_JOB_QUEUE,
)

CONFIG_PATH = Path(__file__).resolve().parents[2] / "r-analysis/configuration.json"


@pytest.fixture
def script_config():
    return json.loads(CONFIG_PATH.read_text())


def test_published_policy_covers_all_selectable_queues(script_config):
    queues = set(script_config["batch_execution_config"]["allowed_job_queues"])
    assert queues == ALLOWED_MATCHING_JOB_QUEUES
    assert DEFAULT_MATCHING_JOB_QUEUE in queues


def test_published_policy_covers_pipeline_commands(script_config):
    assert script_config["batch_execution_config"]["allowed_commands"] == [
        ["extract"],
        ["match"],
        ["match_chunk_{index}"],
        ["summarize"],
    ]


def test_published_policy_keeps_infrastructure_server_controlled(script_config):
    assert script_config["compute_type"] == "batch"
    assert script_config["batch_image"] == (
        "avoided-emissions-ranalysis:production-latest"
    )
    assert "allowed_job_definitions" not in script_config["batch_execution_config"]
    assert set(script_config["batch_execution_config"]) == {
        "allowed_commands",
        "allowed_job_queues",
    }
