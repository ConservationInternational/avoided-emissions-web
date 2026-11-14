"""Pinned driver publication without changing infrastructure or resume policy."""

import copy
import importlib.util
from pathlib import Path
from unittest.mock import Mock

import boto3
import pytest
from botocore.stub import Stubber

DEPLOY = Path(__file__).resolve().parents[2] / "deploy"
SPEC = importlib.util.spec_from_file_location(
    "nextflow_publication", DEPLOY / "publish_nextflow_driver.py"
)
assert SPEC is not None and SPEC.loader is not None
publication = importlib.util.module_from_spec(SPEC)

pytestmark = pytest.mark.unit

ACCOUNT = "123456789012"
REGION = "us-east-1"
PREFIX = f"arn:aws:batch:{REGION}:{ACCOUNT}"
SOURCE_ARN = f"{PREFIX}:job-definition/driver:4"
TARGET_ARN = f"{PREFIX}:job-definition/driver:5"
QUEUE = f"{PREFIX}:job-queue/driver"
REGISTRY = f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com"
IMAGE = f"{REGISTRY}/avoided-emissions-nextflow@sha256:" + "b" * 64


@pytest.fixture
def module(monkeypatch):
    monkeypatch.syspath_prepend(str(DEPLOY))
    SPEC.loader.exec_module(publication)
    return publication


@pytest.mark.parametrize("platform,bootstrap", [("EC2", False), ("FARGATE", True)])
def test_rollout_preserves_definition_and_pins_built_image(
    module, monkeypatch, platform, bootstrap
):
    original = (
        {}
        if bootstrap
        else {"batch_job_definition": SOURCE_ARN, "batch_job_queue": QUEUE}
    )
    original["nextflow_driver"] = {"job_definition": SOURCE_ARN, "job_queue": QUEUE}
    request = Mock(return_value={"data": original})
    monkeypatch.setattr(module, "api_request", request)
    source = {
        "jobDefinitionName": "driver",
        "jobDefinitionArn": SOURCE_ARN,
        "revision": 4,
        "status": "ACTIVE",
        "type": "container",
        "platformCapabilities": [platform],
        "containerProperties": {
            "image": f"{REGISTRY}/avoided-emissions-nextflow@sha256:" + "a" * 64,
            "command": ["python", "/opt/ae-workflow/driver.py"],
            "jobRoleArn": f"arn:aws:iam::{ACCOUNT}:role/driver",
            "executionRoleArn": f"arn:aws:iam::{ACCOUNT}:role/execution",
            "resourceRequirements": [
                {"type": "VCPU", "value": "2"},
                {"type": "MEMORY", "value": "4096"},
            ],
            "environment": [{"name": "AWS_DEFAULT_REGION", "value": REGION}],
        },
        "retryStrategy": {"attempts": 1},
        "timeout": {"attemptDurationSeconds": 605700},
        "propagateTags": True,
        "tags": {"Environment": "production"},
    }
    if platform == "FARGATE":
        source["containerProperties"]["networkConfiguration"] = {
            "assignPublicIp": "DISABLED"
        }
    expected = copy.deepcopy(source)
    for key in ("jobDefinitionArn", "revision", "status"):
        del expected[key]
    expected["containerProperties"]["image"] = IMAGE
    client = boto3.client(
        "batch",
        region_name=REGION,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    ecr = Mock()
    ecr.describe_images.return_value = {
        "imageDetails": [{"imageDigest": "sha256:" + "b" * 64}]
    }
    config = {
        "id": "script",
        "batch_execution_config": {"default_workflow_backend": "nextflow"},
    }
    with Stubber(client) as stub:
        stub.add_response(
            "describe_job_definitions",
            {"jobDefinitions": [source]},
            {"jobDefinitions": [SOURCE_ARN], "status": "ACTIVE"},
        )
        stub.add_response(
            "describe_job_definitions",
            {"jobDefinitions": [source]},
            {"jobDefinitionName": "driver", "status": "ACTIVE"},
        )
        stub.add_response(
            "register_job_definition",
            {
                "jobDefinitionName": "driver",
                "revision": 5,
                "jobDefinitionArn": TARGET_ARN,
            },
            expected,
        )
        result = module.prepare_publication(
            config,
            Mock(),
            "https://api.example",
            client,
            ecr,
            ACCOUNT,
            REGION,
            f"{REGISTRY}/avoided-emissions-nextflow:production-commit",
        )
        stub.assert_no_pending_responses()
    assert result == {
        **config,
        "batch_job_definition": TARGET_ARN,
        "batch_job_queue": QUEUE,
    }
    assert "batch_job_definition" not in config
    assert source["containerProperties"]["image"] != IMAGE
    assert all(
        call.kwargs["params"] == {"include": "environment,nextflow_driver"}
        for call in request.call_args_list
    )
    ecr.describe_images.assert_called_once_with(
        repositoryName="avoided-emissions-nextflow",
        imageIds=[{"imageTag": "production-commit"}],
    )


def test_missing_effective_settings_fail_before_registration(module, monkeypatch):
    monkeypatch.setenv("NEXTFLOW_DRIVER_JOB_DEFINITION", SOURCE_ARN)
    monkeypatch.setenv("NEXTFLOW_DRIVER_JOB_QUEUE", QUEUE)
    monkeypatch.setattr(
        module,
        "api_request",
        Mock(
            return_value={
                "data": {"batch_job_definition": SOURCE_ARN, "batch_job_queue": QUEUE}
            }
        ),
    )
    batch = Mock()
    with pytest.raises(ValueError, match="API did not return effective Nextflow"):
        module.prepare_publication(
            {"id": "script"},
            Mock(),
            "https://api.example",
            batch,
            Mock(),
            ACCOUNT,
            REGION,
            IMAGE,
        )
    batch.register_job_definition.assert_not_called()


@pytest.mark.parametrize(
    "change",
    [
        {"batch_job_definition": TARGET_ARN},
        {"nextflow_driver": {"job_definition": TARGET_ARN, "job_queue": QUEUE}},
        {"nextflow_driver": {"job_definition": SOURCE_ARN, "job_queue": "new-queue"}},
    ],
)
def test_publication_refuses_concurrent_explicit_or_default_changes(
    module, monkeypatch, change
):
    original = {"nextflow_driver": {"job_definition": SOURCE_ARN, "job_queue": QUEUE}}
    request = Mock(
        side_effect=[
            {"data": original},
            {"data": {**original, **change}},
        ]
    )
    monkeypatch.setattr(module, "api_request", request)
    monkeypatch.setattr(
        module,
        "source_definition",
        Mock(
            return_value={
                "jobDefinitionName": "driver",
                "containerProperties": {"image": "old"},
            }
        ),
    )
    monkeypatch.setattr(module, "pin_image", Mock(return_value=IMAGE))
    monkeypatch.setattr(module, "register_definition", Mock(return_value=TARGET_ARN))
    batch = Mock()
    batch.meta.service_model.operation_model.return_value.input_shape.members = {
        "jobDefinitionName": None,
        "containerProperties": None,
    }
    with pytest.raises(RuntimeError, match="changed during publication"):
        module.prepare_publication(
            {"id": "script"},
            Mock(),
            "https://api.example",
            batch,
            Mock(),
            ACCOUNT,
            REGION,
            IMAGE,
        )
    module.source_definition.assert_called_once_with(batch, SOURCE_ARN)


@pytest.mark.parametrize(
    "change,error",
    [
        ({}, None),
        ({"batch_job_definition": SOURCE_ARN}, "batch_job_definition"),
        ({"batch_job_queue": "old-queue"}, "batch_job_queue"),
        ({"batch_image": "old-image"}, "batch_image"),
        ({"batch_execution_config": {}}, "batch_execution_config"),
        (
            {"nextflow_driver": {"job_definition": SOURCE_ARN, "job_queue": QUEUE}},
            "effective driver",
        ),
        ({"public": False}, "not published"),
    ],
)
def test_verification_requires_published_script_with_selected_driver(
    module, monkeypatch, change, error
):
    config = {
        "id": "script",
        "compute_type": "batch",
        "batch_image": "avoided-emissions-ranalysis:production-latest",
        "batch_execution_config": {"default_workflow_backend": "nextflow"},
        "batch_job_definition": TARGET_ARN,
        "batch_job_queue": QUEUE,
    }
    published = {
        **config,
        "public": True,
        "nextflow_driver": {"job_definition": TARGET_ARN, "job_queue": QUEUE},
        **change,
    }
    request = Mock(return_value={"data": published})
    monkeypatch.setattr(module, "api_request", request)
    if error:
        with pytest.raises(RuntimeError, match=error):
            module.verify_publication(config, Mock(), "https://api.example")
    else:
        module.verify_publication(config, Mock(), "https://api.example")
    assert request.call_args.kwargs["params"] == {
        "include": "environment,nextflow_driver"
    }
