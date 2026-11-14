"""Driver migration safety, resource conversion, and API cutover regressions."""

import copy
import importlib.util
import json
from pathlib import Path
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

PATH = Path(__file__).resolve().parents[2] / "deploy/migrate_nextflow_driver.py"
SPEC = importlib.util.spec_from_file_location("fargate_migration", PATH)
assert SPEC is not None and SPEC.loader is not None
migration = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(migration)

pytestmark = pytest.mark.unit

ACCOUNT = "123456789012"
REGION = "us-east-1"
PREFIX = f"arn:aws:batch:{REGION}:{ACCOUNT}"
IMAGE = f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com/driver@sha256:" + "a" * 64
SOURCE_ARN = f"{PREFIX}:job-definition/driver:4"
TARGET_ARN = f"{PREFIX}:job-definition/ae-nextflow-driver-staging-fargate:1"
QUEUE_ARN = f"{PREFIX}:job-queue/ae-nextflow-driver-staging-fargate"


@pytest.fixture
def source():
    return {
        "jobDefinitionName": "driver",
        "jobDefinitionArn": SOURCE_ARN,
        "type": "container",
        "revision": 4,
        "platformCapabilities": ["EC2"],
        "retryStrategy": {"attempts": 3},
        "containerProperties": {
            "image": IMAGE,
            "vcpus": 2,
            "memory": 4096,
            "command": ["python", "/opt/ae-workflow/driver.py"],
            "jobRoleArn": f"arn:aws:iam::{ACCOUNT}:role/driver",
            "executionRoleArn": f"arn:aws:iam::{ACCOUNT}:role/execution",
            "environment": [
                {"name": "AE_NEXTFLOW_SPOT_QUEUE", "value": "ae-spot-gp3"},
                {"name": "AWS_DEFAULT_REGION", "value": "wrong-region"},
            ],
            "volumes": [],
            "mountPoints": [],
            "ulimits": [],
            "privileged": False,
        },
    }


def convert(source):
    return migration.fargate_definition(
        source, "fargate-driver", IMAGE, REGION, "DISABLED", {"Environment": "staging"}
    )


def test_conversion_preserves_workflow_and_roles_without_ec2_settings(source):
    before = copy.deepcopy(source)
    definition = convert(source)
    container = definition["containerProperties"]
    assert source == before
    assert definition["platformCapabilities"] == ["FARGATE"]
    assert definition["retryStrategy"] == {"attempts": 1}
    assert definition["timeout"]["attemptDurationSeconds"] >= 168 * 3600 + 900
    for key in ("command", "jobRoleArn", "executionRoleArn"):
        assert container[key] == source["containerProperties"][key]
    assert {item["type"]: item["value"] for item in container["resourceRequirements"]} == {
        "VCPU": "2", "MEMORY": "4096"
    }
    environment = {item["name"]: item["value"] for item in container["environment"]}
    assert environment == {
        "AE_NEXTFLOW_SPOT_QUEUE": "ae-spot-gp3",
        "AWS_DEFAULT_REGION": REGION,
        "AWS_REGION": REGION,
    }
    assert not {"vcpus", "memory", "privileged", "ulimits", "volumes", "mountPoints"} & container.keys()


@pytest.mark.parametrize(
    "changes, message",
    [
        ({"privileged": True}, "privileged"),
        ({"volumes": [{"name": "docker", "host": {"sourcePath": "/var/run/docker.sock"}}]}, "volumes"),
        ({"linuxParameters": {"sharedMemorySize": 128}}, "linuxParameters"),
        ({"memory": 5000}, "Unsupported Fargate memory"),
        ({"executionRoleArn": ""}, "executionRoleArn"),
        ({"resourceRequirements": [{"type": "GPU", "value": "1"}]}, "GPU"),
    ],
)
def test_conversion_refuses_incompatible_settings(source, changes, message):
    source["containerProperties"].update(changes)
    with pytest.raises(ValueError, match=message):
        convert(source)


def test_pins_source_image_tag_without_selecting_a_new_image():
    ecr = Mock()
    ecr.describe_images.return_value = {"imageDetails": [{"imageDigest": "sha256:" + "a" * 64}]}
    image = migration.pin_image(
        ecr, f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com/driver:production-commit",
        ACCOUNT, REGION,
    )
    assert image == IMAGE
    ecr.describe_images.assert_called_once_with(
        repositoryName="driver", imageIds=[{"imageTag": "production-commit"}]
    )


def test_generated_definition_registers_against_real_batch_api_model(source):
    client = migration.boto3.client(
        "batch", region_name=REGION,
        aws_access_key_id="testing", aws_secret_access_key="testing",
    )
    definition = convert(source)
    with Stubber(client) as stub:
        stub.add_response(
            "describe_job_definitions", {"jobDefinitions": []},
            {"jobDefinitionName": "fargate-driver", "status": "ACTIVE"},
        )
        stub.add_response(
            "register_job_definition",
            {"jobDefinitionArn": TARGET_ARN, "jobDefinitionName": "fargate-driver", "revision": 1},
            definition,
        )
        assert migration.register_definition(client, definition) == TARGET_ARN
        stub.assert_no_pending_responses()


def test_existing_matching_revision_is_reused(source):
    definition = convert(source)
    batch = Mock()
    batch.get_paginator.return_value.paginate.return_value = [{
        "jobDefinitions": [{**definition, "jobDefinitionArn": TARGET_ARN, "revision": 1}]
    }]
    assert migration.register_definition(batch, definition) == TARGET_ARN
    batch.register_job_definition.assert_not_called()


def test_compute_environment_reuse_refuses_spot_instead_of_changing_it():
    batch = Mock()
    batch.describe_compute_environments.return_value = {"computeEnvironments": [{
        "type": "MANAGED",
        "computeResources": {
            "type": "FARGATE_SPOT", "subnets": ["subnet-a"],
            "securityGroupIds": ["sg-a"], "maxvCpus": 16,
        },
    }]}
    with pytest.raises(ValueError, match="differs from plan"):
        migration.ensure_infrastructure(
            batch, "driver", ["subnet-a"], ["sg-a"], 16, {}
        )
    batch.create_compute_environment.assert_not_called()
    batch.create_job_queue.assert_not_called()


def test_provisions_dedicated_fargate_resources_against_batch_api():
    client = migration.boto3.client(
        "batch", region_name=REGION,
        aws_access_key_id="testing", aws_secret_access_key="testing",
    )
    name = "ae-nextflow-driver-staging-fargate"
    compute_arn = f"{PREFIX}:compute-environment/{name}"
    order = [{"order": 1, "computeEnvironment": compute_arn}]
    with Stubber(client) as stub:
        stub.add_response(
            "describe_compute_environments", {"computeEnvironments": []},
            {"computeEnvironments": [name]},
        )
        stub.add_response(
            "create_compute_environment",
            {"computeEnvironmentName": name, "computeEnvironmentArn": compute_arn},
            {
                "computeEnvironmentName": name, "type": "MANAGED", "state": "ENABLED",
                "computeResources": {
                    "type": "FARGATE", "maxvCpus": 16,
                    "subnets": ["subnet-a"], "securityGroupIds": ["sg-a"],
                },
                "tags": {},
            },
        )
        stub.add_response(
            "describe_compute_environments",
            {"computeEnvironments": [{
                "computeEnvironmentName": name, "computeEnvironmentArn": compute_arn,
                "ecsClusterArn": f"arn:aws:ecs:{REGION}:{ACCOUNT}:cluster/driver",
                "status": "VALID", "state": "ENABLED",
            }]},
            {"computeEnvironments": [name]},
        )
        stub.add_response(
            "describe_job_queues", {"jobQueues": []}, {"jobQueues": [name]},
        )
        stub.add_response(
            "create_job_queue", {"jobQueueName": name, "jobQueueArn": QUEUE_ARN},
            {
                "jobQueueName": name, "state": "ENABLED", "priority": 1,
                "computeEnvironmentOrder": order, "tags": {},
            },
        )
        stub.add_response(
            "describe_job_queues",
            {"jobQueues": [{
                "jobQueueName": name, "jobQueueArn": QUEUE_ARN,
                "state": "ENABLED", "status": "VALID",
                "priority": 1, "computeEnvironmentOrder": order,
            }]},
            {"jobQueues": [name]},
        )
        assert migration.ensure_infrastructure(
            client, name, ["subnet-a"], ["sg-a"], 16, {}
        ) == QUEUE_ARN
        stub.assert_no_pending_responses()


def test_failed_canary_is_explicit_and_timeout_terminates_only_canary(monkeypatch):
    batch = Mock()
    batch.submit_job.return_value = {"jobId": "canary-id"}
    batch.describe_jobs.return_value = {
        "jobs": [{"status": "FAILED", "statusReason": "CannotPullContainerError"}]
    }
    with pytest.raises(RuntimeError, match="CannotPullContainerError"):
        migration.run_canary(batch, QUEUE_ARN, TARGET_ARN)
    batch.terminate_job.assert_not_called()
    monkeypatch.setattr(migration.time, "monotonic", Mock(side_effect=[0, 1201]))
    with pytest.raises(TimeoutError):
        migration.run_canary(batch, QUEUE_ARN, TARGET_ARN)
    batch.terminate_job.assert_called_once_with(
        jobId="canary-id", reason="Migration canary interrupted"
    )


def test_existing_api_policy_is_not_overwritten():
    iam = Mock()
    iam.get_role_policy.return_value = {"PolicyDocument": {"Statement": []}}
    with pytest.raises(ValueError, match="not replacing"):
        migration.ensure_api_policy(
            iam, "api-role", "migration-policy", {"Statement": [{"Effect": "Allow"}]}
        )
    iam.put_role_policy.assert_not_called()
    migration.ensure_api_policy(
        iam, "api-role", "migration-policy", {"Statement": []}
    )
    iam.put_role_policy.assert_not_called()


@pytest.fixture
def setup_main(monkeypatch, source):
    aws = Mock()
    clients = {key: Mock() for key in ("sts", "iam", "batch", "ec2", "ecr")}
    aws.client.side_effect = clients.__getitem__
    monkeypatch.setattr(migration.boto3, "Session", Mock(return_value=aws))
    clients["sts"].get_caller_identity.return_value = {"Account": ACCOUNT}
    clients["iam"].get_role_policy.side_effect = ClientError(
        {"Error": {"Code": "NoSuchEntity", "Message": "Policy does not exist"}},
        "GetRolePolicy",
    )
    clients["batch"].describe_job_queues.return_value = {
        "jobQueues": [{"jobQueueArn": f"{PREFIX}:job-queue/ae-ondemand-gp3"}]
    }
    clients["ec2"].describe_subnets.return_value = {
        "Subnets": [{"VpcId": "vpc-a"}]
    }
    clients["ec2"].describe_security_groups.return_value = {
        "SecurityGroups": [{"VpcId": "vpc-a"}]
    }
    clients["ecr"].describe_images.return_value = {
        "imageDetails": [{"imageDigest": "sha256:" + "a" * 64}]
    }
    live = {
        "compute_type": "batch",
        "batch_execution_config": {
            "default_workflow_backend": "nextflow",
            "allowed_workflow_backends": ["nextflow"],
            "allowed_job_queues": ["ae-spot-gp3", "ae-ondemand-gp3"],
        },
        "batch_job_definition": SOURCE_ARN,
        "batch_job_queue": "ae-ondemand-gp3",
    }

    def request(_session, method, _url, **kwargs):
        if method == "PATCH":
            live.update(kwargs["json"])
        else:
            assert kwargs["params"] == {"include": "environment"}
        return {"data": copy.deepcopy(live)}

    api = Mock(side_effect=request)
    monkeypatch.setattr(migration, "api_session", Mock())
    monkeypatch.setattr(migration, "api_request", api)
    monkeypatch.setattr(migration, "source_definition", Mock(return_value=source))
    monkeypatch.setattr(migration, "ensure_infrastructure", Mock(return_value=QUEUE_ARN))
    monkeypatch.setattr(migration, "register_definition", Mock(return_value=TARGET_ARN))
    monkeypatch.setattr(migration, "run_canary", Mock(return_value="canary-id"))
    return clients, api, live


def arguments():
    return [
        "--environment", "staging", "--subnets", "subnet-a",
        "--security-groups", "sg-a", "--assign-public-ip", "DISABLED",
    ]


def test_dry_run_never_writes_aws_api_or_report(setup_main, tmp_path):
    clients, api, _live = setup_main
    report = tmp_path / "rollback.json"
    migration.main(arguments() + ["--report", str(report)])
    migration.ensure_infrastructure.assert_not_called()
    migration.register_definition.assert_not_called()
    migration.run_canary.assert_not_called()
    clients["iam"].put_role_policy.assert_not_called()
    assert all(call.args[1] == "GET" for call in api.call_args_list)
    assert not report.exists()


def test_apply_switches_only_driver_and_saves_effective_rollback(setup_main, tmp_path):
    clients, api, live = setup_main
    original_policy = copy.deepcopy(live["batch_execution_config"])
    report = tmp_path / "rollback.json"
    migration.main(arguments() + ["--apply", "--report", str(report)])
    assert live["batch_job_definition"] == TARGET_ARN
    assert live["batch_job_queue"] == QUEUE_ARN
    assert live["batch_execution_config"] == original_policy
    patch = next(call for call in api.call_args_list if call.args[1] == "PATCH")
    assert set(patch.kwargs["json"]) == set(migration.FIELDS)
    saved = json.loads(report.read_text())
    assert saved["rollback_patch"] == {
        "batch_job_definition": SOURCE_ARN,
        "batch_job_queue": f"{PREFIX}:job-queue/ae-ondemand-gp3",
    }
    policy = json.loads(clients["iam"].put_role_policy.call_args.kwargs["PolicyDocument"])
    assert policy["Statement"][0]["Resource"] == [
        QUEUE_ARN, TARGET_ARN.rsplit(":", 1)[0] + ":*"
    ]


def test_canary_failure_prevents_api_switch_and_permission_changes(setup_main, tmp_path):
    clients, api, _live = setup_main
    migration.run_canary.side_effect = RuntimeError("runtime failed")
    with pytest.raises(RuntimeError, match="runtime failed"):
        migration.main(arguments() + ["--apply", "--report", str(tmp_path / "out.json")])
    clients["iam"].put_role_policy.assert_not_called()
    assert all(call.args[1] == "GET" for call in api.call_args_list)


def test_cutover_detects_concurrent_changes_before_backup_or_patch(monkeypatch, tmp_path):
    request = Mock(return_value={"data": {"batch_job_queue": "changed-by-someone-else"}})
    monkeypatch.setattr(migration, "api_request", request)
    report = tmp_path / "rollback.json"
    with pytest.raises(RuntimeError, match="changed during migration"):
        migration.cutover(
            Mock(), "https://api.example/script/id",
            {"batch_job_queue": "original"}, {}, report, {},
        )
    assert not report.exists()
    assert request.call_count == 1


def test_cutover_retains_rollback_report_on_api_failure(monkeypatch, tmp_path):
    request = Mock(side_effect=[
        {"data": {"batch_job_queue": "original"}},
        RuntimeError("API PATCH failed"),
    ])
    monkeypatch.setattr(migration, "api_request", request)
    report = tmp_path / "rollback.json"
    details = {"rollback_patch": {"batch_job_queue": "original"}}
    with pytest.raises(RuntimeError, match="API PATCH failed"):
        migration.cutover(
            Mock(), "https://api.example/script/id",
            {"batch_job_queue": "original"}, {"batch_job_queue": "new"}, report, details,
        )
    assert json.loads(report.read_text()) == details
