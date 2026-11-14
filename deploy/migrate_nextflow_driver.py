#!/usr/bin/env python3
"""Migrate one environment's trusted Nextflow driver to Batch Fargate On-Demand."""

from __future__ import annotations

import argparse
import copy
import json
import os
import re
import time
from pathlib import Path

import boto3
import requests
from botocore.exceptions import ClientError

SCRIPT_ID = "bd669222-67d3-41f5-828f-0cf462de7175"
API_URLS = {
    "production": "https://api.trends.earth",
    "staging": "https://api-staging.trends.earth",
}
FIELDS = ("batch_job_definition", "batch_job_queue")
CANARY = """
import os
import subprocess
import boto3
subprocess.run(["nextflow", "-version"], check=True)
if os.environ["AWS_REGION"] != os.environ["AWS_DEFAULT_REGION"]:
    raise RuntimeError("Driver AWS region variables do not match")
queues = [os.environ.get("AE_NEXTFLOW_SPOT_QUEUE", "ae-spot-gp3"),
          os.environ.get("AE_NEXTFLOW_ONDEMAND_QUEUE", "ae-ondemand-gp3")]
found = boto3.client("batch").describe_job_queues(jobQueues=queues)["jobQueues"]
if len(found) != len(set(queues)):
    raise RuntimeError("Child queues are not accessible")
if any(q["state"] != "ENABLED" or q["status"] != "VALID" for q in found):
    raise RuntimeError("Child queues are not enabled and valid")
print("Fargate driver runtime, credentials, region, and child queue lookup verified")
"""


def api_request(session, method, url, **kwargs):
    response = session.request(
        method, url, timeout=60, allow_redirects=False, **kwargs
    )
    response.raise_for_status()
    if not 200 <= response.status_code < 300:
        raise RuntimeError(f"Unexpected API status {response.status_code} for {url}")
    return response.json()


def api_session(api_url):
    session = requests.Session()
    token = os.environ.get("TRENDSEARTH_API_TOKEN")
    if not token:
        client_id = os.environ.get("TRENDSEARTH_CLIENT_ID")
        secret = os.environ.get("TRENDSEARTH_CLIENT_SECRET")
        if not client_id or not secret:
            raise ValueError(
                "Set TRENDSEARTH_API_TOKEN (admin JWT), or "
                "TRENDSEARTH_CLIENT_ID and TRENDSEARTH_CLIENT_SECRET"
            )
        token = api_request(
            session,
            "POST",
            f"{api_url}/api/v1/oauth/token",
            json={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": secret,
            },
        )["access_token"]
    session.headers["Authorization"] = f"Bearer {token}"
    return session


def source_definition(batch, identifier):
    definitions = []
    for page in batch.get_paginator("describe_job_definitions").paginate(
        **(
            {"jobDefinitions": [identifier]}
            if ":" in identifier
            else {"jobDefinitionName": identifier}
        ),
        status="ACTIVE",
    ):
        definitions.extend(page["jobDefinitions"])
    if not definitions:
        raise ValueError(f"No active source job definition: {identifier}")
    source = max(definitions, key=lambda item: item["revision"])
    if source["type"] != "container" or "containerProperties" not in source:
        raise ValueError("Source must be a single-container driver job definition")
    return source


def pin_image(ecr, image, account, region):
    match = re.fullmatch(
        rf"{account}\.dkr\.ecr\.{re.escape(region)}\.amazonaws\.com/"
        r"(.+?)(?:@sha256:([0-9a-f]{64})|:([^/:]+))",
        image,
    )
    if not match:
        raise ValueError("Driver image must be in private ECR in this account/region")
    repository, digest, tag = match.groups()
    images = ecr.describe_images(
        repositoryName=repository,
        imageIds=[{"imageDigest": f"sha256:{digest}"} if digest else {"imageTag": tag}],
    )["imageDetails"]
    if len(images) != 1:
        raise ValueError(f"Cannot resolve driver image: {image}")
    return f"{image.split('/')[0]}/{repository}@{images[0]['imageDigest']}"


def fargate_definition(source, name, image, region, public_ip, tags):
    container = copy.deepcopy(source["containerProperties"])
    supported = {
        "image", "command", "jobRoleArn", "executionRoleArn", "environment",
        "secrets", "resourceRequirements", "logConfiguration",
        "readonlyRootFilesystem", "runtimePlatform", "ephemeralStorage",
        "networkConfiguration", "fargatePlatformConfiguration",
    }
    for key in set(container) - supported - {"vcpus", "memory", "privileged"}:
        if container.pop(key):
            raise ValueError(f"Source container setting needs manual migration: {key}")
    if container.pop("privileged", False):
        raise ValueError("Fargate cannot run a privileged driver")
    if not container.get("jobRoleArn") or not container.get("executionRoleArn"):
        raise ValueError("Source must specify both jobRoleArn and executionRoleArn")
    resources = {
        item["type"]: item["value"]
        for item in container.get("resourceRequirements", [])
    }
    for old, new in (("vcpus", "VCPU"), ("memory", "MEMORY")):
        value = container.pop(old, None)
        if value is not None:
            resources.setdefault(new, str(value))
    if set(resources) != {"VCPU", "MEMORY"}:
        raise ValueError("Driver must request CPU/memory only, not GPU resources")
    cpu, memory = float(resources["VCPU"]), int(resources["MEMORY"])
    limits = {
        0.25: (512, 2048, 512), 0.5: (1024, 4096, 1024),
        1: (2048, 8192, 1024), 2: (4096, 16384, 1024),
        4: (8192, 30720, 1024), 8: (16384, 61440, 4096),
        16: (32768, 122880, 8192),
    }
    if cpu not in limits:
        raise ValueError(f"Unsupported driver Fargate vCPU request: {cpu}")
    low, high, step = limits[cpu]
    if not low <= memory <= high or (memory - low) % step:
        raise ValueError(f"Unsupported Fargate memory {memory} MiB for {cpu} vCPU")
    if cpu < 2 or memory < 4096:
        raise ValueError("Keep the documented driver minimum of 2 vCPU / 4 GiB")
    container["resourceRequirements"] = [
        {"type": key, "value": str(value)} for key, value in resources.items()
    ]
    container["image"] = image
    environment = {
        item["name"]: item["value"] for item in container.get("environment", [])
    }
    environment.update(AWS_REGION=region, AWS_DEFAULT_REGION=region)
    container["environment"] = [
        {"name": key, "value": value} for key, value in environment.items()
    ]
    container["networkConfiguration"] = {"assignPublicIp": public_ip}
    container["fargatePlatformConfiguration"] = {"platformVersion": "LATEST"}
    log_driver = container.get("logConfiguration", {}).get("logDriver", "awslogs")
    if log_driver != "awslogs":
        raise ValueError("Migrate the driver's log configuration to awslogs first")
    container.setdefault("logConfiguration", {"logDriver": "awslogs"})
    return {
        "jobDefinitionName": name,
        "type": "container",
        "platformCapabilities": ["FARGATE"],
        "containerProperties": container,
        "parameters": source.get("parameters", {}),
        "retryStrategy": {"attempts": 1},
        "timeout": {
            "attemptDurationSeconds": max(
                source.get("timeout", {}).get("attemptDurationSeconds", 0),
                168 * 3600 + 900,
            )
        },
        "propagateTags": source.get("propagateTags", True),
        "tags": tags,
    }


def wait_valid(describe, key, **kwargs):
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline:
        items = describe(**kwargs)[key]
        if items:
            item = items[0]
            if item["status"] == "INVALID":
                raise RuntimeError(f"Invalid Batch resource: {item.get('statusReason')}")
            if item["status"] == "VALID":
                if item["state"] != "ENABLED":
                    raise RuntimeError("Existing Batch resource is not ENABLED")
                return item
        time.sleep(10)
    raise TimeoutError(f"Timed out waiting for {key}: {kwargs}")


def ensure_infrastructure(batch, name, subnets, groups, max_vcpus, tags):
    existing = batch.describe_compute_environments(computeEnvironments=[name])[
        "computeEnvironments"
    ]
    if existing:
        resource = existing[0]
        compute = resource["computeResources"]
        if (
            resource["type"] != "MANAGED"
            or compute["type"] != "FARGATE"
            or set(compute["subnets"]) != set(subnets)
            or set(compute["securityGroupIds"]) != set(groups)
            or compute["maxvCpus"] != max_vcpus
        ):
            raise ValueError(f"Existing compute environment differs from plan: {name}")
    else:
        batch.create_compute_environment(
            computeEnvironmentName=name,
            type="MANAGED",
            state="ENABLED",
            computeResources={
                "type": "FARGATE", "maxvCpus": max_vcpus,
                "subnets": subnets, "securityGroupIds": groups,
            },
            tags=tags,
        )
    compute = wait_valid(
        batch.describe_compute_environments, "computeEnvironments",
        computeEnvironments=[name],
    )
    order = [{"order": 1, "computeEnvironment": compute["computeEnvironmentArn"]}]
    queues = batch.describe_job_queues(jobQueues=[name])["jobQueues"]
    if queues:
        if queues[0]["computeEnvironmentOrder"] != order:
            raise ValueError(f"Existing queue differs from Fargate plan: {name}")
    else:
        batch.create_job_queue(
            jobQueueName=name, state="ENABLED", priority=1,
            computeEnvironmentOrder=order, tags=tags,
        )
    return wait_valid(
        batch.describe_job_queues, "jobQueues", jobQueues=[name]
    )["jobQueueArn"]


def register_definition(batch, definition):
    existing = []
    for page in batch.get_paginator("describe_job_definitions").paginate(
        jobDefinitionName=definition["jobDefinitionName"], status="ACTIVE"
    ):
        existing.extend(page["jobDefinitions"])
    for item in sorted(existing, key=lambda item: item["revision"], reverse=True):
        if all(item.get(key) == value for key, value in definition.items()):
            return item["jobDefinitionArn"]
    return batch.register_job_definition(**definition)["jobDefinitionArn"]


def ensure_api_policy(iam, role, name, policy):
    try:
        existing = iam.get_role_policy(RoleName=role, PolicyName=name)["PolicyDocument"]
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "NoSuchEntity":
            raise
    else:
        if existing != policy:
            raise ValueError(f"Existing API inline policy differs; not replacing: {name}")
        return
    iam.put_role_policy(
        RoleName=role, PolicyName=name, PolicyDocument=json.dumps(policy)
    )


def run_canary(batch, queue, definition):
    job = batch.submit_job(
        jobName="nextflow-fargate-migration-canary",
        jobQueue=queue,
        jobDefinition=definition,
        containerOverrides={"command": ["python", "-c", CANARY]},
        retryStrategy={"attempts": 1},
        timeout={"attemptDurationSeconds": 300},
    )["jobId"]
    print(f"Waiting for Fargate canary: {job}", flush=True)
    terminal = False
    try:
        deadline = time.monotonic() + 1200
        while time.monotonic() < deadline:
            jobs = batch.describe_jobs(jobs=[job])["jobs"]
            if jobs and jobs[0]["status"] in {"SUCCEEDED", "FAILED"}:
                terminal = True
                if jobs[0]["status"] == "FAILED":
                    raise RuntimeError(
                        f"Fargate canary {job} failed: "
                        f"{jobs[0].get('statusReason')}; inspect its CloudWatch logs"
                    )
                return job
            time.sleep(10)
        raise TimeoutError(f"Fargate canary timed out: {job}")
    finally:
        if not terminal:
            batch.terminate_job(jobId=job, reason="Migration canary interrupted")


def cutover(api, url, original, target, report, details):
    current = api_request(api, "GET", url, params={"include": "environment"})["data"]
    if any(current.get(key) != original.get(key) for key in FIELDS):
        raise RuntimeError("Script driver settings changed during migration; not switching")
    # Keep effective pinned settings even when the original used API fallbacks.
    with report.open("x", encoding="utf-8") as stream:
        json.dump(details, stream, indent=2)
        stream.write("\n")
    api_request(api, "PATCH", f"{url}/config", json=target)
    published = api_request(api, "GET", url, params={"include": "environment"})["data"]
    if any(published.get(key) != value for key, value in target.items()):
        raise RuntimeError(f"API cutover verification failed; rollback details: {report}")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment", required=True, choices=API_URLS)
    parser.add_argument("--profile", help="AWS credential profile")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--script-id", default=SCRIPT_ID)
    parser.add_argument("--source-job-definition", help="Required if Script uses a fallback")
    parser.add_argument("--source-job-queue", help="Required if Script uses a fallback")
    parser.add_argument("--subnets", required=True, nargs="+")
    parser.add_argument("--security-groups", required=True, nargs="+")
    parser.add_argument(
        "--assign-public-ip", required=True, choices=("ENABLED", "DISABLED"),
        help="DISABLED requires NAT or all required VPC endpoints",
    )
    parser.add_argument("--max-vcpus", type=int, default=16)
    parser.add_argument("--api-role-name", default="trendsearth-api-docker-swarm-role")
    parser.add_argument("--report", type=Path, help="New rollback JSON path; required with --apply")
    parser.add_argument("--apply", action="store_true", help="Otherwise only read and print a plan")
    args = parser.parse_args(argv)
    if args.max_vcpus < 2:
        parser.error("--max-vcpus must be at least 2")
    if args.apply and (args.report is None or args.report.exists()):
        parser.error("--apply requires --report pointing to a file that does not exist")
    if args.apply and not args.report.parent.is_dir():
        parser.error("--report parent directory must already exist")
    return args


def main(argv=None):
    args = parse_args(argv)
    api_url = API_URLS[args.environment]
    api = api_session(api_url)
    url = f"{api_url}/api/v1/script/{args.script_id}"
    original = api_request(api, "GET", url, params={"include": "environment"})["data"]
    policy = original.get("batch_execution_config") or {}
    if (
        original.get("compute_type") != "batch"
        or policy.get("default_workflow_backend") != "nextflow"
        or "nextflow" not in policy.get("allowed_workflow_backends", [])
    ):
        raise ValueError("Publish and enable the Nextflow script policy before migrating")
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    account = session.client("sts").get_caller_identity()["Account"]
    batch = session.client("batch")
    iam = session.client("iam")
    iam.get_role(RoleName=args.api_role_name)
    source_id = args.source_job_definition or original.get("batch_job_definition")
    source_queue = args.source_job_queue or original.get("batch_job_queue")
    if not source_id or not source_queue:
        raise ValueError(
            "Supply --source-job-definition and --source-job-queue for API fallbacks"
        )
    for field, override in (
        ("batch_job_definition", args.source_job_definition),
        ("batch_job_queue", args.source_job_queue),
    ):
        if override and original.get(field) and override != original[field]:
            raise ValueError(f"{field} is set on the Script; do not override it")
    source = source_definition(batch, source_id)
    queues = batch.describe_job_queues(jobQueues=[source_queue])["jobQueues"]
    if len(queues) != 1:
        raise ValueError(f"Source driver queue not found: {source_queue}")
    rollback = {
        "batch_job_definition": source["jobDefinitionArn"],
        "batch_job_queue": queues[0]["jobQueueArn"],
    }
    ec2 = session.client("ec2")
    subnets = ec2.describe_subnets(SubnetIds=args.subnets)["Subnets"]
    groups = ec2.describe_security_groups(GroupIds=args.security_groups)["SecurityGroups"]
    if len({item["VpcId"] for item in subnets + groups}) != 1:
        raise ValueError("All Fargate subnets and security groups must be in one VPC")
    name = f"ae-nextflow-driver-{args.environment}-fargate"
    tags = {"Project": "avoided-emissions", "Environment": args.environment}
    image = pin_image(
        session.client("ecr"), source["containerProperties"]["image"], account, args.region
    )
    definition = fargate_definition(
        source, name, image, args.region, args.assign_public_ip, tags
    )
    cpu = next(
        float(item["value"])
        for item in definition["containerProperties"]["resourceRequirements"]
        if item["type"] == "VCPU"
    )
    if args.max_vcpus < cpu:
        raise ValueError("--max-vcpus is smaller than the driver's CPU request")
    print(json.dumps({
        "mode": "APPLY" if args.apply else "DRY RUN",
        "api_url": api_url, "script_id": args.script_id,
        "source": rollback, "target_name": name, "image": image,
        "subnets": args.subnets, "security_groups": args.security_groups,
        "assign_public_ip": args.assign_public_ip, "max_vcpus": args.max_vcpus,
        "api_role": args.api_role_name,
        "driver_resources": definition["containerProperties"]["resourceRequirements"],
        "steps": [
            "Ensure dedicated FARGATE (not Spot) compute environment and queue",
            "Register pinned driver definition, preserving roles and workflow settings",
            "Run driver runtime/credential/child-queue canary",
            "Grant API role access to the new driver",
            "Save rollback report and PATCH/verify only this Script's driver settings",
        ],
    }, indent=2), flush=True)
    if not args.apply:
        return
    queue = ensure_infrastructure(
        batch, name, args.subnets, args.security_groups, args.max_vcpus, tags
    )
    driver = register_definition(batch, definition)
    canary = run_canary(batch, queue, driver)
    partition = driver.split(":")[1]
    policy_name = f"{name}-submit"
    ensure_api_policy(
        iam, args.api_role_name, policy_name, {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": "batch:SubmitJob",
                 "Resource": [queue, f"{driver.rsplit(':', 1)[0]}:*"]},
                {"Effect": "Allow", "Action": "batch:TagResource",
                 "Resource": f"arn:{partition}:batch:{args.region}:{account}:job/*"},
                {"Effect": "Allow", "Action": "iam:PassRole",
                 "Resource": [
                     definition["containerProperties"]["jobRoleArn"],
                     definition["containerProperties"]["executionRoleArn"],
                 ],
                 "Condition": {"StringEquals": {
                     "iam:PassedToService": "ecs-tasks.amazonaws.com"
                 }}},
            ],
        },
    )
    target = {"batch_job_definition": driver, "batch_job_queue": queue}
    cutover(api, url, original, target, args.report, {
        "api_url": api_url, "script_id": args.script_id,
        "rollback_patch": rollback, "target": target,
        "canary_job_id": canary, "api_role": args.api_role_name,
        "added_inline_policy": policy_name,
    })
    print(
        f"Verified driver cutover. Rollback: {args.report}. "
        "Worker queues and old driver resources were not changed."
    )


if __name__ == "__main__":
    main()
