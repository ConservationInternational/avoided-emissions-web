#!/usr/bin/env python3
"""Select a digest-pinned driver revision for script publication."""

import argparse
import copy
import json
import os
import re
from pathlib import Path

import boto3
import requests
import yaml
from migrate_nextflow_driver import (
    api_request,
    pin_image,
    register_definition,
    source_definition,
)


def prepare_publication(config, api, api_url, batch, ecr, account, region, image):
    url = f"{api_url.rstrip('/')}/api/v1/script/{config['id']}"
    params = {"include": "environment,nextflow_driver"}
    original = api_request(api, "GET", url, params=params)["data"]
    driver = original.get("nextflow_driver") or {}
    source_arn = driver.get("job_definition")
    queue = driver.get("job_queue")
    if not source_arn or not queue:
        raise ValueError(
            "API did not return effective Nextflow driver settings. Deploy API "
            "support for include=nextflow_driver and authenticate as an admin; "
            "configure the API's driver defaults or Script overrides."
        )
    if not re.fullmatch(
        rf"arn:aws:batch:{re.escape(region)}:{re.escape(account)}:"
        r"job-definition/[^:]+:\d+",
        source_arn,
    ):
        raise ValueError(
            "Source driver must be a revision-pinned ARN in this account/region"
        )
    source = source_definition(batch, source_arn)
    fields = batch.meta.service_model.operation_model(
        "RegisterJobDefinition"
    ).input_shape.members
    definition = {
        key: copy.deepcopy(value) for key, value in source.items() if key in fields
    }
    definition["containerProperties"]["image"] = pin_image(ecr, image, account, region)
    target_arn = register_definition(batch, definition)
    current = api_request(api, "GET", url, params=params)["data"]
    for field in ("batch_job_definition", "batch_job_queue", "nextflow_driver"):
        if current.get(field) != original.get(field):
            raise RuntimeError("Script driver settings changed during publication")
    print(f"Driver rollout: {source_arn} -> {target_arn}", flush=True)
    print(
        f"Pinned driver image: {definition['containerProperties']['image']}", flush=True
    )
    return {**config, "batch_job_definition": target_arn, "batch_job_queue": queue}


def verify_publication(config, api, api_url):
    url = f"{api_url.rstrip('/')}/api/v1/script/{config['id']}"
    published = api_request(
        api, "GET", url, params={"include": "environment,nextflow_driver"}
    )["data"]
    for field in (
        "compute_type",
        "batch_image",
        "batch_execution_config",
        "batch_job_definition",
        "batch_job_queue",
    ):
        if published.get(field) != config[field]:
            raise RuntimeError(
                f"Publication verification failed for {field}: "
                f"expected {config[field]!r}, got {published.get(field)!r}"
            )
    expected_driver = {
        "job_definition": config["batch_job_definition"],
        "job_queue": config["batch_job_queue"],
    }
    if published.get("nextflow_driver") != expected_driver:
        raise RuntimeError("Published Script's effective driver settings do not match")
    if published.get("public") is not True:
        raise RuntimeError("Script was registered but is not published")
    print("Published Batch image, execution policy, and pinned driver verified.")


def main():
    parser = argparse.ArgumentParser()
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--image")
    action.add_argument("--verify", action="store_true")
    parser.add_argument("--config", default="r-analysis/configuration.json")
    parser.add_argument("--cli-config", default=str(Path.home() / ".tecli.yml"))
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    args = parser.parse_args()
    cli = yaml.safe_load(Path(args.cli_config).read_text())
    api = requests.Session()
    api.headers["Authorization"] = f"Bearer {cli['JWT']}"
    path = Path(args.config)
    config = json.loads(path.read_text())
    if args.verify:
        verify_publication(config, api, cli["url_api"])
        return
    session = boto3.Session(region_name=args.region)
    account = session.client("sts").get_caller_identity()["Account"]
    config = prepare_publication(
        config,
        api,
        cli["url_api"],
        session.client("batch"),
        session.client("ecr"),
        account,
        args.region,
        args.image,
    )
    path.write_text(json.dumps(config, indent=2) + "\n")


if __name__ == "__main__":
    main()
