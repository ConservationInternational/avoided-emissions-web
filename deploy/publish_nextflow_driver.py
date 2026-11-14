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
    original = api_request(api, "GET", url, params={"include": "environment"})["data"]
    source_arn = original.get("batch_job_definition") or os.environ.get(
        "NEXTFLOW_DRIVER_JOB_DEFINITION"
    )
    queue = original.get("batch_job_queue") or os.environ.get(
        "NEXTFLOW_DRIVER_JOB_QUEUE"
    )
    if not source_arn or not queue:
        raise ValueError(
            "Publish explicit driver settings first, or set the environment's "
            "NEXTFLOW_DRIVER_JOB_DEFINITION and NEXTFLOW_DRIVER_JOB_QUEUE variables"
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
    current = api_request(api, "GET", url, params={"include": "environment"})["data"]
    for field in ("batch_job_definition", "batch_job_queue"):
        if current.get(field) != original.get(field):
            raise RuntimeError("Script driver settings changed during publication")
    print(f"Driver rollout: {source_arn} -> {target_arn}", flush=True)
    print(
        f"Pinned driver image: {definition['containerProperties']['image']}", flush=True
    )
    return {**config, "batch_job_definition": target_arn, "batch_job_queue": queue}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    parser.add_argument("--config", default="r-analysis/configuration.json")
    parser.add_argument("--cli-config", default=str(Path.home() / ".tecli.yml"))
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    args = parser.parse_args()
    cli = yaml.safe_load(Path(args.cli_config).read_text())
    api = requests.Session()
    api.headers["Authorization"] = f"Bearer {cli['JWT']}"
    session = boto3.Session(region_name=args.region)
    account = session.client("sts").get_caller_identity()["Account"]
    path = Path(args.config)
    config = prepare_publication(
        json.loads(path.read_text()),
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
