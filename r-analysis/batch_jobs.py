"""AWS Batch job definition and submission for avoided emissions analysis.

Provides functions to register job definitions, submit array jobs for
parallel per-site matching, and monitor job status.
"""

import json
import os

import boto3


# Default container properties for the R analysis container
DEFAULT_VCPUS = 4
DEFAULT_MEMORY_MIB = 16384
DEFAULT_TIMEOUT_SECONDS = 14400  # 4 hours

# ECR repository for the R analysis container
ECR_REPO_NAME = "avoided-emissions-r-analysis"


def get_batch_client():
    return boto3.client("batch")


def get_ecr_image_uri():
    """Build the ECR image URI from environment config."""
    account_id = os.environ.get("AWS_ACCOUNT_ID", "")
    region = os.environ.get("AWS_REGION", "us-east-1")
    tag = os.environ.get("R_ANALYSIS_IMAGE_TAG", "latest")
    return f"{account_id}.dkr.ecr.{region}.amazonaws.com/{ECR_REPO_NAME}:{tag}"


def register_job_definition(job_def_name="avoided-emissions-analysis",
                            image_uri=None, vcpus=None, memory=None):
    """Register or update the AWS Batch job definition.

    Args:
        job_def_name: Name for the job definition.
        image_uri: ECR image URI. Defaults to env var-based construction.
        vcpus: Number of vCPUs per job. Defaults to DEFAULT_VCPUS.
        memory: Memory in MiB per job. Defaults to DEFAULT_MEMORY_MIB.

    Returns:
        The job definition ARN.
    """
    client = get_batch_client()
    image = image_uri or get_ecr_image_uri()

    response = client.register_job_definition(
        jobDefinitionName=job_def_name,
        type="container",
        containerProperties={
            "image": image,
            "vcpus": vcpus or DEFAULT_VCPUS,
            "memory": memory or DEFAULT_MEMORY_MIB,
            "command": ["match", "--config", "/data/config.json"],
            "environment": [
                {"name": "AWS_DEFAULT_REGION",
                 "value": os.environ.get("AWS_REGION", "us-east-1")},
            ],
            "mountPoints": [
                {
                    "sourceVolume": "data",
                    "containerPath": "/data",
                    "readOnly": False,
                }
            ],
            "volumes": [
                {
                    "name": "data",
                    "host": {"sourcePath": "/tmp/ae-data"},
                }
            ],
        },
        timeout={"attemptDurationSeconds": DEFAULT_TIMEOUT_SECONDS},
        retryStrategy={"attempts": 2},
    )
    return response["jobDefinitionArn"]


def submit_extract_job(job_queue, job_definition, config_s3_uri,
                       data_s3_uri):
    """Submit the covariate extraction job (Step 1).

    This is a single job that extracts covariates for all sites.

    Args:
        job_queue: AWS Batch job queue name.
        job_definition: Job definition name or ARN.
        config_s3_uri: S3 URI to the task config JSON.
        data_s3_uri: S3 URI prefix for input/output data.

    Returns:
        The job ID.
    """
    client = get_batch_client()
    response = client.submit_job(
        jobName="ae-extract",
        jobQueue=job_queue,
        jobDefinition=job_definition,
        containerOverrides={
            "command": ["extract", "--config", "/data/config.json"],
            "environment": [
                {"name": "CONFIG_S3_URI", "value": config_s3_uri},
                {"name": "DATA_S3_URI", "value": data_s3_uri},
            ],
        },
    )
    return response["jobId"]


def submit_matching_array_job(job_queue, job_definition, n_sites,
                              config_s3_uri, data_s3_uri,
                              depends_on_job_id=None):
    """Submit a matching array job (Step 2) for parallel site processing.

    Each array element processes one site, identified by
    AWS_BATCH_JOB_ARRAY_INDEX.

    Args:
        job_queue: AWS Batch job queue name.
        job_definition: Job definition name or ARN.
        n_sites: Number of sites (determines array size).
        config_s3_uri: S3 URI to the task config JSON.
        data_s3_uri: S3 URI prefix for input/output data.
        depends_on_job_id: Job ID that must complete first (extraction).

    Returns:
        The array job ID.
    """
    client = get_batch_client()

    depends_on = []
    if depends_on_job_id:
        depends_on = [{"jobId": depends_on_job_id, "type": "SEQUENTIAL"}]

    response = client.submit_job(
        jobName="ae-match",
        jobQueue=job_queue,
        jobDefinition=job_definition,
        arrayProperties={"size": n_sites},
        dependsOn=depends_on,
        containerOverrides={
            "command": ["match", "--config", "/data/config.json"],
            "environment": [
                {"name": "CONFIG_S3_URI", "value": config_s3_uri},
                {"name": "DATA_S3_URI", "value": data_s3_uri},
            ],
        },
    )
    return response["jobId"]


def submit_summarize_job(job_queue, job_definition, config_s3_uri,
                         data_s3_uri, depends_on_job_id=None):
    """Submit the summarization job (Step 3).

    Args:
        job_queue: AWS Batch job queue name.
        job_definition: Job definition name or ARN.
        config_s3_uri: S3 URI to the task config JSON.
        data_s3_uri: S3 URI prefix for input/output data.
        depends_on_job_id: Job ID that must complete first (matching).

    Returns:
        The job ID.
    """
    client = get_batch_client()

    depends_on = []
    if depends_on_job_id:
        depends_on = [{"jobId": depends_on_job_id, "type": "SEQUENTIAL"}]

    response = client.submit_job(
        jobName="ae-summarize",
        jobQueue=job_queue,
        jobDefinition=job_definition,
        dependsOn=depends_on,
        containerOverrides={
            "command": ["summarize", "--config", "/data/config.json"],
            "environment": [
                {"name": "CONFIG_S3_URI", "value": config_s3_uri},
                {"name": "DATA_S3_URI", "value": data_s3_uri},
            ],
        },
    )
    return response["jobId"]


def submit_full_pipeline(job_queue, job_definition, n_sites,
                         config_s3_uri, data_s3_uri):
    """Submit all three pipeline steps with dependencies.

    Returns:
        Dict with job IDs for extract, match, and summarize steps.
    """
    extract_id = submit_extract_job(
        job_queue, job_definition, config_s3_uri, data_s3_uri
    )
    match_id = submit_matching_array_job(
        job_queue, job_definition, n_sites,
        config_s3_uri, data_s3_uri,
        depends_on_job_id=extract_id,
    )
    summarize_id = submit_summarize_job(
        job_queue, job_definition, config_s3_uri, data_s3_uri,
        depends_on_job_id=match_id,
    )
    return {
        "extract_job_id": extract_id,
        "match_job_id": match_id,
        "summarize_job_id": summarize_id,
    }


def get_job_status(job_id):
    """Get the current status of a Batch job.

    Returns:
        Dict with job status information.
    """
    client = get_batch_client()
    response = client.describe_jobs(jobs=[job_id])

    if not response["jobs"]:
        return {"job_id": job_id, "status": "NOT_FOUND"}

    job = response["jobs"][0]
    result = {
        "job_id": job["jobId"],
        "job_name": job["jobName"],
        "status": job["status"],
        "created_at": job.get("createdAt"),
        "started_at": job.get("startedAt"),
        "stopped_at": job.get("stoppedAt"),
    }

    if job.get("arrayProperties"):
        result["array_size"] = job["arrayProperties"].get("size")
        summary = job["arrayProperties"].get("statusSummary", {})
        result["array_status"] = summary

    if job["status"] == "FAILED":
        result["reason"] = job.get("statusReason", "Unknown")

    return result
