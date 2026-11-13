"""Trusted On-Demand launcher; clients cannot supply workflow code or config."""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import re
import secrets
import signal
import subprocess
import time
from pathlib import Path

import boto3
from nextflow_worker import ANALYSIS_FIELDS
from workflow_store import Store

log = logging.getLogger(__name__)
WORKFLOW = Path(__file__).with_name("main.nf")
VERSION = "1.1.0"
JOB_ID = re.compile(r"submitted > job=([0-9a-f-]{36});")


def canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def s3_parts(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://") or "/" not in uri[5:]:
        raise ValueError(f"Expected an S3 object URI: {uri}")
    bucket, key = uri[5:].split("/", 1)
    if not bucket or not key or ".." in key.split("/"):
        raise ValueError(f"Invalid S3 URI: {uri}")
    return bucket, key


def references(value):
    if isinstance(value, str) and value.startswith("s3://"):
        yield value
    elif isinstance(value, dict):
        for item in value.values():
            yield from references(item)
    elif isinstance(value, list):
        for item in value:
            yield from references(item)


def input_manifest(client, params: dict) -> list:
    """Detect mutable raster/reference inputs before resume and publication."""
    manifest = []
    bucket = params["cog_bucket"]
    prefix = params["cog_prefix"].rstrip("/") + "/"
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix
    ):
        for obj in page.get("Contents", []):
            manifest.append(
                [
                    bucket,
                    obj["Key"],
                    obj["ETag"],
                    obj["Size"],
                    obj["LastModified"].isoformat(),
                ]
            )
    if not manifest:
        raise ValueError("The configured COG prefix is empty")
    for uri in sorted(set(references(params.get("reference_layer_uris", {})))):
        bucket, key = s3_parts(uri)
        obj = client.head_object(Bucket=bucket, Key=key)
        manifest.append(
            [
                bucket,
                key,
                obj["ETag"],
                obj["ContentLength"],
                obj["LastModified"].isoformat(),
            ]
        )
    return sorted(manifest)


def bounded_env(name: str, default: int, maximum: int) -> int:
    value = int(os.environ.get(name, default))
    if not 1 <= value <= maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}")
    return value


def prepare(client, store: Store, params: dict, image: str, resume: bool) -> dict:
    if not re.fullmatch(r"[a-zA-Z0-9./:_-]+@sha256:[0-9a-f]{64}", image):
        raise ValueError("The analysis image must be pinned to a SHA256 digest")
    if "_workflow" in params:
        raise ValueError("Client-supplied workflow runtime configuration is forbidden")
    memory = params.get(
        "match_memory_mib", bounded_env("AE_NEXTFLOW_MATCH_MEMORY_MIB", 30720, 122880)
    )
    if type(memory) is not int or not 1 <= memory <= 122880:
        raise ValueError("match_memory_mib must be an integer between 1 and 122880")
    approved_queues = {
        os.environ.get("AE_NEXTFLOW_SPOT_QUEUE", "ae-spot-gp3"),
        os.environ.get("AE_NEXTFLOW_ONDEMAND_QUEUE", "ae-ondemand-gp3"),
    }
    initial_queue = params.get(
        "matching_job_queue", os.environ.get("AE_NEXTFLOW_SPOT_QUEUE", "ae-spot-gp3")
    )
    if (
        not isinstance(initial_queue, str)
        or not initial_queue
        or initial_queue not in approved_queues
    ):
        raise ValueError("matching_job_queue must be an approved workflow worker queue")
    current_inputs = input_manifest(client, params)
    existing = store.json("snapshot.json")
    if existing is not None:
        if existing["source"] != params or existing["image"] != image:
            raise ValueError("Resume requires identical parameters and container image")
        if existing["inputs"] != current_inputs or existing["version"] != VERSION:
            raise ValueError(
                "Resume rejected: raster/reference inputs or workflow changed"
            )
        if not resume:
            raise ValueError(
                "Workflow already initialized; explicit resume is required"
            )
        return existing["analysis"]
    if resume:
        raise ValueError("Cannot resume without the original immutable input snapshot")
    sites_uri = params.get("sites_parquet_s3_uri") or params["sites_s3_uri"]
    bucket, key = s3_parts(sites_uri)
    metadata = client.head_object(Bucket=bucket, Key=key)
    suffix = ".parquet" if key.endswith(".parquet") else ".geojson"
    frozen_key = f"{store.prefix}/inputs/sites{suffix}"
    source = {"Bucket": bucket, "Key": key}
    if metadata.get("VersionId"):
        source["VersionId"] = metadata["VersionId"]
    client.copy_object(
        Bucket=store.bucket,
        Key=frozen_key,
        CopySource=source,
        CopySourceIfMatch=metadata["ETag"],
    )
    seed = params.get("random_seed")
    analysis = {
        key: value
        for key, value in params.items()
        if key in ANALYSIS_FIELDS | {"replicate_block_size", "match_batch_size"}
    }
    analysis["random_seed"] = (
        int(seed) if seed is not None else secrets.randbelow(2**30)
    )
    analysis["sites_parquet_s3_uri"] = f"s3://{store.bucket}/{frozen_key}"
    analysis.pop("sites_s3_uri", None)
    results_uri = params["results_s3_uri"]
    s3_parts(results_uri)
    identity = hashlib.sha256(
        canonical(
            {
                "analysis": analysis,
                "inputs": current_inputs,
                "image": image,
                "version": VERSION,
            }
        )
    ).hexdigest()
    analysis["_workflow"] = {
        "identity": identity,
        "state_uri": store.uri,
        "results_uri": results_uri,
        "match_memory_mib": memory,
        "initial_queue": initial_queue,
    }
    store.write_json(
        "snapshot.json",
        {
            "source": params,
            "image": image,
            "version": VERSION,
            "inputs": current_inputs,
            "analysis": analysis,
        },
        exclusive=True,
    )
    return analysis


def record_jobs(store: Store, logfile: Path) -> set[str]:
    previous = set(store.json("jobs.json") or [])
    if logfile.exists():
        jobs = previous | set(JOB_ID.findall(logfile.read_text(errors="replace")))
    else:
        jobs = previous
    if jobs != previous:
        store.write_json("jobs.json", sorted(jobs))
    return jobs


def stop(process: subprocess.Popen, batch, jobs: set[str], reason: str) -> None:
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=45)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()
    for job in sorted(jobs):
        batch.terminate_job(jobId=job, reason=reason)


def launch(store: Store, analysis: dict, env: dict, resume: bool) -> None:
    Path("analysis.json").write_bytes(canonical(analysis))
    command = [
        "nextflow",
        "-log",
        "nextflow.log",
        "run",
        str(WORKFLOW),
        "-name",
        f"ae_{env['EXECUTION_ID'].replace('-', '_')}",
        "-work-dir",
        env["NEXTFLOW_WORK_URI"],
        "--analysis_file",
        str(Path("analysis.json").resolve()),
        "--state_uri",
        store.uri,
        "--execution_id",
        env["EXECUTION_ID"],
        "--image",
        env["NEXTFLOW_TASK_IMAGE"],
        "--spot_queue",
        analysis["_workflow"]["initial_queue"],
        "--ondemand_queue",
        env.get("AE_NEXTFLOW_ONDEMAND_QUEUE", "ae-ondemand-gp3"),
        "--max_forks",
        str(bounded_env("AE_NEXTFLOW_MAX_FORKS", 20, 100)),
        "--match_memory_mib",
        str(analysis["_workflow"]["match_memory_mib"]),
        "--allow_ondemand",
        str(env.get("AE_NEXTFLOW_ALLOW_ONDEMAND", "false")).lower(),
    ]
    if env.get("AE_NEXTFLOW_JOB_ROLE_ARN"):
        command.extend(["--job_role", env["AE_NEXTFLOW_JOB_ROLE_ARN"]])
    if resume:
        session = store.json("session.json")
        if session is None:
            if store.json("jobs.json"):
                raise ValueError("Cannot resume: original Nextflow session is missing")
            log.info("No tasks were submitted; starting the frozen workflow snapshot")
        else:
            command.extend(["-resume", session["session_id"]])
    process_env = dict(env)
    process_env["NXF_CLOUDCACHE_PATH"] = f"{store.uri}/cache"
    process_env["NXF_IGNORE_RESUME_HISTORY"] = "true"
    process_env["NXF_ANSI_LOG"] = "false"
    hours = float(env.get("NEXTFLOW_MAX_HOURS", "72"))
    if not 0 < hours <= 168:
        raise ValueError("NEXTFLOW_MAX_HOURS must be greater than zero and at most 168")
    duration = hours * 3600
    interrupted = False

    def interrupt(_signum, _frame):
        nonlocal interrupted
        interrupted = True

    previous_term = signal.signal(signal.SIGTERM, interrupt)
    previous_int = signal.signal(signal.SIGINT, interrupt)
    process = None
    started = time.monotonic()
    batch = boto3.client("batch")
    try:
        process = subprocess.Popen(command, env=process_env, start_new_session=True)
        store.write_json("status.json", {"phase": "running"})
        while True:
            jobs = record_jobs(store, Path("nextflow.log"))
            cancelled = interrupted or store.json("cancelled.json") is not None
            expired = time.monotonic() - started > duration
            if cancelled or expired:
                reason = (
                    "Execution cancelled"
                    if cancelled
                    else "Workflow duration budget exhausted"
                )
                stop(process, batch, jobs, reason)
                record_jobs(store, Path("nextflow.log"))
                raise InterruptedError(reason) if cancelled else TimeoutError(reason)
            code = process.poll()
            if code is not None:
                if code:
                    raise subprocess.CalledProcessError(code, command)
                break
            time.sleep(5)
    finally:
        signal.signal(signal.SIGTERM, previous_term)
        signal.signal(signal.SIGINT, previous_int)
        if process is not None and process.poll() is None:
            stop(
                process,
                batch,
                record_jobs(store, Path("nextflow.log")),
                "Driver failed",
            )


def main() -> None:
    env = dict(os.environ)
    store = Store(env["NEXTFLOW_STATE_URI"])
    execution_id = env["EXECUTION_ID"]
    client = boto3.client("s3")
    key = f"{env['PARAMS_S3_PREFIX'].rstrip('/')}/{execution_id}"
    try:
        if store.json("cancelled.json") is not None:
            raise InterruptedError("Execution was cancelled; resume is forbidden")
        params = json.loads(
            gzip.decompress(
                client.get_object(Bucket=env["PARAMS_S3_BUCKET"], Key=f"{key}.json.gz")[
                    "Body"
                ].read()
            )
        )
        resume = env.get("NEXTFLOW_RESUME", "false").lower() == "true"
        analysis = prepare(client, store, params, env["NEXTFLOW_TASK_IMAGE"], resume)
        launch(store, analysis, env, resume)
        if store.json("cancelled.json") is not None:
            raise InterruptedError("Execution cancelled before publication")
        snapshot = store.json("snapshot.json")
        if input_manifest(client, params) != snapshot["inputs"]:
            raise ValueError(
                "Raster/reference inputs changed during execution; results rejected"
            )
        payload = store.read("published/results.json")
        if payload is None:
            raise FileNotFoundError(
                "Nextflow completed without a published result payload"
            )
        json.loads(payload)
        client.put_object(
            Bucket=env["PARAMS_S3_BUCKET"],
            Key=f"{key}_results.json.gz",
            Body=gzip.compress(payload),
            ContentType="application/json",
            ContentEncoding="gzip",
        )
        store.write_json("status.json", {"phase": "succeeded", "success": True})
    except Exception as exc:
        phase = "cancelled" if isinstance(exc, InterruptedError) else "failed"
        store.write_json(
            "status.json", {"phase": phase, "success": False, "error": str(exc)}
        )
        log.exception("Nextflow driver %s", phase)
        raise
    finally:
        for name in ("nextflow.log", "trace.tsv"):
            path = Path(name)
            if path.exists():
                store.write(f"diagnostics/{name}", path.read_bytes())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
