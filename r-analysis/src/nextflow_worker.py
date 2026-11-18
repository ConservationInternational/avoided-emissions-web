"""Analysis commands used by the trusted Nextflow workflow."""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import subprocess
import sys
import tarfile
import time
import uuid
from pathlib import Path

from workflow_store import Store, checkpoint_key, digest
from summary_shards import (
    archive_summary,
    merge_diagnostics,
    merge_report_tables,
    partition_summary,
    restore_summary,
)

log = logging.getLogger(__name__)
SCRIPTS = Path(os.environ.get("R_SCRIPTS_DIR", "/app/scripts"))
# Share of a recovery task's budget filled by measured replicate costs; the
# remainder absorbs solve-time variance and data loading.
REPACK_BUDGET_FRACTION = 0.6
DIAGNOSTICS_SCHEMA_VERSION = 1
GLM_REPLAY_CAUSES = {"glm_error", "glm_nonconvergence", "glm_warning"}
DIAGNOSTIC_RESULTS = (
    "results_matching_diagnostics.jsonl.gz",
    "results_matching_diagnostics_summary.json",
    "results_matching_replay_manifest.json",
)
ANALYSIS_FIELDS = {
    "task_id",
    "task_name",
    "task_description",
    "sites_parquet_s3_uri",
    "sites_s3_uri",
    "cog_bucket",
    "cog_prefix",
    "resolution_m",
    "covariates",
    "exact_match_vars",
    "fc_years",
    "max_treatment_pixels",
    "control_multiplier",
    "min_glm_treatment_pixels",
    "caliper_width",
    "max_controls_per_treatment",
    "matching_method",
    "min_control_distance_km",
    "separation_fallback_mahalanobis",
    "shared_support_controls",
    "n_replicates",
    "random_seed",
    "matching_extent",
    "sites_exclusion_buffer",
    "reference_layer_uris",
    "exact_match_group_mapping",
    "site_id",
}


def diagnostics_contract(params: dict) -> dict:
    enabled = params.get("shared_support_controls", False)
    if type(enabled) is not bool:
        raise ValueError("shared_support_controls must be a boolean")
    return {
        "diagnostics_schema_version": DIAGNOSTICS_SCHEMA_VERSION,
        "matching_policy": "shared_support_v1" if enabled else "legacy",
    }


def workflow_contract(params: dict) -> dict | None:
    contract = diagnostics_contract(params)
    runtime = params["_workflow"]
    frozen = runtime.get("diagnostics_contract")
    # Markerless data is supported only by the in-process legacy fixture helpers.
    if frozen is None and not params.get("shared_support_controls", False):
        return None
    if frozen != contract or type(frozen.get("diagnostics_schema_version")) is not int:
        raise ValueError("Incompatible immutable workflow diagnostic contract")
    return contract


def write_json(path: str | Path, value) -> None:
    Path(path).write_text(json.dumps(value, sort_keys=True))


def load_params(path: str) -> dict:
    params = json.loads(Path(path).read_text())
    runtime = params["_workflow"]
    if not runtime.get("identity") or not runtime.get("state_uri"):
        raise ValueError("Missing immutable workflow identity/state URI")
    if workflow_contract(params) is None:
        raise ValueError("Missing immutable workflow diagnostic contract")
    return params


def configuration(params: dict) -> dict:
    config = {key: value for key, value in params.items() if key in ANALYSIS_FIELDS}
    config.setdefault("shared_support_controls", False)
    contract = workflow_contract(params)
    if contract is not None:
        config.update(contract)
        config["analysis_image"] = params["_workflow"].get("analysis_image")
        config["workflow_version"] = params["_workflow"].get("workflow_version")
    config["data_dir"] = str(Path.cwd())
    config["output_dir"] = str(Path("output").resolve())
    config["matches_dir"] = str(Path("output/matches").resolve())
    Path(config["matches_dir"]).mkdir(parents=True, exist_ok=True)
    return config


def artifact_store(params: dict, suffix: str) -> Store:
    runtime = params["_workflow"]
    return Store(f"{runtime['state_uri']}/artifacts/{runtime['identity']}/{suffix}")


def run_script(script: str, config: dict, seconds: int) -> bool:
    """Return False on deadline; other failures propagate to Nextflow."""
    write_json("config.json", config)
    executable = sys.executable if script.endswith(".py") else "Rscript"
    started = time.monotonic()
    process = subprocess.Popen(
        [
            executable,
            str(SCRIPTS / script),
            "--config",
            str(Path("config.json").resolve()),
        ],
        start_new_session=True,
    )
    try:
        code = process.wait(timeout=seconds)
    except subprocess.TimeoutExpired:
        import signal

        os.killpg(process.pid, signal.SIGKILL)
        process.wait()
        log.error("Analysis subprocess exceeded %ss: %s", seconds, script)
        return False
    log.info("[TIMING] %s: %.2fs", script, time.monotonic() - started)
    if code != 0:
        raise subprocess.CalledProcessError(code, process.args)
    return True


def fetch_uri(uri: str, path: Path) -> None:
    if uri.startswith("s3://"):
        import boto3

        bucket, key = uri[5:].split("/", 1)
        boto3.client("s3").download_file(bucket, key, str(path))
    else:
        import shutil

        shutil.copyfile(uri.removeprefix("file://"), path)


def extract(params: dict, seconds: int) -> None:
    store = artifact_store(params, "extract")
    if store.json("extraction.json.receipt.json") is not None:
        store.restore("extraction.json", Path("extraction.json"))
        return
    config = configuration(params)
    uri = params.get("sites_parquet_s3_uri") or params["sites_s3_uri"]
    sites = Path("sites.parquet" if uri.endswith(".parquet") else "sites.geojson")
    fetch_uri(uri, sites)
    config["sites_file"] = str(sites.resolve())
    if not run_script("01_extract_covariates.py", config, seconds):
        raise TimeoutError("Extraction exceeded its internal deadline")
    controls = artifact_store(params, f"controls/{uuid.uuid4()}")
    keys = []
    for path in sorted(Path("output").rglob("*")):
        if path.is_file():
            key = path.relative_to("output").as_posix()
            if key.startswith("control_pixels/"):
                name = key.removeprefix("control_pixels/")
                if controls.root is not None:
                    import shutil

                    target = controls.root / name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(path, target)
                else:
                    controls.client.upload_file(
                        str(path), controls.bucket, f"{controls.prefix}/{name}"
                    )
                continue
            store.commit(path, key)
            keys.append(key)
    write_json(
        "extraction.json",
        {"uri": store.uri, "files": keys, "control_uri": controls.uri},
    )
    store.commit(Path("extraction.json"), "extraction.json")


def restore_extract(descriptor: dict, output: Path, *, pixels: bool = False) -> None:
    store = Store(descriptor["uri"])
    output.mkdir(parents=True, exist_ok=True)
    for key in descriptor["files"]:
        if key.startswith(("control_pixels/", "matches/")):
            continue
        if not pixels and key == "treatment_pixels.parquet":
            continue
        store.restore(key, output / key)


def fragment_identities(sites_path: Path, ids: set[int]) -> dict[str, dict]:
    import pyarrow.parquet as pq

    if not ids:
        return {}
    columns = ["id_numeric", "site_id", "sub_site_index"]
    if "ae_atoms" in pq.read_schema(sites_path).names:
        columns.append("ae_atoms")
    result = {}
    for row in pq.read_table(
        sites_path, columns=columns, filters=[("id_numeric", "in", sorted(ids))]
    ).to_pylist():
        id_ = row["id_numeric"]
        if str(id_) in result:
            raise ValueError(f"Duplicate extracted fragment identity: {id_}")
        result[str(id_)] = {
            "site_id": str(row["site_id"]),
            "sub_site_index": row["sub_site_index"],
            "atom_run_id": json.loads(row["ae_atoms"])["run_id"]
            if row.get("ae_atoms") is not None
            else None,
            "atom_results_schema_version": 1 if row.get("ae_atoms") is not None else None,
        }
    if set(result) != {str(id_) for id_ in ids}:
        raise ValueError("Expected fragments are missing from committed extraction")
    return result


def prepare(params: dict, descriptor: dict) -> None:
    import pyarrow as pa
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq

    restore_extract(descriptor, Path("output"), pixels=True)
    sites = pq.read_table("output/sites_processed.parquet").to_pylist()
    mapping = params.get("exact_match_group_mapping")
    if mapping is not None:
        eligible = {
            (str(site), int(fragment))
            for unit in mapping.values()
            for site, fragment in unit
        }
        sites = [
            site
            for site in sites
            if (str(site["site_id"]), site["sub_site_index"]) in eligible
        ]
    if mapping:
        order = {
            (str(site), int(fragment)): index
            for index, (site, fragment) in enumerate(
                item for group in mapping.values() for item in group
            )
        }
        sites.sort(
            key=lambda site: order[(str(site["site_id"]), site["sub_site_index"])]
        )
    else:
        sites.sort(key=lambda site: site["id_numeric"])
    key_table = pq.read_table("output/treatment_cell_key.parquet")
    dataset = ds.dataset("output/treatment_pixels.parquet")
    cap = min(int(params.get("match_batch_size", 25)), 25)
    block_size = min(int(params.get("replicate_block_size", 10)), 10)
    n_reps = int(params.get("n_replicates", 1))
    if cap < 1 or block_size < 1 or not 1 <= n_reps <= 1000:
        raise ValueError("Invalid fragment/replicate batching limits")
    group_namespace = params["_workflow"].get("group_namespace")
    store = artifact_store(
        params, f"groups/{group_namespace}" if group_namespace else "groups"
    )
    memory = params["_workflow"]["match_memory_mib"] * 1024**2
    partition_var = (params.get("exact_match_vars") or [None])[0]
    controls = (
        ds.dataset(descriptor["control_uri"], format="parquet", partitioning="hive")
        if partition_var
        else None
    )
    # Reserve most memory for decoded controls and the solver. This bounds
    # treatment preparation; confirmed OOM still has its own recovery policy.
    byte_limit = max(1, memory // 8)
    batches = []
    pending = list(sites)
    while pending:
        group = pending[:cap]
        previous = None
        while True:
            ids = [site["id_numeric"] for site in group]
            mask = pa.compute.is_in(
                key_table["id_numeric"],
                value_set=pa.array(ids, type=key_table["id_numeric"].type),
            )
            group_key = key_table.filter(mask)
            cells = group_key["cell"].unique()
            treatments = dataset.to_table(filter=ds.field("cell").isin(cells))
            control_bytes = 0
            control_files = set()
            if controls is not None:
                values = treatments[partition_var].drop_null().unique().to_pylist()
                for fragment in controls.get_fragments(
                    filter=ds.field(partition_var).isin(values)
                ):
                    control_files.add(fragment.path)
                    metadata = fragment.metadata
                    control_bytes += sum(
                        metadata.row_group(i).total_byte_size
                        for i in range(metadata.num_row_groups)
                    )
            estimated_bytes = treatments.nbytes + group_key.nbytes + control_bytes
            if (
                previous is not None
                and control_files == previous[4]
                and control_bytes > byte_limit
                and previous[2].nbytes + previous[3].nbytes <= byte_limit
            ):
                # Splitting identical control reads cannot reduce their footprint.
                group, ids, group_key, treatments, _ = previous
                break
            if estimated_bytes <= byte_limit or len(group) == 1:
                break
            previous = (group, ids, group_key, treatments, control_files)
            group = group[: max(1, len(group) // 2)]
        group_id = f"g{len(batches):06d}"
        folder = Path(group_id)
        folder.mkdir()
        pq.write_table(group_key, folder / "treatment_cell_key.parquet")
        pq.write_table(treatments, folder / "treatment_pixels.parquet")
        group_store = Store(f"{store.uri}/{group_id}")
        for path in folder.iterdir():
            group_store.commit(path, path.name)
        group_uri = group_store.uri
        for start in range(1, n_reps + 1, block_size):
            batch = {
                "id": f"{group_id}_r{start}",
                "ids": ids,
                "replicates": list(range(start, min(start + block_size, n_reps + 1))),
                "group_uri": group_uri,
                "extract": descriptor,
            }
            batches.append(batch)
        pending = pending[len(group) :]
    Path("batches").mkdir()
    for batch in batches:
        write_json(Path("batches") / f"{batch['id']}.json", batch)
    # A sentinel avoids an absent output glob for analyses with no eligible sites.
    if not batches:
        write_json(
            "batches/empty.json",
            {"id": "empty", "ids": [], "replicates": [], "extract": descriptor},
        )
    write_json(
        "expected.json",
        {
            "ids": [site["id_numeric"] for site in sites],
            "n_replicates": n_reps,
            "extract": descriptor,
        },
    )


def checkpoint_files(params: dict, work: dict) -> list[str]:
    store = artifact_store(params, "checkpoints")
    result = []
    replicates = set(work["replicates"])
    for id_ in work["ids"]:
        for key in store.keys(f"f{id_}/"):
            match = re.fullmatch(
                r"f[0-9]+/((?:mb_)?f[0-9]+_r([0-9]+).*?)\.receipt\.json", key
            )
            if match and int(match[2]) in replicates:
                artifact = checkpoint_key(match[1])
                if artifact != key.removesuffix(".receipt.json"):
                    raise ValueError(f"Incompatible checkpoint fragment: {key}")
                result.append(artifact)
    return result


def match_batch(params: dict, work: dict, seconds: int) -> None:
    if not work["ids"]:
        write_json("outcome.json", {"work": work, "unfinished": []})
        return
    config = configuration(params)
    # Metadata is tiny; pixel/key shards are restored only for this locality group.
    metadata = dict(work["extract"])
    metadata["files"] = [
        key
        for key in metadata["files"]
        if key not in ("treatment_pixels.parquet", "treatment_cell_key.parquet")
    ]
    restore_extract(metadata, Path("output"))
    for name in ("treatment_cell_key.parquet", "treatment_pixels.parquet"):
        Store(work["group_uri"]).restore(name, Path("output") / name)
    checkpoint_store = artifact_store(params, "checkpoints")
    matches = Path(config["matches_dir"])
    for key in checkpoint_files(params, work):
        checkpoint_store.restore(key, matches / Path(key).name)
    contract = workflow_contract(params)
    if contract is not None:
        fragments = fragment_identities(Path("output/sites_processed.parquet"), set(work["ids"]))
        config["diagnostic_fragments"] = fragments
        for id_ in work["ids"]:
            for replicate in work["replicates"]:
                sidecar_path = matches / f"mb_f{id_}_r{replicate}.json"
                if sidecar_path.exists():
                    sidecar = json.loads(sidecar_path.read_text())
                    replays = validate_sidecar(
                        sidecar, id_, replicate, contract, fragments[str(id_)],
                        config.get("task_id", ""),
                    )
                    match_name = sidecar.get("matches_file")
                    if match_name and not (matches / match_name).is_file():
                        raise FileNotFoundError(f"Missing committed matches: {match_name}")
                    if replays:
                        # Restored files already passed the SHA-256 receipt check.
                        verify_sample(matches / replays[0]["sample_file"], replays)
    # Controls are immutable committed objects; expose a real dataset prefix,
    # not the receipt namespace. prepare_controls writes it once at extraction.
    config.update(
        match_work=work,
        match_checkpoints=True,
        match_soft_seconds=max(0, seconds - min(120, seconds // 4)),
        checkpoint_uri=checkpoint_store.uri,
        control_pixels_s3_uri=params["_workflow"].get("control_uri")
        or work["extract"]["control_uri"],
    )
    os.environ["AE_PYTHON"] = sys.executable
    completed = run_script("02_perform_matching.R", config, seconds)
    if not completed:
        raise TimeoutError(
            "Matching exceeded its internal deadline; retry from checkpoints"
        )
    unfinished = [
        {"id": id_, "replicate": replicate}
        for id_ in work["ids"]
        for replicate in work["replicates"]
        if checkpoint_store.json(
            checkpoint_key(f"mb_f{id_}_r{replicate}.json") + ".receipt.json"
        )
        is None
    ]
    write_json(
        "outcome.json",
        {"work": work, "unfinished": unfinished, "item_seconds": item_seconds(work)},
    )


def item_seconds(work: dict) -> dict[str, float]:
    """Slowest known fragment/replicate duration per fragment, in seconds."""
    measured = {}
    path = Path("item_seconds.json")
    if path.exists():
        value = json.loads(path.read_text())
        # R serializes an empty named list as [].
        measured = value if isinstance(value, dict) else {}
    result = {
        str(key): float(value) for key, value in work.get("item_seconds", {}).items()
    }
    for key, value in measured.items():
        result[str(key)] = max(result.get(str(key), 0.0), float(value))
    return {key: value for key, value in result.items() if int(key) in work["ids"]}


def repack(outcomes: list[dict], final: bool, seconds: int = 14280) -> None:
    """Split unfinished items into smaller tasks for the next recovery tier.

    Fragments with a measured per-replicate cost get as many replicates per
    task as fit in a fraction of the next task's budget, so expensive fragments
    fan out across nodes; unmeasured fragments halve their replicate ranges.
    """
    Path("batches").mkdir()
    count = 0
    for outcome in outcomes:
        work = outcome["work"]
        costs = {**work.get("item_seconds", {}), **outcome.get("item_seconds", {})}
        by_id = {}
        for item in outcome["unfinished"]:
            by_id.setdefault(item["id"], []).append(item["replicate"])
        for id_, reps in by_id.items():
            cost = costs.get(str(id_))
            if final:
                size = 1
            elif cost:
                size = max(1, math.floor(REPACK_BUDGET_FRACTION * seconds / cost))
            else:
                size = max(1, math.ceil(len(reps) / 2))
            for start in range(0, len(reps), size):
                batch = {
                    **work,
                    "id": f"{work['id']}_f{id_}_p{start}",
                    "ids": [id_],
                    "replicates": reps[start : start + size],
                    "item_seconds": {str(id_): cost} if cost else {},
                }
                write_json(Path("batches") / f"{batch['id']}.json", batch)
                count += 1
    if not count:
        write_json("batches/empty.json", {"id": "empty", "ids": [], "replicates": []})


def prepare_reconcile(expected: dict, outcomes: list[dict]) -> None:
    unfinished = [item for outcome in outcomes for item in outcome["unfinished"]]
    if unfinished:
        raise RuntimeError(
            f"Matching resource budget exhausted: {unfinished[:20]} ({len(unfinished)} items)"
        )
    Path("reconcile_batches").mkdir()
    ids = expected["ids"]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate expected fragment IDs")
    shards = partition_summary(expected)
    write_json(
        "summary_plan.json", {"expected_ids": ids, "shards": shards}
    )
    for shard in shards:
        write_json(
            Path("reconcile_batches") / f"{shard['shard_id']}.json", shard,
        )


def validate_sidecar(
    sidecar: dict, id_: int, replicate: int, contract: dict | None,
    fragment: dict | None, task_id: str,
) -> list[dict]:
    name = f"mb_f{id_}_r{replicate}.json"
    match_name = sidecar.get("matches_file")
    if match_name is not None and match_name != f"mb_f{id_}_r{replicate}.parquet":
        raise ValueError(f"Invalid matched artifact name: {name}")
    records = sidecar.get("diagnostics" if match_name else "failures", [])
    if (
        not isinstance(records, list)
        or len(records) != 1
        or sidecar.get("failures" if match_name else "diagnostics")
        or (records[0].get("id_numeric"), records[0].get("replicate")) != (id_, replicate)
    ):
        raise ValueError(f"Incomplete or incompatible match sidecar: {name}")
    record = records[0]
    if not match_name and record.get("reason") not in (
        "no_overlap", "no_matches", "no_eligible_treatment",
    ):
        raise ValueError(f"Unrecognized analytical failure: {name}")
    if contract is None:
        return []
    if (
        type(sidecar.get("diagnostics_schema_version")) is not int
        or any(sidecar.get(key) != value for key, value in contract.items())
    ):
        raise ValueError(f"Incompatible sidecar schema or policy: {name}")
    compatibility = sidecar.get("compatibility") or {}
    if (
        "atom_run_id" not in sidecar
        or sidecar["atom_run_id"] != fragment["atom_run_id"]
        or any(
            key not in compatibility or compatibility[key] != value
            for key, value in {
                **contract,
                "task_id": task_id,
                "atom_run_id": fragment["atom_run_id"],
                "atom_results_schema_version": fragment["atom_results_schema_version"],
            }.items()
        )
    ):
        raise ValueError(f"Incompatible sidecar task or atom contract: {name}")

    groups = record.get("group_diagnostics")
    expected_keys = record.get("expected_attempt_keys")
    expected_groups = record.get("expected_groups")
    if (
        not isinstance(groups, list) or not groups or not isinstance(expected_keys, list)
        or not isinstance(expected_groups, list)
    ):
        raise ValueError(f"Missing matching attempt roster or diagnostics: {name}")
    attempts = set()
    observed_groups = set()
    replays = []
    expected_sample = f"f{id_}_r{replicate}_sample.rds"
    for group in groups:
        attempt = group.get("attempt_key")
        stratum = group.get("group")
        if (
            not isinstance(attempt, str) or not attempt or attempt in attempts
            or not isinstance(stratum, str) or stratum in observed_groups
            or group.get("id_numeric") != id_
            or group.get("replicate") != replicate
            or group.get("task_id") != task_id
            or type(group.get("diagnostics_schema_version")) is not int
            or any(group.get(key) != value for key, value in contract.items())
            or any(key not in group or group[key] != value for key, value in fragment.items())
        ):
            raise ValueError(f"Incompatible or duplicate matching attempt: {name}")
        attempts.add(attempt)
        observed_groups.add(stratum)
        replay = group.get("replay")
        requires_replay = (
            group.get("glm_failed") is True
            or group.get("reason_code", group.get("cause")) in GLM_REPLAY_CAUSES
            or (group.get("glm") or {}).get("cause") is not None
        )
        if replay is None and not requires_replay:
            continue
        if (
            not isinstance(replay, dict)
            or replay.get("sample_file") != expected_sample
            or not isinstance(replay.get("sample_md5"), str)
            or not re.fullmatch(r"[0-9a-f]{32}", replay["sample_md5"])
            or "group" not in replay or replay["group"] != group.get("group")
            or replay.get("attempt_key") != attempt
        ):
            raise ValueError(f"Missing or invalid matching replay reference: {attempt}")
        replays.append(replay)
    if (
        any(not isinstance(key, str) for key in expected_keys)
        or len(expected_keys) != len(attempts)
        or set(expected_keys) != attempts
        or sidecar.get("expected_attempt_keys") != expected_keys
        or any(not isinstance(group, str) for group in expected_groups)
        or len(expected_groups) != len(observed_groups)
        or set(expected_groups) != observed_groups
    ):
        raise ValueError(f"Incomplete or conflicting matching attempt roster: {name}")
    return replays


def verify_sample(path: Path, replays: list[dict]) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"Missing committed replay sample: {path.name}")
    checksum = digest(path, "md5")
    if any(replay["sample_md5"] != checksum for replay in replays):
        raise ValueError(f"Matching replay checksum mismatch: {path.name}")


def retain_replay(params: dict, source: Store, replays: list[dict]) -> None:
    path = Path(replays[0]["sample_file"])
    try:
        source.restore(checkpoint_key(path.name), path)
        verify_sample(path, replays)
        results = Store(params["_workflow"]["results_uri"].rstrip("/") + "/matching_replay")
        key = f"{digest(path)}/{path.name}"
        already_published = results.json(key + ".receipt.json") is not None
        receipt = results.commit(path, key)
        if already_published:
            # A valid receipt is not enough if its retained object was removed.
            results.restore(key, path)
        for replay in replays:
            replay.update(
                uri=f"{results.uri}/{receipt['object']}",
                sha256=receipt["sha256"],
                size_bytes=receipt["size"],
            )
            replay.setdefault("analysis_image", params["_workflow"].get("analysis_image"))
    finally:
        path.unlink(missing_ok=True)


def reconcile(params: dict, expected: dict, outcomes: list[dict]) -> None:
    started = time.monotonic()
    unfinished = [item for outcome in outcomes for item in outcome["unfinished"]]
    if unfinished:
        raise RuntimeError(
            f"Matching resource budget exhausted: {unfinished[:20]} ({len(unfinished)} items)"
        )
    store = artifact_store(params, "checkpoints")
    contract = workflow_contract(params)
    ids = expected["ids"]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate expected fragment IDs")
    fragments = {}
    if contract is not None and ids:
        extraction = expected["extract"]
        if "sites_processed.parquet" not in extraction["files"]:
            raise ValueError("Missing processed site metadata from extraction manifest")
        path = Path("sites_processed.parquet")
        Store(extraction["uri"]).restore(path.name, path)
        fragments = fragment_identities(path, set(ids))
    Path("matches").mkdir()
    for id_ in ids:
        for replicate in range(1, expected["n_replicates"] + 1):
            name = f"mb_f{id_}_r{replicate}.json"
            sidecar_path = Path("matches") / name
            store.restore(checkpoint_key(name), sidecar_path)
            sidecar = json.loads(sidecar_path.read_text())
            fragment = fragments.get(str(id_))
            replays = validate_sidecar(
                sidecar, id_, replicate, contract, fragment, params.get("task_id", ""),
            )
            match_name = sidecar.get("matches_file")
            if match_name:
                import pyarrow.parquet as pq

                path = Path("matches") / match_name
                store.restore(checkpoint_key(match_name), path)
                pairs = pq.read_table(path, columns=["id_numeric", "replicate"])
                if fragment is not None and (
                    ("atomic_id" in pq.read_schema(path).names)
                    != (fragment["atom_run_id"] is not None)
                ):
                    raise ValueError(f"Incompatible atom-bearing matches: {match_name}")
                if (
                    not pairs.num_rows
                    or pairs["id_numeric"].unique().to_pylist() != [id_]
                    or pairs["replicate"].unique().to_pylist() != [replicate]
                ):
                    raise ValueError(
                        f"Incompatible matched fragment/replicate: {match_name}"
                    )
            if replays:
                retain_replay(params, store, replays)
                write_json(sidecar_path, sidecar)
    write_json("reconciled.json", {"expected": expected})
    log.info(
        "[TIMING] Reconciled %d fragments x %d replicates in %.2fs",
        len(expected["ids"]),
        expected["n_replicates"],
        time.monotonic() - started,
    )


def archive_matches() -> None:
    with tarfile.open("matches.tar", "w") as archive:
        for path in sorted(Path("matches").iterdir()):
            archive.add(path, arcname=path.name, recursive=False)


def restore_archives(archives: list[str], matches: Path) -> None:
    import shutil

    matches.mkdir(parents=True, exist_ok=True)
    for filename in archives:
        with tarfile.open(filename, "r") as archive:
            for member in archive:
                if (
                    not member.isfile()
                    or Path(member.name).name != member.name
                    or Path(member.name).suffix not in (".json", ".parquet")
                ):
                    raise ValueError(f"Invalid match archive member: {member.name}")
                checkpoint_key(member.name)
                target = matches / member.name
                if target.exists():
                    raise ValueError(f"Duplicate match archive member: {member.name}")
                source = archive.extractfile(member)
                if source is None:
                    raise ValueError(f"Cannot read match archive member: {member.name}")
                with source, target.open("xb") as destination:
                    shutil.copyfileobj(source, destination)


def summarize(
    params: dict,
    reconciled: dict,
    matches: Path,
    seconds: int,
    *,
    workers: int = 1,
    validated_matches: bool = False,
) -> None:
    config = configuration(params)
    if workers < 1:
        raise ValueError("Summary worker count must be positive")
    config["summary_workers"] = workers
    config["validated_match_files"] = validated_matches
    config["matches_dir"] = str(matches.resolve())
    if workflow_contract(params) is not None:
        config.update(
            diagnostic_expected_ids=reconciled["expected"]["ids"],
            diagnostic_expected_replicates=reconciled["expected"]["n_replicates"],
            require_matching_diagnostics=True,
        )
    restore_extract(reconciled["expected"]["extract"], Path("output"))
    if not run_script("03_summarize_results.R", config, seconds):
        raise TimeoutError("Summary exceeded its internal deadline")
    if workflow_contract(params) is not None and not all(
        (Path("output") / name).is_file() for name in DIAGNOSTIC_RESULTS
    ):
        raise ValueError("Summary did not publish required matching diagnostic artifacts")
    publish_results(params)


def publish_results(params: dict) -> None:
    from concurrent.futures import ThreadPoolExecutor

    from main import _collect_results

    started = time.monotonic()
    log.info("Collecting summary result payload")
    payload = _collect_results("output", params["task_id"], log)
    log.info("[TIMING] Result collection: %.2fs", time.monotonic() - started)
    runtime = params["_workflow"]
    results_store = Store(runtime["results_uri"])
    started = time.monotonic()
    log.info("Uploading summary results")
    files = sorted(
        path for path in Path("output").glob("*")
        if path.is_file() and path.name.startswith(("results_", "site_identity"))
    )

    def upload(path: Path) -> str:
        if results_store.root is not None:
            results_store.write(path.name, path.read_bytes())
        else:
            results_store.client.upload_file(
                str(path), results_store.bucket, f"{results_store.prefix}/{path.name}"
            )
        return path.name

    with ThreadPoolExecutor(max_workers=4) as executor:
        for name in executor.map(upload, files):
            log.info("Uploaded summary result: %s", name)
    payload["results_s3_uri"] = results_store.uri
    write_json("results.json", payload)
    log.info(
        "[TIMING] Result upload: %.2fs across %d files",
        time.monotonic() - started, len(files),
    )


def summary_metadata(expected: dict) -> None:
    names = {
        "sites_processed.parquet", "treatment_cell_key.parquet", "formula.json",
        "grid_metadata.json", "ae_atom_counts.parquet",
    }
    descriptor = expected["extract"]
    required = names - {"ae_atom_counts.parquet"}
    if not required.issubset(descriptor["files"]):
        raise ValueError("Missing summary extraction metadata")
    restore_extract(
        {**descriptor, "files": [key for key in descriptor["files"] if key in names]},
        Path("output"),
    )


def summarize_shard(
    params: dict, shard: dict, archive: str, seconds: int, workers: int
) -> None:
    config = configuration(params)
    summary_metadata(shard)
    restore_archives([archive], Path(config["matches_dir"]))
    config.update(
        summary_mode="shard", summary_site_ids=shard["site_ids"],
        summary_workers=workers, validated_match_files=True,
    )
    if not run_script("03_summarize_results.R", config, seconds):
        raise TimeoutError("Summary shard exceeded its internal deadline")
    archive_summary(
        "reports.tar", shard, "reports",
        [path for path in Path("output").iterdir()
         if path.is_file() and (
             path.name.startswith(("results_", "site_identity", "na_exclusion"))
             or path.name == "summary_state.rds"
         )],
    )
    archive_summary(
        "diagnostic_inputs.tar", shard, "diagnostic_inputs",
        [Path("output/diagnostic_state.rds"),
         *sorted(Path(config["matches_dir"]).glob("mb_*.json"))],
    )


def export_diagnostics(
    params: dict, shard: dict, archive: str, seconds: int, workers: int
) -> None:
    folders = restore_summary(
        [archive], {"expected_ids": shard["ids"], "shards": [shard]},
        "diagnostic_inputs", Path("diagnostic_inputs"),
    )
    config = configuration(params)
    config.update(
        diagnostic_state=str((folders[0] / "diagnostic_state.rds").resolve()),
        matches_dir=str(folders[0].resolve()),
        diagnostic_expected_ids=shard["ids"],
        diagnostic_expected_replicates=shard["n_replicates"],
        require_matching_diagnostics=True, summary_workers=workers,
    )
    if not run_script("04_export_diagnostics.R", config, seconds):
        raise TimeoutError("Diagnostic shard exceeded its internal deadline")
    archive_summary(
        "diagnostics.tar", shard, "diagnostics",
        [Path("output") / name for name in DIAGNOSTIC_RESULTS],
    )


def finalize_reports(params: dict, plan: dict, archives: list[str], seconds: int) -> None:
    folders = restore_summary(archives, plan, "reports", Path("report_shards"))
    config = configuration(params)
    summary_metadata(plan["shards"][0])
    merge_report_tables(folders, Path("output"))
    config.update(
        summary_mode="reduce",
        summary_site_ids=[site for shard in plan["shards"] for site in shard["site_ids"]],
        summary_states=[str((folder / "summary_state.rds").resolve()) for folder in folders],
    )
    if not run_script("03_summarize_results.R", config, seconds):
        raise TimeoutError("Summary reducer exceeded its internal deadline")
    archive_summary(
        "final_reports.tar",
        {"shard_id": "s000000", "ids": plan["expected_ids"],
         "fragment_ids": [], "site_ids": []},
        "final_reports",
        [path for path in Path("output").iterdir()
         if path.is_file() and path.name.startswith(
             ("results_", "site_identity", "na_exclusion")
         )],
    )


def publish_summary(
    params: dict, plan: dict, report: str, diagnostics: list[str]
) -> None:
    # The report reducer has already checked complete, disjoint site/atom domains.
    final = {"shard_id": "s000000", "ids": plan["expected_ids"],
             "fragment_ids": [], "site_ids": []}
    folders = restore_summary(
        [report], {"expected_ids": plan["expected_ids"], "shards": [final]},
        "final_reports", Path("final_report"),
    )
    Path("output").mkdir(exist_ok=True)
    import shutil

    for path in folders[0].iterdir():
        if path.name != "shard.json":
            shutil.move(str(path), Path("output") / path.name)
    folders = restore_summary(diagnostics, plan, "diagnostics", Path("diagnostic_shards"))
    diagnostic_summary = merge_diagnostics(folders, Path("output"))
    summary = json.loads(Path("output/results_summary.json").read_text())
    summary["matching_diagnostics"] = diagnostic_summary
    write_json("output/results_summary.json", summary)
    publish_results(params)


def commit(config_path: str, files: list[str]) -> None:
    config = json.loads(Path(config_path).read_text())
    store = Store(config["checkpoint_uri"])
    root = (Path(config["data_dir"]) / "output" / "matches").resolve()
    for filename in files:
        path = Path(filename).resolve()
        if path.parent != root:
            raise ValueError("Checkpoint is outside matches directory")
        item = re.fullmatch(r"mb_f([0-9]+)_r([0-9]+)\.json", path.name)
        if item and "diagnostic_fragments" in config:
            id_, replicate = map(int, item.groups())
            sidecar = json.loads(path.read_text())
            replays = validate_sidecar(
                sidecar, id_, replicate, diagnostics_contract(config),
                config["diagnostic_fragments"][str(id_)], config.get("task_id", ""),
            )
            if replays:
                sample = root / replays[0]["sample_file"]
                verify_sample(sample, replays)
                receipt = store.json(checkpoint_key(sample.name) + ".receipt.json")
                if receipt is None or receipt["sha256"] != digest(sample):
                    raise ValueError(f"Replay sample was not committed: {sample.name}")
            match_name = sidecar.get("matches_file")
            if match_name and store.json(checkpoint_key(match_name) + ".receipt.json") is None:
                raise ValueError(f"Matches were not committed: {match_name}")
        store.commit(path, checkpoint_key(path.name))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=[
            "extract",
            "prepare",
            "match",
            "repack",
            "prepare-reconcile",
            "reconcile",
            "summarize",
            "summarize-shard",
            "export-diagnostics",
            "finalize-reports",
            "publish-summary",
            "commit",
        ],
    )
    parser.add_argument("--params")
    parser.add_argument("--input")
    parser.add_argument("--outcomes", nargs="*", default=[])
    parser.add_argument("--matches")
    parser.add_argument("--archives", nargs="*")
    parser.add_argument("--report")
    parser.add_argument("--archive", action="store_true")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--seconds", type=int, default=14280)
    parser.add_argument("--final", action="store_true")
    parser.add_argument("--config")
    parser.add_argument("--files", nargs="+")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    if args.command == "commit":
        commit(args.config, args.files)
        return
    params = load_params(args.params)
    descriptor = json.loads(Path(args.input).read_text()) if args.input else None
    outcomes = [json.loads(Path(path).read_text()) for path in args.outcomes]
    if args.command == "extract":
        extract(params, args.seconds)
    elif args.command == "prepare":
        prepare(params, descriptor)
    elif args.command == "match":
        match_batch(params, descriptor, args.seconds)
    elif args.command == "repack":
        repack(outcomes, args.final, args.seconds)
    elif args.command == "prepare-reconcile":
        prepare_reconcile(descriptor, outcomes)
    elif args.command == "reconcile":
        reconcile(params, descriptor, outcomes)
        if args.archive:
            archive_matches()
    elif args.command == "summarize":
        if args.archives is not None:
            restore_archives(args.archives, Path("matches"))
            summarize(
                params,
                {"expected": descriptor},
                Path("matches"),
                args.seconds,
                workers=args.workers,
                validated_matches=True,
            )
        else:
            summarize(
                params,
                descriptor,
                Path(args.matches),
                args.seconds,
                workers=args.workers,
            )
    elif args.command == "summarize-shard":
        summarize_shard(params, descriptor, args.archives[0], args.seconds, args.workers)
    elif args.command == "export-diagnostics":
        export_diagnostics(params, descriptor, args.archives[0], args.seconds, args.workers)
    elif args.command == "finalize-reports":
        finalize_reports(params, descriptor, args.archives, args.seconds)
    elif args.command == "publish-summary":
        publish_summary(params, descriptor, args.report, args.archives)


if __name__ == "__main__":
    try:
        main()
    except TimeoutError:
        log.exception("Workflow task reached its time limit")
        sys.exit(75)
