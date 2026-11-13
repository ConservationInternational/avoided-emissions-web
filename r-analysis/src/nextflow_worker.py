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
import time
import uuid
from pathlib import Path

from workflow_store import Store, checkpoint_key

log = logging.getLogger(__name__)
SCRIPTS = Path(os.environ.get("R_SCRIPTS_DIR", "/app/scripts"))
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
    "n_replicates",
    "random_seed",
    "matching_extent",
    "sites_exclusion_buffer",
    "reference_layer_uris",
    "exact_match_group_mapping",
    "site_id",
}


def write_json(path: str | Path, value) -> None:
    Path(path).write_text(json.dumps(value, sort_keys=True))


def load_params(path: str) -> dict:
    params = json.loads(Path(path).read_text())
    runtime = params["_workflow"]
    if not runtime.get("identity") or not runtime.get("state_uri"):
        raise ValueError("Missing immutable workflow identity/state URI")
    return params


def configuration(params: dict) -> dict:
    config = {key: value for key, value in params.items() if key in ANALYSIS_FIELDS}
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
    store = artifact_store(params, "groups")
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
                result.append(key.removesuffix(".receipt.json"))
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
    for key in checkpoint_files(params, work):
        checkpoint_store.restore(key, Path(config["matches_dir"]) / Path(key).name)
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
    write_json("outcome.json", {"work": work, "unfinished": unfinished})


def repack(outcomes: list[dict], final: bool) -> None:
    Path("batches").mkdir()
    count = 0
    for outcome in outcomes:
        work = outcome["work"]
        by_id = {}
        for item in outcome["unfinished"]:
            by_id.setdefault(item["id"], []).append(item["replicate"])
        for id_, reps in by_id.items():
            size = 1 if final else max(1, math.ceil(len(reps) / 2))
            for start in range(0, len(reps), size):
                batch = {
                    **work,
                    "id": f"{work['id']}_f{id_}_p{start}",
                    "ids": [id_],
                    "replicates": reps[start : start + size],
                }
                write_json(Path("batches") / f"{batch['id']}.json", batch)
                count += 1
    if not count:
        write_json("batches/empty.json", {"id": "empty", "ids": [], "replicates": []})


def reconcile(params: dict, expected: dict, outcomes: list[dict]) -> None:
    unfinished = [item for outcome in outcomes for item in outcome["unfinished"]]
    if unfinished:
        raise RuntimeError(
            f"Matching resource budget exhausted: {unfinished[:20]} ({len(unfinished)} items)"
        )
    store = artifact_store(params, "checkpoints")
    Path("matches").mkdir()
    for id_ in expected["ids"]:
        for replicate in range(1, expected["n_replicates"] + 1):
            name = f"mb_f{id_}_r{replicate}.json"
            store.restore(checkpoint_key(name), Path("matches") / name)
            sidecar = json.loads((Path("matches") / name).read_text())
            match_name = sidecar.get("matches_file")
            records = sidecar.get("diagnostics" if match_name else "failures", [])
            if len(records) != 1 or (
                records[0].get("id_numeric"),
                records[0].get("replicate"),
            ) != (id_, replicate):
                raise ValueError(f"Incomplete or incompatible match sidecar: {name}")
            if match_name:
                import pyarrow.parquet as pq

                if Path(match_name).name != match_name:
                    raise ValueError("Invalid match sidecar")
                store.restore(checkpoint_key(match_name), Path("matches") / match_name)
                pairs = pq.read_table(
                    Path("matches") / match_name, columns=["id_numeric", "replicate"]
                )
                if (
                    not pairs.num_rows
                    or pairs["id_numeric"].unique().to_pylist() != [id_]
                    or pairs["replicate"].unique().to_pylist() != [replicate]
                ):
                    raise ValueError(
                        f"Incompatible matched fragment/replicate: {match_name}"
                    )
            elif records[0].get("reason") not in (
                "no_overlap",
                "no_matches",
                "no_eligible_treatment",
            ):
                raise ValueError(f"Unrecognized analytical failure: {name}")
    write_json("reconciled.json", {"expected": expected})


def summarize(params: dict, reconciled: dict, matches: Path, seconds: int) -> None:
    import shutil

    from main import _collect_results

    config = configuration(params)
    restore_extract(reconciled["expected"]["extract"], Path("output"))
    for path in matches.iterdir():
        shutil.copyfile(path, Path(config["matches_dir"]) / path.name)
    if not run_script("03_summarize_results.R", config, seconds):
        raise TimeoutError("Summary exceeded its internal deadline")
    payload = _collect_results("output", params["task_id"], log)
    runtime = params["_workflow"]
    results_store = Store(runtime["results_uri"])
    for path in Path("output").glob("*"):
        if path.is_file() and path.name.startswith(("results_", "site_identity")):
            # Keep the existing download convention (not the checkpoint layout).
            if results_store.root is not None:
                results_store.write(path.name, path.read_bytes())
            else:
                results_store.client.upload_file(
                    str(path),
                    results_store.bucket,
                    f"{results_store.prefix}/{path.name}",
                )
    payload["results_s3_uri"] = results_store.uri
    write_json("results.json", payload)


def commit(config_path: str, files: list[str]) -> None:
    config = json.loads(Path(config_path).read_text())
    store = Store(config["checkpoint_uri"])
    root = (Path(config["data_dir"]) / "output" / "matches").resolve()
    for filename in files:
        path = Path(filename).resolve()
        if path.parent != root:
            raise ValueError("Checkpoint is outside matches directory")
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
            "reconcile",
            "summarize",
            "commit",
        ],
    )
    parser.add_argument("--params")
    parser.add_argument("--input")
    parser.add_argument("--outcomes", nargs="*", default=[])
    parser.add_argument("--matches")
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
        repack(outcomes, args.final)
    elif args.command == "reconcile":
        reconcile(params, descriptor, outcomes)
    elif args.command == "summarize":
        summarize(params, descriptor, Path(args.matches), args.seconds)


if __name__ == "__main__":
    try:
        main()
    except TimeoutError:
        log.exception("Workflow task reached its time limit")
        sys.exit(75)
