"""Site-local summary partitions and completeness-checked result reduction."""

from __future__ import annotations

import csv
import io
import json
import re
import shutil
import tarfile
from collections import Counter
from pathlib import Path

from workflow_store import Store


def partition_summary(expected: dict) -> list[dict]:
    import pyarrow.parquet as pq

    store = Store(expected["extract"]["uri"])
    store.restore("sites_processed.parquet", Path("summary_sites.parquet"))
    store.restore("treatment_cell_key.parquet", Path("summary_key.parquet"))
    sites = pq.read_table(
        "summary_sites.parquet", columns=["id_numeric", "site_id"]
    ).to_pylist()
    pixels = Counter()
    for batch in pq.ParquetFile("summary_key.parquet").iter_batches(columns=["id_numeric"]):
        pixels.update(batch.column(0).to_pylist())
    expected_ids = set(expected["ids"])
    all_ids = [row["id_numeric"] for row in sites]
    if len(all_ids) != len(set(all_ids)) or not expected_ids.issubset(all_ids):
        raise ValueError("Invalid summary fragment roster")
    parents: dict[str, list[int]] = {}
    for row in sites:
        parents.setdefault(str(row["site_id"]), []).append(row["id_numeric"])
    shards = [
        {
            **expected,
            "shard_id": f"s{index:06d}",
            "site_ids": [],
            "fragment_ids": [],
            "ids": [],
        }
        for index in range(min(16, max(1, len(parents))))
    ]
    loads = [0] * len(shards)
    weights = {
        site: sum(1 + pixels[id_] for id_ in ids) * expected["n_replicates"]
        for site, ids in parents.items()
    }
    for site in sorted(parents, key=lambda value: (-weights[value], value)):
        index = min(range(len(shards)), key=lambda value: (loads[value], value))
        shard = shards[index]
        shard["site_ids"].append(site)
        shard["fragment_ids"].extend(parents[site])
        shard["ids"].extend(id_ for id_ in parents[site] if id_ in expected_ids)
        loads[index] += weights[site]
    for shard in shards:
        for key in ("site_ids", "fragment_ids", "ids"):
            shard[key].sort()
    return shards


def archive_summary(
    filename: str, shard: dict, kind: str, files: list[Path]
) -> None:
    names = [path.name for path in files]
    if len(names) != len(set(names)) or "shard.json" in names:
        raise ValueError("Duplicate summary archive member")
    manifest = json.dumps(
        {"schema_version": 1, "kind": kind, "shard": shard, "files": sorted(names)},
        sort_keys=True,
    ).encode()
    with tarfile.open(filename, "w") as archive:
        info = tarfile.TarInfo("shard.json")
        info.size = len(manifest)
        archive.addfile(info, io.BytesIO(manifest))
        for path in sorted(files):
            archive.add(path, arcname=path.name, recursive=False)


def restore_summary(
    archives: list[str], plan: dict, kind: str, root: Path
) -> list[Path]:
    expected = {shard["shard_id"]: shard for shard in plan["shards"]}
    if len(expected) != len(plan["shards"]):
        raise ValueError("Duplicate planned summary shard")
    observed = set()
    folders = []
    root.mkdir()
    for filename in archives:
        with tarfile.open(filename, "r") as archive:
            members = archive.getmembers()
            names = [member.name for member in members]
            if (
                len(names) != len(set(names))
                or "shard.json" not in names
                or any(
                    not member.isfile() or Path(member.name).name != member.name
                    for member in members
                )
            ):
                raise ValueError("Unsafe or duplicate summary archive member")
            source = archive.extractfile("shard.json")
            if source is None:
                raise ValueError("Missing summary shard manifest")
            with source:
                manifest = json.load(source)
            shard = manifest["shard"]
            id_ = shard["shard_id"]
            if (
                not re.fullmatch(r"s[0-9]{6}", id_)
                or id_ in observed
                or id_ not in expected
                or shard != expected[id_]
                or manifest["schema_version"] != 1
                or manifest["kind"] != kind
                or manifest["files"] != sorted(set(names) - {"shard.json"})
            ):
                raise ValueError("Duplicate or incompatible summary shard")
            observed.add(id_)
            folder = root / id_
            folder.mkdir()
            for member in members:
                source = archive.extractfile(member)
                if source is None:
                    raise ValueError("Missing summary archive member")
                with source, (folder / member.name).open("xb") as target:
                    shutil.copyfileobj(source, target)
            folders.append(folder)
    if observed != set(expected):
        raise ValueError("Incomplete summary shards")
    for key in ("ids", "fragment_ids", "site_ids"):
        values = [value for shard in expected.values() for value in shard[key]]
        if len(values) != len(set(values)):
            raise ValueError(f"Overlapping summary shard {key}")
    if set(value for shard in expected.values() for value in shard["ids"]) != set(
        plan["expected_ids"]
    ):
        raise ValueError("Incomplete summary fragment coverage")
    return sorted(folders)


def merge_csv_tables(paths: list[Path], output: Path) -> None:
    headers = []
    offsets = []
    for path in paths:
        with path.open("rb") as stream:
            # Read only the header record, without buffering past its byte offset.
            lines = iter(lambda: stream.readline().decode("utf-8"), "")
            header = next(csv.reader(lines), None)
            if not header or len(header) != len(set(header)):
                raise ValueError(f"Missing or duplicate CSV header: {path.name}")
            headers.append(header)
            offsets.append(stream.tell())
    fields = list(max(headers, key=len))
    if all(header == fields for header in headers):
        header_text = io.StringIO(newline="")
        csv.writer(header_text).writerow(fields)
        with output.open("wb") as target:
            target.write(header_text.getvalue().encode("utf-8"))
            for path, offset in zip(paths, offsets, strict=True):
                with path.open("rb") as source:
                    source.seek(offset)
                    shutil.copyfileobj(source, target)
                    if source.tell() > offset:
                        source.seek(-1, io.SEEK_END)
                        if source.read(1) not in (b"\n", b"\r"):
                            target.write(b"\n")
        return
    for header in headers:
        fields.extend(field for field in header if field not in fields)
    with output.open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=fields)
        writer.writeheader()
        for path in paths:
            with path.open(encoding="utf-8", newline="") as stream:
                writer.writerows(csv.DictReader(stream))


def merge_report_tables(folders: list[Path], output: Path) -> None:
    names = sorted({path.name for folder in folders for path in folder.glob("*.csv")})
    for name in names:
        paths = [folder / name for folder in folders if (folder / name).is_file()]
        merge_csv_tables(paths, output / name)
    quality = None
    for folder in folders:
        value = json.loads((folder / "results_match_quality_summary.json").read_text())
        if quality is None:
            quality = {
                key: value[key] for key in ("covariate_cols", "categorical_cols")
            }
            quality.update(
                summary_stats={}, histograms={}, qq_quantiles={}
            )
        if quality["categorical_cols"] != value["categorical_cols"]:
            raise ValueError("Incompatible summary covariates")
        for key in ("summary_stats", "histograms", "qq_quantiles"):
            entries = value[key] or {}
            for scope, data in entries.items():
                if scope == "__all__":
                    continue
                if scope in quality[key]:
                    raise ValueError("Duplicate site match-quality scope")
                quality[key][scope] = data
    (output / "results_match_quality_summary.json").write_text(json.dumps(quality))


def merge_diagnostics(folders: list[Path], output: Path) -> dict:
    summary = None
    diagnostic_path = output / "results_matching_diagnostics.jsonl.gz"
    replay_path = output / "results_matching_replay_manifest.json"
    first = True
    with diagnostic_path.open("wb") as target, replay_path.open("w") as replay:
        replay.write('{"diagnostics_schema_version":1,"attempts":[')
        for folder in folders:
            value = json.loads(
                (folder / "results_matching_diagnostics_summary.json").read_text()
            )
            shard = json.loads((folder / "shard.json").read_text())["shard"]
            required = len(shard["ids"]) * shard["n_replicates"]
            if (
                value["diagnostics_complete"] is not True
                or value["counts"]["expected_items"] != required
                or value["counts"]["observed_items"] != required
                or value["counts"]["missing_items"] != 0
                or value["counts"]["unexpected_items"] != 0
            ):
                raise ValueError("Incomplete shard matching diagnostics")
            if summary is None:
                summary = value
            else:
                for key in ("diagnostics_schema_version", "matching_policy", "atom_run_id", "artifacts"):
                    if summary[key] != value[key]:
                        raise ValueError("Incompatible shard matching diagnostics")
                for key in ("counts", "by_cause", "by_covariate", "by_replicate", "atom_status_counts"):
                    summary[key] = add_counts(summary[key], value[key])
            # Concatenated gzip members preserve JSONL without central re-encoding.
            with (folder / diagnostic_path.name).open("rb") as source:
                shutil.copyfileobj(source, target)
            with (folder / replay_path.name).open() as source:
                entries = json.load(source)["attempts"]
            for entry in entries:
                if not first:
                    replay.write(",")
                json.dump(entry, replay, separators=(",", ":"))
                first = False
        replay.write("]}")
    if summary is None:
        raise ValueError("Missing diagnostic shards")
    summary["output_bytes"] = {
        "diagnostics": diagnostic_path.stat().st_size,
        "replay": replay_path.stat().st_size,
    }
    (output / "results_matching_diagnostics_summary.json").write_text(json.dumps(summary))
    return summary


def add_counts(left, right):
    if left == [] and isinstance(right, dict):
        return right
    if right == [] and isinstance(left, dict):
        return left
    if isinstance(left, dict) and isinstance(right, dict):
        result = dict(left)
        for key, value in right.items():
            result[key] = add_counts(result[key], value) if key in result else value
        return result
    if type(left) is int and type(right) is int:
        return left + right
    if left == [] and right == []:
        return []
    raise ValueError("Invalid diagnostic count aggregation")
