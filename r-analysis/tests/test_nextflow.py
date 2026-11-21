"""Focused durability, packing, resume, and workflow wiring regressions."""

import contextlib
import csv
import gzip
import json
import math
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "nextflow"))

import driver
import nextflow_worker as worker
from summary_shards import merge_csv_tables
from workflow_store import Store, checkpoint_key, digest


def exercise_fixture(config_path: Path, invalid_late_outcome: bool = False) -> None:
    """Run real worker stages against the synthetic R pipeline fixture."""
    root = config_path.parent
    params = json.loads(config_path.read_text())
    params["_workflow"] = {
        "state_uri": str(root / "state"),
        "identity": "integration",
        "match_memory_mib": 1024,
        "results_uri": str(root / "results"),
        "diagnostics_contract": worker.diagnostics_contract(params),
    }
    params["replicate_block_size"] = 2
    store = worker.artifact_store(params, "extract")
    keys = []
    for path in (root / "output").iterdir():
        if path.is_file():
            store.commit(path, path.name)
            keys.append(path.name)
    descriptor = {
        "uri": store.uri,
        "files": keys,
        "control_uri": str(root / "output/control_pixels"),
    }
    preparation = root / "prepare"
    preparation.mkdir()
    with contextlib.chdir(preparation):
        worker.prepare(params, descriptor)
    expected = json.loads((preparation / "expected.json").read_text())
    outcomes = []
    for batch in sorted((preparation / "batches").glob("*.json")):
        directory = root / batch.stem
        directory.mkdir()
        with contextlib.chdir(directory):
            worker.match_batch(params, json.loads(batch.read_text()), 120)
            outcomes.append(json.loads(Path("outcome.json").read_text()))
    batch = json.loads(min((preparation / "batches").glob("*.json")).read_text())
    checkpoint_store = worker.artifact_store(params, "checkpoints")
    receipt_key = checkpoint_key(
        f"mb_f{batch['ids'][0]}_r{batch['replicates'][0]}.json"
    )
    before = checkpoint_store.json(receipt_key + ".receipt.json")
    assert before is not None
    replay = root / "match-updated"
    replay.mkdir()

    def run_matching_and_verify_reuse(script, config, seconds):
        worker.write_json("config.json", config)
        result = subprocess.run(
            ["Rscript", str(worker.SCRIPTS / script), "--config", "config.json"],
            text=True,
            capture_output=True,
            timeout=seconds,
            check=True,
        )
        assert "  Site " not in result.stderr, result.stderr
        return True

    with (
        contextlib.chdir(replay),
        patch.object(worker, "run_script", side_effect=run_matching_and_verify_reuse),
    ):
        worker.match_batch(params, batch, 120)
        assert not json.loads(Path("outcome.json").read_text())["unfinished"]
    assert checkpoint_store.json(receipt_key + ".receipt.json") == before
    reconciliation = root / "reconcile"
    reconciliation.mkdir()

    def inject_invalid_outcome(directory):
        if not invalid_late_outcome or not (directory / "mb_f2_r2.parquet").exists():
            return
        import pyarrow as pa
        import pyarrow.parquet as pq

        path = directory / "mb_f2_r2.parquet"
        table = pq.read_table(path)
        values = table["total_biomass_2000"].to_pylist()
        values[0] = -9999
        column = table.schema.get_field_index("total_biomass_2000")
        pq.write_table(table.set_column(column, table.schema.field(column), pa.array(values)), path)

    with contextlib.chdir(reconciliation):
        worker.reconcile(params, expected, outcomes)
        inject_invalid_outcome(Path("matches"))
    summary = root / "summary"
    summary.mkdir()
    with contextlib.chdir(summary):
        worker.summarize(
            params,
            json.loads((reconciliation / "reconciled.json").read_text()),
            reconciliation / "matches",
            120,
        )
        payload = json.loads(Path("results.json").read_text())
    assert payload["summary"]["n_sites"] == (1 if invalid_late_outcome else 2), payload["summary"]
    assert payload["summary"]["n_replicates"] == 2, payload["summary"]
    expected_items = len(expected["ids"]) * expected["n_replicates"]
    assert len(list((reconciliation / "matches").glob("mb_*.json"))) == expected_items
    assert (root / "results/results_by_site_total.csv").exists()
    diagnostics = payload["summary"]["matching_diagnostics"]
    assert diagnostics["counts"]["expected_items"] == expected_items
    assert diagnostics["counts"]["observed_items"] == expected_items
    assert set(diagnostics["artifacts"].values()) == set(worker.DIAGNOSTIC_RESULTS)
    for filename in diagnostics["artifacts"].values():
        assert (root / "results" / filename).is_file()

    partitioning = root / "reconcile-partitioning"
    partitioning.mkdir()
    with contextlib.chdir(partitioning):
        worker.prepare_reconcile(expected, outcomes)
    archives = []
    reports = []
    diagnostic_archives = []
    for batch in sorted((partitioning / "reconcile_batches").glob("*.json")):
        descriptor = json.loads(batch.read_text())
        shard = root / f"reconcile-{batch.stem}"
        shard.mkdir()
        with contextlib.chdir(shard):
            worker.reconcile(params, descriptor, [])
            inject_invalid_outcome(Path("matches"))
            worker.archive_matches()
        archives.append(str(shard / "matches.tar"))
        summary = root / f"summary-{batch.stem}"
        summary.mkdir()
        with contextlib.chdir(summary):
            worker.summarize_shard(params, descriptor, archives[-1], 120, 1)
        with (summary / "output/results_covariate_balance_replicates.csv").open(newline="") as stream:
            assert all(row["scope_type"] != "portfolio" for row in csv.DictReader(stream))
        reports.append(str(summary / "reports.tar"))
        diagnostics = root / f"diagnostics-{batch.stem}"
        diagnostics.mkdir()
        with contextlib.chdir(diagnostics):
            worker.export_diagnostics(
                params, descriptor, str(summary / "diagnostic_inputs.tar"), 120, 1
            )
        diagnostic_archives.append(str(diagnostics / "diagnostics.tar"))
    parallel_summary = root / "parallel-summary"
    parallel_summary.mkdir()
    with contextlib.chdir(parallel_summary):
        worker.restore_archives(archives, Path("matches"))
        worker.summarize(
            params,
            {"expected": expected},
            Path("matches"),
            120,
            workers=2,
            validated_matches=True,
        )
        assert json.loads(Path("results.json").read_text()) == payload
    reduced = root / "reduced"
    reduced.mkdir()
    plan = json.loads((partitioning / "summary_plan.json").read_text())
    with contextlib.chdir(reduced):
        worker.finalize_reports(params, plan, reports, 120)
    published = root / "published"
    published.mkdir()
    with contextlib.chdir(published):
        worker.publish_summary(
            params, plan, str(reduced / "final_reports.tar"), diagnostic_archives
        )
        assert json.loads(Path("results.json").read_text()) == payload
        with gzip.open("output/results_matching_diagnostics.jsonl.gz", "rt") as stream:
            distributed_records = [json.loads(line) for line in stream]
    with gzip.open(root / "summary/output/results_matching_diagnostics.jsonl.gz", "rt") as stream:
        original_records = [json.loads(line) for line in stream]
    record_key = lambda record: (
        record["record_type"], record.get("attempt_key", record.get("atomic_id"))
    )
    assert sorted(distributed_records, key=record_key) == sorted(original_records, key=record_key)
    for name in ("results_match_quality_summary.json", "results_matching_diagnostics_summary.json"):
        original = json.loads((root / "summary/output" / name).read_text())
        distributed = json.loads((published / "output" / name).read_text())
        if name == "results_matching_diagnostics_summary.json":
            original.pop("output_bytes")
            distributed.pop("output_bytes")
        assert distributed == original, name
    for path in (root / "summary/output").glob("*.csv"):
        if not path.name.startswith(("results_", "site_identity", "na_exclusion")):
            continue
        with path.open(newline="") as stream:
            expected_rows = list(csv.DictReader(stream))
        with (published / "output" / path.name).open(newline="") as stream:
            actual_rows = list(csv.DictReader(stream))
        if path.name == "results_covariate_balance_replicates.csv":
            keys = (
                "task_id", "replicate", "scope_type", "site_id", "fragment_id",
                "stratum", "scope", "covariate",
            )
            normalize = lambda rows, keys=keys: sorted(
                rows, key=lambda row: tuple(row[key] for key in keys)
            )
        else:
            normalize = lambda rows: sorted(rows, key=lambda row: json.dumps(row, sort_keys=True))
        actual_rows, expected_rows = normalize(actual_rows), normalize(expected_rows)
        assert len(actual_rows) == len(expected_rows), path.name
        for actual, expected_row in zip(actual_rows, expected_rows, strict=True):
            assert actual.keys() == expected_row.keys(), path.name
            for key, value in actual.items():
                if value == expected_row[key]:
                    continue
                assert key not in {
                    "cell", "site_id", "atomic_id", "run_id", "replicate",
                    "year", "group", "match_group",
                }, (path.name, key)
                assert math.isclose(
                    float(value), float(expected_row[key]), rel_tol=1e-12, abs_tol=1e-12
                ), (path.name, key, value, expected_row[key])

    # A legacy shard-local portfolio row must not survive reduction, while
    # nonportfolio rows must be reused rather than recomputed from cov_data.
    altered = root / "altered-report"
    altered.mkdir()
    with tarfile.open(reports[0]) as archive:
        archive.extractall(altered, filter="data")
    balance_path = altered / "results_covariate_balance_replicates.csv"
    with balance_path.open(newline="") as stream:
        reader = csv.DictReader(stream)
        fields = reader.fieldnames
        rows = list(reader)
    reused = next(row for row in rows if row["scope_type"] == "matching_unit")
    reused["worst_abs_smd"] = "987654321"
    legacy = dict(reused, scope_type="portfolio", site_id="__all__")
    with balance_path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows([*rows, legacy])
    altered_archive = root / "altered-report.tar"
    with tarfile.open(altered_archive, "w") as archive:
        for path in sorted(altered.iterdir()):
            archive.add(path, arcname=path.name, recursive=False)
    reuse_reduced = root / "reuse-reduced"
    reuse_reduced.mkdir()
    with contextlib.chdir(reuse_reduced):
        worker.finalize_reports(params, plan, [str(altered_archive), *reports[1:]], 120)
        with Path("output/results_covariate_balance_replicates.csv").open(newline="") as stream:
            rows = list(csv.DictReader(stream))
    assert any(
        row["scope_type"] == "matching_unit" and row["worst_abs_smd"] == "987654321"
        for row in rows
    )
    assert not any(
        row["scope_type"] == "portfolio" and row["worst_abs_smd"] == "987654321"
        for row in rows
    )


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def params(self):
        return {
            "n_replicates": 100,
            "match_batch_size": 2,
            "replicate_block_size": 10,
            "_workflow": {
                "state_uri": str(self.root / "state"),
                "identity": "fixture",
                "match_memory_mib": 1024,
            },
        }

    def summary_extraction(self, ids):
        import pyarrow as pa
        import pyarrow.parquet as pq

        store = Store(str(self.root / "summary-extract"))
        sites = self.root / "summary-sites.parquet"
        key = self.root / "summary-key.parquet"
        pq.write_table(pa.table({
            "id_numeric": pa.array(ids, type=pa.int64()),
            "site_id": pa.array(
                [f"site{(id_ - 1) // 2:03}" for id_ in ids], type=pa.string()
            ),
        }), sites)
        pq.write_table(pa.table({"id_numeric": pa.array(ids, type=pa.int64())}), key)
        store.commit(sites, "sites_processed.parquet")
        store.commit(key, "treatment_cell_key.parquet")
        return {"uri": store.uri, "files": [
            "sites_processed.parquet", "treatment_cell_key.parquet"
        ]}

    def test_driver_logs_invalid_site_details_once(self):
        failure = {
            "site_id": "site-a",
            "reason": "invalid_outcome_inputs",
            "error": "fragment=4, replicate=2, cell=123, total_biomass_2000=-9999",
        }
        with self.assertLogs(driver.log, level="WARNING") as logs:
            driver.log_invalid_outcome_sites(
                {"summary": {"failed_sites": [failure, failure]}}
            )
        self.assertEqual(len(logs.output), 1)
        self.assertIn(failure["error"], logs.output[0])

    def test_commits_are_idempotent_conflict_checked_and_verified(self):
        store = Store(str(self.root / "store"))
        path = self.root / "item"
        path.write_bytes(b"first")
        key = checkpoint_key("mb_f1_r2.json")
        store.commit(path, key)
        store.commit(path, key)
        store.restore(key, self.root / "restored")
        self.assertEqual((self.root / "restored").read_bytes(), b"first")
        path.write_bytes(b"different")
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            store.commit(path, key)
        receipt = store.json(key + ".receipt.json")
        (store.root / receipt["object"]).write_bytes(b"corrupt")
        with self.assertRaisesRegex(ValueError, "Corrupt"):
            store.restore(key, self.root / "restored")
        self.assertFalse((self.root / "restored").exists())
        with self.assertRaises(ValueError):
            store.write("../escape", b"")
        with self.assertRaises(ValueError):
            checkpoint_key("mb_all.json")

    def test_preparation_amortizes_one_shard_across_ten_replicate_blocks(self):
        import pyarrow as pa
        import pyarrow.parquet as pq

        params = self.params()
        params["exact_match_vars"] = ["admin1"]
        source = self.root / "source"
        source.mkdir()
        atom_id = "9007199254740993"
        tables = {
            "sites_processed.parquet": pa.table(
                {
                    "id_numeric": [1, 2],
                    "site_id": ["a", "b"],
                    "sub_site_index": [0, 0],
                    "ae_atoms": [
                        json.dumps({"run_id": "atoms", "atoms": [{"atomic_id": atom_id}]}),
                        json.dumps({"run_id": "atoms", "atoms": [{"atomic_id": "2"}]}),
                    ],
                }
            ),
            "treatment_cell_key.parquet": pa.table(
                {"id_numeric": [1, 2], "cell": [10, 20], "atomic_id": [atom_id, "2"]}
            ),
            "treatment_pixels.parquet": pa.table(
                {"cell": [10, 20], "value": [1.0, 2.0], "admin1": [1, 2]}
            ),
            "ae_atom_counts.parquet": pa.table(
                {"atomic_id": [atom_id, "2"], "site_id": ["a", "b"], "n_forest_pixels": [1, 1]}
            ),
        }
        store = worker.artifact_store(params, "extract")
        for name, table in tables.items():
            path = source / name
            pq.write_table(table, path)
            store.commit(path, name)
        controls = self.root / "controls"
        pq.write_to_dataset(
            pa.table({"admin1": [1] * 20000 + [2] * 20000, "value": range(40000)}),
            controls,
            partition_cols=["admin1"],
        )
        descriptor = {
            "uri": store.uri,
            "files": list(tables),
            "control_uri": str(controls),
        }
        work = self.root / "prepare"
        work.mkdir()
        with contextlib.chdir(work):
            worker.prepare(params, descriptor)
        batches = [json.loads(p.read_text()) for p in (work / "batches").glob("*.json")]
        self.assertEqual(len(batches), 10)
        self.assertEqual(len({batch["group_uri"] for batch in batches}), 1)
        Store(batches[0]["group_uri"]).restore(
            "treatment_pixels.parquet", self.root / "shard"
        )
        self.assertEqual(pq.read_table(self.root / "shard").num_rows, 2)
        Store(batches[0]["group_uri"]).restore(
            "treatment_cell_key.parquet", self.root / "key"
        )
        self.assertEqual(pq.read_table(self.root / "key")["atomic_id"].to_pylist(), [atom_id, "2"])
        # The recovery batch and final summary both restore extraction metadata.
        for name in ("recovery", "summary"):
            output = self.root / name
            worker.restore_extract(batches[0]["extract"], output)
            self.assertEqual(
                pq.read_table(output / "ae_atom_counts.parquet")["atomic_id"].to_pylist(),
                [atom_id, "2"],
            )
            self.assertEqual(
                worker.fragment_identities(output / "sites_processed.parquet", {1})["1"]["atom_run_id"],
                "atoms",
            )
        coverage = [
            (id_, rep)
            for batch in batches
            for id_ in batch["ids"]
            for rep in batch["replicates"]
        ]
        self.assertEqual(len(coverage), 200)
        self.assertEqual(len(set(coverage)), 200)
        self.assertEqual({rep for _, rep in coverage}, set(range(1, 101)))
        params["_workflow"]["group_namespace"] = "new-image"
        upgraded = self.root / "upgraded"
        upgraded.mkdir()
        with contextlib.chdir(upgraded):
            worker.prepare(params, descriptor)
        upgraded_batch = json.loads(
            next((upgraded / "batches").glob("*.json")).read_text()
        )
        self.assertNotEqual(upgraded_batch["group_uri"], batches[0]["group_uri"])
        Store(upgraded_batch["group_uri"]).restore(
            "treatment_pixels.parquet", self.root / "upgraded-shard"
        )
        self.assertEqual(pq.read_table(self.root / "upgraded-shard").num_rows, 2)
        del params["_workflow"]["group_namespace"]
        params["_workflow"].update(identity="small-memory", match_memory_mib=1)
        small = self.root / "small"
        small.mkdir()
        with contextlib.chdir(small):
            worker.prepare(params, descriptor)
        smaller_batches = [
            json.loads(p.read_text()) for p in (small / "batches").glob("*.json")
        ]
        self.assertEqual(len(smaller_batches), 20)
        self.assertTrue(all(len(batch["ids"]) == 1 for batch in smaller_batches))
        params["_workflow"]["identity"] = "shared-control-memory"
        shared_store = worker.artifact_store(params, "extract")
        for name, table in tables.items():
            path = source / name
            if name == "treatment_pixels.parquet":
                table = table.set_column(2, "admin1", pa.array([1, 1]))
            pq.write_table(table, path)
            shared_store.commit(path, name)
        shared = self.root / "shared"
        shared.mkdir()
        with contextlib.chdir(shared):
            worker.prepare(params, {**descriptor, "uri": shared_store.uri})
        shared_batches = [
            json.loads(p.read_text()) for p in (shared / "batches").glob("*.json")
        ]
        self.assertEqual(len(shared_batches), 10)
        self.assertTrue(all(batch["ids"] == [1, 2] for batch in shared_batches))

    def test_repack_only_unfinished_work_and_reconcile_refuses_missing_results(self):
        params = self.params()
        outcomes = [
            {
                "work": {"id": "g1", "ids": [1, 2], "replicates": [1, 2]},
                "unfinished": [{"id": 2, "replicate": 2}],
            }
        ]
        with contextlib.chdir(self.root):
            worker.repack(outcomes, final=True)
            batch = json.loads(next(Path("batches").glob("*.json")).read_text())
            self.assertEqual(batch["ids"], [2])
            self.assertEqual(batch["replicates"], [2])
            with self.assertRaisesRegex(RuntimeError, "budget exhausted"):
                worker.reconcile(params, {"ids": [1], "n_replicates": 1}, outcomes)
            with self.assertRaises(FileNotFoundError):
                worker.reconcile(params, {"ids": [1], "n_replicates": 1}, [])

    def test_repack_spreads_measured_expensive_replicates_across_tasks(self):
        work = {
            "id": "g1",
            "ids": [12, 31],
            "replicates": list(range(21, 31)),
            "item_seconds": {"31": 2500.0},
        }
        with contextlib.chdir(self.root):
            Path("item_seconds.json").write_text(json.dumps({"12": 5, "31": 2900}))
            costs = worker.item_seconds(work)
            self.assertEqual(costs, {"12": 5.0, "31": 2900.0})
            outcome = {
                "work": work,
                "item_seconds": costs,
                "unfinished": [{"id": 31, "replicate": r} for r in range(25, 31)]
                + [{"id": 40, "replicate": r} for r in range(21, 31)],
            }
            worker.repack([outcome], final=False, seconds=14280)
            batches = [
                json.loads(path.read_text())
                for path in sorted(Path("batches").glob("*.json"))
            ]
        expensive = [b for b in batches if b["ids"] == [31]]
        # floor(0.6 * 14280 / 2900) = 2 replicates per task.
        self.assertEqual(
            [b["replicates"] for b in expensive], [[25, 26], [27, 28], [29, 30]]
        )
        self.assertTrue(all(b["item_seconds"] == {"31": 2900.0} for b in expensive))
        unmeasured = [b["replicates"] for b in batches if b["ids"] == [40]]
        self.assertEqual(unmeasured, [list(range(21, 26)), list(range(26, 31))])

    def test_snapshot_rejects_incompatible_resumes(self):
        params = {
            "sites_parquet_s3_uri": "s3://inputs/sites.parquet",
            "results_s3_uri": "s3://results/task",
            "match_memory_mib": 61440,
            "matching_job_queue": "ae-ondemand-gp3",
        }
        image = "registry/analysis@sha256:" + "a" * 64
        store = Store(str(self.root / "state"))
        store.bucket, store.prefix = "state-bucket", "execution"
        client = Mock()
        client.head_object.return_value = {"ETag": '"etag"', "VersionId": "v1"}
        with patch.object(driver, "input_manifest", return_value=[["raster", "etag"]]):
            first = driver.prepare(client, store, params, image, False)
            resumed = driver.prepare(client, store, params, image, True)
            self.assertEqual(first, resumed)
            self.assertEqual(first["_workflow"]["match_memory_mib"], 61440)
            self.assertEqual(first["_workflow"]["initial_queue"], "ae-ondemand-gp3")
            self.assertIs(first["shared_support_controls"], True)
            self.assertEqual(first["_workflow"]["diagnostics_contract"]["matching_policy"], "shared_support_v2")
            self.assertEqual(client.copy_object.call_count, 1)
            updated_image = image.replace("a" * 64, "b" * 64)
            snapshot = store.json("snapshot.json")
            with patch.object(driver, "VERSION", "2.0.0"):
                updated = driver.prepare(client, store, params, updated_image, True)
            self.assertEqual(updated["_workflow"]["identity"], first["_workflow"]["identity"])
            self.assertEqual(updated["_workflow"]["analysis_image"], updated_image)
            self.assertEqual(updated["_workflow"]["workflow_version"], "2.0.0")
            self.assertEqual(
                {key: value for key, value in updated.items() if key != "_workflow"},
                {key: value for key, value in first.items() if key != "_workflow"},
            )
            self.assertEqual(store.json("snapshot.json"), snapshot)
            self.assertEqual(store.json("snapshot.json")["image"], image)
            with self.assertRaisesRegex(ValueError, "identical"):
                driver.prepare(
                    client, store, {**params, "shared_support_controls": False}, image, True
                )
            incompatible = json.loads(json.dumps(snapshot))
            incompatible["analysis"]["_workflow"]["diagnostics_contract"][
                "diagnostics_schema_version"
            ] = 2
            store.write_json("snapshot.json", incompatible)
            with self.assertRaisesRegex(ValueError, "incompatible diagnostic contract"):
                driver.prepare(client, store, params, updated_image, True)
            store.write_json("snapshot.json", snapshot)
            with self.assertRaisesRegex(ValueError, "must be a boolean"):
                driver.prepare(client, store, {**params, "shared_support_controls": "true"}, image, True)
        with (
            patch.object(
                driver, "input_manifest", return_value=[["raster", "changed"]]
            ),
            self.assertRaisesRegex(ValueError, "inputs changed"),
        ):
            driver.prepare(client, store, params, image, True)
        with self.assertRaisesRegex(ValueError, "pinned"):
            driver.prepare(client, store, params, "registry/analysis:latest", False)

    def test_resume_preserves_legacy_default_from_frozen_analysis(self):
        params = {
            "sites_parquet_s3_uri": "s3://inputs/sites.parquet",
            "results_s3_uri": "s3://results/task",
        }
        image = "registry/analysis@sha256:" + "a" * 64
        store = Store(str(self.root / "legacy-state"))
        store.bucket, store.prefix = "state-bucket", "execution"
        client = Mock()
        client.head_object.return_value = {"ETag": '"etag"'}
        with patch.object(driver, "input_manifest", return_value=[["raster", "etag"]]):
            legacy = driver.prepare(
                client, store, {**params, "shared_support_controls": False}, image, False
            )
            snapshot = store.json("snapshot.json")
            snapshot["source"].pop("shared_support_controls")
            store.write_json("snapshot.json", snapshot)
            resumed = driver.prepare(client, store, params, image, True)
        self.assertEqual(resumed, legacy)
        self.assertIs(resumed["shared_support_controls"], False)
        self.assertEqual(resumed["_workflow"]["diagnostics_contract"]["matching_policy"], "legacy")
        self.assertEqual(client.copy_object.call_count, 1)

    def test_driver_validates_matching_preferences_before_loading_inputs(self):
        image = "registry/analysis@sha256:" + "a" * 64
        store = Store(str(self.root / "invalid-input-state"))
        client = Mock()
        for preference in (
            {"match_memory_mib": 0},
            {"match_memory_mib": 122881},
            {"match_memory_mib": True},
            {"match_memory_mib": "30720"},
            {"matching_job_queue": "unapproved-queue"},
            {"matching_job_queue": ["ae-spot-gp3"]},
        ):
            with self.subTest(preference=preference), self.assertRaises(ValueError):
                driver.prepare(client, store, preference, image, False)
        client.get_paginator.assert_not_called()
        client.copy_object.assert_not_called()

    def replay_fixture(self):
        import pyarrow as pa
        import pyarrow.parquet as pq

        params = {
            **self.params(),
            "task_id": "task",
            "shared_support_controls": True,
        }
        contract = worker.diagnostics_contract(params)
        params["_workflow"].update(
            diagnostics_contract=contract,
            results_uri=str(self.root / "results"),
            analysis_image="test-image",
        )
        metadata = self.root / "sites_processed.parquet"
        pq.write_table(
            pa.table({
                "id_numeric": [1], "site_id": ["a"], "sub_site_index": [0],
                "ae_atoms": ['{"run_id":"atoms","atoms":[{"atomic_id":"9007199254740993"}]}'],
            }),
            metadata,
        )
        extraction = worker.artifact_store(params, "extract")
        extraction.commit(metadata, metadata.name)
        expected = {
            "ids": [1], "n_replicates": 1,
            "extract": {"uri": extraction.uri, "files": [metadata.name]},
        }
        sample = self.root / "f1_r1_sample.rds"
        sample.write_bytes(b"sample payload")
        groups = []
        for stratum in ("a", "b"):
            groups.append({
                **contract, "id_numeric": 1, "replicate": 1, "site_id": "a",
                "sub_site_index": 0, "atom_run_id": "atoms",
                "atom_results_schema_version": worker.ATOM_RESULTS_SCHEMA_VERSION,
                "task_id": "task", "group": stratum, "attempt_key": stratum,
                "cause": "glm_warning",
                "replay": {
                    "sample_file": sample.name, "sample_md5": digest(sample, "md5"),
                    "group": stratum, "attempt_key": stratum,
                },
            })
        item = {
            "id_numeric": 1, "replicate": 1, "reason": "no_matches",
            "expected_attempt_keys": ["a", "b"], "expected_groups": ["a", "b"],
            "group_diagnostics": groups,
        }
        sidecar = {
            **contract, "atom_run_id": "atoms",
            "compatibility": {
                **contract, "task_id": "task", "atom_run_id": "atoms",
                "atom_results_schema_version": worker.ATOM_RESULTS_SCHEMA_VERSION,
            },
            "matches_file": None, "expected_attempt_keys": ["a", "b"], "failures": [item],
        }
        return params, expected, sidecar, sample

    def test_reconcile_promotes_one_sample_for_multiple_attempts_and_retries(self):
        import copy

        import pyarrow as pa
        import pyarrow.parquet as pq

        params, expected, sidecar, sample = self.replay_fixture()
        params["_workflow"]["analysis_image"] = "current-image"
        for group in sidecar["failures"][0]["group_diagnostics"]:
            group["replay"]["analysis_image"] = "original-image"
        expected["n_replicates"] = 2
        store = worker.artifact_store(params, "checkpoints")
        store.commit(sample, checkpoint_key(sample.name))
        name = "mb_f1_r1.json"
        sidecar_path = self.root / name
        worker.write_json(sidecar_path, sidecar)
        store.commit(sidecar_path, checkpoint_key(name))
        partial = copy.deepcopy(sidecar)
        partial["matches_file"] = "mb_f1_r2.parquet"
        item = partial.pop("failures")[0]
        partial["diagnostics"] = [item]
        item["replicate"] = 2
        for group, attempt in zip(item["group_diagnostics"], ("c", "d"), strict=True):
            group.update(replicate=2, attempt_key=attempt)
            group["replay"].update(attempt_key=attempt, sample_file="f1_r2_sample.rds")
        matched_group = item["group_diagnostics"][0]
        matched_group.update(cause="matched", replay=None)
        partial["expected_attempt_keys"] = item["expected_attempt_keys"] = ["c", "d"]
        sample2 = self.root / "f1_r2_sample.rds"
        shutil.copyfile(sample, sample2)
        store.commit(sample2, checkpoint_key(sample2.name))
        parquet = self.root / partial["matches_file"]
        pq.write_table(
            pa.table({"id_numeric": [1], "replicate": [2], "atomic_id": ["9007199254740993"]}),
            parquet,
        )
        store.commit(parquet, checkpoint_key(parquet.name))
        partial_name = "mb_f1_r2.json"
        worker.write_json(self.root / partial_name, partial)
        store.commit(self.root / partial_name, checkpoint_key(partial_name))
        archived = []
        for run in ("first", "retry"):
            directory = self.root / run
            directory.mkdir()
            with contextlib.chdir(directory):
                worker.reconcile(params, expected, [])
                worker.archive_matches()
                with tarfile.open("matches.tar") as archive:
                    self.assertEqual(archive.getnames(), [name, partial_name, parquet.name])
                archived.append([
                    json.loads((Path("matches") / filename).read_text())
                    for filename in (name, partial_name)
                ])
        self.assertEqual(*archived)
        shutil.rmtree(store.root)
        replays = [
            group["replay"] for group in archived[0][0]["failures"][0]["group_diagnostics"]
        ]
        self.assertEqual(replays[0]["uri"], replays[1]["uri"])
        self.assertEqual(replays[0]["analysis_image"], "original-image")
        retained = Path(replays[0]["uri"])
        self.assertTrue(retained.is_relative_to(self.root / "results" / "matching_replay"))
        self.assertEqual(retained.read_bytes(), sample.read_bytes())
        self.assertEqual(digest(retained), replays[0]["sha256"])
        partial_groups = archived[0][1]["diagnostics"][0]["group_diagnostics"]
        self.assertIsNone(partial_groups[0]["replay"])
        self.assertEqual(digest(Path(partial_groups[1]["replay"]["uri"])), digest(sample2))

    def test_reconcile_rejects_missing_and_corrupt_committed_replay(self):
        params, expected, sidecar, sample = self.replay_fixture()
        store = worker.artifact_store(params, "checkpoints")
        completion = self.root / "mb_f1_r1.json"
        worker.write_json(completion, sidecar)
        store.commit(completion, checkpoint_key(completion.name))
        missing = self.root / "missing"
        missing.mkdir()
        with contextlib.chdir(missing), self.assertRaisesRegex(FileNotFoundError, "_sample"):
            worker.reconcile(params, expected, [])
        receipt = store.commit(sample, checkpoint_key(sample.name))
        (store.root / receipt["object"]).write_bytes(b"tampered")
        corrupt = self.root / "corrupt"
        corrupt.mkdir()
        with contextlib.chdir(corrupt), self.assertRaisesRegex(ValueError, "Corrupt artifact"):
            worker.reconcile(params, expected, [])
        self.assertFalse((self.root / "results").exists())

    def test_new_checkpoint_requires_complete_matching_contract(self):
        import copy

        params, _, sidecar, _ = self.replay_fixture()
        contract = worker.workflow_contract(params)
        fragment = {
            "atom_run_id": "atoms",
            "atom_results_schema_version": worker.ATOM_RESULTS_SCHEMA_VERSION,
            "site_id": "a", "sub_site_index": 0,
        }
        for field, value, error in (
            ("matching_policy", "legacy", "schema or policy"),
            ("diagnostics_schema_version", None, "schema or policy"),
            ("atom_run_id", None, "atom contract"),
            ("expected_attempt_keys", ["a"], "attempt roster"),
        ):
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, error):
                worker.validate_sidecar({**sidecar, field: value}, 1, 1, contract, fragment, "task")
        for changes, error in (
            ({"replay": None}, "invalid matching replay"),
            ({"atom_results_schema_version": None}, "incompatible|Incompatible"),
        ):
            invalid = copy.deepcopy(sidecar)
            invalid["failures"][0]["group_diagnostics"][0].update(changes)
            with self.assertRaisesRegex(ValueError, error):
                worker.validate_sidecar(invalid, 1, 1, contract, fragment, "task")
        invalid = copy.deepcopy(sidecar)
        groups = invalid["failures"][0]["group_diagnostics"]
        groups[1] = groups[0]
        with self.assertRaisesRegex(ValueError, "duplicate matching attempt"):
            worker.validate_sidecar(invalid, 1, 1, contract, fragment, "task")

    @unittest.skipUnless(shutil.which("Rscript"), "Requires Rscript")
    def test_r_config_defaults_to_legacy_and_rejects_non_boolean_values(self):
        probe = subprocess.run(
            ["Rscript", "-e", 'quit(status=!all(sapply(c("tidyverse","jsonlite","httr"), requireNamespace, quietly=TRUE)))'],
            capture_output=True, check=False,
        )
        if probe.returncode:
            self.skipTest("R config dependencies are not installed")
        cfg = self.root / "config.json"
        required = {
            "data_dir": str(self.root), "max_treatment_pixels": 100,
            "control_multiplier": 2, "min_glm_treatment_pixels": 5,
            "caliper_width": 0.2, "max_controls_per_treatment": 1,
        }
        command = [
            "Rscript", "-e",
            f'source("{ROOT / "scripts/utils.R"}"); cat(parse_config()$shared_support_controls)',
            "--args", "--config", str(cfg),
        ]
        for settings, expected in (
            ({}, "TRUE"), ({"shared_support_controls": True}, "TRUE"),
            ({"shared_support_controls": False}, "FALSE"),
            ({"shared_support_controls": None}, None),
            ({"shared_support_controls": [True]}, None),
        ):
            with self.subTest(settings=settings):
                worker.write_json(cfg, {**required, **settings})
                result = subprocess.run(command, capture_output=True, text=True, check=False)
                if expected is None:
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("shared_support_controls must be a boolean", result.stderr)
                else:
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(result.stdout, expected)

    def test_reconcile_requires_explicit_outcomes_for_each_requested_replicate(self):
        import pyarrow as pa
        import pyarrow.parquet as pq

        params = self.params()
        source = self.root / "mb_f1_r1.json"
        source.write_text(
            json.dumps(
                {
                    "matches_file": None,
                    "failures": [
                        {"id_numeric": 1, "replicate": 1, "reason": "no_overlap"}
                    ],
                }
            )
        )
        store = worker.artifact_store(params, "checkpoints")
        store.commit(source, checkpoint_key(source.name))
        with contextlib.chdir(self.root):
            worker.reconcile(params, {"ids": [1], "n_replicates": 1}, [])
            self.assertTrue(Path("matches/mb_f1_r1.json").exists())
        other = self.root / "bad"
        other.mkdir()
        with (
            contextlib.chdir(other),
            self.assertRaises(FileNotFoundError),
        ):
            worker.reconcile(params, {"ids": [1], "n_replicates": 2}, [])
        sidecar = self.root / "mb_f1_r2.json"
        parquet = self.root / "mb_f1_r2.parquet"
        sidecar.write_text(
            json.dumps(
                {
                    "matches_file": parquet.name,
                    "diagnostics": [{"id_numeric": 1, "replicate": 2}],
                }
            )
        )
        pq.write_table(pa.table({"id_numeric": [2], "replicate": [2]}), parquet)
        store.commit(sidecar, checkpoint_key(sidecar.name))
        store.commit(parquet, checkpoint_key(parquet.name))
        mismatch = self.root / "mismatch"
        mismatch.mkdir()
        with (
            contextlib.chdir(mismatch),
            self.assertRaisesRegex(ValueError, "Incompatible matched fragment"),
        ):
            worker.reconcile(params, {"ids": [1], "n_replicates": 2}, [])

    def test_reconciliation_batches_and_archives_cover_work_once(self):
        params = self.params()
        expected = {
            "ids": list(range(1, 28)), "n_replicates": 1,
            "extract": self.summary_extraction(list(range(1, 30))),
        }
        store = worker.artifact_store(params, "checkpoints")
        for id_ in expected["ids"]:
            sidecar = self.root / f"mb_f{id_}_r1.json"
            sidecar.write_text(
                json.dumps(
                    {
                        "matches_file": None,
                        "failures": [
                            {"id_numeric": id_, "replicate": 1, "reason": "no_overlap"}
                        ],
                    }
                )
            )
            store.commit(sidecar, checkpoint_key(sidecar.name))
        prepare = self.root / "reconcile-prepare"
        prepare.mkdir()
        with contextlib.chdir(prepare):
            worker.prepare_reconcile(expected, [])
        batches = sorted((prepare / "reconcile_batches").glob("*.json"))
        self.assertEqual(len(batches), 15)
        descriptors = [json.loads(path.read_text()) for path in batches]
        self.assertEqual(
            sorted(id_ for item in descriptors for id_ in item["fragment_ids"]),
            list(range(1, 30)),
        )
        self.assertTrue(all(
            len({(id_ - 1) // 2 for id_ in item["fragment_ids"]}) == 1
            for item in descriptors
        ))
        archives = []
        for index, batch in enumerate(batches):
            directory = self.root / f"reconcile-{index}"
            directory.mkdir()
            with contextlib.chdir(directory):
                worker.reconcile(params, json.loads(batch.read_text()), [])
                worker.archive_matches()
            archives.append(str(directory / "matches.tar"))
        with contextlib.chdir(self.root):
            worker.restore_archives(archives, Path("restored"))
            self.assertEqual(
                {p.name for p in Path("restored").iterdir()},
                {f"mb_f{id_}_r1.json" for id_ in expected["ids"]},
            )
            with self.assertRaisesRegex(ValueError, "Duplicate"):
                worker.restore_archives([archives[0], archives[0]], Path("duplicate"))
        with (
            contextlib.chdir(self.root),
            self.assertRaisesRegex(RuntimeError, "exhausted"),
        ):
            worker.prepare_reconcile(expected, [{"unfinished": [{"id": 1}]}])

    def test_empty_reconciliation_and_unsafe_archive(self):
        empty = self.root / "empty"
        empty.mkdir()
        with contextlib.chdir(empty):
            worker.prepare_reconcile({
                "ids": [], "n_replicates": 1,
                "extract": self.summary_extraction([]),
            }, [])
            batch = json.loads(
                next(Path("reconcile_batches").glob("*.json")).read_text()
            )
            self.assertEqual(batch["ids"], [])
            worker.reconcile(self.params(), batch, [])
            worker.archive_matches()
        with contextlib.chdir(self.root):
            worker.restore_archives([str(empty / "matches.tar")], Path("restored"))
            self.assertEqual(list(Path("restored").iterdir()), [])
        unsafe = self.root / "unsafe.tar"
        with tarfile.open(unsafe, "w") as archive:
            member = tarfile.TarInfo("../mb_f1_r1.json")
            member.size = 0
            archive.addfile(member)
        with (
            contextlib.chdir(self.root),
            self.assertRaisesRegex(ValueError, "Invalid match archive member"),
        ):
            worker.restore_archives([str(unsafe)], Path("unsafe"))

    def test_summary_archives_require_complete_disjoint_planned_domains(self):
        import copy

        shards = [
            {"shard_id": f"s{index:06d}", "ids": [index + 1],
             "fragment_ids": [index + 1], "site_ids": [site]}
            for index, site in enumerate(("A", "B"))
        ]
        plan = {"expected_ids": [1, 2], "shards": shards}
        payload = self.root / "payload.csv"
        payload.write_text("site_id\nA\n")

        def pack(descriptors, prefix):
            archives = []
            for index, descriptor in enumerate(descriptors):
                path = self.root / f"{prefix}-{index}.tar"
                worker.archive_summary(str(path), descriptor, "reports", [payload])
                archives.append(str(path))
            return archives

        archives = pack(shards, "valid")
        folders = worker.restore_summary(archives, plan, "reports", self.root / "valid")
        self.assertEqual(len(folders), 2)
        self.assertTrue(all((folder / payload.name).read_text() == payload.read_text()
                            for folder in folders))
        for name, inputs, error in (
            ("missing", archives[:1], "Incomplete"),
            ("duplicate", archives + archives[:1], "Duplicate"),
        ):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, error):
                worker.restore_summary(inputs, plan, "reports", self.root / name)
        wrong = copy.deepcopy(shards)
        wrong[0]["site_ids"] = ["other-parent"]
        with self.assertRaisesRegex(ValueError, "incompatible"):
            worker.restore_summary(pack(wrong, "wrong"), plan, "reports", self.root / "wrong")
        for key in ("ids", "fragment_ids", "site_ids"):
            overlapping = copy.deepcopy(plan)
            overlapping["shards"][1][key] = overlapping["shards"][0][key]
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "Overlapping"):
                worker.restore_summary(
                    pack(overlapping["shards"], key), overlapping, "reports", self.root / key
                )
        with self.assertRaisesRegex(ValueError, "coverage"):
            worker.restore_summary(
                archives, {**plan, "expected_ids": [1, 2, 3]}, "reports", self.root / "coverage"
            )

    def test_csv_merge_copies_matching_schemas_without_reencoding_rows(self):
        first = self.root / "first.csv"
        empty = self.root / "empty.csv"
        last = self.root / "last.csv"
        first.write_bytes(b'site_id,note\r\n001,"two\r\nlines, ""quoted"""\r\n')
        empty.write_bytes(b"site_id,note\n")
        last.write_bytes('site_id,note\n002,"caf\u00e9"\n003,last'.encode("utf-8"))
        output = self.root / "merged.csv"
        with patch("summary_shards.csv.DictReader", side_effect=AssertionError("row parsing")):
            merge_csv_tables([first, empty, last, first], output)
        with output.open(encoding="utf-8", newline="") as stream:
            self.assertEqual(list(csv.reader(stream)), [
                ["site_id", "note"],
                ["001", 'two\r\nlines, "quoted"'],
                ["002", "caf\u00e9"],
                ["003", "last"],
                ["001", 'two\r\nlines, "quoted"'],
            ])
        self.assertIn(first.read_bytes().split(b"\n", 1)[1], output.read_bytes())

    def test_csv_merge_unions_different_headers_including_empty_shards(self):
        paths = [self.root / f"part{index}.csv" for index in range(3)]
        paths[0].write_text("site_id,value\n001,1\n")
        paths[1].write_text("site_id,value,extra\n")
        paths[2].write_text('extra,site_id,note\n2,002,"comma, text"\n')
        output = self.root / "merged.csv"
        merge_csv_tables(paths, output)
        with output.open(newline="") as stream:
            self.assertEqual(list(csv.DictReader(stream)), [
                {"site_id": "001", "value": "1", "extra": "", "note": ""},
                {"site_id": "002", "value": "", "extra": "2", "note": "comma, text"},
            ])

    def test_csv_merge_rejects_missing_and_duplicate_headers(self):
        path = self.root / "invalid.csv"
        for content in ("", "site_id,site_id\nA,B\n"):
            with self.subTest(content=content):
                path.write_text(content)
                with self.assertRaisesRegex(ValueError, "Missing or duplicate CSV header"):
                    merge_csv_tables([path], self.root / "merged.csv")

    def test_summary_partition_cap_preserves_parent_and_unscheduled_fragment_coverage(self):
        expected = {
            "ids": list(range(1, 60)), "n_replicates": 100,
            "extract": self.summary_extraction(list(range(1, 66))),
        }
        with contextlib.chdir(self.root):
            shards = worker.partition_summary(expected)
            self.assertEqual(shards, worker.partition_summary(expected))
        self.assertEqual(len(shards), 16)
        self.assertEqual(sorted(id_ for shard in shards for id_ in shard["ids"]), expected["ids"])
        self.assertEqual(
            sorted(id_ for shard in shards for id_ in shard["fragment_ids"]),
            list(range(1, 66)),
        )
        owners = {}
        for shard in shards:
            for id_ in shard["fragment_ids"]:
                site = (id_ - 1) // 2
                self.assertEqual(owners.setdefault(site, shard["shard_id"]), shard["shard_id"])
        self.assertLessEqual(max(len(shard["fragment_ids"]) for shard in shards), 5)

    def test_internal_timeout_is_not_reported_as_success(self):
        script = self.root / "slow.py"
        script.write_text("import time; time.sleep(60)\n")
        with contextlib.chdir(self.root), patch.object(worker, "SCRIPTS", self.root):
            self.assertFalse(worker.run_script("slow.py", {}, 1))

    def test_driver_requires_final_payload_and_honors_cancel_marker(self):
        state = self.root / "state"
        store = Store(str(state))
        store.write_json("snapshot.json", {"inputs": []})
        client = Mock()
        client.get_object.return_value = {
            "Body": Mock(read=lambda: gzip.compress(b"{}"))
        }
        env = {
            "NEXTFLOW_STATE_URI": str(state),
            "EXECUTION_ID": "execution",
            "PARAMS_S3_BUCKET": "params",
            "PARAMS_S3_PREFIX": "production",
            "NEXTFLOW_TASK_IMAGE": "ignored-in-mocked-prepare",
        }
        with (
            contextlib.chdir(self.root),
            patch.dict(os.environ, env),
            patch.object(driver.boto3, "client", return_value=client),
            patch.object(driver, "prepare", return_value={}),
            patch.object(driver, "input_manifest", return_value=[]),
            patch.object(driver, "launch"),
            self.assertLogs(driver.log, level="ERROR"),
            self.assertRaises(FileNotFoundError),
        ):
            driver.main()
        self.assertEqual(store.json("status.json")["phase"], "failed")
        client.put_object.assert_not_called()
        store.write_json("published/results.json", {"ok": True})
        with (
            contextlib.chdir(self.root),
            patch.dict(os.environ, env),
            patch.object(driver.boto3, "client", return_value=client),
            patch.object(driver, "prepare", return_value={}),
            patch.object(driver, "input_manifest", return_value=[]),
            patch.object(driver, "launch"),
        ):
            driver.main()
        payload = client.put_object.call_args.kwargs
        self.assertEqual(payload["Key"], "production/execution_results.json.gz")
        self.assertEqual(json.loads(gzip.decompress(payload["Body"])), {"ok": True})
        self.assertEqual(store.json("status.json")["phase"], "succeeded")
        self.assertIs(store.json("status.json")["success"], True)
        store.write_json("cancelled.json", {"cancelled": True})
        with (
            contextlib.chdir(self.root),
            patch.dict(os.environ, env),
            patch.object(driver.boto3, "client", return_value=client),
            patch.object(driver, "launch") as launch,
            self.assertLogs(driver.log, level="ERROR"),
            self.assertRaises(InterruptedError),
        ):
            driver.main()
        launch.assert_not_called()
        self.assertEqual(store.json("status.json")["phase"], "cancelled")

    def test_driver_records_native_job_ids_before_completion(self):
        store = Store(str(self.root / "state"))
        logfile = self.root / "nextflow.log"
        first = "11111111-2222-3333-4444-555555555555"
        second = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        logfile.write_text(
            f"[AWS BATCH] Process `MATCH` submitted > job={first}; work-dir=s3://work\n"
        )
        self.assertEqual(driver.record_jobs(store, logfile), {first})
        logfile.write_text(
            f"[AWS BATCH] Process `MATCH` submitted > job={second}; work-dir=s3://work\n"
        )
        self.assertEqual(driver.record_jobs(store, logfile), {first, second})

    def test_driver_reports_known_remaining_and_unknown_jobs(self):
        jobs = {f"job-{number:03}" for number in range(102)}
        batch = Mock()

        def describe(jobs):
            return {
                "jobs": [
                    {
                        "jobId": job,
                        "status": "RUNNING" if job == "job-000" else "SUCCEEDED",
                    }
                    for job in jobs
                    if job != "job-101"
                ]
            }

        batch.describe_jobs.side_effect = describe
        with self.assertLogs(driver.log, level="INFO") as messages:
            driver.log_progress(batch, jobs)
        self.assertIn("total=102, known remaining=1", messages.output[0])
        self.assertIn("succeeded=100, failed=0, unknown=1", messages.output[0])
        self.assertIn("not-yet-submitted work is not counted", messages.output[0])
        self.assertEqual(
            [len(call.kwargs["jobs"]) for call in batch.describe_jobs.call_args_list],
            [100, 2],
        )

        batch.describe_jobs.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "DescribeJobs"
        )
        with self.assertLogs(driver.log, level="WARNING") as messages:
            driver.log_progress(batch, jobs)
        self.assertIn("Could not retrieve Nextflow task progress", messages.output[0])

    def test_driver_publishes_monotonic_estimates_for_completed_milestones(self):
        store = Store(str(self.root / "state"))
        trace = self.root / "trace.tsv"
        trace.write_text(
            "task_id\tname\tstatus\n"
            "1\tEXTRACT\tCOMPLETED\n"
            "2\tPREPARE_MATCH_BATCHES\tCACHED\n"
            "3\tMATCH_BATCH (first)\tFAILED\n"
        )
        self.assertEqual(driver.record_progress(store, trace, 0, "driver-1"), 20)
        self.assertEqual(
            store.json("progress.json"), {"percent": 20, "driver_job_id": "driver-1"}
        )
        with trace.open("a") as stream:
            stream.write("4\tMATCH_BATCH (second)\tCOMPLETED\n")
        self.assertEqual(driver.record_progress(store, trace, 20, "driver-1"), 40)
        trace.write_text("task_id\tname\tstatus\n5\tEXTRACT\tCACHED\n")
        self.assertEqual(driver.record_progress(store, trace, 40, "driver-1"), 40)
        self.assertEqual(store.json("progress.json")["percent"], 40)
        trace.write_text("task_id\tname\tstatus\n6\tSUMMARIZE\tCOMPLETED\n")
        self.assertEqual(driver.record_progress(store, trace, 40, "driver-1"), 99)

    def test_driver_does_not_publish_progress_without_job_identity(self):
        store = Store(str(self.root / "state"))
        trace = self.root / "trace.tsv"
        trace.write_text("task_id\tname\tstatus\n1\tEXTRACT\tCOMPLETED\n")
        with self.assertLogs(driver.log, level="WARNING"):
            self.assertEqual(driver.record_progress(store, trace, 0, None), 0)
        self.assertIsNone(store.json("progress.json"))

    def test_driver_logs_periodically_and_when_nextflow_exits(self):
        store = Store(str(self.root / "state"))
        env = {
            "AWS_REGION": "us-east-1",
            "EXECUTION_ID": "execution-id",
            "NEXTFLOW_TASK_IMAGE": "image",
            "NEXTFLOW_WORK_URI": "s3://work/execution",
        }
        analysis = {
            "_workflow": {
                "match_memory_mib": 1024,
                "initial_queue": "queue",
            }
        }
        process = Mock()
        process.poll.side_effect = [None, None, None, 0, 0]
        jobs = {"job-1"}
        with (
            contextlib.chdir(self.root),
            patch.object(driver.subprocess, "Popen", return_value=process),
            patch.object(driver.boto3, "client") as batch_client,
            patch.object(driver, "record_jobs", return_value=jobs),
            patch.object(driver, "log_progress") as progress,
            patch.object(driver.time, "monotonic", side_effect=[0, 0, 30, 61, 62]),
            patch.object(driver.time, "sleep"),
        ):
            driver.launch(store, analysis, env, False)
        self.assertEqual(progress.call_count, 3)
        for call in progress.call_args_list:
            self.assertEqual(call.args, (batch_client.return_value, jobs))

    def test_driver_launch_matches_api_optional_environment_and_explicit_resume(self):
        store = Store(str(self.root / "state"))
        env = {
            "AWS_REGION": "us-east-1",
            "EXECUTION_ID": "11111111-2222-3333-4444-555555555555",
            "NEXTFLOW_TASK_IMAGE": "registry/analysis@sha256:" + "a" * 64,
            "NEXTFLOW_WORK_URI": "s3://work/execution",
            "NEXTFLOW_MAX_HOURS": "1.5",
        }
        process = Mock()
        process.poll.return_value = 0
        analysis = {
            "_workflow": {
                "match_memory_mib": 30720,
                "initial_queue": "ae-spot-gp3",
                "analysis_image": "registry/analysis@sha256:" + "b" * 64,
            }
        }
        with (
            contextlib.chdir(self.root),
            patch.dict(os.environ, {}, clear=True),
            patch.object(driver.subprocess, "Popen", return_value=process) as popen,
            patch.object(driver.boto3, "client") as batch_client,
        ):
            driver.launch(store, analysis, env, False)
            batch_client.assert_called_once_with("batch", region_name=env["AWS_REGION"])
            command = popen.call_args.args[0]
            self.assertEqual(
                command[command.index("-name") + 1],
                "ae_" + env["EXECUTION_ID"].replace("-", "_"),
            )
            self.assertEqual(command[command.index("--spot_queue") + 1], "ae-spot-gp3")
            self.assertEqual(command[command.index("--max_forks") + 1], "250")
            self.assertNotIn("--job_role", command)
            self.assertEqual(
                popen.call_args.kwargs["env"]["NXF_IGNORE_RESUME_HISTORY"], "true"
            )
            session = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
            store.write_json("session.json", {"session_id": session})
            driver.launch(store, analysis, env, True)
            batch_client.assert_called_with("batch", region_name=env["AWS_REGION"])
            command = popen.call_args.args[0]
            self.assertEqual(command[command.index("-resume") + 1], session)
            self.assertEqual(
                command[command.index("--image") + 1],
                env["NEXTFLOW_TASK_IMAGE"],
            )

    @unittest.skipUnless(
        shutil.which("nextflow"), "requires pinned Nextflow installation"
    )
    def test_local_dag_handles_empty_recovery_and_resumes_extraction(self):
        # A tiny worker isolates channel/staging/cache semantics from geospatial I/O.
        fake = self.root / "worker.py"
        fake.write_text(
            "import json,pathlib,sys\n"
            "p=pathlib.Path\n"
            "cmd=sys.argv[1]\n"
            "if cmd in ('repack','prepare-reconcile'):\n"
            " outcomes=[json.loads(p(path).read_text()) for path in "
            "sys.argv[sys.argv.index('--outcomes')+1:]]\n"
            " if cmd=='repack': assert outcomes\n"
            " if cmd=='prepare-reconcile': assert not any(o['unfinished'] for o in outcomes)\n"
            "if cmd=='match':\n"
            " with p(__file__).with_suffix('.seconds').open('a') as f: "
            "f.write(sys.argv[sys.argv.index('--seconds')+1]+'\\n')\n"
            "retry=p(__file__).with_suffix('.retry')\n"
            "if cmd=='match' and retry.exists(): retry.unlink(); sys.exit(75)\n"
            "if cmd=='publish-summary' and p(__file__).with_suffix('.fail').exists(): sys.exit(2)\n"
            "if cmd=='extract': p('extraction.json').write_text('{}')\n"
            "if cmd=='prepare':\n"
            " p('batches').mkdir(); p('batches/a.json').write_text('{\"ids\":[1],\"phase\":0}'); "
            "p('expected.json').write_text('{}')\n"
            " if p(__file__).with_suffix('.multiple').exists(): "
            "p('batches/b.json').write_text('{\"ids\":[2],\"phase\":0}')\n"
            "if cmd=='match':\n"
            " phase=json.loads(p('batch.json').read_text())['phase']\n"
            " ceiling=2 if p(__file__).with_suffix('.yield').exists() else "
            "int(p(__file__).with_suffix('.recovery').exists())\n"
            " pending=[1] if phase<ceiling else []\n"
            " p('outcome.json').write_text(json.dumps({'unfinished':pending}))\n"
            "if cmd=='repack':\n"
            " phase=2 if '--final' in sys.argv else 1\n"
            " p('batches').mkdir()\n"
            " for i,outcome in enumerate(outcomes): "
            "p(f'batches/recovery{i}.json').write_text(json.dumps({'ids':[1],'phase':phase}))\n"
            "if cmd=='prepare-reconcile':\n"
            " p('reconcile_batches').mkdir(); "
            "p('reconcile_batches/a.json').write_text('{}'); "
            "p('summary_plan.json').write_text('{}')\n"
            " if p(__file__).with_suffix('.multiple').exists(): "
            "p('reconcile_batches/b.json').write_text('{}')\n"
            "if cmd=='reconcile': p('matches.tar').write_text('archive')\n"
            "if cmd=='summarize-shard':\n"
            " p('reports.tar').write_text('reports'); "
            "p('diagnostic_inputs.tar').write_text('diagnostics')\n"
            "if cmd=='export-diagnostics': p('diagnostics.tar').write_text('diagnostics')\n"
            "if cmd=='finalize-reports':\n"
            " archives=sys.argv[sys.argv.index('--archives')+1:sys.argv.index('--seconds')]\n"
            " assert len(archives)==(2 if p(__file__).with_suffix('.multiple').exists() else 1)\n"
            " assert all(p(x).is_file() for x in archives)\n"
            " p('final_reports.tar').write_text('final')\n"
            "if cmd=='publish-summary':\n"
            " p('results.json').write_text('{\"ok\":true}')\n"
        )
        (self.root / "analysis.json").write_text("{}")
        local_config = self.root / "local.config"
        selectors = (
            "EXTRACT",
            "PREPARE_MATCH_BATCHES",
            "PREPARE_RECOVERY",
            "PREPARE_LONG",
            "PREPARE_RECONCILE",
            "RECONCILE",
            "SUMMARIZE_SHARD",
            "EXPORT_DIAGNOSTICS",
            "FINALIZE_REPORTS",
            "PUBLISH_RESULTS",
        )
        local_config.write_text(
            "process.executor='local'\nprocess.container=null\nprocess.resourceLabels=[:]\n"
            "params.match_memory_mib=16\nparams.max_memory_mib=64\n"
            "trace.fields='task_id,name,status,exit,memory'\ntrace.raw=true\n"
            + "\n".join(
                f"process {{ withName: {name} {{ memory='64 MB'; cpus=1 }} }}"
                for name in selectors
            )
        )
        state = self.root / "state"
        command = [
            "nextflow",
            "-c",
            str(local_config),
            "run",
            str(ROOT / "nextflow/main.nf"),
            "-name",
            "fixture",
            "-work-dir",
            str(self.root / "work"),
            "--analysis_file",
            str(self.root / "analysis.json"),
            "--state_uri",
            str(state),
            "--worker",
            str(fake),
        ]
        env = dict(os.environ, NXF_ANSI_LOG="false", NXF_IGNORE_RESUME_HISTORY="true")
        env["PATH"] = str(Path(sys.executable).parent) + os.pathsep + env["PATH"]
        failure = fake.with_suffix(".fail")
        failure.touch()
        fake.with_suffix(".retry").touch()
        result = subprocess.run(
            command,
            cwd=self.root,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=180,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("PUBLISH_RESULTS", result.stdout)
        self.assertIn("Execution is retried (1)", result.stdout)
        seconds = fake.with_suffix(".seconds").read_text().splitlines()
        self.assertEqual(list(map(int, seconds[:2])), [14280, 28680])
        with (self.root / "trace.tsv").open() as stream:
            attempts = list(csv.DictReader(stream, delimiter="\t"))
        progress_store = Store(str(self.root / "progress-state"))
        self.assertEqual(
            driver.record_progress(progress_store, self.root / "trace.tsv", 0, "test"),
            97,
        )
        match_attempts = [
            row for row in attempts if row["name"].startswith("MATCH_BATCH")
        ]
        self.assertEqual(len(match_attempts), 2)
        self.assertTrue(
            all(int(row["memory"]) == 16 * 1024**2 for row in match_attempts)
        )
        self.assertFalse((state / "published/results.json").exists())
        failure.unlink()
        session_id = json.loads((state / "session.json").read_text())["session_id"]
        for extra in (["-resume", session_id], ["-resume", session_id]):
            result = subprocess.run(
                command + extra,
                cwd=self.root,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=180,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(
            json.loads((state / "published/results.json").read_text()), {"ok": True}
        )
        with (self.root / "trace.tsv").open() as stream:
            tasks = list(csv.DictReader(stream, delimiter="\t"))
        self.assertEqual(len(tasks), 9)
        self.assertTrue(all(task["status"] == "CACHED" for task in tasks), tasks)
        self.assertEqual(
            driver.record_progress(progress_store, self.root / "trace.tsv", 90, "test"),
            99,
        )
        fake.with_suffix(".recovery").touch()
        recovered_state = self.root / "recovered"
        command[command.index("-name") + 1] = "recovered"
        command[command.index("--state_uri") + 1] = str(recovered_state)
        result = subprocess.run(
            command,
            cwd=self.root,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=180,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        with (self.root / "trace.tsv").open() as stream:
            tasks = list(csv.DictReader(stream, delimiter="\t"))
        self.assertEqual(len(tasks), 11)
        self.assertTrue((recovered_state / "published/results.json").exists())
        fake.with_suffix(".yield").touch()
        fake.with_suffix(".multiple").touch()
        yielded_state = self.root / "yielded"
        command[command.index("-name") + 1] = "yielded"
        command[command.index("--state_uri") + 1] = str(yielded_state)
        result = subprocess.run(
            command,
            cwd=self.root,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=180,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        with (self.root / "trace.tsv").open() as stream:
            tasks = list(csv.DictReader(stream, delimiter="\t"))
        self.assertEqual(len(tasks), 19)
        self.assertTrue((yielded_state / "published/results.json").exists())


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] in ("--fixture", "--fixture-invalid"):
        exercise_fixture(Path(sys.argv[2]), invalid_late_outcome=sys.argv[1] == "--fixture-invalid")
    else:
        unittest.main()
