"""Focused durability, packing, resume, and workflow wiring regressions."""

import contextlib
import csv
import gzip
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "nextflow"))

import driver
import nextflow_worker as worker
from workflow_store import Store, checkpoint_key


def exercise_fixture(config_path: Path) -> None:
    """Run real worker stages against the synthetic R pipeline fixture."""
    root = config_path.parent
    params = json.loads(config_path.read_text())
    params["_workflow"] = {
        "state_uri": str(root / "state"),
        "identity": "integration",
        "match_memory_mib": 1024,
        "results_uri": str(root / "results"),
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
    reconciliation = root / "reconcile"
    reconciliation.mkdir()
    with contextlib.chdir(reconciliation):
        worker.reconcile(params, expected, outcomes)
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
    assert payload["summary"]["n_sites"] == 2, payload["summary"]
    assert payload["summary"]["n_replicates"] == 2, payload["summary"]
    assert len(list((reconciliation / "matches").glob("mb_*.json"))) == 8
    assert (root / "results/results_by_site_total.csv").exists()


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
        tables = {
            "sites_processed.parquet": pa.table(
                {
                    "id_numeric": [1, 2],
                    "site_id": ["a", "b"],
                    "sub_site_index": [0, 0],
                }
            ),
            "treatment_cell_key.parquet": pa.table(
                {"id_numeric": [1, 2], "cell": [10, 20]}
            ),
            "treatment_pixels.parquet": pa.table(
                {"cell": [10, 20], "value": [1.0, 2.0], "admin1": [1, 2]}
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
        coverage = [
            (id_, rep)
            for batch in batches
            for id_ in batch["ids"]
            for rep in batch["replicates"]
        ]
        self.assertEqual(len(coverage), 200)
        self.assertEqual(len(set(coverage)), 200)
        self.assertEqual({rep for _, rep in coverage}, set(range(1, 101)))
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

    def test_snapshot_reuses_seed_and_rejects_mutated_inputs_and_images(self):
        params = {
            "sites_parquet_s3_uri": "s3://inputs/sites.parquet",
            "results_s3_uri": "s3://results/task",
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
            self.assertEqual(client.copy_object.call_count, 1)
            with self.assertRaisesRegex(ValueError, "identical"):
                driver.prepare(
                    client, store, params, image.replace("a" * 64, "b" * 64), True
                )
        with (
            patch.object(
                driver, "input_manifest", return_value=[["raster", "changed"]]
            ),
            self.assertRaisesRegex(ValueError, "inputs or workflow changed"),
        ):
            driver.prepare(client, store, params, image, True)
        with self.assertRaisesRegex(ValueError, "pinned"):
            driver.prepare(client, store, params, "registry/analysis:latest", False)

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
            "AE_NEXTFLOW_STATE_URI": str(state),
            "EXECUTION_ID": "execution",
            "PARAMS_S3_BUCKET": "params",
            "PARAMS_S3_PREFIX": "production",
            "AE_NEXTFLOW_IMAGE": "ignored-in-mocked-prepare",
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

    def test_driver_launch_matches_api_optional_environment_and_explicit_resume(self):
        store = Store(str(self.root / "state"))
        env = {
            "EXECUTION_ID": "11111111-2222-3333-4444-555555555555",
            "AE_NEXTFLOW_IMAGE": "registry/analysis@sha256:" + "a" * 64,
            "AE_NEXTFLOW_WORK_URI": "s3://work/execution",
            "AE_NEXTFLOW_MAX_HOURS": "1.5",
        }
        process = Mock()
        process.poll.return_value = 0
        analysis = {"_workflow": {"match_memory_mib": 30720}}
        with (
            contextlib.chdir(self.root),
            patch.object(driver.subprocess, "Popen", return_value=process) as popen,
            patch.object(driver.boto3, "client"),
        ):
            driver.launch(store, analysis, env, False)
            command = popen.call_args.args[0]
            self.assertEqual(
                command[command.index("-name") + 1],
                "ae_" + env["EXECUTION_ID"].replace("-", "_"),
            )
            self.assertEqual(command[command.index("--spot_queue") + 1], "ae-spot-gp3")
            self.assertNotIn("--job_role", command)
            self.assertEqual(
                popen.call_args.kwargs["env"]["NXF_IGNORE_RESUME_HISTORY"], "true"
            )
            session = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
            store.write_json("session.json", {"session_id": session})
            driver.launch(store, analysis, env, True)
            command = popen.call_args.args[0]
            self.assertEqual(command[command.index("-resume") + 1], session)

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
            "if cmd=='match':\n"
            " with p(__file__).with_suffix('.seconds').open('a') as f: "
            "f.write(sys.argv[sys.argv.index('--seconds')+1]+'\\n')\n"
            "retry=p(__file__).with_suffix('.retry')\n"
            "if cmd=='match' and retry.exists(): retry.unlink(); sys.exit(75)\n"
            "if cmd=='summarize' and p(__file__).with_suffix('.fail').exists(): sys.exit(2)\n"
            "if cmd=='extract': p('extraction.json').write_text('{}')\n"
            "if cmd=='prepare':\n"
            " p('batches').mkdir(); p('batches/a.json').write_text('{\"ids\":[1],\"phase\":0}'); "
            "p('expected.json').write_text('{}')\n"
            "if cmd=='match':\n"
            " phase=json.loads(p('batch.json').read_text())['phase']\n"
            " pending=[1] if p(__file__).with_suffix('.yield').exists() and phase<2 else []\n"
            " p('outcome.json').write_text(json.dumps({'unfinished':pending}))\n"
            "if cmd=='repack':\n"
            " phase=2 if '--final' in sys.argv else 1\n"
            " p('batches').mkdir(); p('batches/recovery.json').write_text(json.dumps({'ids':[1],'phase':phase}))\n"
            "if cmd=='reconcile':\n"
            " p('matches').mkdir(); "
            "p('reconciled.json').write_text('{}')\n"
            "if cmd=='summarize': p('results.json').write_text('{\"ok\":true}')\n"
        )
        (self.root / "analysis.json").write_text("{}")
        local_config = self.root / "local.config"
        selectors = (
            "EXTRACT",
            "PREPARE_MATCH_BATCHES",
            "PREPARE_RECOVERY",
            "PREPARE_LONG",
            "RECONCILE",
            "SUMMARIZE",
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
        self.assertIn("SUMMARIZE", result.stdout)
        self.assertIn("Execution is retried (1)", result.stdout)
        seconds = fake.with_suffix(".seconds").read_text().splitlines()
        self.assertEqual(list(map(int, seconds[:2])), [14280, 28680])
        with (self.root / "trace.tsv").open() as stream:
            attempts = list(csv.DictReader(stream, delimiter="\t"))
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
        self.assertEqual(len(tasks), 5)
        self.assertTrue(all(task["status"] == "CACHED" for task in tasks), tasks)
        fake.with_suffix(".yield").touch()
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
        self.assertEqual(len(tasks), 9)
        self.assertTrue((yielded_state / "published/results.json").exists())


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--fixture":
        exercise_fixture(Path(sys.argv[2]))
    else:
        unittest.main()
