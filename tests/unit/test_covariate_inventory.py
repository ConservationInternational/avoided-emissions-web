"""Covariate inventory readiness and non-destructive metadata repair."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError
from callbacks import _registration as registration
from config import Config
from dash.exceptions import PreventUpdate
from services import covariate
from tasks.covariate import auto_repair_covariate_metadata

pytestmark = pytest.mark.unit


def _record(name="precip", resolution=1000, status="merged", url="https://cog"):
    return SimpleNamespace(
        covariate_name=name,
        resolution_m=resolution,
        status=status,
        merged_url=url,
        started_at=datetime.now(UTC),
        completed_at=None,
        size_bytes=None,
        gee_task_id=None,
        error_message=None,
    )


def _object(name="precip", resolution=1000):
    key = f"{Config.S3_PREFIX}/cog{covariate.gee_config.get_cog_suffix(resolution)}/{name}.tif"
    return {
        "covariate": name,
        "url": f"https://{Config.S3_BUCKET}.s3.amazonaws.com/{key}",
        "size": 1024,
        "key": key,
    }


@pytest.fixture
def inventory(mocker, mock_db):
    mocker.patch.object(covariate, "get_db", return_value=mock_db)
    mocker.patch.object(Config, "GCS_BUCKET", "")
    objects = mocker.patch("cog_merge.list_s3_cog_objects", return_value=[])
    repair = mocker.patch.object(
        covariate, "repair_covariate_metadata", return_value=True
    )

    def build(records=(), files=()):
        mock_db.query.return_value.all.return_value = records
        objects.side_effect = lambda bucket, prefix, region: [
            obj for obj in files if obj["key"].rsplit("/", 1)[0] == prefix
        ]
        return {
            (row["covariate_name"], row["resolution_m"]): row
            for row in covariate.get_covariate_inventory()
        }

    build.repair = repair
    return build


def test_s3_file_without_metadata_is_automatically_repaired(inventory):
    row = inventory(files=[_object()])[("precip", 1000)]
    assert row["on_s3"]
    assert row["ready_for_analysis"]
    assert row["metadata_repaired"]
    assert row["analysis_readiness"] == "Ready"
    assert row["status"] == "merged"
    inventory.repair.assert_called_once_with("precip", None, resolution_m=1000)


@pytest.mark.parametrize("url", [None, ""])
def test_merged_record_without_url_needs_repair(inventory, url):
    row = inventory([_record(url=url)], [_object()])[("precip", 1000)]
    assert row["metadata_repaired"]
    assert row["ready_for_analysis"]
    inventory.repair.assert_called_once()


def test_ready_requires_file_and_metadata_at_same_resolution(inventory):
    rows = inventory([_record()], [_object(resolution=250)])
    assert not rows[("precip", 1000)]["ready_for_analysis"]
    assert rows[("precip", 1000)]["analysis_readiness"] == "S3 file missing"
    assert rows[("precip", 250)]["metadata_repaired"]
    inventory.repair.assert_called_once_with("precip", None, resolution_m=250)


def test_registered_file_is_ready_and_cannot_be_repaired(inventory):
    row = inventory([_record()], [_object()])[("precip", 1000)]
    assert row["ready_for_analysis"]
    assert row["analysis_readiness"] == "Ready"
    assert not row["metadata_repaired"]
    inventory.repair.assert_not_called()


@pytest.mark.parametrize("status", sorted(covariate.ACTIVE_COVARIATE_STATUSES))
def test_active_processing_is_not_repairable(inventory, status):
    row = inventory([_record(status=status, url=None)], [_object()])[("precip", 1000)]
    assert not row["metadata_repaired"]
    assert row["analysis_readiness"] == "Processing"
    inventory.repair.assert_not_called()


def test_non_gee_layers_support_repair_and_unknown_files_do_not(inventory):
    rows = inventory(
        files=[_object("sdg_baseline"), _object("admin1"), _object("unrecognized")]
    )
    assert rows[("sdg_baseline", 1000)]["metadata_repaired"]
    assert rows[("admin1", 1000)]["metadata_repaired"]
    assert not rows[("unrecognized", 1000)]["metadata_repaired"]
    assert inventory.repair.call_count == 2


def test_repair_failure_is_visible_reported_and_does_not_stop_other_layers(
    inventory, mocker
):
    report = mocker.patch.object(covariate, "report_exception")
    inventory.repair.side_effect = [RuntimeError("S3 access denied"), True]
    rows = inventory(files=[_object("precip"), _object("temp")])
    failed = rows[("precip", 1000)]
    assert not failed["ready_for_analysis"]
    assert failed["analysis_readiness"] == "Metadata repair failed"
    assert "retried" in failed["error_message"]
    assert rows[("temp", 1000)]["ready_for_analysis"]
    report.assert_called_once_with(covariate="precip", resolution_m=1000)


def test_concurrently_registered_layer_is_shown_ready(inventory):
    inventory.repair.return_value = False
    row = inventory(files=[_object()])[("precip", 1000)]
    assert row["ready_for_analysis"]
    assert not row["metadata_repaired"]


def test_periodic_task_repairs_without_gcs_or_admin_visit(repair, mocker):
    db, s3, _ = repair
    db.query.return_value.all.return_value = []
    mocker.patch.object(Config, "GCS_BUCKET", "")
    mocker.patch(
        "cog_merge.list_s3_cog_objects",
        side_effect=lambda bucket, prefix, region: (
            [_object()] if prefix.endswith("cog_1km") else []
        ),
    )
    assert auto_repair_covariate_metadata.run() == {"repaired": 1, "failed": 0}
    layer = db.add.call_args.args[0]
    assert layer.status == "merged"
    assert layer.covariate_name == "precip"
    assert layer.started_by is None
    db.query.return_value.all.return_value = [layer]
    assert auto_repair_covariate_metadata.run() == {"repaired": 0, "failed": 0}
    db.add.assert_called_once()
    s3.head_object.assert_called_once()


def test_periodic_task_reports_failures_and_retries_next_run(repair, mocker):
    db, s3, _ = repair
    db.query.return_value.all.return_value = []
    mocker.patch.object(Config, "GCS_BUCKET", "")
    mocker.patch.object(covariate, "report_exception")
    mocker.patch(
        "cog_merge.list_s3_cog_objects",
        side_effect=lambda bucket, prefix, region: (
            [_object()] if prefix.endswith("cog_1km") else []
        ),
    )
    s3.head_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied"}}, "HeadObject"
    )
    assert auto_repair_covariate_metadata.run() == {"repaired": 0, "failed": 1}
    db.rollback.assert_called_once()
    db.add.assert_not_called()
    s3.head_object.side_effect = None
    assert auto_repair_covariate_metadata.run() == {"repaired": 1, "failed": 0}
    db.add.assert_called_once()


def test_periodic_task_is_registered_and_scheduled():
    from celery_app import celery_app

    entry = celery_app.conf.beat_schedule["auto-repair-covariate-metadata"]
    assert entry == {"task": auto_repair_covariate_metadata.name, "schedule": 120.0}
    assert entry["task"] in celery_app.tasks


@pytest.fixture
def repair(mocker, mock_db):
    mocker.patch.object(covariate, "get_db", return_value=mock_db)
    s3 = mocker.patch.object(covariate.boto3, "client").return_value
    s3.head_object.return_value = {"ContentLength": 1024, "ETag": '"abc"'}
    records = (
        mock_db.query.return_value.filter.return_value.with_for_update.return_value
    )
    records.all.return_value = []
    return mock_db, s3, records


@pytest.mark.parametrize(
    "name,resolution", [("precip", 1000), ("sdg_baseline", 250), ("admin1", 1000)]
)
def test_repair_registers_existing_file_without_changing_files(
    repair, name, resolution
):
    db, s3, _ = repair
    assert covariate.repair_covariate_metadata(name, None, resolution_m=resolution)
    layer = db.add.call_args.args[0]
    obj = _object(name, resolution)
    assert layer.status == "merged"
    assert layer.resolution_m == resolution
    assert layer.covariate_name == name
    assert layer.merged_url == obj["url"]
    assert layer.output_bucket == Config.S3_BUCKET
    assert layer.output_prefix == obj["key"].rsplit("/", 1)[0]
    assert layer.size_bytes == 1024
    assert layer.completed_at
    assert layer.extra_metadata["repaired_from_s3"]
    s3.head_object.assert_called_once_with(Bucket=Config.S3_BUCKET, Key=obj["key"])
    assert [call[0] for call in s3.method_calls] == ["head_object"]
    s3.delete_object.assert_not_called()
    s3.put_object.assert_not_called()
    db.delete.assert_not_called()
    db.commit.assert_called_once()
    db.close.assert_called_once()


def test_repair_preserves_failed_history(repair):
    db, _, records = repair
    old = _record(status="failed", url=None)
    records.all.return_value = [old]
    covariate.repair_covariate_metadata("precip", None)
    assert old.status == "failed"
    assert old.merged_url is None
    db.add.assert_called_once()


def test_repair_is_idempotent(repair):
    db, _, records = repair
    records.all.return_value = [_record()]
    assert not covariate.repair_covariate_metadata("precip", None)
    db.add.assert_not_called()
    db.commit.assert_called_once()


@pytest.mark.parametrize("status", sorted(covariate.ACTIVE_COVARIATE_STATUSES))
def test_repair_refuses_active_jobs(repair, status):
    db, s3, records = repair
    records.all.return_value = [_record(status=status)]
    with pytest.raises(ValueError, match="active"):
        covariate.repair_covariate_metadata("precip", None)
    s3.head_object.assert_not_called()
    db.add.assert_not_called()
    db.rollback.assert_called_once()
    db.close.assert_called_once()


@pytest.mark.parametrize("code", ["404", "NoSuchKey", "AccessDenied"])
def test_repair_does_not_register_missing_or_inaccessible_files(repair, code):
    db, s3, _ = repair
    s3.head_object.side_effect = ClientError({"Error": {"Code": code}}, "HeadObject")
    with pytest.raises(ClientError if code == "AccessDenied" else ValueError):
        covariate.repair_covariate_metadata("precip", None)
    db.add.assert_not_called()
    db.commit.assert_not_called()
    db.rollback.assert_called_once()


def test_repair_rejects_empty_file(repair):
    db, s3, _ = repair
    s3.head_object.return_value = {"ContentLength": 0}
    with pytest.raises(ValueError, match="empty"):
        covariate.repair_covariate_metadata("precip", None)
    db.add.assert_not_called()


@pytest.mark.parametrize("name,resolution", [("../precip", 1000), ("precip", 999)])
def test_repair_rejects_invalid_input_before_accessing_storage(
    repair, name, resolution
):
    db, s3, _ = repair
    with pytest.raises(ValueError):
        covariate.repair_covariate_metadata(name, None, resolution_m=resolution)
    db.execute.assert_not_called()
    s3.head_object.assert_not_called()


def test_commit_failure_rolls_back_repair(repair):
    db, _, _ = repair
    db.commit.side_effect = RuntimeError("database down")
    with pytest.raises(RuntimeError, match="database down"):
        covariate.repair_covariate_metadata("precip", None)
    db.rollback.assert_called_once()
    db.close.assert_called_once()


def test_registered_sdg_layers_appear_in_submission(mocker, mock_db):
    mocker.patch.object(covariate, "get_db", return_value=mock_db)
    mock_db.query.return_value.filter.return_value.all.return_value = [
        _record("sdg_baseline"),
        _record("fc_2000"),
        _record("ecoregion"),
    ]
    assert covariate.get_ready_covariate_names() == ["sdg_baseline", "ecoregion"]


@pytest.fixture
def callbacks(mocker):
    functions = {}

    def callback(*args, **kwargs):
        def capture(function):
            functions[function.__name__] = function
            return function

        return capture

    registration.register_callbacks(
        SimpleNamespace(callback=callback, clientside_callback=lambda *a, **k: None)
    )
    mocker.patch.object(
        registration,
        "get_current_user",
        return_value=SimpleNamespace(id=None, is_admin=True),
    )
    return functions


def test_admin_refresh_shows_ready_count(callbacks, mocker):
    mocker.patch.object(
        registration,
        "get_covariate_inventory",
        return_value=[
            {"resolution": "250 m", "on_s3": True, "ready_for_analysis": True}
        ],
    )
    rows, summary = callbacks["refresh_covariate_inventory"](0, None, None, "250")
    assert len(rows) == 1
    assert "On S3: 1 | Ready: 1" in summary


def test_non_admin_cannot_trigger_inventory_repair(callbacks, mocker):
    mocker.patch.object(
        registration, "get_current_user", return_value=SimpleNamespace(is_admin=False)
    )
    inventory = mocker.patch.object(registration, "get_covariate_inventory")
    with pytest.raises(PreventUpdate):
        callbacks["refresh_covariate_inventory"](0, None, None, "250")
    inventory.assert_not_called()
