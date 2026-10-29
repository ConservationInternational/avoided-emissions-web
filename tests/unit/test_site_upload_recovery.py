import json
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from services import site_upload
from sqlalchemy.dialects import postgresql
from tasks import site_upload as upload_tasks


def _pending_upload(**overrides):
    return SimpleNamespace(
        **{
            "id": uuid.uuid4(),
            "user_id": uuid.uuid4(),
            "status": "pending",
            "phase": "queued",
            "attempt_count": 0,
            "created_at": datetime.now(UTC) - timedelta(hours=1),
            "extra_metadata": {
                "upload_token": "a" * 32,
                "column_mapping": {"start_date": "source_date"},
            },
            **overrides,
        }
    )


def _recovery_db(mocker, rows):
    db = mocker.MagicMock()
    query = db.query.return_value.filter.return_value
    query.order_by.return_value.limit.return_value.with_for_update.return_value.all.return_value = rows
    mocker.patch("models.get_db", return_value=db)
    mocker.patch.object(site_upload, "record_site_upload_dispatch")
    return db, query


def test_committed_job_without_dispatch_reserves_before_requeue(mocker):
    upload = _pending_upload()
    db, query = _recovery_db(mocker, [upload])
    events = []
    db.commit.side_effect = lambda: events.append("commit")
    dispatch = mocker.patch.object(
        upload_tasks.import_user_site_upload_task,
        "apply_async",
        side_effect=lambda **_: events.append("send"),
    )

    result = upload_tasks.recover_site_uploads.run()

    assert result["dispatched"] == [str(upload.id)]
    assert events == ["commit", "send"]
    assert upload.extra_metadata["dispatch_attempts"] == 1
    assert datetime.fromisoformat(upload.extra_metadata["dispatch_at"]).tzinfo
    assert upload.heartbeat_at
    assert upload.status == "pending"
    assert dispatch.call_args.kwargs == {
        "task_id": upload.celery_task_id,
        "kwargs": {
            "upload_id": str(upload.id),
            "user_id": str(upload.user_id),
            "upload_token": "a" * 32,
            "column_mapping": {"start_date": "source_date"},
        },
    }
    query.order_by.return_value.limit.assert_called_once_with(100)
    query.order_by.return_value.limit.return_value.with_for_update.assert_called_once_with(
        key_share=True, skip_locked=True
    )
    clauses = db.query.return_value.filter.call_args.args
    sql = " ".join(
        str(clause.compile(dialect=postgresql.dialect())) for clause in clauses
    )
    assert "metadata ->>" in sql
    assert "heartbeat_at" in sql


@pytest.mark.parametrize(
    "overrides",
    [
        {"attempt_count": upload_tasks.SITE_UPLOAD_MAX_ATTEMPTS},
        {"extra_metadata": {"upload_token": "token", "dispatch_attempts": 6}},
        {"extra_metadata": {}},
    ],
)
def test_recovery_exhaustion_is_terminal_and_does_not_dispatch(mocker, overrides):
    upload = _pending_upload(**overrides)
    _recovery_db(mocker, [upload])
    dispatch = mocker.patch.object(
        upload_tasks.import_user_site_upload_task, "apply_async"
    )

    result = upload_tasks.recover_site_uploads.run()

    assert result["failed"] == [str(upload.id)]
    assert upload.status == "failed"
    assert upload.completed_at
    dispatch.assert_not_called()


def test_recovery_broker_failure_keeps_persisted_throttle(mocker):
    upload = _pending_upload(status="running")
    db, _ = _recovery_db(mocker, [upload])
    mocker.patch.object(
        upload_tasks.import_user_site_upload_task,
        "apply_async",
        side_effect=RuntimeError("broker unavailable"),
    )
    discard = mocker.patch.object(site_upload, "discard_staged_site_upload")

    result = upload_tasks.recover_site_uploads.run()

    assert result["dispatch_errors"] == [str(upload.id)]
    assert upload.extra_metadata["dispatch_attempts"] == 1
    assert upload.extra_metadata["dispatch_at"]
    assert upload.status == "pending"
    db.commit.assert_called_once()
    discard.assert_not_called()


@pytest.mark.parametrize("active", [True, False])
def test_local_stage_cleanup_protects_live_jobs_without_cloud_calls(
    mocker, tmp_path, active
):
    token = "b" * 32
    created = datetime.now(UTC) - timedelta(days=2)
    metadata_path = tmp_path / f"{token}.json"
    data_path = tmp_path / f"{token}.bin"
    metadata_path.write_text(json.dumps({"created_at": created.isoformat()}))
    data_path.write_bytes(b"source")
    mocker.patch.object(site_upload, "SITE_UPLOAD_STAGE_DIR", tmp_path)
    db = mocker.MagicMock()
    db.query.return_value.filter.return_value.first.return_value = (
        (uuid.uuid4(),) if active else None
    )
    mocker.patch.object(site_upload, "get_db", return_value=db)
    cloud = mocker.patch.object(site_upload, "get_s3_client")

    result = site_upload.audit_local_site_upload_stages(apply=True)

    assert metadata_path.exists() == active
    assert data_path.exists() == active
    assert result["protected_local_tokens"] == ([token] if active else [])
    assert result["expired_local_tokens"] == ([] if active else [token])
    assert "pg_try_advisory_xact_lock" in str(db.execute.call_args.args[0])
    cloud.assert_not_called()


def test_durable_active_stage_remains_readable_after_preview_ttl(mocker):
    user_id = uuid.uuid4()
    metadata = {
        "created_at": (datetime.now(UTC) - timedelta(days=2)).isoformat(),
        "user_id": str(user_id),
    }
    mocker.patch.object(site_upload, "_load_staged_upload_meta", return_value=metadata)
    mocker.patch.object(site_upload, "_site_upload_stage_in_use", return_value=True)
    discard = mocker.patch.object(site_upload, "discard_staged_site_upload")

    assert site_upload._validate_staged_upload_access("token", user_id) == metadata
    discard.assert_not_called()


def test_worker_recovery_threshold_exceeds_hard_runtime():
    assert upload_tasks.SITE_UPLOAD_LEASE_SECONDS > (
        upload_tasks.import_user_site_upload_task.time_limit
    )
    assert (
        site_upload.SITE_UPLOAD_WORKER_LEASE_SECONDS
        == upload_tasks.SITE_UPLOAD_LEASE_SECONDS
    )


def test_recovery_and_audit_registered_in_beat_and_task_exports():
    import tasks
    from celery_app import celery_app

    assert tasks.recover_site_uploads is upload_tasks.recover_site_uploads
    assert tasks.audit_site_upload_stages is upload_tasks.audit_site_upload_stages
    assert celery_app.conf.beat_schedule["recover-site-uploads"] == {
        "task": "tasks.recover_site_uploads",
        "schedule": 300.0,
    }
    assert celery_app.conf.beat_schedule["audit-site-upload-stages"]["kwargs"] == {
        "apply": True
    }
