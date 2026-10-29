"""PostgreSQL regressions for upload outbox recovery and local-stage cleanup."""

import json
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from models import UserSiteUpload, get_db
from services import site_upload
from tasks.site_upload import (
    SITE_UPLOAD_LEASE_SECONDS,
    audit_site_upload_stages,
    recover_site_uploads,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def recovery_uploads(mocker, tmp_path):
    user_id = uuid.uuid4()
    mocker.patch.object(site_upload.Config, "S3_BUCKET", "")
    mocker.patch.object(site_upload, "SITE_UPLOAD_STAGE_DIR", tmp_path)
    dispatch = mocker.patch.object(
        site_upload.webapp_tasks.import_user_site_upload_task, "apply_async"
    )
    db = get_db()

    def create(*, status="pending", age_hours=1, attempts=0):
        token = uuid.uuid4().hex
        created_at = datetime.now(UTC) - timedelta(hours=age_hours)
        (tmp_path / f"{token}.bin").write_bytes(b"fixture")
        (tmp_path / f"{token}.json").write_text(
            json.dumps(
                {
                    "created_at": (datetime.now(UTC) - timedelta(days=2)).isoformat(),
                    "user_id": str(user_id),
                }
            )
        )
        upload = UserSiteUpload(
            user_id=user_id,
            original_filename="recovery.geojson",
            status=status,
            created_at=created_at,
            started_at=created_at if status == "running" else None,
            heartbeat_at=created_at,
            attempt_count=attempts,
            extra_metadata={
                "upload_token": token,
                "column_mapping": {
                    "site_id": None,
                    "site_name": None,
                    "start_date": "source_date",
                    "end_date": None,
                },
            },
        )
        db.add(upload)
        db.commit()
        return upload, token

    try:
        yield db, user_id, create, dispatch, tmp_path
    finally:
        db.rollback()
        db.query(UserSiteUpload).filter(UserSiteUpload.user_id == user_id).delete()
        db.commit()
        db.close()


def test_commit_without_dispatch_is_recovered_once_from_durable_mapping(
    mocker, recovery_uploads
):
    db, user_id, _, dispatch, stage_dir = recovery_uploads
    token = uuid.uuid4().hex
    (stage_dir / f"{token}.bin").write_bytes(b"fixture")
    (stage_dir / f"{token}.json").write_text("{}")

    class ProcessCrash(BaseException):
        pass

    dispatch.side_effect = ProcessCrash
    with pytest.raises(ProcessCrash):
        site_upload.create_user_site_upload(
            user_id,
            "crash.geojson",
            token,
            column_mapping={"start_date": "source_date"},
        )
    upload = db.query(UserSiteUpload).filter(UserSiteUpload.user_id == user_id).one()
    assert upload.status == "pending"
    old = datetime.now(UTC) - timedelta(hours=1)
    upload.created_at = old
    upload.extra_metadata = {
        **upload.extra_metadata,
        "dispatch_at": old.isoformat(),
    }
    db.commit()
    dispatch.reset_mock(side_effect=True)

    first = recover_site_uploads.run()
    second = recover_site_uploads.run()

    assert str(upload.id) in first["dispatched"]
    assert str(upload.id) not in second["dispatched"]
    dispatch.assert_called_once()
    kwargs = dispatch.call_args.kwargs
    assert kwargs["kwargs"] == {
        "upload_id": str(upload.id),
        "user_id": str(user_id),
        "upload_token": token,
        "column_mapping": {
            "site_id": None,
            "site_name": None,
            "start_date": "source_date",
            "end_date": None,
        },
    }
    db.expire_all()
    assert db.get(UserSiteUpload, upload.id).extra_metadata["dispatch_attempts"] == 2
    assert site_upload.claim_user_site_upload(upload.id, user_id)["claimed"]
    assert not site_upload.claim_user_site_upload(upload.id, user_id)["claimed"]
    assert (stage_dir / f"{token}.bin").exists()


def test_recovery_does_not_steal_active_imports_and_honors_terminal_and_attempt_guards(
    recovery_uploads,
):
    db, _, create, dispatch, _ = recovery_uploads
    active, _ = create(status="running", age_hours=2, attempts=1)
    stopped, _ = create(status="running", age_hours=3, attempts=1)
    exhausted, _ = create(status="running", age_hours=3, attempts=3)
    cancelled, _ = create(status="cancelled", age_hours=3)
    completed, _ = create(status="completed", age_hours=3)
    assert SITE_UPLOAD_LEASE_SECONDS > 7500

    result = recover_site_uploads.run()

    assert str(stopped.id) in result["dispatched"]
    assert str(exhausted.id) in result["failed"]
    assert str(active.id) not in result["dispatched"]
    assert str(cancelled.id) not in result["dispatched"]
    assert str(completed.id) not in result["dispatched"]
    dispatch.assert_called_once()
    db.expire_all()
    assert db.get(UserSiteUpload, active.id).status == "running"
    assert db.get(UserSiteUpload, stopped.id).status == "pending"
    assert db.get(UserSiteUpload, exhausted.id).status == "failed"
    assert db.get(UserSiteUpload, cancelled.id).status == "cancelled"
    assert db.get(UserSiteUpload, completed.id).status == "completed"


@pytest.mark.parametrize("status", ["pending", "running"])
def test_expired_local_input_of_live_job_survives_periodic_audit(
    mocker, recovery_uploads, status
):
    _, _, create, _, stage_dir = recovery_uploads
    upload, token = create(status=status, age_hours=1)
    cloud = mocker.patch.object(site_upload, "get_s3_client")

    result = audit_site_upload_stages.run(apply=True)

    assert str(upload.id) not in result["stale_upload_ids"]
    assert token in result["protected_local_tokens"]
    assert (stage_dir / f"{token}.json").exists()
    assert (stage_dir / f"{token}.bin").exists()
    cloud.assert_not_called()


@pytest.mark.parametrize("change", ["cancel", "heartbeat"])
def test_audit_cannot_fail_a_concurrently_cancelled_or_refreshed_job(
    recovery_uploads, change
):
    db, _, create, _, _ = recovery_uploads
    upload, _ = create(status="running", age_hours=48)
    upload_id = upload.id
    concurrent = get_db()
    try:
        locked = (
            db.query(UserSiteUpload)
            .filter(UserSiteUpload.id == upload_id)
            .with_for_update(key_share=True)
            .one()
        )
        if change == "cancel":
            locked.status = "cancelled"
        else:
            locked.heartbeat_at = datetime.now(UTC)
        db.flush()

        result = audit_site_upload_stages.run(apply=True)
        assert str(upload_id) not in result["stale_upload_ids"]
        db.commit()
        result = audit_site_upload_stages.run(apply=True)
        assert str(upload_id) not in result["stale_upload_ids"]
        current = concurrent.get(UserSiteUpload, upload_id)
        assert current.status == ("cancelled" if change == "cancel" else "running")
    finally:
        concurrent.close()


def test_stage_cleanup_skips_a_token_being_attached_to_a_durable_job(recovery_uploads):
    db, _, _, _, stage_dir = recovery_uploads
    token = uuid.uuid4().hex
    (stage_dir / f"{token}.bin").write_bytes(b"fixture")
    (stage_dir / f"{token}.json").write_text(
        json.dumps({"created_at": (datetime.now(UTC) - timedelta(days=2)).isoformat()})
    )
    site_upload._lock_site_upload_stage(db, token)
    try:
        result = site_upload.audit_local_site_upload_stages(apply=True)
        assert token in result["protected_local_tokens"]
        assert (stage_dir / f"{token}.bin").exists()
    finally:
        db.rollback()


def test_confirmed_queued_job_survives_many_dispatch_cooldowns(recovery_uploads):
    db, _, create, dispatch, _ = recovery_uploads
    upload, _ = create()
    upload_id = upload.id
    # First recover a missing send, then simulate a long legitimate queue backlog.
    assert str(upload_id) in recover_site_uploads.run()["dispatched"]
    db.expire_all()
    upload = db.get(UserSiteUpload, upload_id)
    assert upload.extra_metadata["dispatch_confirmed_at"]
    attempts = upload.extra_metadata["dispatch_attempts"]
    for cooldowns in range(7, 21):
        old = datetime.now(UTC) - timedelta(minutes=10 * cooldowns)
        upload.created_at = old
        upload.heartbeat_at = old
        upload.extra_metadata = {
            **upload.extra_metadata,
            "dispatch_at": old.isoformat(),
            "dispatch_confirmed_at": old.isoformat(),
        }
        db.commit()
        assert str(upload_id) not in recover_site_uploads.run()["dispatched"]
        assert (
            str(upload_id)
            not in audit_site_upload_stages.run(apply=True)["stale_upload_ids"]
        )
        db.expire_all()
        upload = db.get(UserSiteUpload, upload_id)
        assert upload.status == "pending"
        assert upload.extra_metadata["dispatch_attempts"] == attempts
    dispatch.assert_called_once()
    assert site_upload.claim_user_site_upload(upload_id, upload.user_id)["claimed"]


def test_actual_failed_dispatches_stop_at_six_reservations(recovery_uploads):
    db, _, create, dispatch, _ = recovery_uploads
    upload, _ = create()
    upload_id = upload.id
    dispatch.side_effect = RuntimeError("broker unavailable")
    for _ in range(site_upload.SITE_UPLOAD_MAX_DISPATCH_ATTEMPTS):
        result = recover_site_uploads.run()
        assert str(upload_id) in result["dispatch_errors"]
        db.expire_all()
        upload = db.get(UserSiteUpload, upload_id)
        assert upload.status == "pending"
        assert not upload.extra_metadata["dispatch_confirmed_at"]
        upload.extra_metadata = {
            **upload.extra_metadata,
            "dispatch_at": (datetime.now(UTC) - timedelta(hours=1)).isoformat(),
        }
        db.commit()
    assert str(upload_id) in recover_site_uploads.run()["failed"]
    assert dispatch.call_count == 6
    db.expire_all()
    assert db.get(UserSiteUpload, upload_id).status == "failed"


@pytest.mark.parametrize("expire_via", ["recover", "audit"])
def test_confirmed_queued_job_eventually_expires_explicitly(
    recovery_uploads, expire_via
):
    db, _, create, dispatch, _ = recovery_uploads
    upload, _ = create(age_hours=25)
    upload.extra_metadata = {
        **upload.extra_metadata,
        "dispatch_confirmed_at": upload.created_at.isoformat(),
    }
    db.commit()
    if expire_via == "recover":
        assert str(upload.id) in recover_site_uploads.run()["failed"]
    else:
        assert (
            str(upload.id)
            in audit_site_upload_stages.run(apply=True)["stale_upload_ids"]
        )
    dispatch.assert_not_called()
    db.expire_all()
    assert db.get(UserSiteUpload, upload.id).status == "failed"


def test_initial_successful_publish_is_confirmed_and_not_redispatched(recovery_uploads):
    db, user_id, _, dispatch, stage_dir = recovery_uploads
    token = uuid.uuid4().hex
    (stage_dir / f"{token}.bin").write_bytes(b"fixture")
    (stage_dir / f"{token}.json").write_text("{}")
    created = site_upload.create_user_site_upload(user_id, "queued.geojson", token)
    upload = db.get(UserSiteUpload, uuid.UUID(created["id"]))
    assert upload.extra_metadata["dispatch_confirmed_at"]
    assert dispatch.call_args.kwargs["task_id"] == upload.celery_task_id
    old = datetime.now(UTC) - timedelta(hours=4)
    upload.created_at = old
    upload.extra_metadata = {
        **upload.extra_metadata,
        "dispatch_at": old.isoformat(),
        "dispatch_attempts": 6,
    }
    db.commit()
    result = recover_site_uploads.run()
    assert str(upload.id) not in result["failed"]
    assert str(upload.id) not in result["dispatched"]
    dispatch.assert_called_once()
    assert site_upload.claim_user_site_upload(upload.id, user_id)["claimed"]


@pytest.mark.parametrize("stale_outcome", ["newer_reservation", "cancelled"])
def test_dispatch_confirmation_cannot_overwrite_newer_or_terminal_state(
    recovery_uploads, stale_outcome
):
    db, _, create, _, _ = recovery_uploads
    upload, _ = create()
    task_id = str(uuid.uuid4())
    upload.celery_task_id = (
        str(uuid.uuid4()) if stale_outcome == "newer_reservation" else task_id
    )
    if stale_outcome == "cancelled":
        upload.status = "cancelled"
    db.commit()

    site_upload.record_site_upload_dispatch(upload.id, task_id)

    db.expire_all()
    upload = db.get(UserSiteUpload, upload.id)
    assert not upload.extra_metadata.get("dispatch_confirmed_at")
    assert upload.status == ("cancelled" if stale_outcome == "cancelled" else "pending")


def test_initial_broker_failure_returns_durable_pending_job_for_recovery(
    recovery_uploads,
):
    db, user_id, _, dispatch, stage_dir = recovery_uploads
    token = uuid.uuid4().hex
    (stage_dir / f"{token}.bin").write_bytes(b"fixture")
    (stage_dir / f"{token}.json").write_text("{}")
    dispatch.side_effect = RuntimeError("broker unavailable")

    created = site_upload.create_user_site_upload(user_id, "queued.geojson", token)

    assert created["status"] == "pending"
    assert "broker unavailable" in created["error_message"]
    upload = db.get(UserSiteUpload, uuid.UUID(created["id"]))
    assert not upload.extra_metadata.get("dispatch_confirmed_at")
    assert upload.extra_metadata["dispatch_attempts"] == 1
    upload.created_at = datetime.now(UTC) - timedelta(hours=1)
    upload.extra_metadata = {
        **upload.extra_metadata,
        "dispatch_at": upload.created_at.isoformat(),
    }
    db.commit()
    dispatch.side_effect = None

    assert str(upload.id) in recover_site_uploads.run()["dispatched"]
    db.expire_all()
    assert db.get(UserSiteUpload, upload.id).extra_metadata["dispatch_confirmed_at"]
    assert dispatch.call_count == 2
