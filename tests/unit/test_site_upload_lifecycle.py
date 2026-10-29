import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from services import site_upload


def test_upload_job_commits_before_celery_dispatch(mocker):
    mocker.patch.object(site_upload, "record_site_upload_dispatch")
    upload_id = uuid.uuid4()
    initial_db = mocker.MagicMock()
    initial_db.add.side_effect = lambda upload: setattr(upload, "id", upload_id)
    persisted_db = mocker.MagicMock()
    upload_row = SimpleNamespace(
        id=upload_id,
        user_id=uuid.uuid4(),
        original_filename="sites.geojson",
        celery_task_id=None,
        site_set_id=None,
        site_set_name=None,
        n_features=None,
        n_sites_imported=None,
        phase="queued",
        rows_scanned=0,
        rows_accepted=0,
        rows_skipped=0,
        heartbeat_at=None,
        status="pending",
        created_at=None,
        started_at=None,
        completed_at=None,
        error_message=None,
        extra_metadata={},
    )
    persisted_db.query.return_value.filter.return_value.first.return_value = upload_row
    mocker.patch("services.site_upload.get_db", side_effect=[initial_db, persisted_db])
    events = []
    initial_db.commit.side_effect = lambda: events.append("commit")
    async_result = SimpleNamespace(id="celery-task")
    task = site_upload.webapp_tasks.import_user_site_upload_task
    mocker.patch.object(
        task,
        "apply_async",
        side_effect=lambda **_kwargs: (
            events.append("dispatch"),
            async_result,
        )[1],
    )

    result = site_upload.create_user_site_upload(
        upload_row.user_id,
        "sites.geojson",
        "token",
        column_mapping={"start_date": "date"},
        n_features=None,
    )

    assert events == ["commit", "dispatch"]
    assert result["id"] == str(upload_id)
    assert result["n_features"] is None


def test_upload_claim_is_one_conditional_database_update(mocker):
    upload_id = uuid.uuid4()
    user_id = uuid.uuid4()
    db = mocker.MagicMock()
    db.execute.return_value.scalar_one_or_none.return_value = upload_id
    mocker.patch("services.site_upload.get_db", return_value=db)

    result = site_upload.claim_user_site_upload(upload_id, user_id)

    statement = str(db.execute.call_args.args[0])
    assert result == {"claimed": True, "status": "running"}
    assert "status = 'pending'" in statement
    assert "COALESCE(heartbeat_at, started_at, created_at)" in statement
    assert "attempt_count = attempt_count + 1" in statement
    db.commit.assert_called_once()


def test_broker_failure_leaves_committed_job_pending_for_recovery_and_keeps_stage(
    mocker,
):
    upload_id = uuid.uuid4()
    user_id = uuid.uuid4()
    initial_db = mocker.MagicMock()
    initial_db.add.side_effect = lambda upload: setattr(upload, "id", upload_id)
    failed_db = mocker.MagicMock()
    upload_row = SimpleNamespace(
        id=upload_id,
        status="pending",
        phase="queued",
        completed_at=None,
        error_message=None,
        extra_metadata={},
    )
    failed_db.query.return_value.filter.return_value.with_for_update.return_value.first.return_value = upload_row
    failed_db.query.return_value.filter.return_value.first.return_value = upload_row
    mocker.patch(
        "services.site_upload.get_db", side_effect=[initial_db, failed_db, failed_db]
    )
    mocker.patch.object(
        site_upload,
        "_site_upload_summary_row",
        side_effect=lambda row: {"id": str(row.id), "status": row.status},
    )
    task = site_upload.webapp_tasks.import_user_site_upload_task
    mocker.patch.object(task, "apply_async", side_effect=RuntimeError("broker down"))
    discard = mocker.patch("services.site_upload.discard_staged_site_upload")

    result = site_upload.create_user_site_upload(
        user_id,
        "sites.geojson",
        "stage-token",
        column_mapping={"start_date": "date"},
    )

    assert result == {"id": str(upload_id), "status": "pending"}
    initial_db.commit.assert_called_once()
    assert upload_row.status == "pending"
    assert upload_row.phase == "queued"
    assert upload_row.error_message.endswith("broker down")
    failed_db.commit.assert_called_once()
    discard.assert_not_called()


def test_fresh_duplicate_delivery_does_not_claim_running_job(mocker):
    upload_id = uuid.uuid4()
    user_id = uuid.uuid4()
    db = mocker.MagicMock()
    db.execute.return_value.scalar_one_or_none.return_value = None
    db.query.return_value.filter.return_value.with_for_update.return_value.first.return_value = SimpleNamespace(
        status="running",
        attempt_count=1,
        heartbeat_at=datetime.now(UTC) - timedelta(seconds=5),
        site_set_id=None,
        site_set_name=None,
        n_sites_imported=0,
    )
    mocker.patch("services.site_upload.get_db", return_value=db)

    result = site_upload.claim_user_site_upload(upload_id, user_id)

    assert result["claimed"] is False
    assert result["status"] == "running"
    db.commit.assert_not_called()


def test_cancellation_is_cooperative_and_never_terminates_worker(mocker):
    upload_id = uuid.uuid4()
    user_id = uuid.uuid4()
    upload_row = SimpleNamespace(status="running", celery_task_id="celery-task")
    db = mocker.MagicMock()
    db.query.return_value.filter.return_value.with_for_update.return_value.first.return_value = upload_row
    mocker.patch("services.site_upload.get_db", return_value=db)
    async_result = mocker.MagicMock()
    mocker.patch.object(
        site_upload.webapp_tasks.import_user_site_upload_task,
        "AsyncResult",
        return_value=async_result,
    )

    cancelled, message = site_upload.cancel_user_site_upload(upload_id, user_id)

    assert cancelled
    assert message == "Upload cancelled."
    assert upload_row.status == "cancelled"
    assert upload_row.phase == "cancelled"
    db.commit.assert_called_once()
    async_result.revoke.assert_called_once_with(terminate=False)


def test_terminal_upload_status_cannot_be_regressed(mocker):
    upload_row = SimpleNamespace(status="completed")
    db = mocker.MagicMock()
    db.query.return_value.filter.return_value.with_for_update.return_value.first.return_value = upload_row
    mocker.patch("services.site_upload.get_db", return_value=db)

    updated = site_upload.update_user_site_upload_status(
        uuid.uuid4(), status="running", phase="importing"
    )

    assert updated is False
    assert upload_row.status == "completed"
    db.commit.assert_not_called()
