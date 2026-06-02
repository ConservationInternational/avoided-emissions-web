"""Celery task: import staged user site uploads into the database."""

import logging
from datetime import datetime, timezone


from celery_app import celery_app
from config import report_exception

logger = logging.getLogger(__name__)


@celery_app.task(
    name="tasks.import_user_site_upload",
    bind=True,
    max_retries=0,
    acks_late=True,
    reject_on_worker_lost=True,
    soft_time_limit=7200,
    time_limit=7500,
)
def import_user_site_upload_task(
    self, upload_id, user_id, upload_token, column_mapping=None
) -> dict:
    """Persist a staged site upload asynchronously.

    Parameters
    ----------
    self : celery.Task
        Bound Celery task instance.
    upload_id : str
        Upload-job UUID stored in ``user_site_uploads``.
    user_id : str
        Owning user UUID.
    upload_token : str
        Token pointing at the staged upload payload.
    column_mapping : dict | None
        Canonical site-field to source-column mapping selected in the UI.

    Returns
    -------
    dict
        Completion payload with job status plus imported site-set identifiers.
    """
    import uuid

    from services import (
        UserSiteUpload,
        get_db,
        save_user_site_set_from_staged,
        update_user_site_upload_status,
    )

    upload_uuid = uuid.UUID(str(upload_id))
    user_uuid = uuid.UUID(str(user_id))

    db = get_db()
    try:
        upload = (
            db.query(UserSiteUpload)
            .filter(
                UserSiteUpload.id == upload_uuid, UserSiteUpload.user_id == user_uuid
            )
            .first()
        )
        if upload and upload.status == "cancelled":
            return {
                "status": "cancelled",
                "site_set_id": None,
                "site_set_name": None,
                "n_sites": 0,
            }
    finally:
        db.close()

    update_user_site_upload_status(
        upload_uuid,
        status="running",
        started_at=datetime.now(timezone.utc),
        n_sites_imported=0,
        error_message=None,
    )

    try:
        detail = save_user_site_set_from_staged(
            user_uuid,
            upload_token,
            column_mapping=column_mapping,
            upload_id=upload_uuid,
        )
        update_user_site_upload_status(
            upload_uuid,
            status="completed",
            completed_at=datetime.now(timezone.utc),
            site_set_id=uuid.UUID(str(detail["id"])),
            site_set_name=detail["name"],
            n_sites_imported=detail["n_sites"],
            ingest_stats=detail.get("ingest_stats"),
            error_message=None,
        )
        return {
            "status": "completed",
            "site_set_id": detail["id"],
            "site_set_name": detail["name"],
            "n_sites": detail["n_sites"],
        }
    except Exception as exc:
        logger.exception("Asynchronous user site upload failed")
        report_exception()
        update_user_site_upload_status(
            upload_uuid,
            status="failed",
            completed_at=datetime.now(timezone.utc),
            n_sites_imported=0,
            error_message=str(exc),
        )
        raise
