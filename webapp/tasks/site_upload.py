"""Celery tasks for importing and auditing staged site uploads."""

import logging
import uuid
from datetime import UTC, datetime, timedelta

from celery_app import celery_app
from config import report_exception

logger = logging.getLogger(__name__)

SITE_UPLOAD_LEASE_SECONDS = 8400
SITE_UPLOAD_MAX_ATTEMPTS = 3


@celery_app.task(
    name="tasks.import_user_site_upload",
    bind=True,
    max_retries=3,
    acks_late=True,
    reject_on_worker_lost=True,
    soft_time_limit=7200,
    time_limit=7500,
)
def import_user_site_upload_task(
    self, upload_id, user_id, upload_token, column_mapping=None
) -> dict:
    """Claim and import a staged site upload exactly once."""
    from models import UserSiteUpload, get_db
    from services.site_upload import (
        _SiteUploadCancelled,
        claim_user_site_upload,
        discard_staged_site_upload,
        save_user_site_set_from_staged,
        update_user_site_upload_status,
    )

    upload_uuid = uuid.UUID(str(upload_id))
    user_uuid = uuid.UUID(str(user_id))
    claim = claim_user_site_upload(
        upload_uuid,
        user_uuid,
        lease_seconds=SITE_UPLOAD_LEASE_SECONDS,
        max_attempts=SITE_UPLOAD_MAX_ATTEMPTS,
    )
    if not claim["claimed"]:
        if claim["status"] == "running":
            raise self.retry(
                countdown=SITE_UPLOAD_LEASE_SECONDS + 5,
                max_retries=SITE_UPLOAD_MAX_ATTEMPTS,
            )
        return {
            "status": claim["status"],
            "site_set_id": claim.get("site_set_id"),
            "site_set_name": claim.get("site_set_name"),
            "n_sites": claim.get("n_sites", 0),
        }

    try:
        detail = save_user_site_set_from_staged(
            user_uuid,
            upload_token,
            column_mapping=column_mapping,
            consume=False,
            upload_id=upload_uuid,
        )
        try:
            discard_staged_site_upload(upload_token, user_uuid)
        except Exception:
            logger.warning(
                "Could not remove successful upload stage %s",
                upload_uuid,
                exc_info=True,
            )
        return {
            "status": "completed",
            "site_set_id": detail["id"],
            "site_set_name": detail["name"],
            "n_sites": detail["n_sites"],
        }
    except _SiteUploadCancelled:
        try:
            discard_staged_site_upload(upload_token, user_uuid)
        except Exception:
            logger.warning(
                "Could not remove cancelled upload stage %s", upload_uuid, exc_info=True
            )
        return {
            "status": "cancelled",
            "site_set_id": None,
            "site_set_name": None,
            "n_sites": 0,
        }
    except Exception as exc:
        logger.exception("Asynchronous user site upload failed")
        report_exception()

        db = get_db()
        try:
            upload = (
                db.query(UserSiteUpload)
                .filter(
                    UserSiteUpload.id == upload_uuid,
                    UserSiteUpload.user_id == user_uuid,
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
            rows_scanned = upload.rows_scanned if upload else 0
            rows_accepted = upload.rows_accepted if upload else 0
            rows_skipped = upload.rows_skipped if upload else 0
            prior_meta = dict(upload.extra_metadata or {}) if upload else {}
        finally:
            db.close()

        update_user_site_upload_status(
            upload_uuid,
            status="failed",
            phase="failed",
            completed_at=datetime.now(UTC),
            n_sites_imported=rows_accepted,
            rows_scanned=rows_scanned,
            rows_accepted=rows_accepted,
            rows_skipped=rows_skipped,
            error_message=str(exc),
            diagnostics={
                **prior_meta.get("diagnostics", {}),
                "failure": str(exc)[:2000],
                "failed_at": datetime.now(UTC).isoformat(),
            },
        )
        raise


@celery_app.task(name="tasks.recover_site_uploads")
def recover_site_uploads(*, limit=100) -> dict:
    """Reserve bounded redispatches of committed jobs left without a worker.

    Reservations commit before broker I/O, so a crash on either side of send
    can only produce a delayed retry or duplicate delivery, never lost work.
    Confirmed queued deliveries are not resent and expire after 24 hours.
    Failed/unknown sends have six reservations separated by ten minutes.
    Conditional worker claims make duplicate deliveries harmless.
    """
    from models import UserSiteUpload, get_db
    from services.site_upload import (
        SITE_UPLOAD_DISPATCH_INTERVAL_SECONDS,
        SITE_UPLOAD_MAX_DISPATCH_ATTEMPTS,
        SITE_UPLOAD_PENDING_EXPIRY_SECONDS,
        record_site_upload_dispatch,
    )
    from sqlalchemy import and_, func, or_

    now = datetime.now(UTC)
    dispatch_before = now - timedelta(seconds=SITE_UPLOAD_DISPATCH_INTERVAL_SECONDS)
    running_before = now - timedelta(seconds=SITE_UPLOAD_LEASE_SECONDS)
    pending_before = now - timedelta(seconds=SITE_UPLOAD_PENDING_EXPIRY_SECONDS)
    result = {"dispatched": [], "failed": [], "dispatch_errors": []}
    reservations = []
    db = get_db()
    try:
        dispatch_at = UserSiteUpload.extra_metadata["dispatch_at"].as_string()
        confirmed_at = UserSiteUpload.extra_metadata[
            "dispatch_confirmed_at"
        ].as_string()
        rows = (
            db.query(UserSiteUpload)
            .filter(
                or_(
                    and_(
                        UserSiteUpload.status == "pending",
                        UserSiteUpload.created_at < dispatch_before,
                        or_(
                            confirmed_at.is_(None),
                            UserSiteUpload.created_at < pending_before,
                        ),
                    ),
                    and_(
                        UserSiteUpload.status == "running",
                        func.coalesce(
                            UserSiteUpload.heartbeat_at,
                            UserSiteUpload.started_at,
                            UserSiteUpload.created_at,
                        )
                        < running_before,
                    ),
                ),
                or_(dispatch_at.is_(None), dispatch_at < dispatch_before.isoformat()),
            )
            .order_by(UserSiteUpload.created_at, UserSiteUpload.id)
            .limit(max(1, min(int(limit), 100)))
            .with_for_update(key_share=True, skip_locked=True)
            .all()
        )
        for upload in rows:
            meta = dict(upload.extra_metadata or {})
            token = meta.get("upload_token")
            dispatch_attempts = int(meta.get("dispatch_attempts", 0))
            if upload.status == "pending" and upload.created_at < pending_before:
                upload.status = upload.phase = "failed"
                upload.completed_at = now
                upload.error_message = (
                    "Upload expired after 24 hours waiting for a worker."
                )
                result["failed"].append(str(upload.id))
                continue
            if (
                not token
                or upload.attempt_count >= SITE_UPLOAD_MAX_ATTEMPTS
                or dispatch_attempts >= SITE_UPLOAD_MAX_DISPATCH_ATTEMPTS
            ):
                upload.status = upload.phase = "failed"
                upload.completed_at = now
                upload.error_message = (
                    "Upload recovery exhausted its bounded retry limit."
                    if token
                    else "Upload recovery cannot find the persisted stage token."
                )
                result["failed"].append(str(upload.id))
                continue
            task_id = str(uuid.uuid4())
            upload.status = "pending"
            upload.phase = "queued"
            upload.started_at = None
            upload.heartbeat_at = now
            upload.error_message = None
            upload.celery_task_id = task_id
            upload.extra_metadata = {
                **meta,
                "dispatch_at": now.isoformat(),
                "dispatch_attempts": dispatch_attempts + 1,
                "dispatch_confirmed_at": None,
            }
            reservations.append(
                (
                    task_id,
                    {
                        "upload_id": str(upload.id),
                        "user_id": str(upload.user_id),
                        "upload_token": str(token),
                        "column_mapping": meta.get("column_mapping"),
                    },
                )
            )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    for task_id, kwargs in reservations:
        try:
            import_user_site_upload_task.apply_async(task_id=task_id, kwargs=kwargs)
            record_site_upload_dispatch(kwargs["upload_id"], task_id)
            result["dispatched"].append(kwargs["upload_id"])
        except Exception:
            logger.exception("Could not redispatch site upload %s", kwargs["upload_id"])
            result["dispatch_errors"].append(kwargs["upload_id"])
    return result


@celery_app.task(name="tasks.audit_site_upload_stages")
def audit_site_upload_stages(*, apply=False, older_than_hours=24) -> dict:
    """Audit stale upload rows and local stages without making cloud calls.

    ``apply`` only removes local stage files and marks expired in-flight
    database rows failed. Shared S3 stages are never listed or deleted here.
    """
    from config import Config
    from models import UserSiteUpload, get_db
    from services.site_upload import (
        SITE_UPLOAD_PENDING_EXPIRY_SECONDS,
        audit_local_site_upload_stages,
    )
    from sqlalchemy import and_, func, or_

    now = datetime.now(UTC)
    stale_before = now - timedelta(
        seconds=max(SITE_UPLOAD_LEASE_SECONDS, max(1, int(older_than_hours)) * 3600)
    )
    audit = {
        "stale_upload_ids": [],
        "expired_local_tokens": [],
        "protected_local_tokens": [],
        "remote_stages_skipped": bool(Config.S3_BUCKET),
        "applied": bool(apply),
    }

    db = get_db()
    try:
        stale = (
            db.query(UserSiteUpload)
            .filter(
                or_(
                    and_(
                        UserSiteUpload.status == "pending",
                        UserSiteUpload.created_at
                        < now
                        - timedelta(
                            seconds=max(
                                SITE_UPLOAD_PENDING_EXPIRY_SECONDS,
                                max(1, int(older_than_hours)) * 3600,
                            )
                        ),
                    ),
                    and_(
                        UserSiteUpload.status == "running",
                        func.coalesce(
                            UserSiteUpload.heartbeat_at,
                            UserSiteUpload.started_at,
                            UserSiteUpload.created_at,
                        )
                        < stale_before,
                    ),
                )
            )
            .limit(100)
            .with_for_update(key_share=True, skip_locked=True)
            .all()
        )
        for upload in stale:
            audit["stale_upload_ids"].append(str(upload.id))
            if apply:
                upload.status = "failed"
                upload.phase = "failed"
                upload.completed_at = now
                upload.error_message = "Upload expired during cleanup audit."
        if apply and stale:
            db.commit()
        elif stale:
            db.rollback()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    audit.update(audit_local_site_upload_stages(apply=apply, now=now))
    return audit
