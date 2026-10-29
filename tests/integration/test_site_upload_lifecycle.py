"""Real row-lock regressions for independently reported import progress."""

import json
import uuid

import pytest
from models import UserSiteSet, UserSiteUpload, get_db
from services.site_upload import (
    cancel_user_site_upload,
    update_user_site_upload_status,
)
from sqlalchemy import text

pytestmark = pytest.mark.integration


def test_progress_and_cancel_do_not_deadlock_with_unpublished_site_set(mocker):
    upload_id, user_id = uuid.uuid4(), uuid.uuid4()
    writer = get_db()
    try:
        writer.add(
            UserSiteUpload(
                id=upload_id,
                user_id=user_id,
                original_filename="lock-fixture.geojson",
                status="running",
            )
        )
        writer.commit()
        writer.add(
            UserSiteSet(
                user_id=user_id,
                source_upload_id=upload_id,
                name="Unpublished fixture",
                original_filename="lock-fixture.geojson",
                file_size_bytes=100,
                file_format="geojson",
                n_sites=0,
            )
        )
        writer.flush()
        for cancel in (False, True):
            progress = get_db()
            progress.execute(text("SET lock_timeout = '500ms'"))
            progress.commit()
            mocker.patch("services.site_upload.get_db", return_value=progress)
            if cancel:
                ok, _ = cancel_user_site_upload(upload_id, user_id)
                assert ok
            else:
                assert update_user_site_upload_status(
                    upload_id, status="running", phase="validating", rows_scanned=10
                )
        writer.rollback()
        writer.expire_all()
        persisted = writer.get(UserSiteUpload, upload_id)
        assert persisted.status == "cancelled"
        assert persisted.rows_scanned == 10
        assert (
            not writer.query(UserSiteSet)
            .filter(UserSiteSet.source_upload_id == upload_id)
            .count()
        )
    finally:
        writer.rollback()
        writer.query(UserSiteSet).filter(
            UserSiteSet.source_upload_id == upload_id
        ).delete()
        writer.query(UserSiteUpload).filter(UserSiteUpload.id == upload_id).delete()
        writer.commit()
        writer.close()


def test_cancel_before_publication_rolls_back_real_import_and_retains_source(
    mocker, tmp_path
):
    from services import site_upload

    upload_id, user_id = uuid.uuid4(), uuid.uuid4()
    mocker.patch.object(site_upload.Config, "S3_BUCKET", "")
    mocker.patch.object(site_upload, "SITE_UPLOAD_STAGE_DIR", tmp_path)
    blob = json.dumps(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"start": "2000-01-01"},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                    },
                }
            ],
        }
    ).encode()
    token = site_upload.stage_site_upload(blob, "cancel-fixture.geojson", user_id)
    db = get_db()
    try:
        db.add(
            UserSiteUpload(
                id=upload_id,
                user_id=user_id,
                original_filename="cancel-fixture.geojson",
                status="running",
            )
        )
        db.commit()
        original_update = site_upload.update_user_site_upload_status

        def cancel_at_finalizing(*args, **kwargs):
            result = original_update(*args, **kwargs)
            if kwargs.get("phase") == "finalizing":
                assert cancel_user_site_upload(upload_id, user_id)[0]
            return result

        mocker.patch.object(
            site_upload,
            "update_user_site_upload_status",
            side_effect=cancel_at_finalizing,
        )
        with pytest.raises(site_upload._SiteUploadCancelled):
            site_upload.save_user_site_set_from_staged(
                user_id,
                token,
                {"start_date": "start"},
                upload_id=upload_id,
                consume=False,
            )
        db.expire_all()
        assert db.get(UserSiteUpload, upload_id).status == "cancelled"
        assert (
            db.query(UserSiteSet)
            .filter(UserSiteSet.source_upload_id == upload_id)
            .count()
            == 0
        )
        assert (tmp_path / f"{token}.bin").exists()
    finally:
        db.rollback()
        db.query(UserSiteSet).filter(UserSiteSet.source_upload_id == upload_id).delete()
        db.query(UserSiteUpload).filter(UserSiteUpload.id == upload_id).delete()
        db.commit()
        db.close()
