"""Authoritative polygon preparation against a disposable migrated PostGIS DB."""

import uuid
from types import SimpleNamespace
from unittest.mock import patch

import geopandas as gpd
import pytest
import shapely
from models import get_db
from services.reference_layers import (
    _write_reference_parquet,
    compute_exact_match_groups_with_splitting,
)
from shapely.geometry import box
from sqlalchemy import text

pytestmark = pytest.mark.integration


@pytest.fixture
def reference_db():
    db = get_db()
    db.execute(text("DELETE FROM geoboundaries_adm1"))
    try:
        yield db
    finally:
        db.rollback()
        db.close()


def insert_region(db, key, geometry):
    db.execute(
        text(
            """INSERT INTO geoboundaries_adm1
            (shape_group, shape_id, shape_name, geom)
            VALUES ('TST', :key, 'Same display name',
                ST_Multi(ST_GeomFromText(:geometry, 4326)))"""
        ),
        {"key": key, "geometry": geometry.wkt},
    )


def split_with_db(db, frame):
    proxy = SimpleNamespace(execute=db.execute, close=lambda: None)
    with patch("services.reference_layers.get_db", return_value=proxy):
        return compute_exact_match_groups_with_splitting(frame, ["admin1"])


def test_authoritative_queries_preserve_scattered_sites_and_residual(
    reference_db, tmp_path
):
    insert_region(reference_db, "west", box(0, 0, 0.95, 1))
    insert_region(reference_db, "east", box(0.95, 0, 1, 1))
    insert_region(reference_db, "remote", box(100, 0, 100.5, 1))
    frame = gpd.GeoDataFrame(
        {
            "site_id": [str(uuid.uuid4()), str(uuid.uuid4())],
            "display_site_id": ["duplicate", "duplicate"],
        },
        geometry=[box(0, 0, 1, 1), box(100, 0, 101, 1)],
        crs=4326,
    )
    result, groups = split_with_db(reference_db, frame)
    assert len(result) == 4
    assert 0 in groups
    for source, group in result.groupby("site_id"):
        original = frame.loc[frame.site_id == source].geometry.iloc[0]
        assert (
            shapely.symmetric_difference(
                original, shapely.union_all(group.geometry)
            ).area
            < 1e-12
        )
    path = tmp_path / "references.parquet"
    assert _write_reference_parquet(reference_db, "admin1", path, batch_size=1) == 3
    exported = gpd.read_parquet(path)
    assert exported.region_id.is_unique
    assert exported.crs.to_epsg() == 4326


def test_reference_overlaps_fail_before_any_submission(reference_db):
    insert_region(reference_db, "a", box(0, 0, 0.8, 1))
    insert_region(reference_db, "b", box(0.2, 0, 1, 1))
    frame = gpd.GeoDataFrame(
        {"site_id": ["immutable"]}, geometry=[box(0, 0, 1, 1)], crs=4326
    )
    with pytest.raises(ValueError, match="Overlapping reference coverage"):
        split_with_db(reference_db, frame)


def test_empty_reference_table_is_not_treated_as_uncovered(reference_db):
    frame = gpd.GeoDataFrame(
        {"site_id": ["immutable"]}, geometry=[box(0, 0, 1, 1)], crs=4326
    )
    with pytest.raises(ValueError, match="not ready"):
        split_with_db(reference_db, frame)


@pytest.mark.parametrize("variables", [[], ["pa"], ["admin1"]])
def test_submission_keeps_database_artifact_and_group_identities_consistent(
    mocker, tmp_path, variables
):
    from datetime import date

    from geoalchemy2 import WKTElement
    from models import (
        AnalysisTask,
        GeoBoundaryADM1,
        TaskSite,
        UserSiteFeature,
        UserSiteSet,
    )
    from services.analysis_task import _complete_analysis_task_submission

    db = get_db()
    site_set_id, task_id, user_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    feature_ids = [uuid.uuid4(), uuid.uuid4()]
    region_ids = [f"{task_id}-a", f"{task_id}-b"]
    try:
        db.add(
            UserSiteSet(
                id=site_set_id,
                user_id=user_id,
                name="Duplicate labels",
                original_filename="fixture.geojson",
                file_size_bytes=100,
                file_format="geojson",
                n_sites=2,
            )
        )
        db.flush()
        for feature_id in feature_ids:
            db.add(
                UserSiteFeature(
                    id=feature_id,
                    site_set_id=site_set_id,
                    site_id="duplicate",
                    site_name="Duplicate display name",
                    start_date=date(2000, 1, 1),
                    geom=WKTElement(
                        shapely.MultiPolygon([box(0, 0, 1, 1)]).wkt, srid=4326
                    ),
                )
            )
        db.add(
            AnalysisTask(
                id=task_id,
                name="Fixture",
                submitted_by=user_id,
                submit_session_id="local-test-session",
                site_set_id=site_set_id,
                status="submitting",
                covariates=["elev"],
                config={"exact_match_vars": variables, "min_site_area_ha": 0},
            )
        )
        insert_region(db, region_ids[0], box(0, 0, 0.95, 1))
        insert_region(db, region_ids[1], box(0.95, 0, 1, 1))
        db.commit()
        artifact_path = tmp_path / "prepared.parquet"

        def capture_artifact(frame, _task_id):
            frame.to_parquet(artifact_path, index=False)
            return "s3://test-only/prepared.parquet"

        mocker.patch(
            "services.analysis_task.upload_sites_parquet_to_s3",
            side_effect=capture_artifact,
        )
        mocker.patch("services.analysis_task.get_reference_layer_uris", return_value={})
        mocker.patch("session_store.get_access_token", return_value="local-test-token")
        api = mocker.patch("trendsearth_client.TrendsEarthClient").return_value
        api.create_execution.return_value = {"data": {"id": "test-execution"}}
        _complete_analysis_task_submission(str(task_id), str(user_id))
        db.expire_all()
        task = db.get(AnalysisTask, task_id)
        assert task.status == "submitted"
        artifact = gpd.read_parquet(artifact_path)
        rows = db.query(TaskSite).filter(TaskSite.task_id == task_id).all()
        assert set(artifact.site_id) == set(map(str, feature_ids))
        assert artifact.display_site_id.eq("duplicate").all()
        assert not artifact.duplicated(["site_id", "sub_site_index"]).any()
        assert len(rows) == len(artifact) == (4 if variables == ["admin1"] else 2)
        assert sum(row.area_ha for row in rows) == pytest.approx(artifact.area_ha.sum())
        report = task.extra_metadata["preparation"]
        assert report["input_sites"] == report["retained_sites"] == 2
        assert report["fragments"] == len(rows)
        assert report["uncovered_area_ha"] == 0
        params = api.create_execution.call_args.args[1]
        if variables:
            packed = params["exact_match_group_mapping"]
            members = [tuple(member) for group in packed.values() for member in group]
            assert set(members) == set(zip(artifact.site_id, artifact.sub_site_index))
    finally:
        db.rollback()
        db.query(TaskSite).filter(TaskSite.task_id == task_id).delete()
        db.query(AnalysisTask).filter(AnalysisTask.id == task_id).delete()
        db.query(UserSiteFeature).filter(
            UserSiteFeature.site_set_id == site_set_id
        ).delete()
        db.query(UserSiteSet).filter(UserSiteSet.id == site_set_id).delete()
        db.query(GeoBoundaryADM1).filter(
            GeoBoundaryADM1.shape_id.in_(region_ids)
        ).delete()
        db.commit()
        db.close()
