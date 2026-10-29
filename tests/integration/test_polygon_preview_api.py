"""Owned lookup and tile queries against the disposable migrated PostGIS DB."""

import uuid
from datetime import date
from types import SimpleNamespace

import api_routes
import pytest
import services.site_set as site_set_service
from flask import Flask
from geoalchemy2.elements import WKTElement
from models import AnalysisTask, UserSiteFeature, UserSiteSet, get_db
from sqlalchemy import text

pytestmark = pytest.mark.integration


@pytest.fixture
def preview_api(monkeypatch):
    db = get_db()
    owner = uuid.uuid4()
    collection = UserSiteSet(
        user_id=owner,
        name="Duplicate display labels",
        original_filename="sites.geojson",
        file_size_bytes=1000,
        file_format="geojson",
        n_sites=2,
    )
    db.add(collection)
    db.flush()
    features = [
        UserSiteFeature(
            site_set_id=collection.id,
            site_id="duplicate",
            site_name=f"Site {offset}",
            start_date=date(2000, 1, 1),
            geom=WKTElement(
                f"MULTIPOLYGON((({offset} 0,{offset + 1} 0,"
                f"{offset + 1} 1,{offset} 1,{offset} 0)))",
                srid=4326,
            ),
        )
        for offset in (0, 2)
    ]
    db.add_all(features)
    task = AnalysisTask(
        name="Prepared task",
        submitted_by=owner,
        site_set_id=collection.id,
        covariates=[],
        extra_metadata={"preparation": {"input_sites": 2, "fragments": 3}},
    )
    db.add(task)
    db.flush()
    proxy = SimpleNamespace(
        query=db.query, execute=db.execute, flush=db.flush, close=lambda: None
    )
    monkeypatch.setattr(api_routes, "get_db", lambda: proxy)
    monkeypatch.setattr(site_set_service, "get_db", lambda: proxy)
    user = SimpleNamespace(id=str(owner), is_authenticated=True)
    monkeypatch.setattr(api_routes.flask_login, "current_user", user)
    app = Flask(__name__)
    app.config.update(TESTING=True, LOGIN_DISABLED=True)
    app.register_blueprint(
        api_routes.create_api_blueprint(
            SimpleNamespace(limit=lambda _rate: lambda function: function)
        )
    )
    try:
        yield app.test_client(), collection, features, task, user
    finally:
        db.rollback()
        db.close()


def test_owned_extents_distinguish_duplicate_display_ids(preview_api):
    client, collection, features, _task, _user = preview_api
    responses = [
        client.get(f"/api/site-feature/{collection.id}/{feature.id}")
        for feature in features
    ]
    assert all(response.status_code == 200 for response in responses)
    assert [response.json["display_site_id"] for response in responses] == [
        "duplicate",
        "duplicate",
    ]
    assert responses[0].json["site_id"] != responses[1].json["site_id"]
    assert responses[0].json["bounds"]["west"] == 0
    assert responses[1].json["bounds"]["west"] == 2


def test_live_tiles_include_canonical_and_display_identity(preview_api):
    client, collection, features, task, _user = preview_api
    for path in (
        f"/api/sites-tiles/{collection.id}/0/0/0",
        f"/api/task-sites-tiles/{task.id}/0/0/0",
    ):
        response = client.get(path)
        assert response.status_code == 200
        assert response.mimetype == "application/vnd.mapbox-vector-tile"
        assert b"display_site_id" in response.data
        assert b"duplicate" in response.data
        for feature in features:
            assert str(feature.id).encode() in response.data


def test_polygon_tiles_preserve_holes_and_source_geometry(preview_api):
    client, collection, features, task, _user = preview_api
    features[0].geom = WKTElement(
        "MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0),"
        "(0.25 0.25,0.25 0.75,0.75 0.75,0.75 0.25,0.25 0.25)))",
        srid=4326,
    )
    db = api_routes.get_db()
    db.flush()
    before = db.execute(
        text("SELECT ST_AsBinary(geom) FROM user_site_features WHERE id=:id"),
        {"id": features[0].id},
    ).scalar_one()
    for path in (
        f"/api/sites-tiles/{collection.id}/7/64/63",
        f"/api/task-sites-tiles/{task.id}/7/64/63",
    ):
        response = client.get(path)
        assert response.status_code == 200
        assert b"duplicate" in response.data
        assert str(features[0].id).encode() in response.data
    valid, holes, after = db.execute(
        text("""
            SELECT ST_IsValid(tile_geom),
                   ST_NumInteriorRings(tile_geom), ST_AsBinary(geom)
            FROM (
                SELECT geom, ST_AsMVTGeom(
                    ST_Transform(geom,3857), ST_TileEnvelope(7,64,63),
                    4096,256,true
                ) AS tile_geom
                FROM user_site_features WHERE id=:id
            ) q
        """),
        {"id": features[0].id},
    ).one()
    assert valid
    assert holes == 1
    assert bytes(after) == bytes(before)


def test_cross_owner_lookup_and_preparation_are_hidden(preview_api):
    client, collection, features, task, user = preview_api
    user.id = str(uuid.uuid4())
    paths = [
        f"/api/site-feature/{collection.id}/{features[0].id}",
        f"/api/task-site-feature/{task.id}/{features[0].id}",
        f"/api/tasks/{task.id}/preparation/report",
    ]
    assert all(client.get(path).status_code == 404 for path in paths)


def test_unloaded_map_selection_filters_grid_by_immutable_feature_id(preview_api):
    _client, collection, features, _task, _user = preview_api
    rows = site_set_service.get_user_site_set_preview_rows(
        str(collection.id),
        filter_model={
            "feature_id": {
                "filterType": "text",
                "type": "equals",
                "filter": str(features[1].id),
            }
        },
    )
    assert len(rows) == 1
    assert rows[0]["feature_id"] == str(features[1].id)
