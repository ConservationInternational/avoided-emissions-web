from datetime import date

import geopandas as gpd
import numpy as np
import pytest
from services.site_upload import apply_site_column_mapping
from services.site_upload_validation import (
    normalize_site_date,
    normalize_site_text,
    validate_wgs84_polygon,
)
from shapely.geometry import Polygon


def test_mapping_reads_from_immutable_source_columns():
    sites = gpd.GeoDataFrame(
        {
            "identifier": ["site-1"],
            "label": ["Name"],
            "intervention": ["2020-03-04"],
        },
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])],
        crs="EPSG:4326",
    )

    mapped, errors, _warnings = apply_site_column_mapping(
        sites,
        {
            "site_id": "label",
            "site_name": "identifier",
            "start_date": "intervention",
        },
    )

    assert not errors
    assert mapped.loc[0, "site_id"] == "Name"
    assert mapped.loc[0, "site_name"] == "site-1"
    assert mapped.loc[0, "start_date"] == date(2020, 3, 4)


def test_optional_display_fields_and_duplicate_ids_remain_valid():
    sites = gpd.GeoDataFrame(
        {"intervention": ["2020-03-04", "2020-03-05"]},
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]),
            Polygon([(2, 0), (3, 0), (3, 1), (2, 0)]),
        ],
        crs="EPSG:4326",
    )

    mapped, errors, _warnings = apply_site_column_mapping(
        sites,
        {"site_id": None, "site_name": None, "start_date": "intervention"},
    )

    assert not errors
    assert mapped["site_id"].isna().all()
    assert mapped["site_name"].isna().all()
    assert mapped["_site_upload_row_error"].isna().all()


def test_duplicate_display_ids_remain_separate_mapped_rows():
    sites = gpd.GeoDataFrame(
        {
            "id": ["same", "same"],
            "name": ["First", "Second"],
            "date": ["2020-03-04", "2020-03-04"],
        },
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]),
            Polygon([(2, 0), (3, 0), (3, 1), (2, 0)]),
        ],
        crs="EPSG:4326",
    )

    mapped, errors, _warnings = apply_site_column_mapping(
        sites,
        {"site_id": "id", "site_name": "name", "start_date": "date"},
    )

    assert not errors
    assert list(mapped["site_id"]) == ["same", "same"]
    assert list(mapped["site_name"]) == ["First", "Second"]


def test_unknown_and_missing_mapping_fields_are_reported():
    sites = gpd.GeoDataFrame(
        {"date": ["2020-03-04"]},
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])],
        crs="EPSG:4326",
    )

    _mapped, errors, _warnings = apply_site_column_mapping(
        sites, {"start_date": "date", "unexpected": "field"}
    )
    assert any("Unknown site mapping field" in error for error in errors)

    _mapped, errors, _warnings = apply_site_column_mapping(
        sites, {"site_id": None, "site_name": None}
    )
    assert any("source column for 'start_date'" in error for error in errors)


@pytest.mark.parametrize(
    ("start_date", "end_date", "expected_error"),
    [
        (2000, None, "numeric value"),
        ("2020-02-01", "2020-01-31", "precedes"),
        ("2020-02-01", "not-a-date", "end_date"),
    ],
)
def test_mapping_reports_invalid_dates_for_partial_import(
    start_date, end_date, expected_error
):
    sites = gpd.GeoDataFrame(
        {"start": [start_date], "end": [end_date]},
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])],
        crs="EPSG:4326",
    )

    mapped, errors, warnings = apply_site_column_mapping(
        sites,
        {"site_id": None, "site_name": None, "start_date": "start", "end_date": "end"},
    )

    assert not errors
    assert expected_error in warnings[0]
    assert mapped["_site_upload_row_error"].notna().all()


@pytest.mark.parametrize("value", [2000, 20000101, 0.0, "2000", "01/02/2000"])
def test_dates_reject_ambiguous_values(value):
    with pytest.raises((TypeError, ValueError)):
        normalize_site_date(value, field="start_date", row_number=1, required=True)


def test_dates_accept_iso_and_validate_optional_end():
    assert normalize_site_date(
        "2020-03-04", field="start_date", row_number=2, required=True
    ) == date(2020, 3, 4)
    assert (
        normalize_site_date(None, field="end_date", row_number=2, required=False)
        is None
    )


def test_display_text_enforces_database_lengths():
    assert normalize_site_text("  id  ", field="site_id", row_number=1) == "id"
    with pytest.raises(ValueError, match="100 characters"):
        normalize_site_text("x" * 101, field="site_id", row_number=7)


@pytest.mark.parametrize(
    "coordinates",
    [
        [(181, 0), (182, 0), (182, 1), (181, 0)],
        [(0, 91), (1, 91), (1, 92), (0, 91)],
    ],
)
def test_wgs84_bounds_reject_invalid_geometry(coordinates):
    with pytest.raises(ValueError):
        validate_wgs84_polygon(Polygon(coordinates), row_number=3)


def test_wgs84_bounds_reject_nonfinite_geometry(monkeypatch):
    class NonFinitePolygon:
        is_empty = False
        geom_type = "Polygon"
        bounds = (float("nan"), 0, 1, 1)

    monkeypatch.setattr(
        "services.site_upload_validation.BaseGeometry", NonFinitePolygon
    )
    monkeypatch.setattr(
        "services.site_upload_validation.get_coordinates",
        lambda _geometry: np.array([[float("nan"), 0], [1, 0]]),
    )
    with pytest.raises(ValueError, match="non-finite"):
        validate_wgs84_polygon(NonFinitePolygon(), row_number=3)


@pytest.mark.parametrize(
    "geometry",
    [
        Polygon(
            [(0, 0), (4, 0), (4, 4), (0, 4), (0, 0)],
            holes=[[(1, 1), (2, 1), (2, 2), (1, 2), (1, 1)]],
        ),
        Polygon([(179, 10), (-179, 10), (-179, 11), (179, 11), (179, 10)]),
        Polygon([(0, 89), (1, 89), (1, 90), (0, 90), (0, 89)]),
    ],
)
def test_wgs84_accepts_holes_antimeridian_and_polar_features(geometry):
    assert validate_wgs84_polygon(geometry, row_number=1) is geometry
