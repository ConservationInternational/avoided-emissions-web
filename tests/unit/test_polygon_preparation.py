"""Conservation, immutable identity and bounded-export regressions."""

import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pandas as pd
import pytest
import shapely
from services.polygon_partition import equal_area_ha, partition_by_regions, polygonal
from services.reference_layers import compute_exact_match_groups_with_splitting
from services.site_set import _stream_site_set_to_parquet_buf
from shapely.geometry import Polygon, box


@pytest.mark.parametrize(
    "source,regions,expected",
    [
        (box(0, 0, 1, 1), [box(0, 0, 0.5, 1)], 2),
        (box(0, 0, 12, 1), [box(i, 0, i + 1, 1) for i in range(12)], 12),
        (box(0, 0, 1, 1), [box(0, 0, 0.95, 1), box(0.95, 0, 1, 1)], 2),
        (box(0, 0, 1, 1), [], 1),
        (
            Polygon(
                [(0, 0), (3, 0), (3, 3), (0, 3)],
                holes=[[(1, 1), (2, 1), (2, 2), (1, 2)]],
            ),
            [box(0, 0, 1.5, 3)],
            2,
        ),
    ],
)
def test_partitions_conserve_every_polygon(source, regions, expected):
    indices, pieces, keys = partition_by_regions(
        [source], list(map(str, range(len(regions)))), regions, "test"
    )
    assert len(pieces) == expected
    assert indices.tolist() == [0] * expected
    assert shapely.symmetric_difference(source, shapely.union_all(pieces)).area < 1e-12
    assert sum(part.area for part in pieces) == pytest.approx(source.area)
    if not regions:
        assert keys.tolist() == [None]


def test_overlapping_reference_regions_fail_visibly():
    with pytest.raises(ValueError, match="Overlapping reference coverage"):
        partition_by_regions(
            [box(0, 0, 1, 1)],
            ["first", "second"],
            [box(0, 0, 0.7, 1), box(0.3, 0, 1, 1)],
            "admin1",
        )


def test_polygon_repair_ignores_nested_nonpolygon_members():
    source = shapely.GeometryCollection(
        [
            box(0, 0, 1, 1),
            shapely.GeometryCollection([shapely.LineString([(3, 0), (3, 1)])]),
        ]
    )
    assert polygonal(source).equals(box(0, 0, 1, 1))


def test_high_latitude_area_reconciles_after_adding_boundary_vertices():
    source = box(0, 70, 12, 72)
    regions = [box(0, y, 12, y + 0.5) for y in [70, 70.5, 71, 71.5]]
    _, pieces, _ = partition_by_regions(
        [source], ["a", "b", "c", "d"], regions, "high-latitude"
    )
    source_area = equal_area_ha([source])[0]
    fragment_area = equal_area_ha(pieces).sum()
    assert fragment_area == pytest.approx(source_area, rel=1e-8)
    assert shapely.symmetric_difference(source, shapely.union_all(pieces)).area == 0


@pytest.mark.parametrize("variables", [[], ["pa"], ["admin1"]])
def test_splitting_rejects_undeclared_crs_and_duplicate_internal_keys(variables):
    frame = gpd.GeoDataFrame(
        {"site_id": ["same", "same"]},
        geometry=[box(0, 0, 1, 1), box(2, 0, 3, 1)],
    )
    with pytest.raises(ValueError, match="EPSG:4326"):
        compute_exact_match_groups_with_splitting(frame, variables)
    with pytest.raises(ValueError, match="unique immutable"):
        compute_exact_match_groups_with_splitting(frame.set_crs(4326), variables)


def test_binary_only_matching_preserves_duplicate_display_ids():
    frame = gpd.GeoDataFrame(
        {
            "site_id": [str(uuid.uuid4()), str(uuid.uuid4())],
            "display_site_id": ["duplicate", "duplicate"],
        },
        geometry=[box(0, 0, 1, 1), box(2, 0, 3, 1)],
        crs=4326,
    )
    result, mapping = compute_exact_match_groups_with_splitting(frame, ["pa"])
    assert result.display_site_id.tolist() == ["duplicate", "duplicate"]
    assert len(set(mapping[1])) == 2


def test_reference_keys_are_not_region_names_and_residual_is_unmatched():
    frame = gpd.GeoDataFrame(
        {"site_id": ["immutable"], "display_site_id": ["label"]},
        geometry=[box(0, 0, 3, 1)],
        crs=4326,
    )
    db = MagicMock()
    db.execute.return_value.fetchall.return_value = [
        ("country:a", box(0, 0, 1, 1).wkb),
        ("country:b", box(1, 0, 2, 1).wkb),
    ]
    with patch("services.reference_layers.get_db", return_value=db):
        result, mapping = compute_exact_match_groups_with_splitting(frame, ["admin1"])
    assert len(result) == 3
    assert 0 in mapping
    assert sum(part.area for part in result.geometry) == pytest.approx(3)
    query = str(db.execute.call_args.args[0])
    assert "geom_simp" not in query
    assert "shape_name" not in query
    assert "ST_AsBinary(geom)" in query
    assert result.attrs["reference_versions"]["admin1"]


def test_multiple_layers_and_reordered_inputs_preserve_keys_and_strata():
    frame = gpd.GeoDataFrame(
        {
            "site_id": ["immutable-a", "immutable-b"],
            "display_site_id": ["same", "same"],
        },
        geometry=[box(0, 70, 2, 72), box(4, 70, 6, 72)],
        crs=4326,
    )
    regions = [
        [("country:a", box(0, 70, 1, 72).wkb), ("country:b", box(1, 70, 2, 72).wkb)],
        [("10", box(0, 70, 2, 71).wkb), ("20", box(0, 71, 2, 72).wkb)],
    ]
    db = MagicMock()
    db.execute.return_value.fetchall.side_effect = regions * 2
    with patch("services.reference_layers.get_db", return_value=db):
        first, groups = compute_exact_match_groups_with_splitting(
            frame, ["admin1", "ecoregion"]
        )
        second, _ = compute_exact_match_groups_with_splitting(
            frame.iloc[::-1], ["ecoregion", "admin1"]
        )
    assert len(first) == 5
    assert groups[0] == [("immutable-b", 0)]
    assert len({key for members in groups.values() for key in members}) == 5
    covered = first[first.site_id == "immutable-a"]
    assert set(zip(covered.admin1, covered.ecoregion)) == {
        ("country:a", "10"),
        ("country:a", "20"),
        ("country:b", "10"),
        ("country:b", "20"),
    }
    assert first.area_ha.sum() == pytest.approx(
        equal_area_ha(frame.geometry).sum(), rel=1e-8
    )
    columns = ["site_id", "sub_site_index"]
    assert shapely.equals(
        first.sort_values(columns).geometry.to_numpy(),
        second.sort_values(columns).geometry.to_numpy(),
    ).all()


def test_export_writes_multiple_valid_geoparquet_row_groups():
    import pyarrow.parquet as pq

    rows = [
        SimpleNamespace(
            site_id=str(uuid.uuid4()),
            display_site_id="duplicate",
            site_name=f"name{i}",
            start_date="2000-01-01",
            end_date=None,
            area_ha=1.0,
            geom_wkb=box(i, 0, i + 0.5, 0.5).wkb,
        )
        for i in range(5)
    ]
    result = MagicMock()
    result.__iter__.return_value = iter(rows)
    db = MagicMock()
    db.execute.return_value = result
    stream, count = _stream_site_set_to_parquet_buf("set", db, batch_size=2)
    try:
        assert count == 5
        assert pq.ParquetFile(stream).num_row_groups == 3
        stream.seek(0)
        frame = gpd.read_parquet(stream)
        assert len(frame) == 5
        assert frame.display_site_id.eq("duplicate").all()
        assert frame.site_id.is_unique
        assert pd.to_datetime(frame.start_date).dt.year.eq(2000).all()
        assert frame.crs.to_epsg() == 4326
    finally:
        stream.close()
    result.close.assert_called_once()


def test_stale_reference_exports_cannot_be_used_for_preparation():
    from datetime import UTC, datetime, timedelta

    from services.reference_layers import get_reference_layer_uris

    now = datetime.now(UTC)
    db = MagicMock()
    db.query.return_value.all.side_effect = [
        [
            SimpleNamespace(
                layer_name="admin1",
                s3_uri="s3://bucket/immutable.parquet",
                schema_version=2,
                exported_at=now - timedelta(hours=1),
            )
        ],
        [SimpleNamespace(table_name="geoboundaries_adm1", completed_at=now)],
    ]
    with patch("services.reference_layers.get_db", return_value=db):
        assert get_reference_layer_uris(["admin1"]) == {}


def test_read_only_artifact_audit_checks_source_coverage(tmp_path):
    from scripts.audit_polygon_preparation import audit_fragments

    fragments = gpd.GeoDataFrame(
        {"site_id": ["immutable", "immutable"], "sub_site_index": [0, 1]},
        geometry=[box(0, 0, 0.95, 1), box(0.95, 0, 1, 1)],
        crs=4326,
    )
    sources = gpd.GeoDataFrame(
        {"site_id": ["immutable"]}, geometry=[box(0, 0, 1, 1)], crs=4326
    )
    fragment_path, source_path = (
        tmp_path / "fragments.parquet",
        tmp_path / "sources.parquet",
    )
    fragments.to_parquet(fragment_path)
    sources.to_parquet(source_path)
    report = audit_fragments(fragment_path, source_path)
    assert report["source_coverage_checked"]
    assert report["fragments"] == 2
    fragments.iloc[:1].to_parquet(fragment_path)
    with pytest.raises(ValueError, match="Coverage differs"):
        audit_fragments(fragment_path, source_path)
