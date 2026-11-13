"""Extraction regressions; run with the analysis image's Python dependencies."""

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import rasterio
import xarray as xr
from rasterio.transform import from_origin
from rasterio.windows import Window
from shapely.geometry import Point, box, mapping

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))
spec = importlib.util.spec_from_file_location(
    "extract_covariates", SCRIPTS / "01_extract_covariates.py"
)
extract = importlib.util.module_from_spec(spec)
spec.loader.exec_module(extract)


class ExtractionTests(unittest.TestCase):
    def test_sparse_global_extent_never_loads_a_global_raster(self):
        with tempfile.TemporaryDirectory() as output:
            root = Path(output)
            path = root / "global.tif"
            width, height = 146100, 32846
            resolution = 1 / 480
            transform = from_origin(-160.35, 24.26, resolution, resolution)
            with rasterio.open(
                path,
                "w",
                driver="GTiff",
                width=width,
                height=height,
                count=1,
                dtype="float32",
                crs=4326,
                transform=transform,
                tiled=True,
                blockxsize=256,
                blockysize=256,
                SPARSE_OK=True,
                BIGTIFF="YES",
            ) as dst:
                for col, row in [(2, 2), (width - 4, height - 4)]:
                    dst.write(
                        np.ones((2, 2), dtype="float32"),
                        1,
                        window=Window(col, row, 2, 2),
                    )
            geometries = []
            expected_cells = []
            for col, row in [(2, 2), (width - 4, height - 4)]:
                west, north = transform * (col, row)
                east, south = transform * (col + 2, row + 2)
                geometries.append(box(west, south, east, north))
                expected_cells.extend(
                    (row + r) * width + col + c for r in range(2) for c in range(2)
                )
            sites = gpd.GeoDataFrame(
                {"site_id": ["A", "B"], "id_numeric": [1, 2]},
                geometry=geometries,
                crs=4326,
            )
            destination = root / "output"
            destination.mkdir()
            config = {
                "output_dir": str(destination),
                "cog_bucket": "test",
                "cog_prefix": "test",
                "covariates": [],
                "fc_years": [2014],
                "exact_match_vars": [],
                "resolution_m": 250,
                "matching_extent": mapping(geometries[0].union(geometries[1])),
            }
            original_open = extract._open_single_cog
            original_load = extract._load_layer_fresh

            def load(uri, name, bounds, grid, rows, cols):
                self.assertLessEqual(max(grid[1]), 1024)
                return original_load(uri, name, bounds, grid, rows, cols)

            with (
                patch.object(
                    extract,
                    "_open_single_cog",
                    side_effect=lambda uri, name: original_open(str(path), name),
                ),
                patch.object(extract, "_load_layer_fresh", side_effect=load) as reader,
            ):
                extract.extract_covariates(config, sites)
            pixels = pd.read_parquet(destination / "treatment_pixels.parquet")
            self.assertEqual(sorted(pixels.cell), sorted(expected_cells))
            self.assertEqual(reader.call_count, 6)  # 3 layers, 2 occupied tiles
            with open(destination / "grid_metadata.json") as source:
                metadata = json.load(source)
            self.assertEqual((metadata["width"], metadata["height"]), (width, height))

    def test_tiled_extraction_preserves_pixels_memberships_and_global_grid(self):
        with tempfile.TemporaryDirectory() as output:
            root = Path(output)
            resolution = 1 / 480
            transform = from_origin(-160.35, 24.26, resolution, resolution)
            height, width = 5, 7
            cells = np.arange(height * width).reshape(height, width)
            layers = {
                "temp": cells.astype("float32"),
                "fc_2000": np.where(cells % 3 == 0, 0, 100).astype("float32"),
                "fc_2014": np.where(cells % 3 == 0, 0, 80).astype("float32"),
                "fc_2015": np.where(cells % 5 == 0, 0, 60).astype("float32"),
                "total_biomass_2000": (cells + 20).astype("float32"),
                "admin1": np.where(cells % 4 == 0, -9999, cells % 2).astype("float32"),
            }
            for name, values in layers.items():
                with rasterio.open(
                    root / f"{name}.tif",
                    "w",
                    driver="GTiff",
                    width=width,
                    height=height,
                    count=1,
                    dtype="float32",
                    crs="EPSG:4326",
                    transform=transform,
                    nodata=-9999,
                ) as dst:
                    dst.write(values, 1)

            def rectangle(left, top, right, bottom):
                west, north = transform * (left, top)
                east, south = transform * (right, bottom)
                return box(west, south, east, north)

            sites = gpd.GeoDataFrame(
                {
                    "site_id": ["A", "B", "C"],
                    "id_numeric": [1, 2, 3],
                    "sub_site_index": [0, 0, 0],
                },
                geometry=[
                    rectangle(0, 0, 3, 5),
                    rectangle(6, 0, 7, 5),
                    rectangle(1, 0, 4, 3),
                ],
                crs=4326,
            )
            config = {
                "cog_bucket": "test",
                "cog_prefix": "test",
                "covariates": ["temp"],
                "fc_years": [2014, 2015],
                "exact_match_vars": ["admin1"],
                "resolution_m": 250,
                "matching_extent": mapping(rectangle(0, 0, 7, 5)),
                "sites_exclusion_buffer": mapping(rectangle(4, 0, 5, 2)),
                "exact_match_group_mapping": {"1": [["A", 0], ["C", 0]]},
            }
            original_open = extract._open_single_cog
            original_load = extract._load_layer_fresh
            for tile_size in (1024, 2):
                destination = root / str(tile_size)
                destination.mkdir()

                def load(uri, name, bounds, grid, rows, cols, tile_size=tile_size):
                    self.assertLessEqual(max(grid[1]), tile_size)
                    return original_load(uri, name, bounds, grid, rows, cols)

                with (
                    patch.object(extract, "_EXTRACTION_TILE_SIZE", tile_size),
                    patch.object(
                        extract,
                        "_open_single_cog",
                        side_effect=lambda uri, name: original_open(
                            str(root / f"{name}.tif"), name
                        ),
                    ),
                    patch.object(extract, "_load_layer_fresh", side_effect=load),
                ):
                    extract.extract_covariates(
                        {**config, "output_dir": str(destination)}, sites
                    )
            for filename, order in [
                ("treatment_pixels.parquet", ["cell"]),
                ("treatment_cell_key.parquet", ["cell", "id_numeric"]),
                ("control_pixels", ["cell"]),
            ]:
                frames = []
                for tile_size in (1024, 2):
                    frame = pd.read_parquet(root / str(tile_size) / filename)
                    if "admin1" in frame:
                        frame["admin1"] = frame.admin1.astype("int32")
                    frames.append(frame.sort_values(order).reset_index(drop=True))
                pd.testing.assert_frame_equal(*frames)
            with (
                open(root / "1024" / "grid_metadata.json") as single,
                open(root / "2" / "grid_metadata.json") as tiled,
            ):
                self.assertEqual(json.load(single), json.load(tiled))

    def test_prepared_high_latitude_area_is_preserved_and_validated(self):
        source = box(0, 70, 12, 72)
        sites = gpd.GeoDataFrame(
            {
                "site_id": ["immutable-a"],
                "display_site_id": ["original label"],
                "site_name": ["A prepared site"],
                "start_date": ["2000-01-01"],
                "area_ha": [9682188.444773627],
            },
            geometry=[source],
            crs=4326,
        )
        with tempfile.TemporaryDirectory() as output:
            path = str(Path(output) / "prepared.parquet")
            sites.to_parquet(path)
            loaded = extract.load_sites(path)
            self.assertAlmostEqual(
                loaded.area_ha.iloc[0], sites.area_ha.iloc[0], places=6
            )
            self.assertTrue(loaded.geometry.iloc[0].equals(source))
            self.assertEqual(loaded.display_site_id.iloc[0], "original label")
            sites["area_ha"] = 1.0
            sites.to_parquet(path)
            with self.assertRaisesRegex(ValueError, "areas disagree"):
                extract.load_sites(path)

    def test_overlapping_features_keep_independent_pixel_membership(self):
        sites = gpd.GeoDataFrame(
            {
                "site_id": ["immutable-a", "immutable-b"],
                "display_site_id": ["duplicate", "duplicate"],
                "id_numeric": [1, 2],
            },
            geometry=[box(0, 0, 2, 2), box(1, 0, 3, 2)],
            crs=4326,
        )
        memberships = extract._treatment_memberships(
            sites, from_origin(0, 2, 1, 1), 3, 2, np.array([1.5, 0.5]), 1, 1
        )
        self.assertEqual(len(memberships), 8)
        self.assertEqual(memberships.groupby("id_numeric").size().tolist(), [4, 4])
        self.assertEqual(memberships.cell.nunique(), 6)
        self.assertFalse(memberships.duplicated(["cell", "id_numeric"]).any())

    def test_gdal_settings_apply_to_rasterio_and_worker_threads(self):
        from rasterio.env import get_gdal_config

        with patch.dict(os.environ), extract._configure_gdal():
            self.assertEqual(get_gdal_config("GDAL_CACHEMAX"), 1024**3)
            self.assertEqual(get_gdal_config("GDAL_HTTP_MAX_RETRY"), 5)
            with ThreadPoolExecutor(max_workers=1) as pool:
                self.assertEqual(
                    pool.submit(get_gdal_config, "GDAL_HTTP_MAX_RETRY").result(),
                    5,
                )

    def test_transient_raster_errors_retry_without_osgeo(self):
        with (
            patch.object(
                extract,
                "_open_single_cog",
                side_effect=[
                    rasterio.errors.RasterioIOError("HTTP response_code=503"),
                    rasterio.errors.RasterioIOError("not a supported file format"),
                ],
            ) as open_cog,
            patch.object(extract.time, "sleep") as sleep,
        ):
            with self.assertRaisesRegex(
                rasterio.errors.RasterioIOError, "not a supported file format"
            ):
                extract._load_layer_fresh(
                    "test.tif", "test", (0, 0, 1, 1), (), np.array([0]), np.array([0])
                )
            self.assertEqual(open_cog.call_count, 2)
            sleep.assert_called_once()

    def test_nonzero_nodata_is_missing_without_losing_valid_zeros(self):
        with tempfile.TemporaryDirectory() as output:
            path = Path(output) / "biomass.tif"
            transform = from_origin(0, 1, 0.1, 0.1)
            with rasterio.open(
                path,
                "w",
                driver="GTiff",
                width=3,
                height=3,
                count=1,
                dtype="float32",
                crs="EPSG:4326",
                transform=transform,
                nodata=-9999,
            ) as dst:
                dst.write(
                    np.array([[-9999, 0, 1], [1, 1, 1], [1, 1, 1]], dtype=np.float32), 1
                )
            values = extract._load_layer_fresh(
                str(path),
                "total_biomass_2000",
                (0, 0.7, 0.3, 1),
                (transform, (3, 3)),
                np.array([0, 0, 0]),
                np.array([0, 1, 2]),
            )
            self.assertTrue(np.isnan(values[0]))
            self.assertEqual(values[1:].tolist(), [0, 1])

    def test_empty_covariates_use_an_intercept_only_formula(self):
        self.assertEqual(
            extract.build_matching_formula([])["formula_str"], "treatment ~ 1"
        )

    def test_buffer_distance_is_consistent_at_different_latitudes(self):
        from pyproj import Geod

        geod = Geod(ellps="WGS84")
        for lat in (0, 60, -60, 80):
            sites = gpd.GeoDataFrame(geometry=[Point(10, lat)], crs="EPSG:4326")
            buffer = extract.shape(extract.compute_sites_exclusion_buffer(sites, 10))
            for azimuth in (0, 90, 180, 270):
                inside = Point(*geod.fwd(10, lat, azimuth, 9800)[:2])
                outside = Point(*geod.fwd(10, lat, azimuth, 10200)[:2])
                self.assertTrue(buffer.covers(inside), (lat, azimuth, "inside"))
                self.assertFalse(buffer.covers(outside), (lat, azimuth, "outside"))

    def test_distant_sites_do_not_exclude_the_intervening_land(self):
        sites = gpd.GeoDataFrame(
            geometry=[Point(-10, 0), Point(10, 0)], crs="EPSG:4326"
        )
        buffer = extract.shape(extract.compute_sites_exclusion_buffer(sites, 10))
        self.assertFalse(buffer.covers(Point(0, 0)))

    def test_buffers_crossing_the_antimeridian_stay_local(self):
        from pyproj import Geod

        geod = Geod(ellps="WGS84")
        sites = gpd.GeoDataFrame(
            geometry=[
                box(-179.98, -24.45, -179.95, -24.40),
                box(179.95, -16.80, 179.98, -16.75),
                Point(10, 0),
            ],
            crs="EPSG:4326",
        )
        buffer = extract.shape(extract.compute_sites_exclusion_buffer(sites, 10))
        self.assertTrue(buffer.is_valid)
        self.assertGreaterEqual(buffer.bounds[0], -180)
        self.assertLessEqual(buffer.bounds[2], 180)
        # Each buffer is about 0.03 x 0.05 deg plus 10 km on each side.
        self.assertLess(buffer.area, 3 * 0.1)
        for lon, lat in ((-179.95, -24.42), (179.95, -16.77)):
            across = Point(*geod.fwd(lon, lat, 90 if lon > 0 else 270, 5000)[:2])
            self.assertTrue(buffer.covers(across), (lon, lat))
            self.assertFalse(buffer.covers(Point(0, lat)), (lon, lat))
        self.assertFalse(buffer.covers(Point(90, -20)))

    def test_buffer_width_guard_rejects_wrapped_buffers(self):
        with self.assertRaisesRegex(ValueError, "wrap around the globe"):
            extract._check_buffer_width(
                box(-180, -25, 180, -24), box(179.9, -24.5, 179.95, -24.4), 10
            )

    def test_unmapped_fragments_are_neither_treatment_nor_controls(self):
        with tempfile.TemporaryDirectory() as output:
            transform = from_origin(0, 0.025, 1 / 120, 1 / 120)
            ref = (
                xr.DataArray(
                    np.ones((3, 3)),
                    dims=("y", "x"),
                    coords={
                        "x": (np.arange(3) + 0.5) / 120,
                        "y": 0.025 - (np.arange(3) + 0.5) / 120,
                    },
                )
                .rio.write_crs("EPSG:4326")
                .rio.write_transform(transform)
            )
            sites = gpd.GeoDataFrame(
                {
                    "site_id": ["A", "B"],
                    "sub_site_index": [0, 0],
                    "id_numeric": [1, 2],
                },
                geometry=[
                    box(0, 0, 1 / 120, 0.025),
                    box(2 / 120, 0, 0.025, 0.025),
                ],
                crs="EPSG:4326",
            )
            config = {
                "output_dir": output,
                "cog_bucket": "test",
                "cog_prefix": "test",
                "covariates": [],
                "fc_years": [2014, 2015],
                "exact_match_vars": ["admin1"],
                "resolution_m": 1000,
                "matching_extent": mapping(box(0, 0, 0.025, 0.025)),
                "exact_match_group_mapping": {"1": [["A", 0]]},
            }

            def layer(uri, name, bounds, grid, rows, cols):
                if name.startswith("fc_"):
                    # The top-left treatment pixel never has forest.
                    return np.where((rows == 0) & (cols == 0), 0, 100).astype(
                        np.float32
                    )
                return np.ones(len(rows), dtype=np.float32)

            with (
                patch.object(extract, "open_reference_grid", return_value=ref),
                patch.object(extract, "_load_layer_fresh", side_effect=layer),
            ):
                extract.extract_covariates(config, sites)
            treatment = pd.read_parquet(Path(output) / "treatment_pixels.parquet")
            controls = pq.ParquetDataset(Path(output) / "control_pixels").read()
            key = pd.read_parquet(Path(output) / "treatment_cell_key.parquet")
            self.assertEqual(sorted(treatment.cell), [3, 6])
            self.assertEqual(sorted(controls.column("cell").to_pylist()), [1, 4, 7])
            self.assertEqual(key.groupby("id_numeric").size().to_dict(), {1: 3, 2: 3})

    def test_matching_extent_uses_used_regions_and_neighbours(self):
        regions = gpd.GeoDataFrame(
            {"region_id": ["a", "b", "c"]},
            geometry=[box(0, 0, 1, 1), box(1, 0, 2, 1), box(5, 0, 6, 1)],
            crs="EPSG:4326",
        )
        sites = gpd.GeoDataFrame(
            {"admin1": ["a", None]},
            geometry=[box(0.2, 0.2, 0.4, 0.4), box(5.2, 0.2, 5.4, 0.4)],
            crs="EPSG:4326",
        )
        with patch.object(extract, "_load_reference_layer", return_value=regions):
            extent = extract.shape(
                extract.compute_matching_extent(
                    sites, ["admin1"], {"admin1": "s3://x/admin1.parquet"}, "/tmp"
                )
            )
            self.assertAlmostEqual(extent.area, 2.0)
            self.assertFalse(extent.covers(Point(5.5, 0.5)))
            # Without region IDs, every region in the sites bbox is used.
            bbox_extent = extract.shape(
                extract.compute_matching_extent(
                    sites.drop(columns="admin1"),
                    ["admin1"],
                    {"admin1": "s3://x/admin1.parquet"},
                    "/tmp",
                )
            )
            self.assertAlmostEqual(bbox_extent.area, 3.0)

    def test_exclusion_preserves_treatment_and_empty_control_schema(self):
        with tempfile.TemporaryDirectory() as output:
            transform = from_origin(0, 0.025, 1 / 120, 1 / 120)
            ref = (
                xr.DataArray(
                    np.ones((3, 3)),
                    dims=("y", "x"),
                    coords={
                        "x": (np.arange(3) + 0.5) / 120,
                        "y": 0.025 - (np.arange(3) + 0.5) / 120,
                    },
                )
                .rio.write_crs("EPSG:4326")
                .rio.write_transform(transform)
            )
            sites = gpd.GeoDataFrame(
                {"site_id": ["A"], "id_numeric": [1]},
                geometry=[box(0, 0, 1 / 120, 0.025)],
                crs="EPSG:4326",
            )
            config = {
                "output_dir": output,
                "cog_bucket": "test",
                "cog_prefix": "test",
                "covariates": [],
                "fc_years": [2014, 2015],
                "exact_match_vars": ["admin1"],
                "resolution_m": 1000,
                "matching_extent": mapping(box(0, 0, 0.025, 0.025)),
                "sites_exclusion_buffer": mapping(box(-1, -1, 1, 1)),
            }
            with (
                patch.object(extract, "open_reference_grid", return_value=ref),
                patch.object(
                    extract,
                    "_load_layer_fresh",
                    side_effect=lambda uri, name, bounds, grid, rows, cols: np.full(
                        len(rows),
                        100 if name.startswith("fc_") else 1,
                        dtype=np.float32,
                    ),
                ),
            ):
                extract.extract_covariates(config, sites)
                empty_output = Path(output) / "empty"
                empty_output.mkdir()
                sites.geometry = [box(0.1, 0.1, 0.11, 0.11)]
                extract.extract_covariates(
                    {**config, "output_dir": str(empty_output)}, sites
                )
            treatment = pd.read_parquet(Path(output) / "treatment_pixels.parquet")
            controls = pq.ParquetDataset(Path(output) / "control_pixels").read()
            self.assertEqual(len(treatment), 3)
            self.assertEqual(controls.num_rows, 0)
            self.assertIn("admin1", controls.column_names)
            empty_treatment = pd.read_parquet(empty_output / "treatment_pixels.parquet")
            empty_controls = pq.ParquetDataset(empty_output / "control_pixels").read()
            self.assertEqual(len(empty_treatment), 0)
            self.assertEqual(empty_controls.num_rows, 0)
            self.assertIn("total_biomass_2000", empty_treatment)


if __name__ == "__main__":
    unittest.main()
