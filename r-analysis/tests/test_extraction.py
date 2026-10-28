"""Extraction regressions; run with the analysis image's Python dependencies."""

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import rasterio
import xarray as xr
from rasterio.transform import from_origin
from shapely.geometry import Point, box, mapping

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))
spec = importlib.util.spec_from_file_location(
    "extract_covariates", SCRIPTS / "01_extract_covariates.py"
)
extract = importlib.util.module_from_spec(spec)
spec.loader.exec_module(extract)


class ExtractionTests(unittest.TestCase):
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
