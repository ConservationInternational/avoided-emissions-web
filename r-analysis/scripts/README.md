# Avoided Emissions R Analysis Scripts

## Overview

These R scripts implement the avoided emissions propensity score matching
analysis. The pipeline has three main steps:

1. **Extract covariates** - Load covariate rasters (from GCS COGs) and extract
   pixel values for treatment sites and control regions.
2. **Perform matching** - Run propensity score matching to pair treatment and
   control pixels with similar characteristics.
3. **Summarize results** - Compute avoided emissions (MgCO2e) for each site
   by comparing forest loss between matched treatment and control pixels.

## AWS Batch Integration

The container is designed to run on AWS Batch. For multi-site analyses:

- **Step 1 (extract)**: Runs as a single job, extracting covariates for all
  sites and their control regions.
- **Step 2 (match)**: Runs as an array job on AWS Batch, with each array
  element processing one site in parallel.
- **Step 3 (summarize)**: Runs as a single job after all matching completes,
  aggregating per-site results.

## Configuration

All scripts read a JSON configuration file specifying:

```json
{
    "task_id": "uuid-string",
    "data_dir": "/data",
    "gcs_bucket": "my-bucket",
    "gcs_prefix": "avoided-emissions/covariates",
    "sites_file": "/data/input/sites.gpkg",
    "covariates": [
        "lc_2015_agriculture", "precip", "temp", "elev", "slope",
        "dist_cities", "dist_roads", "crop_suitability",
        "pop_2015", "pop_growth", "total_biomass"
    ],
    "exact_match_vars": ["region", "ecoregion", "pa"],
    "fc_years": [2000, 2001, "...", 2023],
    "max_treatment_pixels": 1000,
    "control_multiplier": 50,
    "min_site_area_ha": 100,
    "min_glm_treatment_pixels": 15
}
```
