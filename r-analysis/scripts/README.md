# Avoided Emissions R Analysis Scripts

## Overview

These R scripts implement the avoided emissions propensity score matching
analysis. The pipeline has three main steps:

1. **Extract covariates** (`01_extract_covariates.py`) - Load covariate rasters
   (from COGs on S3/GCS-accessible storage) and extract pixel values for
   treatment sites and control regions.
2. **Perform matching** (`02_perform_matching.R`) - Run propensity score
   matching to pair treatment and control pixels with similar characteristics.
3. **Summarize results** (`03_summarize_results.R`) - Compute avoided emissions (MgCO2e) for each site
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
   "cog_bucket": "my-cog-bucket",
   "cog_prefix": "avoided-emissions/covariates",
    "sites_file": "/data/input/sites.gpkg",
    "covariates": [
        "precip", "temp", "elev", "slope",
      "dist_cities", "friction_surface",
        "pop_2015", "pop_growth", "total_biomass"
    ],
   "exact_match_vars": ["admin0", "admin1", "admin2", "ecoregion", "pa"],
    "matching_extent": {"type": "Polygon", "coordinates": [[[...]]]},
   "fc_years": [2000, 2001, "...", 2023],
    "max_treatment_pixels": 1000,
    "control_multiplier": 50,
    "min_site_area_ha": 100,
    "min_glm_treatment_pixels": 15
}
```

## Matching Methodology (`02_perform_matching.R`)

### Algorithm

Matching uses the [MatchIt](https://cran.r-project.org/package=MatchIt) package
with 1:1 nearest-neighbour matching (`replace = FALSE`). Exact matching is
enforced on the variables listed in `exact_match_vars` (typically
`admin0 + ecoregion + pa`), meaning treatment and control pixels must share the
same value for every exact-match variable before distance is considered.

### Distance metric selection

For each site the script selects one of three distance metrics in order of
preference:

| Priority | Condition | Distance metric used |
|----------|-----------|----------------------|
| 1 | Fewer than `min_glm_treatment_pixels` treatment pixels **and** no pre-computed group scores available | Mahalanobis (no caliper) |
| 2 | `separation_fallback_mahalanobis = true` **and** quasi-complete separation detected in the site's data | Mahalanobis (no caliper) |
| 3 | T:C ratio > `IMBALANCE_RATIO_THRESHOLD` (= 2.0) within the site's exact-match stratum | Mahalanobis (no caliper) |
| 4 | Pre-computed group scores available (conditions 2–3 not triggered) | Propensity score — pre-computed from the group GLM |
| 5 | Otherwise | Propensity score — per-site logistic GLM |

Propensity-score matching uses a caliper of `caliper_width` standard deviations
(default 0.75). Mahalanobis matching uses no caliper so that a match is always
attempted when the other methods fail.

**Important:** Conditions 2 and 3 always override pre-computed group scores.
Group GLM scores can become degenerate when the combined treatment from many
co-batched sites creates severe class imbalance within a stratum, or when the
per-site data is perfectly separated — in both cases the pre-computed scores are
discarded and Mahalanobis distance is used instead.

### Group cache and shared GLM (`group_by_exact_matches`)

When `group_by_exact_matches = true` (the default) and the job is processing
more than one site, sites that share the same exact-match stratum (same
`admin0 + ecoregion + pa`) are batched together. A single group-level logistic
GLM is fitted on the pooled treatment and control pixels for the group, and the
resulting propensity scores are reused for every site in the group. This avoids
fitting N separate GLMs and reduces Arrow I/O and spatial distance filtering to
one operation per group instead of one per site.

The group GLM is skipped (each site falls back to its own per-site GLM or
Mahalanobis) when:

- The group has fewer than `min_glm_treatment_pixels` treatment pixels.
- Quasi-complete separation is detected in the pooled group data.
- The group GLM throws an error.

### Quasi-complete separation detection (`separation_fallback_mahalanobis`)

Before fitting any propensity model the script calls `check_separation()`, which
checks for:

- **Factor variables**: any level present exclusively in treatment or control.
- **Numeric variables**: non-overlapping ranges between treatment and control.

If separation is found and `separation_fallback_mahalanobis = true`, Mahalanobis
distance is used and the offending variables are dropped from the distance
formula (so they do not dominate the metric). If
`separation_fallback_mahalanobis = false` the site is marked as failed.

### T:C imbalance fallback (`IMBALANCE_RATIO_THRESHOLD`)

When the number of treatment pixels in a site's exact-match stratum exceeds the
number of control pixels by a factor of more than `IMBALANCE_RATIO_THRESHOLD`
(hard-coded to 2.0), the propensity-score approach is abandoned regardless of
whether pre-computed group scores are available. The caliper on a propensity
model fitted under severe class imbalance compresses treatment scores toward 1
and control scores toward 0, leaving no overlap for the caliper to match.
Mahalanobis distance has no caliper and is therefore robust to this condition.

## Current Step Outputs

### Step 1 (`01_extract_covariates.py`)

- `sites_processed.parquet`
- `treatment_cell_key.parquet`
- `treatment_pixels.parquet` — covariates for every pixel inside a treatment site
- `control_pixels/` — hive-partitioned Arrow dataset of all candidate control
  pixels (partitioned by the primary exact-match variable so match workers can
  stream only the relevant partitions directly from S3)
- `formula.json`
- `site_id_key.csv`
- `grid_metadata.json`

### Step 2 (`02_perform_matching.R`)

- `matches/m_<id_numeric>.rds` for successful site matches
- `matches/failed_<id_numeric>.json` failure marker when a site cannot be matched

### Step 3 (`03_summarize_results.R`)

- `results_by_site_year.csv`
- `results_by_site_total.csv`
- `results_pixel_year_emissions.csv`
- `results_summary.json`
- `results_sampling_by_site.csv`
- `results_failed_sites.csv`
- `results_pixel_covariates.csv`
- `results_covariate_balance.csv`
- `results_propensity_scores.csv`
- `results_pixel_locations.csv`
- `results_match_quality_summary.json`
