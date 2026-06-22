# Analysis Outputs

This document describes every file produced by the analysis pipeline,
including their S3 locations, file formats, and column schemas. It also
covers how results are imported back into the web application database.

## Contents

- [S3 Bucket Layout](#s3-bucket-layout)
- [Intermediate Files (Step 1 → Step 2)](#intermediate-files-step-1--step-2)
- [Match Files (Step 2 → Step 3)](#match-files-step-2--step-3)
- [Final Result Files (Step 3)](#final-result-files-step-3)
  - [results\_by\_site\_year.csv](#results_by_site_yearcsv)
  - [results\_by\_site\_total.csv](#results_by_site_totalcsv)
  - [results\_pixel\_year\_emissions.csv](#results_pixel_year_emissionscsv)
  - [results\_pixel\_covariates.csv](#results_pixel_covariatescsv)
  - [results\_covariate\_balance.csv](#results_covariate_balancecsv)
  - [results\_propensity\_scores.csv](#results_propensity_scorescsv)
  - [results\_match\_quality\_summary.json](#results_match_quality_summaryjson)
  - [results\_sampling\_by\_site.csv](#results_sampling_by_sitecsv)
  - [results\_pixel\_locations.csv](#results_pixel_locationscsv)
  - [results\_failed\_sites.csv](#results_failed_sitescsv)
- [Failure Markers](#failure-markers)
- [Biomass-to-CO₂e Conversion](#biomass-to-co2e-conversion)
- [Confidence Intervals](#confidence-intervals)
- [Database Import](#database-import)

---

## S3 Bucket Layout

All task-related data uses the `S3_BUCKET` and `S3_PREFIX` environment
variables as the root. The layout within that root is:

```
s3://{S3_BUCKET}/{S3_PREFIX}/
│
├── cog_1km/                        # Merged covariate COGs (1 km)
│   └── {covariate_name}.tif
│
├── cog_250m/                       # Merged covariate COGs (250 m)
│   └── {covariate_name}.tif
│
├── reference/                      # Reference layer parquets
│   └── {layer_name}.parquet
│
├── site-upload-stage/              # Staged file uploads (TTL 12 h)
│   ├── {token}.bin
│   └── {token}.json
│
├── sites/                          # Uploaded site sets per task
│   └── {task_id}/
│       └── sites.parquet
│
├── {task_id}/                      # Task working area
│   │
│   ├── params.json.gz              # Compressed task parameters
│   │
│   ├── intermediate/               # Step 1 → Step 2 hand-off
│   │   ├── sites_processed.parquet
│   │   ├── treatment_cell_key.parquet
│   │   ├── treatments_and_controls.parquet
│   │   ├── formula.json
│   │   ├── site_id_key.csv
│   │   ├── grid_metadata.json
│   │   └── matches/
│   │       ├── m_{id_numeric}[_rep{k}].rds
│   │       └── failed_{id_numeric}[_rep{k}].json
│   │
│   └── results/                    # Step 3 final outputs
│       ├── results_by_site_year.csv
│       ├── results_by_site_total.csv
│       ├── results_pixel_year_emissions.csv
│       ├── results_pixel_covariates.csv
│       ├── results_covariate_balance.csv
│       ├── results_propensity_scores.csv
│       ├── results_match_quality_summary.json
│       ├── results_sampling_by_site.csv
│       ├── results_pixel_locations.csv
│       └── results_failed_sites.csv
```

---

## Intermediate Files (Step 1 → Step 2)

These files are produced by `01_extract_covariates.py` and consumed by
`02_perform_matching.R`.

| File | Format | Description |
|---|---|---|
| `sites_processed.parquet` | GeoParquet | Site polygons with computed `area_ha`, `start_year`, `end_year`, `id_numeric` |
| `treatment_cell_key.parquet` | Parquet | Mapping from raster cell index → `site_id` + `id_numeric` for all treatment pixels |
| `treatments_and_controls.parquet` | Parquet | All treatment and control candidate pixels with covariate values; columns: `cell`, `site_id`, `id_numeric`, `treatment`, `area_ha`, all covariate columns, `fc_YYYY` columns |
| `formula.json` | JSON | Propensity score formula string (e.g. `"treatment ~ precip + temp + ..."`) |
| `site_id_key.csv` | CSV | Integer `id_numeric` ↔ string `site_id` mapping |
| `grid_metadata.json` | JSON | Grid origin, resolution, CRS — used to convert cell indices to lon/lat coordinates |

---

## Match Files (Step 2 → Step 3)

These files are produced by `02_perform_matching.R` (one per site per
replicate) and consumed by `03_summarize_results.R`.

### Per-site match file

**Pattern**: `matches/m_{id_numeric}[_rep{k}].rds`  
**Format**: R data frame serialised with `saveRDS()`

| Column | Type | Description |
|---|---|---|
| `cell` | integer | Raster cell index |
| `site_id` | character | Site identifier |
| `id_numeric` | integer | Integer site index |
| `area_ha` | numeric | Site area in hectares |
| `treatment` | logical | `TRUE` = treatment pixel; `FALSE` = control |
| `sampled_fraction` | numeric | Fraction of treatment pixels included (< 1 if subsampled) |
| `total_biomass_2025` | numeric | Total biomass (Mg/ha) — always extracted regardless of formula |
| `match_group` | integer | Matched set identifier; treatment and its controls share the same value |
| `match_weight` | numeric | Matching weight (controls sum to equal the number of matched treatment pixels) |
| `sampling_weight` | numeric | Inverse of `sampled_fraction`; scales sample estimates to full-site estimates |
| `fc_YYYY` | numeric | Forest cover fraction (0–1) for each analysis year |
| `pscore` | numeric | Fitted propensity score (`NA` if Mahalanobis distance was used) |
| `n_control_sampled` | integer | Number of control pixels sampled for this site |
| `n_control_pool` | integer | Total eligible control pixels before sampling |
| `sub_site_index` | integer | `0` for standard sites; `1+` when cross-site grouping splits a site |

---

## Final Result Files (Step 3)

All files are produced by `03_summarize_results.R` and written to
`{results_s3_uri}`.

---

### results\_by\_site\_year.csv

**Granularity**: One row per `(site_id, year)`  
**Includes**: Pre-intervention baseline years and all intervention years

| Column | Type | Description |
|---|---|---|
| `site_id` | string | Site identifier |
| `site_name` | string | Human-readable site name |
| `year` | integer | Calendar year |
| `is_pre_intervention` | boolean | `TRUE` if year is before `start_date` |
| `is_post_intervention` | boolean | `TRUE` if year is on or after `start_date` |
| `extrapolated_treatment_defor_ha` | numeric | Estimated treatment deforestation (ha) extrapolated to full site area |
| `extrapolated_control_defor_ha` | numeric | Estimated counterfactual deforestation (ha) extrapolated to full site area |
| `extrapolated_forest_loss_avoided_ha` | numeric | Avoided deforestation (ha) = control − treatment, extrapolated |
| `extrapolated_treatment_emissions_mgco2e` | numeric | Treatment emissions (MgCO₂e), extrapolated |
| `extrapolated_control_emissions_mgco2e` | numeric | Counterfactual emissions (MgCO₂e), extrapolated |
| `extrapolated_emissions_avoided_mgco2e` | numeric | **Primary result**: avoided emissions (MgCO₂e), extrapolated to full site |
| `sample_treatment_defor_ha` | numeric | Treatment deforestation (ha) from matched sample only (no extrapolation) |
| `sample_control_defor_ha` | numeric | Counterfactual deforestation (ha) from matched sample only |
| `sample_forest_loss_avoided_ha` | numeric | Avoided deforestation (ha) from matched sample only |
| `sample_treatment_emissions_mgco2e` | numeric | Treatment emissions (MgCO₂e), matched sample only |
| `sample_control_emissions_mgco2e` | numeric | Counterfactual emissions (MgCO₂e), matched sample only |
| `sample_emissions_avoided_mgco2e` | numeric | Avoided emissions (MgCO₂e), matched sample only |
| `*_ci_lower`, `*_ci_upper` | numeric | 2.5th / 97.5th percentile CI bounds (present only when `n_replicates > 1`) |

---

### results\_by\_site\_total.csv

**Granularity**: One row per `site_id`  
**Covers**: Intervention period only (`start_date` ≤ year ≤ `end_date`)

| Column | Type | Description |
|---|---|---|
| `site_id` | string | Site identifier |
| `site_name` | string | Human-readable site name |
| `area_ha` | numeric | Site area (ha) |
| `first_year` | integer | First year of intervention period |
| `last_year` | integer | Last year of intervention period |
| `n_years` | integer | Number of analysis years in intervention period |
| `extrapolated_forest_loss_avoided_ha` | numeric | Total avoided deforestation (ha) over intervention period, extrapolated |
| `extrapolated_emissions_avoided_mgco2e` | numeric | **Primary result**: total avoided emissions (MgCO₂e), extrapolated |
| `sample_forest_loss_avoided_ha` | numeric | Total avoided deforestation (ha), matched sample only |
| `sample_emissions_avoided_mgco2e` | numeric | Total avoided emissions (MgCO₂e), matched sample only |
| `n_sample_pixels` | integer | Total matched treatment pixels (summed across all years) |
| `sampled_fraction` | numeric | Pixel-count-weighted mean sampling fraction |
| `n_treatment_pixels` | integer | Total treatment pixels before subsampling |
| `*_ci_lower`, `*_ci_upper` | numeric | CI bounds for extrapolated metrics (when `n_replicates > 1`) |

---

### results\_pixel\_year\_emissions.csv

**Granularity**: One row per `(cell, year)` for matched pixels  
**Note**: Only replicate 1 is included when `n_replicates > 1`

| Column | Type | Description |
|---|---|---|
| `cell` | integer | Raster cell index |
| `site_id` | string | Site identifier |
| `year` | integer | Calendar year |
| `treatment` | boolean | `TRUE` = treatment pixel |
| `sampled_fraction` | numeric | Sampling fraction for this pixel's site |
| `sampling_weight` | numeric | Inverse of `sampled_fraction` |
| `match_group` | integer | Matched set identifier |
| `match_weight` | numeric | Matching weight |
| `forest_at_year_end` | numeric | Forest cover fraction at end of year |
| `forest_change_ha` | numeric | Annual forest area change (ha), negative = loss |
| `emissions_mgco2e` | numeric | Annual emissions (MgCO₂e) for this pixel |

---

### results\_pixel\_covariates.csv

**Granularity**: One row per matched pixel (all replicates, all sites)

| Column | Type | Description |
|---|---|---|
| `cell` | integer | Raster cell index |
| `site_id` | string | Site identifier |
| `treatment` | boolean | `TRUE` = treatment pixel |
| `match_group` | integer | Matched set identifier |
| `match_weight` | numeric | Matching weight |
| *covariate columns* | numeric | One column per covariate in the matching formula (e.g. `precip`, `temp`, …) |
| `defor_pre_intervention` | numeric | Pre-intervention deforestation rate (if included in formula) |

---

### results\_covariate\_balance.csv

**Granularity**: One row per `(site_id, covariate)`, plus an aggregate `"__all__"` row per covariate

| Column | Type | Description |
|---|---|---|
| `site_id` | string | Site identifier, or `"__all__"` for aggregate |
| `covariate` | string | Covariate name |
| `mean_treatment` | numeric | Weighted mean among treatment pixels |
| `mean_control` | numeric | Weighted mean among matched control pixels |
| `pooled_sd` | numeric | Pooled standard deviation (from unmatched distributions) |
| `smd` | numeric | Standardized Mean Difference = (mean_T − mean_C) / pooled_sd |

---

### results\_propensity\_scores.csv

**Granularity**: One row per matched pixel (treatment and control)

| Column | Type | Description |
|---|---|---|
| `cell` | integer | Raster cell index |
| `site_id` | string | Site identifier |
| `treatment` | boolean | `TRUE` = treatment pixel |
| `match_group` | integer | Matched set identifier |
| `match_weight` | numeric | Matching weight |
| `pscore` | numeric | Fitted propensity score; `NA` if Mahalanobis distance matching was used |

---

### results\_match\_quality\_summary.json

**Format**: Nested JSON (pre-aggregated for web UI rendering)

```json
{
  "summary_stats": {
    "__all__": {
      "n_treatment": 1234,
      "n_control": 1234,
      "n_sites": 10
    },
    "{site_id}": { "n_treatment": 98, "n_control": 98 }
  },
  "histograms": {
    "__all__": {
      "{covariate}": {
        "bin_edges": [0.0, 0.1, ...],
        "treatment_pct": [0.05, 0.12, ...],
        "control_pct": [0.06, 0.11, ...]
      }
    },
    "{site_id}": { ... }
  },
  "qq_quantiles": {
    "__all__": {
      "quantiles": [0.01, 0.05, ...],
      "treatment_values": [0.12, 0.18, ...],
      "control_values": [0.11, 0.17, ...]
    },
    "{site_id}": { ... }
  },
  "covariate_cols": ["precip", "temp", ...]
}
```

This compact summary is loaded by the web app to render the Love plot and
QQ plot without downloading the full covariate or propensity score CSV files.

---

### results\_sampling\_by\_site.csv

**Granularity**: One row per site

| Column | Type | Description |
|---|---|---|
| `id_numeric` | integer | Integer site index |
| `site_id` | string | Site identifier |
| `sampled_fraction` | numeric | Fraction of treatment pixels used (1.0 if no subsampling) |
| `sampled_percent` | numeric | `sampled_fraction × 100` |
| `was_subsampled` | boolean | `TRUE` if treatment was subsampled |

---

### results\_pixel\_locations.csv

**Granularity**: One row per matched pixel

| Column | Type | Description |
|---|---|---|
| `cell` | integer | Raster cell index |
| `site_id` | string | Site identifier |
| `treatment` | boolean | `TRUE` = treatment pixel |
| `lon` | numeric | Pixel centroid longitude (EPSG:4326) |
| `lat` | numeric | Pixel centroid latitude (EPSG:4326) |

Used to plot treatment (green) and control (blue) pixel locations on the
Task Detail map.

---

### results\_failed\_sites.csv

**Granularity**: One row per site that failed during Step 2 matching

| Column | Type | Description |
|---|---|---|
| `id_numeric` | integer | Integer site index |
| `site_id` | string | Site identifier |
| `site_name` | string | Human-readable site name |
| `error` | string | Error message from the R subprocess or failure marker |
| `timestamp` | string | ISO 8601 timestamp of failure |
| `array_index` | integer | AWS Batch array job index |
| `failure_marker_file` | string | S3 key of the `failed_*.json` marker |

---

## Failure Markers

When an individual site's matching step crashes (out-of-memory, R error,
timeout), the batch array job writes a JSON failure marker to
`intermediate/matches/`:

```json
{
  "array_index": 42,
  "id_numeric": 7,
  "site_id": "SITE_ABC",
  "error": "Error in optmatch: ...",
  "timestamp": "2025-06-01T14:32:11Z",
  "replicate": 0
}
```

The summarisation step collects these markers and writes them into
`results_failed_sites.csv`. Failed sites are excluded from the aggregated
results but are surfaced as warnings in the web UI.

---

## Biomass-to-CO₂e Conversion

Annual emissions for each matched pixel are computed in
`03_summarize_results.R` as follows:

$$\text{biomass}_{t} = \text{total\_biomass\_2025} \times \text{fc}_{t}$$

$$\Delta \text{biomass}_{t} = \text{biomass}_{t-1} - \text{biomass}_{t}$$

$$\text{emissions}_t = \Delta \text{biomass}_{t} \times 0.5 \times 3.67$$

where:

- $\text{total\_biomass\_2025}$ is total live biomass (Mg/ha) at the
  reference year 2025, loss-masked so pixels that were deforested before
  2025 carry their biomass at the time of loss.
- $\text{fc}_t$ is the Hansen GFC forest cover fraction (0–1) at year $t$.
- **0.5** is the biomass-to-carbon conversion factor (carbon is ~50 % of
  dry biomass by mass).
- **3.67** is the carbon-to-CO₂ molecular weight ratio ($44 / 12$).
- Positive $\Delta \text{biomass}$ (loss) produces positive emissions.

Avoided emissions for a site-year is the difference between the counterfactual
(control) emissions and the observed (treatment) emissions:

$$\text{avoided\_emissions}_{s,t} = \text{emissions}_{C,s,t} - \text{emissions}_{T,s,t}$$

Extrapolated site-level values scale the sample estimate by the inverse of
`sampled_fraction` to account for treatment subsampling:

$$\text{extrapolated\_avoided} = \text{sample\_avoided} \times \frac{1}{\text{sampled\_fraction}}$$

---

## Confidence Intervals

When `n_replicates > 1`, the summarisation step computes per-metric
confidence intervals across the replicate distribution:

$$\text{CI lower} = \text{quantile}_{0.025}(\text{metric across replicates})$$

$$\text{CI upper} = \text{quantile}_{0.975}(\text{metric across replicates})$$

The 95 % CI columns are appended with `_ci_lower` and `_ci_upper` suffixes
to the corresponding point-estimate column names. When `n_replicates = 1`
these columns are absent from the output files.

---

## Database Import

When the Celery task detects that an execution has succeeded it downloads
the results from S3 and imports them into two database tables:

| CSV file | Database table | Notes |
|---|---|---|
| `results_by_site_year.csv` | `task_result` | One row per (site, year); primary source for time series charts |
| `results_by_site_total.csv` | `task_result_total` | One row per site; used for summary tables and downloads |

Columns from the CSVs are mapped directly to ORM model fields in
`webapp/models/`. Rows are inserted in batches of ~1,000 to avoid
PostgreSQL parameter limits.

After import the `AnalysisTask.status` is set to `"succeeded"` and
`AnalysisTask.completed_at` is recorded. The full result CSVs remain
accessible on S3 for the lifetime of the task and are served as pre-signed
download links from the Task Detail page.
