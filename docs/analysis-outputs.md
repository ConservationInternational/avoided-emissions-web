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
- [Replicate Ranges](#replicate-ranges)
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
│   │   ├── treatment_pixels.parquet
│   │   ├── control_pixels/          # hive-partitioned by first exact-match var
│   │   ├── formula.json
│   │   ├── site_id_key.csv
│   │   ├── grid_metadata.json
│   │   ├── prep_summary.json
│   │   └── matches/
│   │       ├── m_{id_numeric}_rep{k}.rds
│   │       ├── m_{id_numeric}_na_exclusion.json
│   │       ├── failed_{id_numeric}_rep{k}.json
│   │       └── failed_array_{index}.json
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
| `sites_processed.parquet` | Parquet | One row per site fragment (sites split across exact-match regions have several), with `site_id`, `sub_site_index`, `id_numeric`, `start_year`, `end_year`, `area_ha` |
| `treatment_cell_key.parquet` | Parquet | Raster cell index → `id_numeric` for every treatment pixel |
| `treatment_pixels.parquet` | Parquet | Covariate, `area_ha` (cell area), `total_biomass_2000`, `fc_2000`, and outcome-window `fc_YYYY` values for treatment pixels |
| `control_pixels/` | Parquet dataset | The same columns for candidate control pixels, partitioned by the first exact-match variable. A missing partition value is stored as `-1` |
| `formula.json` | JSON | Propensity score formula (e.g. `"treatment ~ precip + temp + ..."`) |
| `site_id_key.csv` | CSV | Integer `id_numeric` ↔ string `site_id` mapping |
| `grid_metadata.json` | JSON | Grid transform and size, used to convert cell indices to lon/lat |
| `prep_summary.json` | JSON | `matching_extent` and `sites_exclusion_buffer` |

Every covariate layer must be on the same pixel grid; extraction fails if a
layer's transform or shape differs from the reference layer.

---

## Match Files (Step 2 → Step 3)

These files are produced by `02_perform_matching.R` (one per site fragment
and replicate) and consumed by `03_summarize_results.R`.

### Per-fragment match file

**Pattern**: `matches/m_{id_numeric}_rep{k}.rds`  
**Format**: R data frame serialised with `saveRDS()`

| Column | Type | Description |
|---|---|---|
| `cell` | integer | Raster cell index |
| `site_id`, `id_numeric` | character, integer | Site and fragment identifiers |
| `replicate` | integer | Replicate number |
| `treatment` | logical | `TRUE` = treatment pixel; `FALSE` = control |
| `group` | character | Exact-match stratum |
| `match_group` | character | Matched set identifier, prefixed with the stratum |
| `match_weight` | numeric | 1 for treatment; controls in a set sum to the number of treated pixels in it |
| `sampling_weight` | numeric | `n_eligible / n_matched` for the pixel's stratum |
| `pscore` | numeric | Fitted propensity score (`NA` for Mahalanobis matching) |
| `area_ha` | numeric | Cell area (ha) |
| `total_biomass_2000` | numeric | Unmasked year-2000 whole-cell biomass density (Mg/ha) |
| `fc_2000` | numeric | Forest cover in the biomass reference year, always extracted |
| `fc_YYYY` | numeric | Forest cover (percent, 0–100) per year |
| covariates | numeric | Matching covariates, including `defor_pre_intervention` when used |
| `n_site_pixels`, `n_eligible`, `n_sampled`, `n_matched` | integer | Treatment pixel counts for the fragment |
| `n_control_pool`, `n_control_sampled` | integer | Control pixel counts for the fragment |

The `group_diagnostics` attribute lists, per stratum, the treatment and
control counts, the number matched, the distance used, and any separation
or matching error.

---

## Final Result Files (Step 3)

All files are produced by `03_summarize_results.R` and written to
`{results_s3_uri}`. An exact-match stratum of a site fragment is included
only when it was matched in every replicate; site results sum their included
strata and fragments. Pixel-level downloads use the same inclusion rule.

Columns prefixed `extrapolated_` are scaled to all eligible treatment pixels
(see [matching.md](matching.md#sampling-weights-and-coverage)); columns
prefixed `sample_` use matched pixels only. When `n_replicates > 1`, each
metric is the mean across replicates and has `_ci_lower` / `_ci_upper`
columns with the 2.5% / 97.5% quantiles across replicates (see
[Replicate Ranges](#replicate-ranges)).

---

### results\_by\_site\_year.csv

**Granularity**: One row per `(site_id, year)`, including five
pre-intervention years

| Column | Type | Description |
|---|---|---|
| `site_id`, `site_name` | string | Site |
| `year` | integer | Calendar year |
| `is_pre_intervention` | boolean | `year < start_year` |
| `is_post_intervention` | boolean | `year > end_year` |
| `{prefix}_treatment_defor_ha` | numeric | Treatment forest loss (ha) |
| `{prefix}_control_defor_ha` | numeric | Counterfactual (matched control) forest loss (ha) |
| `{prefix}_forest_loss_avoided_ha` | numeric | Control − treatment forest loss (ha) |
| `{prefix}_treatment_emissions_mgco2e` | numeric | Treatment emissions (MgCO₂e) |
| `{prefix}_control_emissions_mgco2e` | numeric | Counterfactual emissions (MgCO₂e) |
| `{prefix}_emissions_avoided_mgco2e` | numeric | **Primary result** with the `extrapolated` prefix |
| `n_sample_pixels` | integer | Matched treatment pixels (mean across replicates) |
| `sampled_fraction` | numeric | Sampled / eligible treatment pixels |

`{prefix}` is `extrapolated` or `sample`.

---

### results\_by\_site\_total.csv

**Granularity**: One row per site; sums over the intervention period
(`start_year` ≤ year ≤ `end_year`), computed within each replicate

| Column | Type | Description |
|---|---|---|
| `site_id`, `site_name` | string | Site |
| `area_ha` | numeric | Site area (ha) |
| `first_year`, `last_year`, `n_years` | integer | Intervention years covered |
| metric columns | numeric | As in `results_by_site_year.csv` |
| `n_treatment_pixels` | integer | All treatment pixels in the site |
| `n_eligible_pixels` | integer | Treatment pixels eligible for matching |
| `n_sample_pixels` | integer | Matched treatment pixels (mean across replicates) |
| `sampled_fraction` | numeric | Sampled / eligible treatment pixels |
| `n_represented_pixels` | numeric | Eligible treatment pixels in strata matched in every replicate |
| `coverage_fraction` | numeric | Represented / all treatment pixels |
| `match_rate` | numeric | Matched / sampled treatment pixels |
| `n_control_pool`, `n_control_sampled` | numeric | Control pixel counts |

---

### results\_pixel\_year\_emissions.csv

**Granularity**: One row per `(cell, year)` for matched pixels in replicate 1

| Column | Type | Description |
|---|---|---|
| `cell`, `site_id`, `year`, `treatment` | | Pixel, site, year, and group |
| `id_numeric`, `group` | integer, string | Site fragment and exact-match stratum |
| `match_group`, `match_weight`, `sampling_weight` | | Matching and stratum weights |
| `forest_loss_ha` | numeric | Forest lost during the year (ha) |
| `emissions_mgco2e` | numeric | Emissions from that loss (MgCO₂e) |

---

### results\_pixel\_covariates.csv

**Granularity**: One row per matched pixel in replicate 1

Columns: `site_id`, `cell`, `treatment`, `match_group`, `match_weight`, and
one column per matching covariate (including `defor_pre_intervention` when
used).

---

### results\_covariate\_balance.csv

**Granularity**: One row per `(site_id, covariate)` (categorical covariates:
one row per level, named `covariate=level`), plus aggregate rows with
`site_id = "__all__"`

| Column | Type | Description |
|---|---|---|
| `site_id` | string | Site identifier, or `"__all__"` |
| `covariate` | string | Covariate (or `covariate=level`) |
| `mean_treatment`, `mean_control` | numeric | Match-weighted means |
| `sd_treatment` | numeric | Standard deviation among treatment pixels |
| `smd` | numeric | (mean_T − mean_C) / sd_treatment; difference in proportions for levels; `NA` when undefined |

---

### results\_propensity\_scores.csv

**Granularity**: One row per matched pixel in replicate 1

Columns: `cell`, `site_id`, `treatment`, `match_group`, `match_weight`,
`pscore` (`NA` for Mahalanobis matching).

---

### results\_match\_quality\_summary.json

**Format**: Nested JSON, pre-aggregated for the web app

```json
{
  "summary_stats": {
    "__all__": {
      "n_treatment": 1234, "n_control": 2468, "n_sites": 10,
      "n_treatment_total": 50000, "n_control_sampled": 60000,
      "n_control_pool": 900000
    },
    "{site_id}": { "...": "..." }
  },
  "histograms": {
    "__all__": {
      "{covariate}": {
        "bin_edges": [0.0, 0.1],
        "treatment_pct": [5.0],
        "control_pct": [6.0]
      }
    }
  },
  "qq_quantiles": {
    "__all__": {
      "quantiles": [0.0, 0.002],
      "treatment_values": [0.12],
      "control_values": [0.11]
    }
  },
  "covariate_cols": ["precip", "temp"]
}
```

Histograms (numeric covariates) and QQ quantiles use the matching weights.
The web app requires this file; it is not regenerated from other outputs.

---

### results\_sampling\_by\_site.csv

One row per site: `site_id`, `sampled_fraction`, `sampled_percent`,
`was_subsampled`.

---

### results\_pixel\_locations.csv

One row per matched pixel in replicate 1: `cell`, `site_id`, `treatment`,
`match_group`, `lon`, `lat` (pixel centre, EPSG:4326). Used to plot matched
pixels on the Task Detail map.

---

### results\_failed\_sites.csv

**Granularity**: One row per site fragment that was not included

| Column | Type | Description |
|---|---|---|
| `id_numeric` | integer | Fragment index |
| `site_id`, `site_name` | string | Site |
| `sub_site_index` | integer | Fragment within the site (0 for unsplit sites) |
| `error` | string | Reason the fragment was not included |

---

### results\_summary.json

Task-level summary: `n_sites`, `n_failed_sites` (distinct sites), `n_replicates`,
`bounds_label`, extrapolated and sample totals, `total_area_ha`,
`year_range`, `failed_sites`, `worker_failures`, `subsampled_sites`,
`na_exclusion_summary`, `methodology_warnings`, `group_diagnostics`, and
`portfolio`. `portfolio.yearly` holds the all-site series plotted by the web
app (`treatment_defor_ha`, `control_defor_ha`, `forest_loss_avoided_ha`,
`emissions_avoided_mgco2e`, `treatment_emissions_mgco2e`,
`control_emissions_mgco2e`, `cum_treatment_defor_ha`,
`cum_control_defor_ha`, their bounds, and `n_sites`); `portfolio.total`
holds the all-site totals and bounds.

---

## Failure Markers

Step 2 writes `matches/failed_{id_numeric}_rep{k}.json` when a fragment
cannot be matched (for example, no shared exact-match stratum, separation
without the fallback, or no feasible match):

```json
{
  "id_numeric": 7,
  "site_id": "SITE_ABC",
  "site_name": "Site ABC",
  "replicate": 1,
  "error": "No matches found (treatment=40, control=2000)",
  "reason": "no_matches",
  "match_file": "m_7_rep1.rds",
  "group_diagnostics": [ ... ]
}
```

If the R process crashes, the Python wrapper writes
`matches/failed_array_{index}.json` with the array index and error, without
attributing it to a site. Step 3 ignores a marker whose `match_file` exists
(a retry succeeded) and reports every fragment without a match file for
every replicate as failed, using the marker's error when available.

---

## Biomass-to-CO₂e Conversion

`total_biomass_2000` is unmasked year-2000 whole-cell biomass density
(Mg per hectare of cell). For each matched pixel with cell area $A$ (ha),
year-2000 forest cover $\text{fc}_{2000}$ (percent), and forest cover
$\text{fc}_t$:

$$\text{biomass}_t = \text{total\_biomass\_2000} \times A \times \frac{\text{fc}_t}{\text{fc}_{2000}}$$

$$\text{emissions}_t = (\text{biomass}_{t-1} - \text{biomass}_t) \times 0.5 \times \frac{44}{12}$$

- **0.5** is the carbon fraction of dry biomass.
- **44/12** converts carbon to CO₂.
- Annual forest loss is $(\text{fc}_{t-1} - \text{fc}_t) / 100 \times A$.
- The summary records `biomass_reference_year = 2000`, including in API
  results and task metadata.

Avoided emissions are the weighted control emissions minus the treatment
emissions. Extrapolated values multiply each matched pixel by its stratum's
`sampling_weight`.

---

## Replicate Ranges

When `n_replicates > 1`, every metric is first aggregated within each
replicate at the reported level (site-year, site total, all-site year,
all-site cumulative, all-site total). The reported value is the mean across
replicates and the bounds are the 2.5% and 97.5% quantiles:

$$\text{lower} = Q_{0.025}(\text{metric}_1, \ldots, \text{metric}_R)$$

These bounds are the range across matching replicates (pixel sampling and
matching variability only), not confidence intervals. They are not additive:
the all-site bounds come from all-site replicate totals, never from summing
site bounds. With one replicate there are no bound columns.

---

## Database Import

When an execution finishes, the polling task validates the results payload
and imports it:

| CSV file | Database table | Notes |
|---|---|---|
| `results_by_site_year.csv` | `task_results` | One row per (site, year) |
| `results_by_site_total.csv` | `task_results_total` | One row per site |
| `results_summary.json` | `analysis_tasks.metadata` | Failures, warnings, bounds label, and the all-site `portfolio` series |

A payload with missing values or duplicate site rows is rejected and the task
is marked failed. Other import errors leave the task running so the next
poll retries. Rows are inserted in bulk, replacing any earlier rows for the
task. The task is marked succeeded only after a successful import.

Results from earlier versions of the pipeline are not supported; rerun those
tasks.
