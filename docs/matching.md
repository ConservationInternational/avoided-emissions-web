# Matching Methodology and Parameters

The analysis estimates *avoided* deforestation by comparing what actually
happened inside each conservation site (the **treatment**) against what would
have happened without the intervention — estimated from a matched set of
unprotected pixels (the **control**). This is done via **propensity score
matching (PSM)**, which selects controls that look as similar as possible to
treatment pixels on a set of pre-intervention covariates.

## Contents

- [Overview of Propensity Score Matching](#overview-of-propensity-score-matching)
- [Analysis Pipeline Steps](#analysis-pipeline-steps)
- [Matching Parameter Reference](#matching-parameter-reference)
  - [Matching Method](#matching-method)
  - [Caliper Width](#caliper-width)
  - [Controls per Treatment (Ratio)](#controls-per-treatment-ratio)
  - [Exact Match Variables](#exact-match-variables)
  - [Mahalanobis Distance Fallback](#mahalanobis-distance-fallback)
  - [Replicates](#replicates)
  - [Minimum Control Distance](#minimum-control-distance)
  - [Pixel Sampling Limits](#pixel-sampling-limits)
  - [Cross-Site Grouping](#cross-site-grouping)
  - [Random Seed](#random-seed)
- [Covariate Balance Assessment](#covariate-balance-assessment)
- [Default Parameter Values](#default-parameter-values)

---

## Overview of Propensity Score Matching

### What is the propensity score?

The propensity score is the probability that a pixel is a treatment pixel
(i.e. inside a conservation site), given its observed covariate values:

$$e(X) = P(\text{treatment} = 1 \mid X)$$

This single number summarises the multivariate covariate profile of each
pixel. By matching treatment pixels to control pixels with similar propensity
scores, we produce a comparison group that is balanced on the observed
covariates — mimicking a randomised experiment.

### GLM-based propensity scores

By default the propensity score is estimated with a **logistic regression**
(binomial GLM):

$$\text{logit}(e(X)) = \beta_0 + \beta_1 X_1 + \cdots + \beta_k X_k$$

The covariates $X_1 \ldots X_k$ are the user-selected matching covariates
(e.g. `precip`, `temp`, `elev`, …). The fitted probability from this model
is the propensity score used for matching.

### Counterfactual

After matching, the deforestation rate observed in each control pixel is
used as the counterfactual — what would have happened inside the site if the
intervention had not occurred. The difference in deforestation rate between
treatment and matched controls gives the conservation effect, which is then
converted to avoided biomass loss and CO₂e emissions
(see [analysis-outputs.md](analysis-outputs.md#biomass-to-co2e-conversion)).

---

## Analysis Pipeline Steps

The matching analysis runs in three steps inside the `r-analysis` Docker
container (see [covariate-pipeline.md](covariate-pipeline.md#stage-4--pixel-extraction)
for Step 1 details):

| Step | Script | What it does |
|---|---|---|
| 1 — extract | Python | Samples covariate values for treatment and control candidate pixels from COG files; computes matching extent and control exclusion buffer |
| 2 — match | R | Fits the propensity score model and performs matching; runs as an AWS Batch array job (one array element per site) |
| 3 — summarize | R | Aggregates matched pairs to compute annual deforestation rates and avoided emissions |

---

## Matching Parameter Reference

### Matching Method

**Parameter**: `matching_method`  
**Default**: `"optimal"`  
**Options**: `"optimal"` | `"nearest"`

| Method | Package | Algorithm | Notes |
|---|---|---|---|
| `optimal` | `optmatch` | Network flow optimisation (RELAX-IV solver) | Minimises total absolute propensity score distance across all matched pairs globally; slower but produces better overall balance |
| `nearest` | `MatchIt` | Greedy nearest-neighbour | Faster; matches each treatment pixel to the closest available control in propensity score space; can leave some treatment pixels poorly matched if controls are scarce |

For most analyses `"optimal"` is recommended. Use `"nearest"` if the site
has many thousands of pixels and optimal matching is too slow.

---

### Caliper Width

**Parameter**: `caliper_width`  
**Default**: `0.2`  
**Units**: Standard deviations of the propensity score distribution  
**Range**: `0` (disabled) to any positive number; values > 0.5 are rarely useful

The caliper is a maximum allowable distance between matched treatment and
control pixels in propensity score space. Pairs separated by more than the
caliper width are not permitted.

A caliper of 0.2 SD is the conventional default (Rosenbaum & Rubin 1985).
Tighter calipers improve balance at the cost of fewer matched pairs (some
treatment pixels may be left unmatched). Set to `0` to disable the caliper
and match all treatment pixels.

**Note**: The caliper applies only when a GLM-based propensity score is used.
It is ignored when the [Mahalanobis fallback](#mahalanobis-distance-fallback)
is active.

---

### Controls per Treatment (Ratio)

**Parameter**: `max_controls_per_treatment`  
**Default**: `1` (1:1 matching)  
**Range**: `0` (full matching) or any positive integer

Controls how many control pixels are matched to each treatment pixel.

| Value | Behaviour |
|---|---|
| `1` | 1:1 matching — one control per treatment pixel |
| `k > 1` | Up to k controls per treatment pixel |
| `0` | Full matching — variable ratio; every pixel (treatment and control) is matched into a set |

Increasing the ratio increases statistical power (more matched pairs) at the
cost of potentially worse per-pair balance. Full matching (`0`) is the most
flexible option and often achieves the best overall balance, but matched-set
weights must be applied carefully downstream.

When multiple controls are matched to a single treatment pixel, their
contribution to the emissions calculation is weighted so that the total
control weight equals the number of treatment pixels.

---

### Exact Match Variables

**Parameter**: `exact_match_vars`  
**Default**: `["admin1", "ecoregion", "pa"]`  
**Available variables**: `admin0`, `admin1`, `admin2`, `ecoregion`, `pa`

Exact matching forces treatment and control pixels to be drawn from the
same stratum for each exact-match variable. For example, with
`exact_match_vars = ["ecoregion"]`, a treatment pixel in the Amazon
ecoregion can only be matched to control pixels also in the Amazon
ecoregion.

This prevents spurious matches across ecologically or administratively
distinct regions where the relationships between covariates and
deforestation are fundamentally different.

**Trade-off**: More exact-match variables reduce the available control pool
within each stratum. If a site is the only protected area in its
country × ecoregion combination there may be no eligible controls, and the
site will be excluded from the analysis.

To use `ecoregion` or `pa` as a continuous covariate rather than a
stratification variable, add them to the covariate list instead of (or as
well as) the exact-match list. These two variables are marked as
*dual-purpose* in the UI.

---

### Mahalanobis Distance Fallback

**Parameter**: `separation_fallback_mahalanobis`  
**Default**: `false` (must be explicitly enabled)

When logistic regression cannot reliably estimate the propensity score — due
to complete or quasi-complete separation — the model produces extreme or
undefined coefficients. The Mahalanobis fallback handles these cases by
switching to **Mahalanobis distance matching** instead of propensity score
matching.

#### When the fallback activates automatically

Even when `separation_fallback_mahalanobis` is `false`, the fallback is
triggered automatically in these situations:

1. **Too few treatment pixels** — fewer than `min_glm_treatment_pixels`
   (default 15) treatment pixels in a site. A logistic regression cannot
   be reliably fitted with so few positives.
2. **Complete separation** — one or more factor-coded covariates (e.g. a
   categorical variable where one level appears only in treatment pixels)
   cause the GLM to fail entirely.
3. **Severe imbalance** — the treatment-to-control ratio exceeds 2:1 within
   a stratum, making GLM propensity scores unreliable.

When the fallback is active:
- Variables that cause separation are **excluded** from the Mahalanobis
  distance formula to avoid them dominating the distance measure.
- The caliper parameter is ignored.
- `pscore` values in the output data are `NA`.

If `separation_fallback_mahalanobis = false` and separation is detected for
a site, that site is marked as failed (see
[analysis-outputs.md](analysis-outputs.md#failure-markers)).

---

### Replicates

**Parameter**: `n_replicates`  
**Default**: `1` (no confidence intervals)  
**Range**: `1` to `1000` (practical maximum ~100 for reasonable runtime)

Running multiple replicates repeats the matching with different random
samples of control pixels, producing a distribution of estimates from which
**95 % confidence intervals** are derived.

For each replicate $k$:
- A fresh random sample of control candidate pixels is drawn from the
  control pool (of size `max_treatment_pixels × control_multiplier`).
- The full matching procedure is repeated independently.
- A result file `m_{id_numeric}_rep{k}.rds` is written.

The summarisation step aggregates across replicates by computing the 2.5th
and 97.5th percentiles of each metric to form the lower and upper CI bounds.

**Performance note**: Runtime scales approximately linearly with
`n_replicates`. A task with 50 sites and `n_replicates = 10` requires
10× as much compute as the same task with `n_replicates = 1`.

---

### Minimum Control Distance

**Parameter**: `min_control_distance_km`  
**Default**: `10` km  
**Range**: `0` (disabled) to any positive number

Pixels within this distance of any treatment polygon boundary are excluded
from the control pool. This prevents matching treatment pixels against
nearby unprotected pixels that may be influenced by spillover effects from
the intervention (e.g. wildlife dispersal, edge effects).

The buffer is computed within the extract step from the simplified union of
all site polygons, using a 0.001° pre-simplification tolerance for performance.

---

### Pixel Sampling Limits

**Parameter**: `max_treatment_pixels`  
**Default**: `1000` pixels per site

**Parameter**: `control_multiplier`  
**Default**: `50` controls per treatment pixel

Large sites can have tens of thousands of treatment pixels. Running optimal
matching at that scale is computationally infeasible. When a site exceeds
`max_treatment_pixels`, a **random subsample** of treatment pixels is drawn
(without replacement). The `sampled_fraction` column in the results records
what fraction of treatment pixels were used.

The control pool for each site is drawn as a random sample of size
`max_treatment_pixels × control_multiplier` from the full set of eligible
control pixels. A larger `control_multiplier` gives the matching algorithm
more options and generally improves balance, at the cost of more memory and
computation.

**Parameter**: `min_site_area_ha`  
**Default**: `100` ha

Sites smaller than this threshold are excluded from the analysis entirely.
Very small sites produce too few treatment pixels for a reliable propensity
score estimate.

**Parameter**: `min_glm_treatment_pixels`  
**Default**: `15` pixels

When a stratum (or the whole site after subsetting) contains fewer than
this many treatment pixels, the GLM step is skipped and Mahalanobis
distance is used instead.

---

### Cross-Site Grouping

**Parameter**: `group_by_exact_matches`  
**Default**: `false`

When enabled, sites that share all exact-match strata values are **pooled
into a single matching problem**. This is useful when individual sites are
too small to yield reliable propensity score estimates but neighbouring sites
with similar characteristics can collectively provide a large enough sample.

Results are still reported per site (the sub-site index tracks which original
polygon each matched pixel came from), but the propensity score model and
matching are run once across the combined group.

---

### Random Seed

**Parameter**: `random_seed`  
**Default**: `null` (not reproducible)  
**Range**: `1` to `2,147,483,647`

When set, the random seed is used for treatment pixel subsampling and
control pool sampling. Setting the same seed reproduces the same matched
dataset. Useful for debugging or when sharing exact results with
collaborators.

---

## Covariate Balance Assessment

After matching, covariate balance is assessed using the **Standardized Mean
Difference (SMD)** for each covariate:

$$\text{SMD} = \frac{\bar{X}_T - \bar{X}_C}{s_p}$$

where $\bar{X}_T$ and $\bar{X}_C$ are the weighted means of the covariate
for treatment and matched control pixels respectively, and $s_p$ is the
pooled standard deviation (computed from the **unmatched** distributions).

The conventional threshold for acceptable balance is $|\text{SMD}| < 0.1$.
The web app displays automated warnings when:
- Any covariate has $|\text{SMD}| \geq 0.25$ (Critical)
- More than 20 % of covariates have $|\text{SMD}| > 0.1$ (Caution)

See the **Love plot** on the Task Detail page for a visual summary.

---

## Default Parameter Values

All defaults are defined in `webapp/services/analysis_task.py`:

| Parameter | Default | Notes |
|---|---|---|
| `matching_method` | `"optimal"` | |
| `caliper_width` | `0.2` | Propensity score SDs |
| `max_controls_per_treatment` | `1` | 1:1 matching |
| `exact_match_vars` | `["admin1", "ecoregion", "pa"]` | |
| `separation_fallback_mahalanobis` | `false` | Must be opted in |
| `n_replicates` | `1` | No CIs by default |
| `min_control_distance_km` | `10` | km |
| `max_treatment_pixels` | `1000` | pixels per site |
| `control_multiplier` | `50` | controls per treatment pixel |
| `min_site_area_ha` | `100` | ha |
| `min_glm_treatment_pixels` | `15` | pixels |
| `group_by_exact_matches` | `false` | |
| `random_seed` | `null` | |
| `resolution_m` | `1000` | 1 km grid |
