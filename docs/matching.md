# Matching Methodology and Parameters

The analysis estimates *avoided* deforestation by comparing observed forest
loss inside each conservation site (the **treatment**) with a counterfactual
estimated from matched outside-site pixels (the **controls**). Controls may
be protected or unprotected, depending on the selected settings. **Propensity
score matching (PSM)** selects controls with similar observed
pre-intervention covariates.

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
  - [Sampling Weights and Coverage](#sampling-weights-and-coverage)
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
pixel. Matching treatment and control pixels with similar scores improves
comparability on observed covariates. Treatment assignment remains
observational, and unmeasured confounding can remain.

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
container:

| Step | Script | What it does |
|---|---|---|
| 1 — extract | Python | Samples covariate values for treatment and control candidate pixels from COG files; computes matching extent and control exclusion buffer |
| 2 — match | R | Within each exact-match stratum of each site, fits a propensity model and matches sampled treatment pixels to controls; each AWS Batch array element handles one bounded unit of site fragments for one replicate |
| 3 — summarize | R | Computes annual forest loss and emissions for matched pixels, aggregates within each replicate, and reports means and replicate ranges per site-year, per site, and for all sites |

### Site preparation and control geography

Preparation applies the whole-site area threshold before splitting sites
across selected reference regions. Splitting preserves positive area and
reference strata; overlapping reference coverage fails explicitly rather than
silently discarding area. Retained/excluded sites, fragment counts, uncovered
area, and reference versions are recorded in the preparation report.

Candidate controls are outside all submitted sites, within the selected
matching geography, and outside the requested distance buffer. Treatment and
controls must share every selected exact-match value. Finer geographic
constraints can improve comparability but also remove all controls.
See the [setup guide](analysis-setup.md#exact-matching) for interpretation.

### Eligible pixels

A pixel is eligible for matching when it has forest cover in the site's
baseline year (`start_year − 6`, or the first forest-cover year if earlier),
complete covariate and outcome data (`total_biomass_2000`, `fc_2000`, and every
forest-cover year), and a value for every exact-match variable. Treatment and
control pixels use the same rules. Pixels without forest at baseline are
ineligible because they cannot lose forest.

### Pre-intervention deforestation covariate

For sites starting in 2006 or later, `defor_pre_intervention` (percent change
in forest cover from `start_year − 6` to `start_year − 1`) is added to the
propensity model. It ends the year before the intervention, so no
intervention-period outcome influences the matches.

---

## Matching Parameter Reference

### Matching Method

**Parameter**: `matching_method`  
**Default**: `"optimal"`  
**Options**: `"optimal"` | `"nearest"`

| Method | Package | Algorithm | Notes |
|---|---|---|---|
| `optimal` | `optmatch` | Network flow optimisation (RELAX-IV solver) | Minimises total absolute propensity score distance across all matched pairs globally; typically requires more time and memory |
| `nearest` | `MatchIt` | Greedy nearest-neighbour | Faster; matches each treatment pixel to the closest available control in propensity score space; can leave some treatment pixels poorly matched if controls are scarce |

For most analyses `"optimal"` is recommended. Use `"nearest"` if the site
has many thousands of pixels and optimal matching is too slow.

---

### Caliper Width

**Parameter**: `caliper_width`  
**Default**: `0.2`  
**Units**: Within-stratum standard deviations of the logit of the propensity score (sampled treatment and controls)
**Range**: `0` (disabled) to any positive number; values > 0.5 are rarely useful

The caliper is the maximum allowable distance between matched treatment and
control pixels on the logit propensity scale, computed separately within
each exact-match stratum. A caliper of 0.2 SD of the logit (Austin 2011) is
the conventional default. Tighter calipers improve balance at the cost of
fewer matched pixels; unmatched treatment pixels are reported (see
[Sampling weights and coverage](#sampling-weights-and-coverage)). Set to `0`
to disable the caliper.

Both matching methods apply the caliper identically. It applies only to
propensity-score matching, never to Mahalanobis distance matching.

With optimal k:1 matching, a stratum in which the caliper leaves some
treatment pixels with fewer than k eligible controls cannot be solved and is
recorded as unmatched in the group diagnostics.

---

### Controls per Treatment (Ratio)

**Parameter**: `max_controls_per_treatment`  
**Default**: `1` (1:1 matching)  
**Range**: `0` (full matching) or any positive integer

Controls how many control pixels are matched to each treatment pixel.

| Value | Behaviour |
|---|---|
| `1` | 1:1 matching — one control per treatment pixel |
| `k > 1` | k controls per treatment pixel |
| `0` | Full matching — variable-ratio matched sets, subject to eligibility and distance constraints. Requires `matching_method = "optimal"` |

Increasing the ratio includes more controls per treatment pixel and may
increase information, with the possibility of worse per-pair balance. Full
matching (`0`) allows variable-ratio sets and often achieves strong overall
balance. Apply matched-set weights in downstream calculations.

Within each matched set, treatment pixels have weight 1 and control weights
are scaled so that total control weight equals the number of treatment
pixels in the set. Both matching methods use the same weights.

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

This avoids comparisons across ecologically or administratively distinct
regions where covariate relationships with deforestation may differ.

More exact-match variables reduce the available control pool within each
stratum. If a site is the only protected area in its country × ecoregion
combination, it may have no eligible controls and be excluded.

`ecoregion` and `pa` can each serve as a covariate or a stratification
variable. Select one role for each; the UI marks them as *dual-purpose*.

Exact matching on `pa` compares protected treatment pixels with controls in
other protected areas. Leave `pa` out of the exact-match list to allow
unprotected controls.

If site polygons overlap, each overlapping pixel is assigned to only one site
(the last one rasterized).

---

### Mahalanobis Distance Fallback

**Parameter**: `separation_fallback_mahalanobis`  
**Default**: `false` (must be explicitly enabled)

Terms that are constant within the sampled stratum are removed first,
including one-level factors. If no term varies, every pairing is equally
good on the selected covariates; matching uses equal distances, no caliper,
and no fitted propensity scores.

Otherwise, the distance used within each exact-match stratum is chosen as follows:

1. **Fewer than `min_glm_treatment_pixels`** treatment pixels — Mahalanobis
   distance on all covariates. A logistic regression cannot be fitted
   reliably with so few positives.
2. **Separation** — detected before fitting (a factor level, or one side of a
   numeric or binary covariate's range, present in only one group) or while
   fitting (the GLM errors, does not converge, or gives a *treatment* pixel a
   fitted probability numerically 0 or 1, meaning no control resembles it).
   - With `separation_fallback_mahalanobis = true`, the stratum is matched on
     Mahalanobis distance with the separating variables removed.
   - With `false`, the stratum is not matched and the reason is recorded in
     the group diagnostics. A site with no matched stratum fails.
3. **Otherwise** — propensity-score matching with a per-stratum logistic
   model.

For Mahalanobis matching no caliper is applied and `pscore` is `NA`.

A fit that pushes only *control* pixels to a fitted probability of 0 is
accepted. Those controls are unlike every treatment pixel and are never chosen
as matches. R's warning "fitted probabilities numerically 0 or 1 occurred" is
kept in the diagnostics for such fits, with the number of treatment and
control pixels at each bound, but it does not stop the match. Controls far
from the site widen the spread of the linear predictor, and with it a caliper
expressed in standard deviations; judge these matches by their covariate
balance.

### Shared-support controls

**Parameter**: `shared_support_controls`
**Default**: `true` for new analyses (matching policy `shared_support_v1`)

Set this to `false` to use the legacy policy. Recomputed tasks and resumed
workflows retain their recorded setting; historical tasks and saved presets
without the setting use the legacy policy.

Before replicate sampling, each eligible fragment/exact-match stratum is
checked for direct numeric covariates constant across all its treatment pixels.
With this policy enabled, controls must equal those treatment values, jointly
across the constrained covariates. For example, zero-cropland treatment is
compared only with zero-cropland controls. Equality uses stored values, not a
near-zero tolerance. Transformed, interaction, and categorical terms do not
receive this restriction.

No treatment is removed and no covariate constraint is dropped. Terms constant
after selection can be omitted from the fitted formula because their equality
is enforced by control selection. The requested control multiplier is a
sampling ceiling, not a minimum required pool size. Strata without remaining
controls are explicitly unmatchable. GLM separation handling, calipers,
control-distance exclusions, and replicate inclusion rules are unchanged.

Atoms retain their identity. The model and restriction remain at the existing
parent fragment/stratum level. Constancy in one atom or one random sample does
not establish constancy across the eligible treatment stratum. Every atom with
eligible pixels is sampled in every replicate (see
[Pixel Sampling Limits](#pixel-sampling-limits)).

Both policies produce versioned diagnostics for every replicate, including
failed strata in partly successful fragments. See
[matching diagnostic exports](analysis-outputs.md#matching-diagnostic-exports)
for structured causes, exact failed-fit replay inputs, and atom inclusion
evidence. Changing the policy requires a fresh compatible matching run; do not
reuse old non-atom checkpoints for atom-enabled inputs.

---

### Replicates

**Parameter**: `n_replicates`  
**Default**: `1`  
**Range**: `1` to `1000`

Each replicate repeats the pixel sampling and matching with a different
random draw (seeded per fragment and replicate, so results do not depend on
how replicates are grouped into array elements via `replicate_block_size`).
Step 3 aggregates each replicate separately, then reports the mean and the
2.5% / 97.5% quantiles across replicates at each reported level (site-year,
site total, and all sites). Bounds are never summed across sites or years.

These bounds are the **range across matching replicates**: they reflect
variability from pixel sampling and matching. They omit spatial dependence
and model uncertainty, so they are not confidence intervals for avoided
emissions. Fewer than 50 replicates gives imprecise bounds; the web app shows
a warning.

Estimates are built from **cells**: an atom within one exact-match stratum of
a site fragment, or the whole stratum when the input has no atoms. A cell is
included when it has matches in at least 80% of the replicates. In a replicate
where an included cell did not match, it takes its own mean over the
replicates where it did, so every replicate total covers the same population.
Strata with no included cell produce an `inconsistent_strata` warning; a
fragment with none is reported as failed. Pixel downloads and balance
diagnostics use the same included cells as the estimates.

Runtime scales approximately linearly with `n_replicates`.

---

### Minimum Control Distance

**Parameter**: `min_control_distance_km`  
**Default**: `10` km  
**Range**: `0` (disabled) to any positive number

Pixels within this distance of any treatment polygon boundary are excluded
from the control pool. This prevents matching treatment pixels against
nearby unprotected pixels that may be influenced by spillover effects from
the intervention (e.g. wildlife dispersal, edge effects).

Extraction computes the buffer once and rasterizes it using pixel centres;
treatment pixels remain eligible.
Nearby geometries share local azimuthal-equidistant projections (5-degree
tiles), rather than a global equal-area projection that distorts distances
with latitude. Boundaries are simplified by at most the smaller of 100 m
or 1% of the requested distance before buffering. Buffer distances are
approximate for very large polygons that span far beyond their local tile.
The PostGIS submission fallback buffers actual site geometries, not the
bounding box enclosing them.

---

### Pixel Sampling Limits

**Parameter**: `max_treatment_pixels`  
**Default**: `1000` pixels per fragment/exact-match stratum

**Parameter**: `control_multiplier`  
**Default**: `50` controls per treatment pixel

Large sites can have tens of thousands of treatment pixels. Running optimal
matching at that scale can be computationally infeasible. When a stratum exceeds
`max_treatment_pixels`, a **random subsample** of treatment pixels is drawn
(without replacement). The `sampled_fraction` column in the results records
what fraction of treatment pixels were used.

For atom-enabled inputs the subsample is drawn by atom, so that every atom has
its own sample in every replicate:

- An atom with 50 or fewer eligible pixels in the stratum is sampled in full.
- Every larger atom contributes at least 50 pixels.
- What is left of `max_treatment_pixels` is shared among the larger atoms in
  proportion to their remaining pixels.

The minimum takes precedence over the cap, so a stratum holding many small
atoms can draw somewhat more than `max_treatment_pixels`.

The sampled control pool for each stratum has up to
`sampled treatment count × control_multiplier` eligible control pixels.
This is a ceiling, not a required minimum or the final matching ratio.
A larger `control_multiplier` gives the matching algorithm
more options and generally improves balance, at the cost of more memory and
computation.

**Parameter**: `min_site_area_ha`  
**Default**: `100` ha

Sites smaller than this threshold are excluded from the analysis entirely.
Very small sites produce too few treatment pixels for a reliable propensity
score estimate. The threshold applies to whole sites before they are split
across exact-match regions, so a large site keeps small fragments.

**Parameter**: `min_glm_treatment_pixels`  
**Default**: `15` pixels

When a stratum (or the whole site after subsetting) contains fewer than
this many treatment pixels, the GLM step is skipped and Mahalanobis
distance is used instead.

---

### Sampling Weights and Coverage

Totals describe **eligible treatment pixels in included cells**, and nothing
else. They are never divided by coverage or multiplied by site area to stand
for the rest of a site.

**Expansion.** In each included cell, matched treatment pixels and their
controls are weighted by `n_eligible / n_matched`, counted for that cell and
replicate. A control matched to treatment pixels of several cells carries each
cell's own factor for its share.

**Observed loss.** The observed (treatment) loss of an included cell is not
estimated from the sample. It is the sum over every eligible treatment pixel
of the cell, so it has no sampling error and is the same in every replicate.
The expanded sample is kept as a check: `treatment_census_check` in
`results_summary.json` compares it with the census for each site.

**Low match rates.** Expanding by `n_eligible / n_matched` assumes that
unmatched treatment pixels resemble matched ones. Where fewer than 50% of a
cell's sampled pixels match on average, that assumption is not made: the cell
is weighted by `n_eligible / n_sampled`, both arms come from the matched
sample, only the matched share of the cell counts as represented, and the
result is marked `partial`.

**What is not represented.** Pixels that are not eligible (no forest at
baseline, missing data, or no shared stratum), pixels of cells matched in fewer
than 80% of replicates, and the unmatched share of low-match-rate cells.

Per-site results report `n_treatment_pixels` (all site pixels),
`n_eligible_pixels`, `n_represented_pixels` (eligible pixels the estimates
stand for), `n_sample_pixels` (matched), `coverage_fraction`
(represented / all site pixels), `forest_coverage_fraction` (represented /
baseline forest pixels) and `match_rate` (matched / sampled). A site or atom
below 80% forest coverage is `partial`. The web app warns when a fraction is
below 80% and shows the share of baseline forest that the task totals
represent.

The thresholds (50 pixels per atom, 80% of replicates, 50% match rate) are
fixed and are written to `results_summary.json` under `estimator`.

### Batch work allocation

The submission setting `match_batch_size` (default 25) limits the number of
site fragments in one match unit. Fragments are packed in exact-match-region
order to share control-data reads, but a large region is split across units
instead of assigning an unbounded number of sites to one worker. Each array
element processes one unit and one replicate. Arrays larger than 10,000
elements are split into chunks without changing per-site statistical models.

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

$$\text{SMD} = \frac{\bar{X}_T - \bar{X}_C}{s_T}$$

where $\bar{X}_T$ and $\bar{X}_C$ are the match-weighted means of the
covariate for treatment and matched control pixels, and $s_T$ is the standard
deviation of the matched treatment pixels (the usual convention for the
average treatment effect on the treated). The SMD does not depend on the
scale of the weights. Categorical covariates are assessed per level as the
difference in proportions. When $s_T = 0$ and the means differ, the SMD is
undefined and the web app shows a warning.

The conventional threshold for acceptable balance is $|\text{SMD}| < 0.1$.
The web app displays automated warnings when:
- Any covariate has $|\text{SMD}| \geq 0.25$ (Critical)
- More than 20 % of covariates have $|\text{SMD}| > 0.1$ (Caution)

Covariate histograms and propensity-score QQ plots use the matching weights.
See the **Love plot** on the Task Detail page for a visual summary.

---

## Default Parameter Values

All defaults are defined in `webapp/services/analysis_task.py`:

| Parameter | Default | Notes |
|---|---|---|
| `matching_method` | `"optimal"` | |
| `caliper_width` | `0.2` | Logit propensity score SDs |
| `max_controls_per_treatment` | `1` | 1:1 matching |
| `exact_match_vars` | `["admin1", "ecoregion", "pa"]` | |
| `separation_fallback_mahalanobis` | `false` | Must be opted in |
| `shared_support_controls` | `true` | Joint control restriction for treatment-constant numeric covariates |
| `n_replicates` | `1` | No replicate range by default |
| `min_control_distance_km` | `10` | km |
| `max_treatment_pixels` | `1000` | pixels per fragment/exact-match stratum |
| `control_multiplier` | `50` | controls per treatment pixel |
| `min_site_area_ha` | `100` | ha |
| `min_glm_treatment_pixels` | `15` | pixels |
| `random_seed` | `null` | |
| `resolution_m` | `1000` | 1 km grid |

The [setup guide](analysis-setup.md) displays current application defaults
from the form configuration, including generated random seeds and execution
settings. For older tasks, consult recorded settings rather than current
defaults. See [limitations and references](limitations.md) for identifying
assumptions, temporal leakage, spatial dependence, and scientific citations.
