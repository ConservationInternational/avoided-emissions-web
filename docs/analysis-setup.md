# Setting up an analysis

This guide covers the controls exposed by the submission form. Defaults shown
in the app are read from the same configuration as the form. Values inside
double braces in this source document are substituted when displayed.

## Sites and dates

Upload reusable site sets through **Admin > Site Uploads**, then select a site
set on the **Task & Sites** tab. Supported inputs include GeoJSON, GeoPackage,
and supported vector datasets in ZIP/TAR archives. Map the source columns:
only the intervention start date is required; IDs, names, and end dates are
optional. Enter dates as explicit calendar dates (YYYY-MM-DD).

Geometries must be polygons or multipolygons. GeoJSON uses EPSG:4326; other
formats must declare their CRS. Preparation validates geometry, filters whole
sites by minimum area, and splits sites across reference regions while
reporting retained, excluded, and uncovered area. Generated immutable site IDs
keep duplicate display labels distinct.

The start and end dates determine the annual intervention window. An omitted
end date uses the available analysis endpoint. Review preparation output and
available forest years, especially for very early interventions.

## Resolution

**Default: {{resolution_m}} nominal metres.** Choose **1 km** or **250 m**.
The grids are EPSG:4326 at 30 and 7.5 arc-seconds, aligned to the same origin.
Actual cell dimensions and areas vary with latitude.

The selected grid determines output pixel size. Source resolution determines
the detail available in each layer; resampling cannot add source information.
The form lists covariates ready at the selected resolution, and availability
can differ between the 1 km and 250 m grids.

## Presets

Save and load covariates, exact-match variables, and matching settings together.
Review the loaded settings and layer availability before submission. Presets
store configuration; data versions and scientific suitability still require
review.

Historical presets without shared-support settings use the legacy policy.
Recomputing a task retains its recorded policy and settings while generating
a fresh seed; record the seed when you need an exactly repeatable sample.

## Covariates

Choose variables describing both treatment selection and forest-loss risk.
Use the [variable reference](covariates.md) to check sources, dates, units,
processing, and limitations before selection.

Check whether each variable was measured before the intervention and could
have been affected by it. Population snapshots and growth periods may overlap
early interventions. The default selection provides a starting point. Adding
variables may improve comparability while increasing missing-data exclusions,
overlap problems, separation, or model complexity.

Forest-cover outcomes and the historical biomass input are extracted
automatically. Pre-intervention deforestation is automatically added when the
forest record permits it.

## Exact matching

Choose at least one of country (`admin0`), province/state (`admin1`), district
(`admin2`), ecoregion, and protected-area status (`pa`). The initial selection
is `admin1`, `ecoregion`, and `pa`.

Exact matching forces equality on all selected attributes, avoiding
cross-region comparisons while reducing the eligible control pool.
`ecoregion` and `pa` can be predictors **or** exact-match constraints, never
both simultaneously. Other administrative IDs are exact-match constraints.

The protected-area mask includes IUCN categories Ia-IV only. Matching on
`pa` compares protected treatment to protected controls. Omitting that
constraint admits both protected and unprotected controls, subject to the
other matching restrictions.

## Max treatment pixels

**Default: {{max_treatment_pixels}}. Form range: 1-100,000, integer.**
Maximum treatment pixels sampled without replacement per exact-match stratum
of each site fragment. Each stratum has its own cap.

For atom-enabled inputs the sample is drawn by atom. An atom with 50 or fewer
eligible pixels in a stratum is sampled in full and every larger atom
contributes at least 50, so a stratum holding many atoms can draw more than
the cap.

Larger samples can improve representation and increase optimal-matching
runtime. Review sampling fractions and match rates to see how many treatment
pixels obtained matches.

## Control multiplier

**Default: {{control_multiplier}}. Form range: 1-500, integer.**
Sample up to this many candidate controls per sampled treatment pixel within
each stratum.

This setting caps the candidate pool. The separate controls-per-treatment
setting determines the final matched ratio. Eligibility may leave fewer
candidates than the cap. Increasing the cap gives the algorithm more choices
at a memory and runtime cost.

## Minimum site area

**Default: {{min_site_area_ha}} hectares. Form range: 0-100,000.**
The whole-site area threshold is applied before reference-region splitting,
so a retained site can contain smaller fragments. Small sites may have too
few eligible pixels for meaningful matching.

## Minimum GLM treatment pixels

**Default: {{min_glm_treatment_pixels}}. Form range: 1-10,000, integer.**
When the sampled treatment count within a stratum falls below this threshold,
the pipeline uses Mahalanobis distance and skips the logistic propensity model.

This small-sample fallback applies independently of the separation-fallback
checkbox. Mahalanobis matching has no propensity score or caliper.

## Caliper width

**Default: {{caliper_width}}. Form range: 0-5 standard deviations.**
Maximum distance in units of the within-stratum standard deviation of the
logit propensity score, across sampled treatment and controls.

Set to **0** to disable. Tighter calipers can reject poor comparisons, reduce
match rates, or make a requested optimal ratio infeasible. Calipers apply to
propensity-score matching; Mahalanobis and equal-distance matching skip them.

## Controls per treatment

**Default: {{max_controls_per_treatment}}. Form choices: 1, 3, 5, or 0
(full matching).**

The final matched-set ratio is set independently of the candidate-control
multiplier. More controls can increase information and may worsen match
quality. Controls are weighted so their total weight in a set equals the
number of treatment pixels.

Full matching permits variable-ratio sets and requires **optimal** matching.
Eligibility, exact-match restrictions, and calipers continue to apply.

## Minimum control distance

**Default: {{min_control_distance_km}} km. Form range: 0-500 km.**
Exclude controls near submitted treatment polygons to reduce spillover risk.
Set to **0** to disable. Larger buffers shrink the pool and may force more
distant comparisons.

Extraction computes approximate local-projection buffers and rasterizes them
by pixel centre. Spillovers or displacement can still affect controls outside
the buffer.

## Matching method

**Default: {{matching_method}}. Choices: optimal or nearest.**

- **Optimal (optmatch):** minimize total matching distance subject to the
  specified constraints; may require substantially more memory and time.
- **Nearest neighbour (MatchIt):** greedy matching, usually faster; available
  controls and match order can affect which treatment pixels obtain matches.

Review balance diagnostics for either algorithm. Full matching requires the
optimal method; nearest-neighbour matching supports fixed ratios.

## Shared-support controls

**Default for new analyses: {{shared_support_controls}}.**
Before sampling, restrict controls jointly to the treatment values of direct
numeric covariates constant throughout an eligible fragment/stratum.

For example, all-zero cropland treatment is compared with zero-cropland
controls. Equality uses exact stored values. The restriction filters
candidate controls, leaves the treatment pixels and covariate list unchanged,
and applies only to direct numeric terms.

If no controls remain, the stratum is explicitly unmatchable. The setting
checks shared support only for treatment-constant direct numeric covariates.

Historical tasks and presets missing the setting use the legacy policy.
Consult recorded settings and diagnostics to identify the policy used by an
older task. See the [methodology](matching.md#shared-support-controls).

## Separation fallback

**Default: {{separation_fallback_mahalanobis}}.**
Perfect or near-perfect separation makes logistic fitting unreliable.
When enabled, use Mahalanobis distance after removing separating terms.
Otherwise the stratum is not matched and diagnostics record the cause.
A site with no matched stratum fails.

Enabling fallback changes the information used for the match and can weaken
comparability. Examine which terms were removed; a successful match alone
does not establish an unbiased estimate.

## Replicates

**Default: {{n_replicates}}. Form range: 1-1,000, integer.**
Repeat sampling and matching with different reproducible random draws.
Results report means and 2.5%/97.5% quantiles across replicates.

These ranges summarize sampling and matching variability. They omit spatial
dependence, measurement, and model uncertainty, so they should not be
interpreted as confidence intervals.
With one replicate there is no informative between-replicate range.
Fewer than 50 replicates gives imprecise bounds and triggers a warning.

A stratum (for atom-enabled inputs, an atom within a stratum) contributes when
it matched in at least 80% of replicates. A replicate in which it did not
match takes its mean over the others. More replicates increase cost.

## Random seed

**Form range: 1-2,147,483,647, integer; initially generated randomly.**
Keep the seed to repeat pixel sampling with the same inputs, settings, and
pipeline version. Use the refresh button to request a new seed.

Reproducibility also depends on dataset and software versions, site geometries,
and settings. The form generates a seed; API submissions can omit one, making
the random sampling non-reproducible.

## Matching memory

**Default: {{match_memory_gb}} GB. Choices: 30, 60, or 120 GB.**
Initial memory per Nextflow matching worker. Confirmed out-of-memory failures
retry at higher memory, up to 120 GB. Larger allocations cost more.
This execution setting controls resource allocation.

## Worker queue

**Default: {{matching_job_queue}}. Choices: ae-spot-gp3 or ae-ondemand-gp3.**
Spot workers are the normal lower-cost choice. On-demand workers cost
substantially more. Queue selection controls scheduling and cost; statistical
settings determine the matching method.

## Review before submitting

Check dates, variable timing, geography, buffer, candidate pool, ratio,
caliper, fallback policies, seed, and estimated workload on the review tab.
After completion, read preparation and [quality diagnostics](interpreting-results.md)
before using the results.
