# Setting up an analysis

This guide covers the controls exposed by the submission form. Defaults shown
in the app are read from the same configuration as the form. Values inside
double braces in this source document are substituted when displayed.

## Sites and dates

Upload reusable site sets through **Admin > Site Uploads**, then select a site
set on the **Task & Sites** tab. Supported inputs include GeoJSON, GeoPackage,
and supported vector datasets in ZIP/TAR archives. Map the source columns:
only the intervention start date is required; IDs, names, and end dates are
optional. Use explicit dates, not numeric years or spreadsheet serial dates.

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

A finer grid creates more pixels and can increase runtime and cost; it does
not add detail to a coarser source dataset. The form only lists covariates
ready at the selected resolution. A documented layer may not yet be deployed
at both resolutions.

## Presets

Save and load covariates, exact-match variables, and matching settings together.
Review the loaded settings and layer availability before submission. A preset
does not freeze datasets or guarantee a scientifically appropriate comparison.

Historical presets without shared-support settings use the legacy policy.
Recomputing a task retains its recorded policy and settings while generating
a fresh seed; record the seed when you need an exactly repeatable sample.

## Covariates

Choose variables describing both treatment selection and forest-loss risk.
Use the [variable reference](covariates.md) to check sources, dates, units,
processing, and limitations before selection.

Avoid variables measured after intervention when they may have been affected
by it. Default selection is a starting point, not a guarantee of
pre-intervention timing: population snapshots and growth periods may overlap
early interventions. More variables can improve comparability but also cause
missing-data exclusions, poor overlap, separation, or excessive model complexity.

Forest-cover outcomes and the historical biomass input are extracted
automatically. Pre-intervention deforestation is automatically added when the
forest record permits it.

## Exact matching

Choose at least one of country (`admin0`), province/state (`admin1`), district
(`admin2`), ecoregion, and protected-area status (`pa`). The initial selection
is `admin1`, `ecoregion`, and `pa`.

Exact matching forces equality on all selected attributes. It prevents
cross-region comparisons, but can leave no eligible controls.
`ecoregion` and `pa` can be predictors **or** exact-match constraints, never
both simultaneously. Other administrative IDs are exact-match constraints.

The protected-area mask includes IUCN categories Ia-IV only. Matching on
`pa` can compare protected treatment to other protected controls.
Omit that constraint if unprotected controls should be eligible; this does
not itself exclude protected controls from the pool.

## Max treatment pixels

**Default: {{max_treatment_pixels}}. Form range: 1-100,000, integer.**
Maximum treatment pixels sampled without replacement per exact-match stratum
of each site fragment, not a single cap across the entire submitted site.

Larger samples can improve representation but make optimal matching expensive.
Review sampling fractions and match rates; a large requested sample does not
ensure all sampled treatment pixels can be matched.

## Control multiplier

**Default: {{control_multiplier}}. Form range: 1-500, integer.**
Sample up to this many candidate controls per sampled treatment pixel within
each stratum.

This is a candidate-pool ceiling, **not** the final controls-per-treatment
ratio and not a minimum required number of controls. Increasing it gives the
algorithm more choices at a memory and runtime cost. A scarce pool is not
automatically a failure if feasible matches remain.

## Minimum site area

**Default: {{min_site_area_ha}} hectares. Form range: 0-100,000.**
The whole-site area threshold is applied before reference-region splitting,
so a retained site can contain smaller fragments. Small sites may have too
few eligible pixels for meaningful matching.

## Minimum GLM treatment pixels

**Default: {{min_glm_treatment_pixels}}. Form range: 1-10,000, integer.**
Below this sampled treatment count within a stratum, use Mahalanobis distance
instead of fitting a logistic propensity model.

This small-sample fallback does not require the separate separation-fallback
checkbox. Mahalanobis matching has no propensity score and no caliper.

## Caliper width

**Default: {{caliper_width}}. Form range: 0-5 standard deviations.**
Maximum distance in units of the within-stratum standard deviation of the
logit propensity score, across sampled treatment and controls.

Set to **0** to disable. Tighter calipers can reject poor comparisons but
reduce match rates or make a requested optimal ratio infeasible. A caliper
does not apply to Mahalanobis or equal-distance matching.

## Controls per treatment

**Default: {{max_controls_per_treatment}}. Form choices: 1, 3, 5, or 0
(full matching).**

The final matched-set ratio differs from the candidate-control multiplier.
More controls can increase information but force worse matches. Controls are
weighted so their total weight in a set equals the number of treatment pixels.

Full matching permits variable-ratio sets and requires **optimal** matching.
It does not override eligibility, exact-match restrictions, or calipers.

## Minimum control distance

**Default: {{min_control_distance_km}} km. Form range: 0-500 km.**
Exclude controls near submitted treatment polygons to reduce spillover risk.
Set to **0** to disable. Larger buffers shrink the pool and may force more
distant comparisons.

Extraction computes approximate local-projection buffers and rasterizes them
by pixel centre. This is not a guarantee of zero interference between sites
and controls.

## Matching method

**Default: {{matching_method}}. Choices: optimal or nearest.**

- **Optimal (optmatch):** minimize total matching distance subject to the
  specified constraints; may require substantially more memory and time.
- **Nearest neighbour (MatchIt):** greedy matching, usually faster; available
  controls and match order can affect which treatment pixels obtain matches.

Neither algorithm guarantees acceptable covariate balance. Inspect diagnostics.
Full matching is not supported with nearest neighbour.

## Shared-support controls

**Default for new analyses: {{shared_support_controls}}.**
Before sampling, restrict controls jointly to the treatment values of direct
numeric covariates constant throughout an eligible fragment/stratum.

For example, all-zero cropland treatment is compared with zero-cropland
controls. Equality uses stored values, not a tolerance. This restriction
does not drop treatment pixels or discard covariates, and it does not apply
to categorical, interaction, or transformed terms.

If no controls remain, the stratum is explicitly unmatchable. This setting
does not establish general multivariate overlap.

Historical tasks and presets missing the setting use the legacy policy.
Consult recorded settings and diagnostics, not today's default, for an old
task. See the [methodology](matching.md#shared-support-controls).

## Separation fallback

**Default: {{separation_fallback_mahalanobis}}.**
Perfect or near-perfect separation makes logistic fitting unreliable.
When enabled, use Mahalanobis distance after removing separating terms.
Otherwise the stratum is not matched and diagnostics record the cause.
A site with no matched stratum fails.

Enabling fallback changes the information used for the match and can weaken
comparability. Examine which terms were removed; a successful job is not
evidence of an unbiased estimate.

## Replicates

**Default: {{n_replicates}}. Form range: 1-1,000, integer.**
Repeat sampling and matching with different reproducible random draws.
Results report means and 2.5%/97.5% quantiles across replicates.

These ranges describe sampling and matching variability, **not confidence
intervals**. They omit spatial dependence, measurement and model uncertainty.
With one replicate there is no informative between-replicate range.
Fewer than 50 replicates gives imprecise bounds and triggers a warning.

Only strata matched in every replicate contribute to the estimates. More
replicates increase cost and may expose inconsistent strata.

## Random seed

**Form range: 1-2,147,483,647, integer; initially generated randomly.**
Keep the seed to repeat pixel sampling with the same inputs, settings, and
pipeline version. Use the refresh button to request a new seed.

A seed alone does not freeze changing datasets, software, site geometries,
or settings. The pipeline permits an unspecified seed, which is not
reproducible; the form normally supplies one.

## Matching memory

**Default: {{match_memory_gb}} GB. Choices: 30, 60, or 120 GB.**
Initial memory per Nextflow matching worker. Confirmed out-of-memory failures
retry at higher memory, up to 120 GB. Larger allocations cost more.
This is an execution setting, not a scientific matching parameter.

## Worker queue

**Default: {{matching_job_queue}}. Choices: ae-spot-gp3 or ae-ondemand-gp3.**
Spot workers are the normal lower-cost choice. On-demand workers cost
substantially more. Queue selection affects scheduling and cost, not the
intended statistical method.

## Review before submitting

Check dates, variable timing, geography, buffer, candidate pool, ratio,
caliper, fallback policies, seed, and estimated workload on the review tab.
After completion, read preparation and [quality diagnostics](interpreting-results.md)
before using the results.
