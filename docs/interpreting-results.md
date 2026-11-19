# Interpreting results

## Start with preparation and status

Check retained and excluded sites, uncovered reference area, split fragments,
and reference versions. Display labels may repeat; the immutable site ID and
downloadable ID/label mapping identify distinct sites.

An estimated site has a usable comparison. A partial site represents part
of its forest population or has failed fragments. Failed and not-applicable
sites have no usable estimate; inspect their recorded reasons.

## Coverage and match rates

- **Eligible pixels** have baseline forest and complete required inputs.
- **Represented pixels** are the eligible pixels the estimates stand for:
  those in strata matched in at least 80% of replicates. Where fewer than 50%
  of a stratum's sampled pixels match, only its matched share counts.
- **Coverage fraction** is represented pixels divided by all site pixels.
- **Forest coverage fraction** is represented pixels divided by forest pixels.
- **Sampled fraction** is sampled divided by eligible treatment pixels.
- **Match rate** is matched divided by sampled treatment pixels.

The pipeline warns when coverage or match rate is below 80%. A partial status
reflects forest coverage below 80%, failed fragments, or a stratum match rate
below 50%. The task page states the share of baseline forest that the task
totals represent. These thresholds flag
results for review; assess the diagnostics before interpreting an estimate.

The app's additional matched-pixel quality checks flag match rates below
25% as caution and below 5% as critical. These are separate from the
pipeline's 80% coverage/match-rate warning.

Extrapolated totals describe eligible pixels in included strata. Counterfactual
loss scales matched controls to those pixels; observed loss is summed over all
of them directly. The totals represent the full uploaded polygon only when all
of its area is represented, and are never scaled up to cover the remainder.
Scaling assumes unmatched treatment within an included stratum resembles
matched treatment, an assumption that is dropped when fewer than 50% of the
stratum's sampled pixels match. Excluded strata are omitted.

## Covariate balance

The Love plot summarizes differences between matched groups. For numeric
covariates the standardized mean difference (SMD) is:

$$
\mathrm{SMD} = \frac{\bar X_T - \bar X_C}{s_T}.
$$

The means use matching weights, and the denominator is the standard deviation
among matched treatment pixels. Categorical variables are assessed per level
using differences in proportions.

As a conventional guide, absolute SMD below 0.1 is desirable. The app marks
any absolute SMD at least 0.25 as critical, and more than 20% of covariates
above 0.1 as caution. SMD is undefined when treatment variance is zero and
the means differ. Balance for that covariate remains unresolved.

Read the [variable metadata](covariates.md) when interpreting units, categories,
and timing. Balance checks cover modeled variables; assess potential omitted
drivers separately.

## Distributions and propensity scores

Use weighted covariate histograms and propensity-score comparisons alongside
the Love plot. Similar means can hide different distributions.
Mahalanobis or equal-distance matches have no fitted propensity scores, so
their missing scores do not indicate probabilities of zero.

Check separation, missing-data, no-control, no-overlap, infeasible-ratio, and
inconsistent-stratum diagnostics. Changing settings to obtain more matches
can also change the scientific comparison.

## Forest loss and emissions

Avoided loss and emissions equal the weighted counterfactual outcome minus
the treatment outcome. Positive values indicate less observed treatment loss;
negative values indicate more.

Emissions are calculated from each pixel's change in forest cover, unmasked
year-2000 whole-cell total biomass, a carbon fraction of 0.5, and 44/12 to
convert carbon to CO2. Matching and sampling weights are applied to control
pixel outcomes; treatment outcomes are summed over all eligible pixels. See the
[calculation reference](analysis-outputs.md#biomass-to-co2e-conversion).

These model-based estimates cover forest-loss-associated emissions.
A complete greenhouse-gas inventory or credit certification requires
additional analyses.

## Checks on the comparison

`results_summary.json` carries three per-site checks. None changes an estimate.

- **Observed loss against the sample** (`treatment_census_check`): the expanded
  matched sample should agree with the loss summed over all eligible pixels.
- **Baseline stock** (`baseline_stock_balance`): matched treatment and control
  pixels should hold similar forest area and biomass at baseline. Loss is
  reported in hectares and tonnes, so an arm with less forest can lose less.
- **Pre-intervention loss** (`pre_intervention_check`): treatment and
  counterfactual loss should be close in the years before the intervention.

A large gap in the second or third check means the matched controls are a poor
comparison for that site, whatever its covariate balance shows.

## Replicate ranges

The reported bounds are 2.5% and 97.5% quantiles across matching replicates.
They summarize variability from random pixel sampling and matching and omit
other sources of uncertainty. Interpret them as descriptive matching ranges,
not confidence intervals.

Use at least 50 replicates for more stable quantile estimates. Additional
replicates do not address confounding or spatial dependence. Strata matched in
at least 80% of replicates contribute.

Observed loss comes from every eligible pixel and does not vary between
replicates, so the ranges reflect the counterfactual only.

For a combined site group or period, sum outcomes within each replicate,
then compute the mean and quantiles. Lower and upper bounds are not additive;
avoid summing them across sites or years. The range shown beside the task
total is computed this way, from the summed replicates.

## Maps and downloads

Map layers provide spatial context. Use each layer's **Metadata** link to
check its source, resolution, units, and time period when deciding whether
it is appropriate for matching. Documentation describes supported datasets;
map availability depends on deployed COGs and permissions.

The [output reference](analysis-outputs.md) explains file schemas.
Pixel downloads and balance diagnostics describe the included strata.
Many matched-pixel exports cover replicate 1 only.
Use diagnostic exports to investigate excluded strata and failed fits.

Read [limitations and references](limitations.md) before presenting findings.
