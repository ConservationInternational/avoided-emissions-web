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
- **Represented pixels** belong to eligible strata matched in every replicate.
- **Coverage fraction** is represented pixels divided by all site pixels.
- **Forest coverage fraction** is represented pixels divided by forest pixels.
- **Sampled fraction** is sampled divided by eligible treatment pixels.
- **Match rate** is matched divided by sampled treatment pixels.

The pipeline warns when coverage or match rate is below 80%. A partial status
also reflects low forest coverage or failed fragments. These thresholds flag
results for review; assess the diagnostics before interpreting an estimate.

The app's additional matched-pixel quality checks flag match rates below
25% as caution and below 5% as critical. These are separate from the
pipeline's 80% coverage/match-rate warning.

Extrapolated totals scale matched pixels to eligible pixels in consistently
matched strata. They represent the full uploaded polygon only when all of its
area is represented. This assumes unmatched treatment within included strata
resembles matched treatment. Completely excluded strata are omitted.

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
convert carbon to CO2. Matching and sampling weights are applied to pixel
outcomes. See the
[calculation reference](analysis-outputs.md#biomass-to-co2e-conversion).

These model-based estimates cover forest-loss-associated emissions.
A complete greenhouse-gas inventory or credit certification requires
additional analyses.

## Replicate ranges

The reported bounds are 2.5% and 97.5% quantiles across matching replicates.
They summarize variability from random pixel sampling and matching and omit
other sources of uncertainty. Interpret them as descriptive matching ranges,
not confidence intervals.

Use at least 50 replicates for more stable quantile estimates. Additional
replicates do not address confounding or spatial dependence. Only consistently
matched strata contribute.

For a combined site group or period, sum outcomes within each replicate,
then compute the mean and quantiles. Lower and upper bounds are not additive;
avoid summing them across sites or years.

## Maps and downloads

Map layers provide spatial context. Use each layer's **Metadata** link to
check its source, resolution, units, and time period when deciding whether
it is appropriate for matching. Documentation describes supported datasets;
map availability depends on deployed COGs and permissions.

The [output reference](analysis-outputs.md) explains file schemas.
Pixel downloads and balance diagnostics describe consistently included strata.
Many matched-pixel exports cover replicate 1 only.
Use diagnostic exports to investigate excluded strata and failed fits.

Read [limitations and references](limitations.md) before presenting findings.
