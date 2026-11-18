# Interpreting results

## Start with preparation and status

Check retained and excluded sites, uncovered reference area, split fragments,
and reference versions. Display labels may repeat; the immutable site ID and
downloadable ID/label mapping identify distinct sites.

An estimated site has a usable comparison. A partial site represents only
part of its forest population or has failed fragments. Failed and
not-applicable sites are not zero-effect sites: inspect their recorded reasons.

## Coverage and match rates

- **Eligible pixels** have baseline forest and complete required inputs.
- **Represented pixels** belong to eligible strata matched in every replicate.
- **Coverage fraction** is represented pixels divided by all site pixels.
- **Forest coverage fraction** is represented pixels divided by forest pixels.
- **Sampled fraction** is sampled divided by eligible treatment pixels.
- **Match rate** is matched divided by sampled treatment pixels.

The pipeline warns when coverage or match rate is below 80%. A partial status
also reflects low forest coverage or failed fragments. These thresholds
highlight limitations; they do not establish that estimates above them are
unbiased.

The app's additional matched-pixel quality checks flag match rates below
25% as caution and below 5% as critical. These are separate from the
pipeline's 80% coverage/match-rate warning.

Extrapolated totals scale matched pixels to eligible pixels in consistently
matched strata, not automatically to every hectare of the uploaded polygon.
This assumes unmatched treatment within those strata resembles matched
treatment. Completely excluded strata are not represented.

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
above 0.1 as caution. Undefined SMD, such as unequal means with zero treatment
variance, is a warning rather than evidence of balance.

Read the [variable metadata](covariates.md) when interpreting units, categories,
and timing. Good observed balance cannot demonstrate balance on omitted drivers.

## Distributions and propensity scores

Use weighted covariate histograms and propensity-score comparisons alongside
the Love plot. Similar means can hide different distributions.
Missing propensity scores are expected for Mahalanobis or equal-distance
matches; they are not fitted probabilities of zero.

Check separation, missing-data, no-control, no-overlap, infeasible-ratio, and
inconsistent-stratum diagnostics. Changing settings to obtain more matches
can also change the scientific comparison.

## Forest loss and emissions

Avoided loss and emissions equal the weighted counterfactual outcome minus
the treatment outcome. Positive values indicate less observed treatment loss;
negative values indicate more.

Emissions use unmasked year-2000 whole-cell total biomass, change in forest
cover relative to year-2000 cover, a carbon fraction of 0.5, and 44/12 to
convert carbon to CO2. They are not obtained by multiplying every avoided
hectare by a global biomass constant. See the
[calculation reference](analysis-outputs.md#biomass-to-co2e-conversion).

These are model-based forest-loss-associated emissions estimates, not a
complete greenhouse-gas inventory or automatic certification of carbon credits.

## Replicate ranges

The reported bounds are 2.5% and 97.5% quantiles across matching replicates.
They measure variability from random pixel sampling and matching, **not
confidence intervals** or total uncertainty.

Use at least 50 replicates for less unstable quantile estimates, while
recognizing that more replicates do not address confounding or spatial
dependence. Only consistently matched strata contribute.

For a combined site group or period, sum outcomes within each replicate,
then compute the mean and quantiles. Do not sum lower and upper bounds from
individual sites or years.

## Maps and downloads

Map layers provide context, not evidence that a covariate is causally
appropriate. Use each layer's **Metadata** link to check its source,
resolution, units, and time period. Documentation describes supported
datasets; map availability depends on deployed COGs and permissions.

The [output reference](analysis-outputs.md) explains file schemas.
Pixel downloads and balance diagnostics describe consistently included strata;
many matched-pixel exports describe replicate 1, not every replicate.
Use diagnostic exports to investigate excluded strata and failed fits.

Read [limitations and references](limitations.md) before presenting findings.
