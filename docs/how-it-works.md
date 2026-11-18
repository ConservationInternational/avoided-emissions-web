# How avoided-emissions matching works

## The question

How much forest loss, and associated carbon emissions, might a conservation
site have experienced without its intervention? We can observe what happened
inside the site, but not the alternative history of the same place without
the intervention. That alternative is the **counterfactual**.

The app estimates a counterfactual using comparison pixels outside all
submitted treatment sites. It chooses pixels with similar observed
characteristics, such as terrain, climate, population, accessibility, and
pre-intervention forest loss. This is called **matching**.

## The workflow

1. **Define the intervention.** Upload site polygons and their intervention
   dates, then choose the sites to analyse.
2. **Choose a comparison.** Select covariates, exact-match constraints, and
   other [matching settings](analysis-setup.md).
3. **Find similar pixels.** The pipeline checks data completeness and baseline
   forest, applies geographic restrictions, samples pixels, and matches
   treatment pixels to eligible controls.
4. **Compare outcomes.** It compares annual forest loss and biomass-related
   emissions in treatment pixels with their weighted matched controls.
5. **Check credibility.** Review [balance, coverage, and warnings](interpreting-results.md)
   before interpreting the estimated effect.

Candidate controls are not necessarily unprotected land. For example, exact
matching on protected-area status can compare a protected treatment site
with other protected areas. The selected settings define the comparison.

## A small example

Suppose the weighted comparison represents 100 hectares of eligible treatment
area. During one year, treatment pixels lose 2 hectares of forest, while their
matched controls imply a counterfactual loss of 5 hectares.

The estimated avoided forest loss is **5 - 2 = 3 hectares**. If the treatment
and counterfactual emissions are 400 and 900 MgCO2e respectively, estimated
avoided emissions are **900 - 400 = 500 MgCO2e**.

These illustrative numbers are already weighted totals. In real analyses,
biomass varies among pixels: avoided emissions are computed from pixel
outcomes, not by multiplying avoided hectares by an arbitrary carbon factor.

A negative estimate means treatment loss or emissions exceeded the matched
counterfactual. It should not be hidden or interpreted as proof that the
intervention caused harm.

## What this estimate does and does not establish

Matching improves comparability on measured characteristics. It does **not**
turn an observational comparison into a randomized experiment. Unmeasured
drivers, inappropriate time periods, poor overlap, or spillovers can still
bias results.

Estimates represent eligible pixels in strata matched in every replicate,
not automatically the entire submitted site. Repeating the matching measures
sampling and matching variability; its ranges are **not confidence intervals**
and do not capture all scientific uncertainty.

Read the [detailed methodology](matching.md) for the calculation and the
[limitations](limitations.md) before making causal or accounting claims.

## Glossary

| Term | Meaning |
|---|---|
| Treatment | Pixels inside an intervention site. |
| Candidate control | An outside-site pixel passing eligibility and geographic restrictions, before final matching. |
| Matched control | A candidate retained in a matched set and assigned a matching weight. |
| Covariate | An observed characteristic used to make groups comparable. |
| Exact-match stratum | A group sharing every selected exact-match attribute. Matches stay within this group. |
| Propensity score | The modelled probability of being a treatment pixel, given the selected covariates. |
| Caliper | A maximum allowed distance between propensity scores on the logit scale. |
| Balance | How similar treatment and control covariate distributions are after matching. |
| Coverage | How much of the site's population is represented by the estimate. |
| MgCO2e | Metric tonnes of carbon-dioxide equivalent. |
