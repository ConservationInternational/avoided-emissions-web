# Limitations and references

## Observational identification

A causal interpretation requires that measured pre-intervention covariates
adequately account for differences affecting both intervention placement and
forest loss, with suitable overlap between treatment and controls.
Unmeasured governance, tenure, enforcement, threats, and economic changes can
violate that assumption. Treatment assignment remains observational.

Results principally concern the eligible treatment population in consistently
matched strata. Extrapolation within a stratum assumes unmatched treatment
resembles matched treatment. Entirely unmatched strata cannot be recovered
by larger weights.

## Temporal leakage

A variable recorded after intervention can be an outcome of the intervention.
Matching on it can bias the comparison. Check population, accessibility,
cropland, SDG, and carbon-layer dates against each intervention period.

The current pipeline requires unmasked year-2000 total biomass as its
historical emissions reference. Loss-masked 2025 biomass represents remaining
stock and can encode subsequent forest loss.

## Spatial dependence and interference

Nearby pixels share environmental and social conditions and have spatial
dependence. A distance buffer reduces proximity to treatment sites. Residual
spillovers, displacement, leakage, and other interventions can still affect
controls.

Smaller grid cells increase pixel counts; spatial dependence limits the
corresponding increase in independent information. Resampling coarse layers
to 250 m does not add source detail.

## Measurement and model uncertainty

Source datasets have errors, incomplete coverage, changing versions, and
different definitions. Hansen tree-cover loss does not always represent
permanent deforestation. The pipeline's forest-cover series excludes
regrowth. Biomass and carbon-conversion assumptions introduce additional
uncertainty.

The logistic model, selected covariates, fallback policy, matching algorithm,
caliper, and control geography affect estimates. Balance diagnostics reveal
some problems but cannot validate all assumptions.

Replicate quantiles summarize sampling and matching variability. They omit
uncertainty from spatial dependence, measurement, model choice, and unmeasured
confounding, so they should not be interpreted as confidence intervals.

## Reproducibility and historical results

Retain input geometry, intervention dates, selected covariates, exact-match
variables, all settings, random seed, dataset/reference versions, software
revision, and diagnostic exports.

Historical tasks or saved presets missing shared-support settings retain the
legacy policy; new tasks normally enable `shared_support_v1`. Recomputing or
resuming uses recorded policy where applicable. Check task provenance to
identify the settings, data, and biomass inputs used in older results.

## Scientific references

- Rosenbaum and Rubin (1983), The central role of the propensity score in
  observational studies for causal effects.
  [Biometrika](https://doi.org/10.1093/biomet/70.1.41).
- Austin (2011), Optimal caliper widths for propensity-score matching.
  [Pharmaceutical Statistics](https://doi.org/10.1002/pst.433).
- Hansen et al. (2013), High-resolution global maps of 21st-century forest
  cover change. [Science](https://doi.org/10.1126/science.1244693).
- Mokany et al. (2006), Critical analysis of root:shoot ratios in terrestrial
  biomes. [Global Change Biology](https://doi.org/10.1111/j.1365-2486.2005.001043.x).
- Weiss et al. (2018), A global map of travel time to cities.
  [Nature](https://doi.org/10.1038/nature25181).
- Potapov et al. (2022), Global maps of cropland extent and change.
  [Nature Food](https://doi.org/10.1038/s43016-021-00429-z).
- Noon et al. (2022), Mapping the irrecoverable carbon in Earth's ecosystems.
  [Nature Sustainability](https://doi.org/10.1038/s41893-021-00803-6).

## Dataset references

The [searchable variable reference](covariates.md) provides dataset-specific
links, versions, units, processing, and caveats for every supported layer.
For imported references or externally prepared COGs, use task provenance to
identify the deployed artifact version; publication dates alone may not
identify it.
