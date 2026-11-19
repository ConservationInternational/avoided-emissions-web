# In-App Documentation Section Plan

## Goal

Add a documentation section accessible from the app's top menubar and linked
from relevant places throughout the app. Provide both a plain-language
description of the method and a detailed explanation of the matching
methodology, with guidance for every matching setting and metadata for every
available covariate and exact-match reference layer.

Reuse the [existing documentation](index.md), reconciling its content against
the implementation before publishing it in the app.

## 1. Navigation and Documentation Layout

- Add **Documentation** to the shared top menubar in
  [common.py](../webapp/layouts/common.py).
- Introduce `/docs` pages with a responsive contents sidebar, clear headings,
  and stable section anchors.
- Support direct links, refresh, browser back/forward, and mobile navigation.
- Recommended access policy: make general documentation readable without
  signing in, including for shared-results viewers. Keep task data and
  administrative details behind existing access controls.

## 2. Content Structure

### How It Works

Provide an accessible explanation of:

- Treatment sites and comparison pixels.
- Why matching is needed and what a counterfactual means.
- How matched forest-loss differences become avoided-emissions estimates.
- A small worked example and a glossary.

Explain that matching improves comparability on observed characteristics;
it does not make an observational study equivalent to a randomized experiment.

### Setting Up an Analysis

Document site inputs, resolution, covariate selection, exact matching,
presets, and every exposed matching option. For each option, explain:

- Its purpose, default, units, and allowed values.
- Trade-offs and interactions with other settings.
- When a user might change it.
- Relevant warnings and failure conditions.

### Covariates and Reference Layers

Provide an entry for every available covariate and exact-match layer, with
searchable names and categories.

### Detailed Methodology

Explain:

- Pixel eligibility and baseline forest requirements.
- Geographic control selection and distance exclusions.
- Site preparation, fragments, and exact-match stratification.
- Propensity-score estimation and matching algorithms.
- Calipers, control ratios, and matched-set weights.
- Shared-support restrictions and fallback behavior.
- Sampling, replicates, and included strata.
- Coverage, represented population, and scaling to treatment area.
- Forest-loss and avoided-emissions calculations.

Explicitly describe which population the estimate represents. Explain why
replicate ranges are not confidence intervals.

### Interpreting Results

Cover balance plots, coverage, match rates, excluded strata, diagnostic
warnings, negative estimates, downloads, and replicate ranges.

### Limitations and References

Describe observational assumptions, unmeasured confounding, temporal leakage,
spillovers, spatial dependence, and model uncertainty. Include scientific
citations and links to source datasets.

## 3. Covariate and Control-Selection Metadata

Each variable entry should provide:

- Display name and internal identifier.
- Meaning, units, category codes, and scaling.
- Source, dataset version, citation, and authoritative link.
- Temporal coverage, native resolution, analysis resolution, and aggregation
  method.
- NoData handling and known limitations.
- Its role: model predictor, exact-match constraint, eligibility input, or
  outcome.
- Guidance on pre-intervention suitability and potential post-intervention
  bias.

Distinguish candidate control pixels, matched controls, and exact-match
reference layers. Explain that matching on protected-area status can select
controls in other protected areas, rather than necessarily selecting
unprotected land.

Cover dynamically available variables, not just the hard-coded default list.
Include SDG layers and derived pre-intervention deforestation.

## 4. Contextual Links Throughout the App

### Submission Form

In [submit.py](../webapp/layouts/submit.py):

- Add short explanations and **Learn more** links beside matching options.
- Provide per-variable metadata links near the covariate and exact-match
  selectors.
- Link resolution, sampling, control distance, ratio, caliper, fallback, and
  replicate settings to their exact documentation sections.
- Clearly distinguish the candidate-control multiplier from the matched
  controls-per-treatment ratio.

### Task Details, Results, and Maps

- Link recorded settings to their explanations.
- Link balance, coverage, exclusions, and warnings to interpretation guidance.
- Add metadata links to relevant map layers and covariate plots.

Keep essential guidance visible rather than relying solely on hover tooltips.
Open contextual documentation separately so users do not lose an unfinished
analysis form.

## 5. Implementation and Content Maintenance

- Render repository-owned Markdown within the existing Dash app; avoid a
  separate documentation service.
- Keep one canonical copy of explanatory content.
- Reuse defaults from
  [analysis_task.py](../webapp/services/analysis_task.py), source-layer
  definitions, and existing availability services rather than duplicating
  them.
- Keep scientific metadata separate from operational export status.
  Distinguish a documented dataset from one currently available at a given
  resolution.
- Package documentation in the application image. The current
  [Dockerfile](../webapp/Dockerfile) does not copy the documentation directory.
- Reconcile existing inconsistencies before publication, including conflicting
  descriptions of whether a variable can simultaneously serve as a covariate
  and exact-match constraint.
- Explain historical matching policies alongside current behavior so the
  documentation remains useful for older results.

## 6. Verification and Acceptance Criteria

- Documentation is reachable from every shared menubar.
- Every exposed matching setting and selectable variable has a working
  contextual destination.
- Internal links and anchors pass automated checks.
- Deep links and equations render correctly in the deployed image.
- Keyboard, mobile, and screen-reader navigation work.
- Opening help preserves submission values.
- Defaults and scientific descriptions are checked against the pipeline and
  focused tests, including the existing
  [pipeline tests](../r-analysis/tests/test_pipeline.R).
- Matching behavior, defaults, and task permissions remain unchanged.

## Delivery Order

1. Reconcile existing content with the implementation and define complete
   variable metadata.
2. Add documentation rendering, routing, navigation, and image packaging.
3. Add contextual links to submission, task details, results, and maps.
4. Validate accessibility, link coverage, deployed rendering, and scientific
   accuracy.

## Scope

This work adds documentation and contextual help. It does not change the
matching algorithms, analysis defaults, estimates, or task access controls.
