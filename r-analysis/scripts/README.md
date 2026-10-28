# Avoided Emissions Analysis Scripts

## Overview

The pipeline has three steps:

1. **Extract covariates** (`01_extract_covariates.py`) - read covariate COGs
   (all on one pixel grid) and write treatment and candidate control pixel
   values.
2. **Perform matching** (`02_perform_matching.R`) - within each exact-match
   stratum of each site fragment, sample pixels and match treatment to
   control pixels.
3. **Summarize results** (`03_summarize_results.R`) - compute forest loss and
   avoided emissions (MgCO2e) for matched pixels and report means and
   replicate ranges.

Pure statistical functions shared by steps 2 and 3 live in
`stats_functions.R` and are unit-tested in `../tests`. The full methodology
is described in [docs/matching.md](../../docs/matching.md) and the outputs in
[docs/analysis-outputs.md](../../docs/analysis-outputs.md).

## AWS Batch Integration

- **Step 1 (extract)** runs as a single job.
- **Step 2 (match)** runs as an array job. Each element processes one work
  unit (from `exact_match_group_mapping`, or one fragment without a mapping)
  for one replicate:
  `unit = index %/% n_replicates`, `replicate = index %% n_replicates + 1`.
  Submission packs at most `match_batch_size` fragments into each unit.
  Pixel data is loaded once per element.
- **Step 3 (summarize)** runs as a single job after matching completes.

## Matching (`02_perform_matching.R`)

For each site fragment:

1. Keep eligible pixels: forest cover in the baseline year
   (`start_year - 6`), complete covariates and outcome inputs, and a shared
   exact-match stratum. Controls within `min_control_distance_km` of any site
   are excluded.
2. For sites starting in 2006 or later, add `defor_pre_intervention`
   (forest cover change from `start_year - 6` to `start_year - 1`).
3. For each replicate, sample up to `max_treatment_pixels` treatment pixels
   and `control_multiplier` controls per sampled treatment pixel in each
   stratum.
4. In each stratum, choose the distance:
   - fewer than `min_glm_treatment_pixels` treatment pixels: Mahalanobis;
   - separation (detected before fitting, or GLM non-convergence / fitted
     probabilities of 0 or 1): Mahalanobis without the separating variables
     if `separation_fallback_mahalanobis` is set, otherwise the stratum is
     not matched;
   - otherwise: logistic propensity score, with a caliper of `caliper_width`
     SDs of the linear predictor.
5. Match with `optmatch` (`optimal`: k:1 pair matching or full matching) or
   `MatchIt` (`nearest`: greedy k:1). Control weights in each matched set sum
   to the number of treated pixels. Stratum weights
   `n_eligible / n_matched` scale results to eligible pixels in that stratum.
   Summary includes only strata matched in every replicate, including in
   the pixel-level downloads and balance diagnostics.

## Tests

```bash
cd r-analysis/tests
R_SCRIPTS_DIR=../scripts Rscript -e "testthat::test_dir('.')"
```

`test_stats_functions.R` needs only `dplyr` and `testthat`.
`test_pipeline.R` runs steps 2 and 3 end to end on synthetic data and needs
the analysis container's packages (`arrow`, `sf`, `optmatch`, `MatchIt`); it
is skipped when they are missing. To run everything in the container image:

```bash
docker build -t ae-ranalysis r-analysis
docker run --rm --entrypoint bash \
    -e R_SCRIPTS_DIR=/app/scripts \
    -v "$PWD/r-analysis/scripts:/app/scripts:ro" -v "$PWD/r-analysis/tests:/tests:ro" \
    ae-ranalysis -c "Rscript -e \"install.packages('testthat')\" && cd /tests && \
    Rscript -e \"testthat::test_dir('.')\""
```

Extraction regressions use the Python dependencies already in the image:

```bash
docker run --rm --entrypoint python \
    -v "$PWD/r-analysis:/pipeline:ro" \
    ae-ranalysis /pipeline/tests/test_extraction.py
```

The extraction requirements keep NumPy below 2 for the system GDAL ABI and
PyArrow below 26, which requires NumPy 2.
