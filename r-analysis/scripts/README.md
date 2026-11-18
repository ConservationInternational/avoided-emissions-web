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

For atom-enabled inputs, extraction counts site and baseline-forest pixels for
every atom, including fragments without a matching stratum. Pixels from any
site are excluded from the control pool, and control-only tiles contribute no
atom counts.

## Nextflow on AWS Batch

Nextflow exclusively manages extraction, locality-batched matching, bounded
recovery, reconciliation, and summarization. Matching workers reuse loaded pixels
across replicate blocks and publish durable sample, stratum, and replicate
checkpoints. See the [operational contract](../../docs/matching-reliability-plan.md#8-implementation-and-operational-contract).
Standalone commands remain available for local debugging, not API dispatch.

Recovery tiers are skipped when no unfinished work remains. Reconciliation
accepts an empty final-tier outcome list and still verifies every expected
replicate against its durable checkpoints before summarization.

### API execution policy

[configuration.json](../configuration.json) publishes the Batch execution policy
required by the trends.earth API. Its default and only approved backend is
`nextflow`, including single-site and direct API submissions. It does not approve
legacy stage commands or pipeline descriptors. Initial matching memory
(up to 120 GiB) and queue preferences are validated by this workflow's driver; the driver,
workflow code, roles, timeouts, retry policy, and resource ceilings remain
server-controlled. Driver and workers use S3 and their job IAM roles.

The driver logs a Batch task summary every 60 seconds and when Nextflow exits.
The known remaining count includes submitted tasks in active Batch states; it
does not estimate tasks that Nextflow has not yet submitted. Unknown Batch jobs
are reported separately. Counts include attempts from previous resumes. Driver
logs are available through the API's execution `batch-logs` endpoint.
It also publishes an estimated percentage based on completed workflow
milestones, not elapsed time or a known total task count. The API reports 100%
only after validating successful completion and the published results.

Republish this configuration to **each target API** before enabling hardened
execution validation there. Apply the API's Batch policy database migration
first. Existing published script records do not automatically receive policy
changes from this repository. The publishing workflow selects the matching
`staging-latest` or `production-latest` Batch image.

Configure and enable the trusted driver on the API before deploying the webapp;
an unconfigured driver fails explicitly rather than using legacy orchestration.
See the [deployment guide](../../deploy/README.md#job-definitions).

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
   stratum. With `shared_support_controls: true`, first restrict controls
   jointly to values of direct numeric covariates constant across all eligible
   treatment in that stratum. Treatment and atom memberships are unchanged;
   the flag defaults to true. Set it to false to retain legacy control
   selection. Resumed workflows keep their frozen policy.
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

## Reconciliation and summary

After matching, Nextflow validates checkpoints in parallel batches of up to
25 fragments. Each batch produces one uncompressed archive, avoiding a separate
Nextflow staging operation for every sidecar and Parquet file. Missing,
corrupt, incomplete, or incompatible checkpoints fail reconciliation.

The summary processes validated match files in bounded parallel batches, then
merges them in input order. It retains the existing whole-site invalidation,
complete-stratum, per-replicate, and portfolio aggregation rules. The Batch
summary task requests four CPUs; the local profile uses one. Diagnostic JSONL
and replay-manifest encoding also use these workers in bounded, ordered batches.
Distinct diagnostic counts use hash sets rather than repeatedly copying growing
collections, and schema validation shares the diagnostic export's read pass.
Logs report match index/aggregation and each finalization phase, diagnostic
progress every 1,000 sidecars, and result collection/upload timings.

Atom estimates and coverage use only complete strata, just like site estimates.
The summary treats atom IDs as text when joining match data and declarations,
including when Parquet stores the IDs as integers.

Resuming a previous workflow still requires the original snapshot, inputs, and
checkpoints, but not the original driver version or currently deployed image.
Every resumed stage, including extraction and matching, uses the currently
deployed image. The original snapshot and checkpoint identity remain unchanged;
the resumed runtime records the current image and driver version. Matching code
fingerprints remain provenance, not a version gate for reusing checkpoints.
Statistical settings, input/atom identity, RNG state, and replay checksums still
gate reuse. Historical replay image references are preserved. A changed diagnostic
contract or changed parameters/raster/reference inputs still rejects resume.
New matching checkpoints validate their diagnostic schema,
matching policy, and atom/input compatibility. Do not mix pre-contract
checkpoints with new matching runs, or reuse non-atom outputs for atom-enabled
inputs. Use a fresh task for policy comparisons.

Every replicate writes structured diagnostics, including failed strata in
successful fragments. Summary publishes a gzip JSONL stream, compact counts,
and a failed-fit replay manifest under the normal task results prefix.
Reconciliation promotes referenced sample checkpoints to durable result
storage and verifies their checksums before declaring the work complete.
See [output contracts](../../docs/analysis-outputs.md#matching-diagnostic-exports).

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
    -e R_SCRIPTS_DIR=/pipeline/scripts \
    -v "$PWD/r-analysis:/pipeline:ro" \
    ae-ranalysis -c "Rscript -e \"install.packages('testthat')\" && cd /pipeline/tests && \
    Rscript -e \"testthat::test_dir('.')\""
```

Extraction regressions use the Python dependencies already in the image:

```bash
docker run --rm --entrypoint python \
    -v "$PWD/r-analysis:/pipeline:ro" \
    ae-ranalysis /pipeline/tests/test_extraction.py
```

### Shared-support pilot

Run the persisted, synthetic 100-replicate baseline/policy comparison in the
analysis image, using a new output directory:

```bash
R_SCRIPTS_DIR=../scripts Rscript run_shared_support_pilot.R /path/to/new-pilot
```

Run this command from `r-analysis/tests`. It retains configuration, logs,
samples, atom outputs, diagnostic exports and a report. It checks recovery of
shared-zero strata, unchanged mixed-atom cases, parent/atom reconciliation,
retained covariate constraints and absolute SMD at or below 0.1. It also
replays a replicate-2 GLM failure with checksum and environment validation.
The report records timing and diagnostic artifact sizes.
It is not a production deployment or a substitute for an atom-enabled real-data
pilot. For that pilot, export fresh Part A inputs, submit a new AE task, import
Part B atom results, and check original-site reconstruction and atom-union
rollups. Historical parent-only results cannot be prorated into atom results.
