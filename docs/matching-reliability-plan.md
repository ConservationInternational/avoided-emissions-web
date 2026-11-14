# Nextflow plan for matching performance and interruption recovery

Date: 2026-11-13. Status: Nextflow-only implementation; AWS canary required before deployment.

## 1. Decision and scope

Use **Nextflow on AWS Batch** to orchestrate extraction, locality-aware matching
batches, bounded recovery, and summarization. Keep the existing Python/R analysis
and containers. Do not build a custom distributed scheduler, work-lease service,
or replacement-job dependency controller. Do not migrate to Slurm or multi-node
parallel jobs for this change.

Use open-source Nextflow with standard AWS Batch/S3 staging and application-level
checkpoints. No licensed orchestration or checkpointing products are in scope.
AWS compute, storage, and request costs still apply.

The solution must handle arbitrary input sites and replicate counts, without
profiling previous runs or manually identifying expensive sites. Instrumentation
supports optimization; it is not a prerequisite for correct scheduling.

Keep locality-aware batching: a logical fragment/replicate is not necessarily a
Batch job. Workers reuse loaded inputs across several replicates and fragments.
For example, 100 fragments with 100 replicates create 10,000 logical work items,
not necessarily 10,000 jobs.

No finite resource budget guarantees completion of an arbitrary indivisible
optimal-matching problem. Exhausted budgets must produce explicit failures or
clearly labeled partial results, never silent methodology changes.

## 2. Evidence and existing integration points

The supplied example is a regression case, not a workload template:

- Array child 13 processes 25 fragments and replicates 31-40.
- Downloads take seconds, but one fragment's first replicate takes approximately
  3 h 54 min, matching 1,000 treatments against 50,000 sampled controls.
- The internal Python timeout kills R after 14,400 seconds. A failure marker is
  uploaded and the container exits successfully, preventing retries for that
  timeout.
- R writes results only at the end of the whole batch, losing earlier completed
  work when interrupted.
- Screenshots show another child exhausting five attempts with
  `Host EC2 ... terminated`. Host loss is established; distinguish Spot
  reclamation from other host terminations using AWS attempt/event metadata.

Original implementation at diagnosis (the legacy dispatcher has since been removed):

- [Submission/defaults](../webapp/services/analysis_task.py): up to 25 fragments
  and 10 replicates per child; four-hour match limit; five requested attempts.
- [R matching](../r-analysis/scripts/02_perform_matching.R): work selection,
  sampling, matching, seed derivation, and end-of-batch output.
- Former Python/Batch orchestration: subprocess timeout, failure markers,
  S3 transfers, execution parameters, and script invocation.
- [Summary](../r-analysis/scripts/03_summarize_results.R): only fragments matched
  in every requested replicate contribute results.
- [Execution policy](../r-analysis/configuration.json): approved Spot and
  On-Demand queues.

The trends.earth API owns Batch submission. Its companion implementation adds
the trusted driver launcher, authorization, cancellation, monitoring, and explicit
failed-execution resume. Deploy both repositories together.

## 3. Ownership and deployment

1. The API authorizes the analysis, records immutable inputs/configuration,
   launches one Nextflow workflow, and exposes status, results, and cancellation.
   Nextflow alone owns its child Batch jobs and dependencies. Disable the old
   pipeline dispatcher for migrated executions; never run both orchestrators.
2. Initially run the Nextflow driver as a supervised service/job on reliable
   On-Demand capacity. Persist driver metadata and S3 work/cache paths. Record
   the Nextflow session ID and provide controlled `-resume` after driver failure.
   Do not assume task resume automatically restarts a dead driver.
3. Use a pinned supported Nextflow release, pinned container digests, and
   versioned workflow/configuration. Validate required features against that
   release rather than copying options from newer documentation.
4. Configure the `awsbatch` executor, existing approved queues, S3 work storage,
   per-process concurrency limits, executor queue size, and submission rate.
   Keep AWS compute-environment limits as the aggregate capacity backstop across
   simultaneous analyses.
5. Apply least-privilege launcher/job IAM roles and private work/checkpoint paths.
   Do not widen workload execution policy or expose arbitrary user commands,
   containers, queue overrides, or workflow configuration.
6. Preserve standalone debugging commands. All new avoided-emissions API
   executions use Nextflow; drain legacy executions before deployment.

## 4. Workflow and data-local batching

Implemented workflow:

```text
EXTRACT -> PREPARE_MATCH_BATCHES -> MATCH_BATCH
                                      |
                           explicit unfinished-work outputs
                                      |
                            PREPARE_RECOVERY -> MATCH_RECOVERY
                                                   |
                                       bounded long-tier fallback
                                                   |
                                  RECONCILE -> SUMMARIZE -> PUBLISH
```

1. Wrap existing extraction and summarization commands as Nextflow processes.
   Do not nest the old pipeline runner inside Nextflow or duplicate its S3
   staging. Choose one staging owner for each artifact.
2. After extraction, create immutable descriptors for eligible fragments,
   replicate ranges, and shared control partitions. Use existing packing caps
   (25 fragments, 10 replicates) as initial caps, not indivisible completion
   requirements. Bound groups using extracted row/byte metadata and memory
   headroom, without historical site labels.
3. Keep R alive within each matching task. Load the group's required inputs once,
   reuse prepared data across replicates, and evict bounded caches when changing
   groups. Avoid downloading whole-task treatment/key files for every task;
   prepare filtered locality inputs once where worthwhile.
4. Split replicate ranges across a bounded number of tasks to use available
   capacity. Do not default to one task per replicate. Cap process concurrency
   and group fan-out to balance throughput against repeated reads.
5. Initially process replicates serially within a worker to preserve the existing
   memory model. Benchmark optional node-local parallelism separately; each
   solver needs memory, and R processes do not automatically share all inputs.
6. Emit explicit no-eligible/no-overlap outcomes without pointless solver work.
   Preserve fragment/replicate IDs and a persisted seed independently of task
   layout. Record the intentional change when previously unseeded tasks acquire
   a reproducible task seed.
7. Prefer ordinary Nextflow task submission initially. AWS job arrays are an
   optional submission optimization, not a requirement or a one-item-per-child
   design constraint.

## 5. Resume, checkpoints, and bounded tail handling

### Completed Nextflow tasks

Use Nextflow task caching and `-resume` to reuse completed extraction, preparation,
matching, and summary tasks. Preserve both task metadata and work outputs.
Use S3-backed cloud cache for driver recovery (`NXF_CLOUDCACHE_PATH`), with the
original session ID. Pass immutable artifact identities as task inputs so
overwritten S3 objects or changed sampling/matching configuration cannot reuse
incompatible results.

### Progress inside a batched R task

Nextflow's ordinary cache does not checkpoint a running R process:

1. Write durable results after each fragment/replicate, with independent stratum
   checkpoints where needed for long fragments. Upload data before committing
   its completion sidecar.
2. Store incremental checkpoints under a stable, versioned analysis/work identity
   in S3, not only an attempt's scratch directory. Nextflow retries may use
   different directories. Skip only committed, compatible checkpoints.
3. Persist enough sampling/model state to resume the same problem. Do not
   resample when retrying, splitting, or escalating resources. Preserve stable
   sampling semantics and fragment-wide counts/weights.
4. Make completion idempotent and avoid conflicting writers. Nextflow owns task
   scheduling, but externally published checkpoints still require safe commits.
5. Before starting another item, check a soft deadline and reserve upload/shutdown
   headroom. A planned yield emits a validated unfinished-work descriptor; it is
   successful workflow handoff, not successful analytical completion.
6. Use a small, explicit recovery DAG instead of a custom dynamic work queue:
   repack unfinished items into smaller locality batches, run `MATCH_RECOVERY`,
   then allow one bounded long-tier pass for remaining stragglers. Preserve
   completed checkpoints at every pass. Pass counts and resource ceilings are
   deployment policy, not site-specific tuning.
7. A hard timeout or crash is a failure, not a normal yield. Retry the same task
   from its checkpoints under the bounded policy below. Never turn arbitrary
   exceptions into apparently complete outputs.

Nextflow does not automatically split failed tasks into smaller tasks. The small
descriptor/repacking stage is application-specific; do not claim built-in adaptive
splitting. Exact-match strata can run independently, but never arbitrarily split
one connected optimal-matching problem into treatment/control chunks.

## 6. Host-loss retries, resource escalation, and finalization

1. Use Nextflow retry policies (`errorStrategy`, `maxRetries`, and `maxErrors`)
   plus dynamic `memory`, `time`, and `queue` directives for bounded resource
   tiers and approved On-Demand fallback.
2. Explicitly configure and test `aws.batch.maxSpotAttempts`: documented defaults
   changed to zero in Nextflow 24.10. Verify which host-loss reasons the pinned
   executor recognizes. Do not equate every `Host EC2*` failure with Spot reclaim.
3. Assign one owner to each retry layer. If native Batch retries are enabled,
   verify their interaction with Nextflow and bound the combined attempt count.
   Do not stack five native attempts and five workflow retries unintentionally.
4. Validate classification using actual Batch attempt/status metadata. Retry
   verified host loss; increase memory for confirmed OOM; increase time for
   confirmed timeout. Exit code 137 alone is insufficient to identify either
   host loss or OOM. Do not blindly retry deterministic input/application errors.
   If a small executor-status adapter is needed, keep it narrowly scoped.
5. After bounded Spot failures, resubmit through Nextflow on the approved
   On-Demand queue. This requires deployment approval for its cost implications.
   Checkpoint restore supplies progress; a terminal AWS job itself is not
   restarted. Track logical recovery budgets across handoff stages as well as
   retries so a new stage does not reset the overall ceiling.
6. Derive the Python/R internal deadline from the task's effective time budget,
   leaving transfer/shutdown headroom. Increasing Nextflow/AWS time alone must
   not leave the internal 14,400-second kill unchanged. AWS hard timeout is not
   natively retried, but the workflow can resubmit within its policy.
7. Reconcile all expected logical outputs before summary. Nextflow's dependencies
   replace the original array-parent dependency chain; no custom replacement
   jobs are submitted behind its back.
8. Default to an explicit failed workflow when recovery budgets are exhausted.
   Use `finish` rather than fail-fast termination where appropriate to preserve
   successful sibling work for resume. Partial summaries require an explicit
   policy and must retain the existing all-requested-replicates eligibility rule.
   Do not use blanket `ignore` to make incomplete runs appear successful.
9. Enforce bounded attempts, resource tiers, and workflow duration; monitor spend
   and configure cost alerts. Nextflow concurrency limits alone are not a hard
   dollar-spend limit. Cancellation must stop the driver and its Batch tasks and
   prevent automatic restart.

## 7. Validation and rollout

1. Build a minimal Nextflow proof of concept around existing commands and
   immutable fixtures before changing API ownership. Test locally and on AWS.
2. Verify identical seeded results across batching, retries, handoff stages, and
   concurrency layouts; preserve matching objective, calipers, ratios, weights,
   summary eligibility, and existing output compatibility.
3. Use heterogeneous generated inputs, many cheap fragments with 100 replicates,
   empty eligible sets, multiple strata, and unseen slow cases. Verify input-load
   counts/bytes are amortized across replicates, not one whole-data load per item.
4. Inject a host interruption in a dedicated non-production environment. Test
   automatic retry, exhausted-Spot On-Demand fallback, checkpoint reuse, and
   summary after recovery. Test OOM, internal/AWS timeout, deterministic failure,
   cancellation, driver restart/resume, and exhausted budgets separately.
5. Confirm Nextflow cache reuse and checkpoint compatibility after failed runs,
   including protection against stale checkpoints or changed input/configuration.
6. Measure bytes read, S3 requests, staging time, startup/idle time, matching CPU,
   peak memory, retries, walltime, and cost. Compare against current fixed batching
   and one-item-per-job execution. Same-region S3 reads need not incur transfer
   fees, but still consume requests, bandwidth, and paid compute time.
7. Profile model fitting, distance construction, and optimization to guide later
   improvements. Do not change samples/control pools, calipers, or matching method
   silently; extra CPUs do not guarantee a serial solver speedup.
8. Configure Nextflow infrastructure and API launch/status/cancellation
   integration, canary mixed workloads, and drain legacy executions before
   republishing the Nextflow-only script policy and deploying the webapp.

Acceptance: arbitrary inputs require no manual slow-site classification;
multi-replicate batches reuse data; host loss restarts automatically within
bounds; completed work survives retry/resume; incomplete work never becomes
silent success; failures beyond approved resources remain explicit and resumable.

## 8. Implementation and operational contract

### Code and defaults

- [Workflow and dependencies](../r-analysis/nextflow/main.nf),
  [pinned executor policy](../r-analysis/nextflow/nextflow.config), and
  [driver](../r-analysis/nextflow/driver.py).
- [Stage worker](../r-analysis/src/nextflow_worker.py) prepares shared treatment
  shards, bounds initial groups using treatment/control Parquet metadata, and
  repacks only explicitly unfinished items. Recovery first separates fragments
  and halves replicate ranges, then isolates remaining items for the long tier.
  Recovery processes are skipped when there is no unfinished work; empty
  descriptors do not launch no-op matching jobs. Shared control partitions are
  not split merely because their unchanged footprint exceeds memory headroom.
- [Artifact store](../r-analysis/src/workflow_store.py) publishes checksummed
  immutable objects before conditional completion receipts. The matching script
  persists sampled data plus RNG state, stratum results, and per-replicate outputs.
  Reconciliation requires an explicit compatible success or scientific failure
  for every expected fragment/replicate.
- Amazon Corretto **25**, Nextflow **26.10.6**, open-source AWS plugin **3.12.1**;
  no Fusion or Wave.
  The driver image preinstalls the plugin. Analysis and driver images must be
  deployed as digests, with a pinned driver Batch job-definition revision.
- Default: **20 concurrent tasks per workflow**, at most 25 fragments/10
  replicates per initial matching task, serial solves inside each task.
  Fragment count is after spatial splitting, not necessarily input site count.
  These limits do not imply 20 EC2 instances: Batch packs tasks onto its nodes.
- Native Batch attempts: **one**; Nextflow: at most **three submissions per
  process task**. Initial, recovery, and final matching are three fixed passes,
  at most nine submissions along one logical item's recovery path.
- Matching time: 4/8/16 hours across retries; final tier: 8/16/16 hours.
  Initial matching memory: 30 GiB by default, optionally 60/120 GiB through
  the webapp; confirmed-OOM ceiling: 120 GiB.
  Extraction starts at 128 GiB, confirmed-OOM ceiling 512 GiB; preparation and
  summary start at 32 GiB with a 128 GiB ceiling. Host failures alone do not
  increase memory. Summary time is 6/12/24 hours.
- Driver duration defaults to 72 hours (server-configurable, maximum 168).
  An indivisible solve exceeding approved resources fails explicitly; no
  fallback changes the statistical matching method.

### Deployment

1. Deploy the companion API changes, including the publisher-controlled backend
   default and allowlist. The avoided-emissions script defaults to and permits
   only `nextflow`; other API scripts retain their existing dispatch behavior.
   Drain legacy avoided-emissions executions before republishing the policy.
   There is no selectable legacy backend or fallback.
2. The publishing workflow builds the analysis image and separate Nextflow driver
   image. To build the latter manually from the repository root:

   ```bash
   docker build -f r-analysis/nextflow/Dockerfile \
     -t avoided-emissions-nextflow:local r-analysis
   ```

3. Register a **single-node, On-Demand driver** Batch job definition with its
   image digest, at least 2 vCPUs/4 GiB, sufficient ephemeral disk, and one native
   attempt. Its hard Batch timeout must exceed the configured driver budget plus
   shutdown headroom. The image's default command is
   `python /opt/ae-workflow/driver.py`; the API leaves that command untouched.
4. Configure API-side `NEXTFLOW_ENABLED`, `NEXTFLOW_DRIVER_JOB_DEFINITION` (revision ARN),
   `NEXTFLOW_DRIVER_JOB_QUEUE`, `NEXTFLOW_WORK_ROOT`, `NEXTFLOW_STATE_ROOT`,
   `NEXTFLOW_CHILD_JOB_QUEUES`, and optionally `NEXTFLOW_MAX_HOURS`.
   Enable its feature flag explicitly. Workflow-specific `AE_NEXTFLOW_*`
   settings belong in this workflow's driver job definition, not the API.
   The API resolves the analysis image digest and supplies execution-specific
   work/state URIs; clients cannot choose workflow code or configuration.
5. Grant the driver role only the required scoped S3 input/state/work/result
   access, ECR image lookup, Batch register/submit/describe/terminate/list and
   approved child-role `iam:PassRole`. Child roles need scoped artifact/input
   access; Batch compute instances need standard ECR/ECS/logging permissions.
   Check these against the pinned executor in a dedicated AWS environment.
6. Driver environment controls include `AE_NEXTFLOW_MAX_FORKS` (default 20,
   maximum 100), generic `NEXTFLOW_MAX_HOURS`, and
   `AE_NEXTFLOW_MATCH_MEMORY_MIB` (default 30720, maximum 122880).
   Set server-controlled `AE_NEXTFLOW_ALLOW_ONDEMAND=true` **only after cost
   approval**. Otherwise retries/final passes remain on the initial queue.
   The driver validates `match_memory_mib` (1-122880) and
   `matching_job_queue` against its configured Spot/On-Demand queues and freezes
   those preferences with the snapshot. An explicitly selected On-Demand queue
   does not require automatic fallback. Enforce aggregate Batch vCPU limits.
7. Preserve work outputs, cloud cache, snapshot, checkpoints, and session
   together. Set lifecycle retention longer than the advertised resume window;
   never expire only the work/cache prefix while promising resumability.
   Restrict mutation of execution artifacts. Unreferenced immutable objects
   from interrupted uploads can be reclaimed when the entire execution expires.
8. Configure AWS Budgets/CloudWatch alerts and complete the interruption/OOM/
   timeout/cancellation canaries in section 7 before deploying the webapp.

### Resubmission and input compatibility

Fresh identical API submissions intentionally start independent workflows.
Explicit resume targets the **original failed execution/session**, not a new
execution with matching JSON. The API owns authorization and idempotence.
Use `POST /api/v1/execution/<id>/resume` with the owner's/admin's
`execution:write` permission. Only a failed Nextflow execution with verified
child-job cleanup is resumable. Original parameter objects are reused unchanged.

The driver freezes the site artifact and seed in `snapshot.json`; initially
unseeded submissions intentionally acquire a reproducible seed. Resume requires
identical original parameters, image digest, workflow version, and raster/
reference object metadata. The driver checks the raster/reference manifest
before execution/resume and before publishing results, rejecting changed inputs.
Use write-once/versioned raster releases: metadata checks detect changes but do
not make mutable raster prefixes transactionally immutable during a running read.

The driver exports `session.json`, submitted child IDs in `jobs.json`,
`status.json` (`phase: "succeeded", success: true` only after result publication),
and diagnostics under the execution state prefix. Cancellation
uses an authoritative `cancelled.json` marker before stopping the driver/tasks.
Never clear that marker to resume a cancelled execution.

### Verification boundary

Local regression coverage includes Nextflow task retry, deterministic failure,
failed-workflow resume without repeating extraction, empty recovery handoffs,
replicate-block coverage and shared shards, checksum/conflicting commits,
strict reconciliation, frozen-input rejection, and seeded R checkpoint/stratum
resume for both matching methods. The driver container is built locally.

No production infrastructure is changed by this implementation. Real AWS host
interruption, IAM, queue placement, cloud-cache persistence after driver loss,
cost/scale, and deployment integration must pass the dedicated canary before
general enablement. Optional intra-node replicate parallelism remains deferred,
as specified above.

## References

- [Nextflow AWS Batch executor](https://docs.seqera.io/nextflow/reference/executor/aws-batch)
- [Nextflow cache and resume](https://docs.seqera.io/nextflow/cache-and-resume)
- [Nextflow dynamic task resources](https://docs.seqera.io/nextflow/process#dynamic-task-resources)
- [Nextflow AWS configuration](https://docs.seqera.io/nextflow/reference/config/aws)
- [Nextflow executor limits](https://docs.seqera.io/nextflow/reference/config/executor)
- [AWS Batch retries](https://docs.aws.amazon.com/batch/latest/userguide/job_retries.html)
- [AWS Batch timeouts](https://docs.aws.amazon.com/batch/latest/userguide/job_timeouts.html)
