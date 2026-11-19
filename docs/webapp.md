# Web Application Guide

The web application is a [Dash](https://dash.plotly.com/) (Plotly) single-page
application backed by Flask, PostgreSQL/PostGIS, Celery, and Valkey. It is the
primary user interface for uploading sites, submitting analysis tasks, monitoring
progress, and viewing results.

## Contents

- [Authentication and Roles](#authentication-and-roles)
- [Documentation and Contextual Help](#documentation-and-contextual-help)
- [Site Upload](#site-upload)
- [Submitting a Task](#submitting-a-task)
- [Task Monitoring](#task-monitoring)
- [Viewing Results](#viewing-results)
- [Sharing Results](#sharing-results)
- [Admin Panel](#admin-panel)
- [Background Workers](#background-workers)

---

## Documentation and Contextual Help

Select **Documentation** in the top menubar to read the public
[overview](how-it-works.md), [setup guide](analysis-setup.md),
[variable reference](covariates.md), [methodology](matching.md),
[results guide](interpreting-results.md), and [limitations](limitations.md).
These pages are available without signing in, including to shared-results
viewers; they do not grant access to task data.

**Learn more** links beside submission settings open the corresponding section
in a separate tab, preserving unfinished form values. Expand the metadata
lists near covariate and exact-match selectors for per-variable links.
Recorded task settings, quality warnings, match-quality plots, and map layers
also link to explanations or metadata.

The variable catalog can be searched by identifier, readable name, category,
or source. It documents supported datasets independently of their current
availability at a particular analysis resolution.

## Authentication and Roles

Users sign in with their Trends.Earth account using OpenID Connect
(authorization code + PKCE, confidential client). There are no local
passwords or registrations; the app keeps no user table and keys all data by
the Trends.Earth subject (`sub`).

New users can select **Register with Trends.Earth** on the sign-in page to
open the [Trends.Earth registration page](https://api.trends.earth/register).
After registering, return to this app and sign in to request access.

Two app roles exist, granted in Trends.Earth:

| Role | Capabilities |
|---|---|
| `member` | Upload sites, submit tasks, view their own results, share links |
| `admin` | All member capabilities plus: approve users and change roles, trigger GEE exports, manage covariates, view and manage all tasks |

Trends.Earth superadmins are always `admin`.

### Access requests and approval

The first sign-in records a **pending** access request in Trends.Earth, and
the user sees an "Access requested" page. Trends.Earth emails:

- every app admin of Avoided Emissions who can approve users (linking to
  **Admin → Users** in this app), and
- the Trends.Earth admins/superadmins configured as notification recipients
  for the app in the Trends.Earth API UI (linking to its **App Access** tab).

Anyone in both groups gets one email. Admins approve, revoke or change roles
from **Admin → Users**; the app calls the Trends.Earth app-users API with the
admin's own access token. Revoked users are signed out the next time their
access token is refreshed (within about five minutes).

### Sessions and tokens

The Flask session cookie holds only an opaque session id. The session record
(identity, access token, rotating refresh token and ID token) lives in Valkey
so the Celery workers can submit analyses with the user's token. Refreshes
are serialised per session with a Redis-compatible lock, because Trends.Earth
revokes the whole refresh-token family if a rotated token is reused. Sessions
last up to 30 days (`OIDC_SESSION_MAX_AGE`) while Trends.Earth keeps honouring
the refresh token.

Signing out revokes the refresh token, clears the session and ends the
Trends.Earth browser session via its logout endpoint. Password resets happen
on Trends.Earth (a link is on the **Profile** page).

### Profile

The **Profile** page shows the Trends.Earth name, email and app role, and lets
users delete all of their Avoided Emissions data (tasks, results, share links,
site sets, uploads and presets). This does not affect their Trends.Earth
account or their access to the app.

---

## Site Upload

Sites are uploaded as GeoJSON or GeoPackage files. The required attribute
fields are documented in the main [README](../README.md#site-input-format).
Archives may contain exactly one supported dataset, including Shapefile.
The upload panel shows the limits before transfer: 800 MiB per HTTP request
(including form data); archives allow 2,000 files, 512 MiB per member,
1 GiB expanded total and at most 200:1 compression.

### Upload flow

1. The user selects a file in the **Upload Sites** panel. Transfer progress and
   server-side preview are separate phases; **Cancel transfer** stops an active
   request.
2. The file is staged to S3 (`{S3_PREFIX}/site-upload-stage/`) under a
   time-limited token (12-hour TTL).
3. A bounded schema/sample preview supplies mapping choices with example values.
   The user selects a required start-date column and optional ID, name and
   end-date columns, then confirms the mapping.
4. A durable `UserSiteUpload` record is queued for
   `tasks.import_user_site_upload`. The worker validates and imports features
   into PostGIS; import progress and diagnostics appear in **Recent Import History**.
5. On success the reusable set appears in the **Site Set Catalog** and submission
   selector. The catalog is independent of the latest 50 import jobs, includes
   legacy sets and supports pagination, renaming, archiving/restoring and deletion.

### Validation

The upload task checks:

- Polygon or multipolygon geometries are repaired and validated, including
  finite coordinate/bounds checks. GeoJSON defaults to RFC 7946 WGS84; other
  datasets require a declared CRS and are transformed to EPSG:4326.
- A required `start_date` mapping is supplied. Optional IDs and names are
  generated when omitted.
- `start_date` (and optional `end_date`) must use ISO 8601 calendar-date
  format (YYYY-MM-DD); numeric years and spreadsheet serial dates are rejected.
- Duplicate display `site_id` values are accepted. Immutable feature UUIDs are
  canonical analysis identities; original labels are retained as
  `display_site_id`.

Validation errors and import diagnostics are displayed with the import history.
Mapping and upload status changes are announced to assistive technology.
Import history includes the active phase and scanned/accepted/skipped counts;
failure diagnostics are readable directly in the table. Claims and publication
are durable and idempotent. Cancellation is cooperative: it records a terminal
state, lets the importer roll back unpublished data, and does not kill its worker
process. Failed inputs remain staged until their expiry rather than being
silently deleted before diagnosis.

Beat recovers interrupted dispatches every five minutes, at most 100 jobs per
pass. Failed or unknown sends have a ten-minute cooldown and six dispatch
reservations; confirmed broker deliveries are not repeatedly sent or failed
just because the heavy queue is busy. Delayed dispatch is shown as a warning,
not a successful queue notification. Imports allow three worker claims.
Pending jobs explicitly expire after 24 hours; stage tokens themselves remain
valid for 12 hours, so an import must start within that source-retention window.
Running jobs are only
recovered after 140 minutes without activity, beyond the 125-minute worker hard
limit. Recovery rechecks cancellation and terminal state before dispatching.
Restart workers and Beat when deploying these changes.

Administrators can inventory stale jobs and expired **local** staging without
accessing cloud storage:

```bash
PYTHONPATH=webapp:. python webapp/scripts/audit_site_uploads.py
# Review the inventory before explicitly applying cleanup:
PYTHONPATH=webapp:. python webapp/scripts/audit_site_uploads.py --apply
```

Beat also applies local cleanup hourly. Live jobs' source tokens are protected;
row/advisory locks prevent cleanup from racing current progress or registration.
Production S3 staging still requires an appropriate bucket lifecycle policy;
this local-only audit does not enumerate or delete cloud objects.

### Map and table preview

The preview loads vector tiles and immediately fits stored bounds, without
downloading every site's centroid. Selecting a table row fetches only that
feature's exact extent. Selecting an unloaded map feature filters the server-backed
grid by its immutable feature key; **Clear selection** restores all rows.
Display labels need not be unique. Failed tiles retry and then show a visible
retry control; expired authentication is distinguished from empty tiles.
Changing sets or leaving the page cancels stale requests and cleans up listeners.
Polygon tiles use PostGIS MVT clipping, validity handling and tile-grid
quantization directly. They do not repeatedly run topology-preserving
simplification on entire source geometries before clipping; that step stalled
on the FY26 dataset's million-vertex features. Display generalization never
changes stored analytical geometries or their GeoParquet exports.

### Conservative analysis preparation

Preparation uses authoritative reference boundaries. Display simplifications
are used for map rendering.
All positive polygon coverage survives splitting, including tiny cross-stratum
pieces and uncovered residuals. Reference overlaps of any size are assigned
once to the first region in lexicographic string-ID order (for example, `10`
precedes `2`). Each conflict produces a warning identifying the layer, input
piece, winning and losing region IDs, and overlap area in square metres.
Preparation also logs the count, total and maximum resolved overlap per layer;
totals count overlaps in overlapping input sites separately. This temporary
priority applies only to prepared site fragments; stored reference boundaries,
reference exports and rasters remain unchanged pending boundary normalization.
A missing selected reference table must be imported before preparation can proceed.

The minimum-area setting applies to whole input features before splitting.
Fragments lacking any selected polygon-region key remain in the downloadable
artifact but are excluded from matching. The persisted **Site Preparation**
report distinguishes minimum-area exclusions, unmatched fragments/sites,
covered/uncovered hectares, repairs, and reference fingerprints. It is also
available when every input fails the minimum-area filter. Reference GeoParquets
have content-addressed paths; stale exports are not used after a newer import.
Hectares use Mollweide (ESRI:54009) with boundaries densified to at most
0.005 degrees for measurement, in bounded batches. This avoids area changing
simply because a split adds vertices along a curved projected edge. The original
geometry coordinates are not densified or simplified in the analysis artifact.
Extraction validates the same area convention before reading covariates.

Analysis `site_id` uses the immutable feature UUID; display labels are stored
separately.
Extraction supports multiple feature memberships in a raster cell, so overlapping
features remain independent reports. Portfolio totals sum these reports; they
are not a unique-area union of overlapping source features. The results'
**Site ID / label mapping** CSV maps canonical IDs to original labels and names.

### Atom-level KPI inputs and results

KPI exports may add an `ae_atoms` text property to each parent site's source
feature. Its JSON value is
`{"run_id":"<KPI job id>","atoms":[{"atomic_id":123,"geometry":<GeoJSON Polygon or MultiPolygon>}]}`.
Every parent row in an atom-enabled input must declare the same non-empty
`run_id`; atom IDs must be globally unique positive integers. Within each
parent, atom geometries must be valid, disjoint, and exactly partition the
parent polygon. Upload/import preserves this property in source attributes,
and preparation carries it through exact-match fragment splitting. Extraction
compares the union of the fragments (including a single fragment) with the atom
union using the same area tolerance as the region splitter, so numerical
roundoff does not reject otherwise valid partitions. Every rasterized treatment cell
must still be assigned to an atom. A declared
atom parent cannot be silently removed by the minimum-site-area filter; such
submissions fail with the excluded parent IDs.

Atom-enabled results include:

- `results_by_atom_replicate_year.csv`: one row per
  `(run_id, atomic_id, replicate, year)` for every atom with estimates, with
  the parent `site_id`, the inclusive intervention-period flag, and the six
  additive extrapolated treatment/control/avoided loss and emissions metrics.
  It includes all available annual result years, including pre-intervention
  years, and every replicate.
- `results_atom_status.csv`: one row for every declared atom, including atoms
  with no grid pixels. Columns:

  | Column | Description |
  |---|---|
  | `run_id`, `atomic_id`, `site_id` | Atom and its parent site |
  | `status` | `estimated`, `partial`, `failed`, or `not_applicable` |
  | `status_reason` | Why the atom is not fully estimated |
  | `n_site_pixels`, `n_forest_pixels` | Grid pixels, and those with forest at the parent baseline year |
  | `n_eligible_pixels` | Forest pixels with complete inputs in a stratum that has controls |
  | `n_supported_pixels` | Eligible pixels the estimates stand for |
  | `forest_coverage_fraction` | `n_supported_pixels / n_forest_pixels` |
  | `eligible_area_ha`, `supported_area_ha` | The same two counts as cell area |
  | `n_sample_pixels` | Matched treatment pixels, mean across replicates |
  | `min_replicates_matched` | Fewest replicates in which any of the atom's included cells matched |
  | `treatment_arm` | `census` when observed loss is the sum over all eligible pixels; `sample` when a match rate below 50% leaves it to the matched sample |
  | `first_year`, `last_year`, `start_year`, `end_year` | Result-year and parent intervention-year bounds |

- `results_summary.json`: `atom_results_schema_version: 2` and
  `atom_run_id: <run_id>`.

Coverage is counted from the atom's eligible pixels in included cells, not from
whether the atom has an estimate. An atom spanning two strata, one of which is
not matched, is covered only by the pixels in the other. Below 80% forest
coverage an atom is `partial`; with no supported pixel it is `failed`.

Every atom with eligible pixels is sampled in every replicate: atoms of 50 or
fewer eligible pixels in a stratum are sampled in full, larger ones contribute
at least 50 (see [matching.md](matching.md#pixel-sampling-limits)). Matched
pixels are scaled to the atom's own eligible pixels in each stratum, and its
observed loss is taken from all of them. When a matched set contains multiple
treatment atoms, each control's outcome is assigned across those atoms in
proportion to the treatment-unit matching weights; no parent total is
apportioned by polygon area. An atom's stratum is used when it matched in at
least 80% of replicates; a replicate in which it did not match takes the mean
of the others, never zero. Parent site and portfolio estimates are sums of
atom rows, so their replicate-year totals equal the sum of the corresponding
atom rows. Both atom CSVs are available from the normal task results prefix
and via the `atom_replicates` and `atom_status` result-download types.

Matching runs also publish
[per-replicate matching diagnostics](analysis-outputs.md#matching-diagnostic-exports)
with per-atom sampling/matching counts and final inclusion evidence. Preserve
the atom run identity when joining downstream original-site memberships:
wholly duplicated sites can have usable atom statistics without owning a
matching unit, while missing atom domains must remain explicit rather than
being replaced by zeroes or copied parent totals.

Results written before atom result schema 2 reported full forest coverage for
any atom with an estimate. They are not accepted downstream; rerun the task.

The read-only audit command can inventory historical jobs and references or
validate downloaded artifacts. Run from the repository root with the application's
dependencies and database configuration:

```bash
PYTHONPATH=webapp:. python webapp/scripts/audit_polygon_preparation.py --database
PYTHONPATH=webapp:. python webapp/scripts/audit_polygon_preparation.py \
    --fragments prepared.parquet
PYTHONPATH=webapp:. python webapp/scripts/audit_polygon_preparation.py \
    --fragments prepared.parquet --sources canonical-source-snapshot.parquet
PYTHONPATH=webapp:. python webapp/scripts/audit_polygon_preparation.py \
    --benchmark-sites 1000
```

The optional source snapshot must use the same immutable `site_id` values;
original upload display labels cannot substitute for these keys. Artifact
validation fails on duplicate keys, overlapping fragments, inconsistent stored
hectares, or changed source coverage. Database inventory only flags historical
jobs for review; it cannot prove an old run conserved geometry and never rewrites
or resubmits historical outputs.

---

## Submitting a Task

A task ties a site set to a specific set of matching parameters and
dispatches a job to AWS Batch via the trends.earth API.

### Task submission form

The form is on the **New Task** page and has three sections:

**1. Site set selection**  
Choose from previously uploaded site sets. The number of sites and total
area are shown alongside each option.

**2. Covariate selection**  
A checkbox list of all available (merged and ready) covariates. The default
set is pre-selected. Covariates that have not been fully merged are shown as
disabled. See [covariates.md](covariates.md) for the full list and
[covariate-pipeline.md](covariate-pipeline.md) for how COGs are prepared.

**3. Matching parameters**  
Grouped matching settings with defaults pre-filled. See
[matching.md](matching.md) for a full explanation of each parameter.

### Submission flow

1. The form is validated client-side (required fields, numeric ranges).
2. On submission the webapp calls `services.queue_analysis_task()`, which:
   - Creates an `AnalysisTask` record with status `queued`.
   - Dispatches a Celery task (`tasks.submit_analysis_task`) to handle the
     slow part of submission in the background.
3. The background Celery task:
   - Exports reference layer parquets from PostGIS if they are stale.
   - Uploads the site GeoParquet to S3 (`{S3_PREFIX}/sites/{task_id}/`).
   - Registers the execution with the trends.earth API and receives an
     `execution_id`.
   - Updates the `AnalysisTask` record with the `execution_id` and sets
     status to `running`.
4. The trends.earth API dispatches the job to AWS Batch.

If submission fails (e.g. trends.earth API error) the task status is set to
`failed` and the error message is stored on the `AnalysisTask` record.

Site artifact uploads encode GeoParquet in row groups capped at 500 features
and 250,000 coordinates (except an individual larger feature). The artifact
is staged in a temporary file and uploaded with a single-threaded, 8 MiB
multipart transfer, avoiding whole-dataset WKB and Parquet buffers. Submission
workers must use disk-backed temporary storage, not a RAM-backed `tmpfs`.
Rebuild and redeploy the submission worker to apply this change.

### Preparation audit and analysis units

Preparation runs after submission, not on every form edit. The task detail page
shows the persisted **Site Preparation** summary: input/retained/excluded site
counts, fragment counts, area reconciliation, reference versions and any recorded
repair/exclusion diagnostics. Original sites and split analysis units are
distinguished; the maps continue to show original site boundaries.

Task owners can download the stored JSON report from
`/api/tasks/<task_id>/preparation/report` and the split GeoParquet from
`/api/tasks/<task_id>/preparation/sites`. Both endpoints enforce task ownership.
The GeoParquet link signs the artifact already saved during preparation, including
when later API submission fails; it does not rerun splitting. Older tasks may
have no preparation report.

### Concurrency and queueing

The web app imposes no per-user concurrency limit itself — limits are
enforced by the trends.earth API and AWS Batch queue capacity.

---

## Task Monitoring

The **Tasks** page shows a live-updating table of all tasks for the current
user (admins see all tasks). The status column reflects the latest value
polled from the trends.earth API.
The **Progress** column displays the API's execution percentage as a bar and
numeric label. It refreshes with the table every 30 seconds; the background
poller also runs every 30 seconds, so updates may take two refresh cycles to
appear. A dash means no percentage has been reported, rather than 0% complete.
Percentages are driver estimates, not the fraction of submitted Batch tasks
that have finished. Resuming a failed analysis resets its cached progress.

The sortable **Resolution** column shows each task's analysis resolution (for
example, **250 m** or **1 km**). Older tasks without a saved resolution use the
original **1 km** default. Numeric filters use metres.

Select **View** in the **Params** column to open the task's saved configuration,
including covariates and exact-match variables, as pretty-printed JSON in a
scrollable dialog. These settings are shown there rather than in separate columns.

The **Elapsed** column shows time from submission to completion as **HH:MM:SS**.
For submitted or running tasks it updates with each table refresh. Hours can
exceed 24; a dash indicates missing timestamps. Numeric filters use seconds.

Table filters match each column's type: number columns use numeric
comparisons, date columns use a date picker based on your local calendar day
(a dash or **Ongoing** counts as blank), and yes/no columns such as
**Archived** offer **Yes** or **No**.

The **Resume** action is enabled for failed Nextflow tasks with a tracked API
execution that you own (or administer). It resumes the same execution using
retained checkpoints; cancelled tasks cannot be resumed. **Recompute** opens
the submission form with the task's settings to start a new task instead.
Cancel and resume show progress and prevent repeated clicks while the request
is in progress. Both actions require confirmation on the task list and detail
page. The table temporarily disables its action buttons during a
request. The API may reject a resume while cleanup is still underway.
The **Logs** column opens the execution's user-visible task logs, with manual refresh and
automatic refresh every 30 seconds while the dialog is open. Logs are available
only for tasks with a tracked API execution and require API access.

### Status values

| Status | Meaning |
|---|---|
| `queued` | Submission Celery task is waiting to run |
| `submitting` | Submission Celery task is running |
| `running` | Executing on AWS Batch |
| `succeeded` | Results imported successfully |
| `failed` | Analysis or submission error |

### Background polling

A Celery beat task (`tasks.poll_batch_tasks`) runs every 30 seconds. For each
task in `running` state it calls the trends.earth API to get the latest
execution status. When the API reports the execution is complete the task
downloads the result files from S3, imports them into the database, and
sets the task status to `succeeded`.

A second beat task (`tasks.expire_stale_submitting_tasks`) runs every 2
minutes. If a task has been in `submitting` state for more than 30 minutes
it is assumed the Celery worker died and is marked `failed`.

---

## Viewing Results

Clicking a completed task opens the **Task Detail** page, which has five
panels:

The Overview loads without fetching map geometry or per-site yearly results.
On the Plots tab, the map and its source-field filters load separately from
the initial charts. On Results Tables, the per-site yearly grid loads after
the sites and totals grids. These changes do not truncate the tables or change
the contents of CSV downloads.
The **Sites with Potential Quality Issues** grid loads more rows as you scroll.
Hover over an issue cell to read its full text.

The Overview's **Site Matching Status** grid lists every site in the task, one
row per site. It combines the match quality rating (Critical, Caution, OK, or
No results), quality issues, site fragments that could not be included and
why, and whether the site was subsampled (with the percentage sampled). Rows
load as you scroll; sorting and filtering apply to all sites, not only the
loaded rows. Filter the **Quality** column by rating. Warnings that apply to
the task as a whole, such as overall covariate balance, appear above the grid;
there is no separate warning banner above the tabs.

**Raw Results** lists the main download files. If matching replay files are
available, use **Download matching replay links (.txt)** for a text file with
one link per replay file. The links are generated on demand and expire after
one hour.

### Measuring task-page performance

On staging, use the browser's Network and Performance panels to capture these
intervals: **View Task → Overview usable**, **Plots → first chart usable**,
**Results Tables → first grid usable**, and **filter change → updated chart
usable**. Also record time to map and yearly-grid readiness to distinguish a
faster first render from a slower full load. Record response sizes for the
Dash `/_dash-update-component` requests. Their `Server-Timing` headers break
down task data reads, geometry, replicate calculations, cache access, and
overall request time. The difference between the overall request time and its
callback span includes Dash serialization and other request overhead; use the
browser trace to measure client rendering separately.

Compare p50 and p95 on the same fixtures before and after deployment: tasks
with at most 200 sites and tasks above 200 sites, signed-in and shared-link
visits, and cold versus repeated views. Include an exact repeat of a site or
group filter to measure a warm derived-result cache hit. Use new fixtures or
an isolated cache for cold tests; do not flush the shared Redis instance.

New imports with an output URI store the computed quality warnings alongside
the results. Older tasks compute them on demand. The Redis cache for derived
results uses 15-minute entries and a 512 KiB per-entry limit. Set
`TASK_RESULTS_CACHE_URL` to use a separate Redis instance, or set it to an
empty string to disable this cache. Otherwise it uses the session Redis URL.

### Summary table

A per-site table showing, for each site:

- Total avoided forest loss (ha) over the intervention period
- Total avoided emissions (MgCO₂e) over the intervention period
- Range across matching replicates (if the task was run with `n_replicates > 1`;
  this reflects sampling and matching variability, not a confidence interval)
- Number of matched treatment pixels used
- Sampling fraction (< 1 if treatment was subsampled)

### Results plots

Annual and cumulative deforestation comparisons use line charts for project
sites and matched controls, with shaded ranges across matching replicates when
available. Annual and cumulative avoided emissions use line charts with the
same styling and replicate-range bands when available; the site-level annual
emissions comparison uses the same style. Avoided emissions and forest loss by
year use bar charts, with replicate ranges shown as error bars when available.
The forest-loss-by-year chart appears immediately after
cumulative deforestation; the avoided-emissions line charts appear immediately
before the avoided-emissions-by-year chart. The yearly bar charts independently
highlight the three largest positive and three most negative sites using that
metric's **2025** values. Separate site-ID legends identify these groups, with
six distinct colors from Paul Tol's color-blind-friendly muted qualitative
palette. These site colors stay fixed across years, even when a site's
contribution changes sign.
All remaining sites are combined into **All other sites (positive)** in blue and
**All other sites (negative)** in orange, splitting contributions by their sign
in each plotted year rather than netting them together. Group filters recalculate
the ranking within the selected group; error bars retain the overall replicate
range. If no nonzero 2025 site data exists, the charts show only the signed remainder
groups and explain why there are no individually highlighted sites.
Forest-loss error bars use dark charcoal to keep both limits visible against the bars.
Click a site's legend entry or bar to select it and zoom the map to that site.
The **All other sites** legend entries retain the usual show/hide behavior.
Individual-site bars show both the site ID and site name on hover.

After the yearly emissions chart, two bar charts show up to 20 sites with the
largest positive total avoided emissions and the 20 with the most negative
totals. These use blue and vermillion respectively, with site IDs on the X axis
and emissions avoided (MgCO₂e) on the Y axis. Click a bar to select and zoom to
that site. Rankings respect the selected group; these two charts are omitted
when only one site is selected.
Year-axis tick labels use whole years without thousands separators or decimals.

The plots initially summarize all sites. Select a source field and value to show
the corresponding group, or select an individual site to show its results.
**By site ID** sorts the site selector by
the original display ID and shows `SITE ID (SITE NAME)`. **Alphabetical** sorts by
site name and shows `SITE NAME (SITE ID)`. Sites without a distinct name show only
their ID. Switching order preserves the selected site.

The optional field and value filter above the site map uses attributes from the
uploaded source file. It filters the map, site selector, and plots; other results
tabs are unchanged. For imported site sets, source fields and values are loaded
with the plot controls, independently of the deferred map.
Matching sites are highlighted on the map. Selecting a site
on the map clears the group filter and shows that site's plots. Clear the
selection to restore all sites.
Browser **Back** and **Forward** restore previous site selections, completed
group filters, and results tabs without reloading the whole page. The map also
returns to its saved center, zoom, and rotation. Panning and zooming alone do not
add history entries. Map positions stay in browser history, not in shared URLs;
opening a direct or shared link still fits the selected site or the full map.
Loading indicators appear in the tab or section being updated. The map,
yearly results table, filtered plots, and Match Quality each show their own
progress without blocking navigation or hiding results that are already visible.
When a plot selection updates the charts, its loading indicator stays visible
at the bottom of the viewport, even if you are scrolled down.
Group ranges are calculated from the selected sites' yearly outcomes within
each matching replicate, not by adding site-level bounds. Site sets must be
reimported to retain source attributes, and analyses rerun to produce the
yearly replicate outcomes needed for group ranges. Deployments must also apply
database migration `0037a1b2c3f8` before importing site attributes.

The site map appears near the top of this section. It shows the site polygons
and, when pixel location data is available, matched treatment (green) and control
(blue) pixel centroids. Covariate COG overlays can be toggled via the layer panel.
Selecting a site zooms the map to it; clicking a site on the map selects it and
loads its plots. Plot URLs retain the selected site or group and can be reloaded
or shared.

### Match quality diagnostics

Automatically computed quality warnings are shown as alert banners above the
results. See the [README match quality section](../README.md#automated-match-quality-checks)
for the thresholds used.

Two diagnostic plots are available in a tabbed panel:

**Love plot** — Standardized Mean Difference (SMD) for each covariate before
and after matching. Conventionally, |SMD| < 0.1 indicates good balance. The
dashed reference lines are drawn at ±0.1 and ±0.25.

**Propensity score QQ plot** — Quantile-quantile comparison of propensity
scores between matched treatment and control pixels. A straight diagonal
line indicates identical distributions.

Both plots can be filtered to show aggregate statistics across all sites or
a single selected site.

### Downloads

The **Download** button generates a ZIP file containing all result CSVs
(see [analysis-outputs.md](analysis-outputs.md)) pre-signed for direct S3
download. Links expire after 1 hour.

---

## Sharing Results

Any task owner (or admin) can generate a **read-only share link** from the
task detail page. Share links:

- Require no authentication — anyone with the link can view the results.
- Expire after a configurable number of days (default 30 days, maximum 365).
- Can be revoked at any time by the task owner.
- Grant access to the task detail page only — not to raw S3 files.

When a share link is created or copied from the **Results Plots** tab, it opens
that tab with the current site or group selection, including for viewers who
are not signed in. Existing share links shown in the modal also include the
current selection when copied from that tab.

Share links are stored in the `TaskShareLink` table and validated on each
request.

---

## Admin Panel

The Admin panel is accessible only to users with the `admin` app role. It has
three sections:

### Users

A table of the Trends.Earth users who have requested or been granted access
to this app, filterable by status (pending, active, revoked). The
access-request email links to `/admin?status=pending`. Admins can:

- Approve a pending request (with the selected role).
- Change a user's role (`member` / `admin`).
- Revoke access.
- Delete a user's Avoided Emissions data.

### Covariates

Shows the status of every covariate in the system. For each covariate:

- **GEE export status** — whether all GEE batch tasks have completed
  successfully and tiles are in GCS.
- **Merge status** — whether all tiles have been merged into a single COG
  on S3.

Admins can trigger a new GEE export for any covariate from this panel.
Exports that are already in progress show their GEE task IDs and current
progress.

### Matching presets

On the submission form's **Matching Setup** tab, a single preset control saves
and loads matching covariates, exact match variables, and the other matching
settings together. Presets are private to each user; saving with the same name
replaces the entire preset. Loading a preset leaves the random seed unchanged.
The **Restrict controls to shared support** checkbox enables
`shared_support_controls` (on by default for new analyses). It restricts controls
to treatment values for numeric covariates constant within an eligible matching stratum;
it does not enable Mahalanobis fallback. The checkbox is saved and restored
with presets, and pre-filled when recomputing a task. Older presets without
this setting load it as off.
The unified preset migration discards the old separate covariate and
matching-settings presets.

Recomputing a task pre-fills its original analysis resolution as well as its
matching configuration, while generating a fresh random seed.

---

## Background Workers

The app runs three Celery worker processes (configured in
`deploy/docker-compose.develop.yml`):

| Worker | Queue | Handles |
|---|---|---|
| `worker` | `celery` (default) | Task submission, result polling, GEE export polling, site upload processing |
| `merge-worker` | `merge` | COG tile merging (GDAL — CPU/memory intensive), vector rasterization |
| `beat` | — | Celery beat scheduler (dispatches periodic tasks) |

### Periodic tasks (beat schedule)

| Task | Interval | Purpose |
|---|---|---|
| `tasks.poll_batch_tasks` | 30 s | Poll trends.earth API for execution status; import results on completion |
| `tasks.expire_stale_submitting_tasks` | 2 min | Mark stuck `submitting` tasks as `failed` |
| `tasks.poll_gee_exports` | 60 s | Check GEE batch task progress; trigger COG merge on completion |
| `tasks.auto_merge_unmerged_covariates` | 2 min | Merge any COG tiles that completed since the last merge pass |
| `tasks.export_reference_layers_task` | Monthly (1st, 15th at 04:00 UTC) | Re-export PostGIS reference layers (admin, ecoregion, pa) to S3 |
