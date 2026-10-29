# Web Application Guide

The web application is a [Dash](https://dash.plotly.com/) (Plotly) single-page
application backed by Flask, PostgreSQL/PostGIS, Celery, and Redis. It is the
primary user interface for uploading sites, submitting analysis tasks, monitoring
progress, and viewing results.

## Contents

- [Authentication and Roles](#authentication-and-roles)
- [Site Upload](#site-upload)
- [Submitting a Task](#submitting-a-task)
- [Task Monitoring](#task-monitoring)
- [Viewing Results](#viewing-results)
- [Sharing Results](#sharing-results)
- [Admin Panel](#admin-panel)
- [Background Workers](#background-workers)

---

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
(identity, access token, rotating refresh token and ID token) lives in Redis
so the Celery workers can submit analyses with the user's token. Refreshes
are serialised per session with a Redis lock, because Trends.Earth revokes the
whole refresh-token family if a rotated token is reused. Sessions last up to
30 days (`OIDC_SESSION_MAX_AGE`) while Trends.Earth keeps honouring the
refresh token.

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
- `start_date` (and optional `end_date`) parse as ISO 8601 dates.
- Dates must be explicit dates, not numeric years or serial dates.
- Duplicate display `site_id` values are accepted. Immutable feature UUIDs are
  canonical analysis identities; original labels are retained as `display_site_id`.

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

Preparation uses authoritative reference boundaries, not display simplifications.
All positive polygon coverage survives splitting, including tiny cross-stratum
pieces and uncovered residuals. Overlapping reference coverage is an error with
the layer and region identified; it is never resolved by double-counting or
assigning a sliver to a different stratum. A missing selected reference table
must be imported before preparation can proceed.

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

Analysis `site_id` is the immutable feature UUID, not the display label.
Extraction supports multiple feature memberships in a raster cell, so overlapping
features remain independent reports. Portfolio totals sum these reports; they
are not a unique-area union of overlapping source features. The results'
**Site ID / label mapping** CSV maps canonical IDs to original labels and names.

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

Clicking a completed task opens the **Task Detail** page, which has four
panels:

### Summary table

A per-site table showing, for each site:

- Total avoided forest loss (ha) over the intervention period
- Total avoided emissions (MgCO₂e) over the intervention period
- Range across matching replicates (if the task was run with `n_replicates > 1`;
  this reflects sampling and matching variability, not a confidence interval)
- Number of matched treatment pixels used
- Sampling fraction (< 1 if treatment was subsampled)

### Time series chart

An interactive Plotly line chart of annual avoided emissions (MgCO₂e) per
site, including the pre-intervention baseline period. Ranges across matching
replicates are shown as a shaded ribbon when available.

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

### Map

An OpenLayers map showing the site polygon(s) and, when pixel location data
is available, the matched treatment (green) and control (blue) pixel
centroids. Covariate COG overlays can be toggled on and off via the layer
panel.

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

Presets (saved covariate selections) can also be managed here.

### Matching settings presets

Admins can create and name preset combinations of matching parameters to
make it easier for users to reproduce standard analysis configurations.

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
