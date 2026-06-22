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

The app uses Flask-Login with bcrypt password hashing. Two roles exist:

| Role | Capabilities |
|---|---|
| `user` | Upload sites, submit tasks, view their own results, share links |
| `admin` | All user capabilities plus: approve/reject new users, trigger GEE exports, manage covariates, view all tasks |

### Sign-up and approval

New users register with an email address and password. They are created with
`is_approved = False` and cannot submit tasks until an admin approves them.
Admins see pending registrations in the **Admin → Users** panel and can approve
or reject each account.

### Password reset

Users can request a password reset link from the login page. The link is sent
via SparkPost (transactional email). If `SPARKPOST_API_KEY` is not set the
reset URL is logged to the console instead. Reset tokens expire after 1 hour.

---

## Site Upload

Sites are uploaded as GeoJSON or GeoPackage files. The required attribute
fields are documented in the main [README](../README.md#site-input-format).

### Upload flow

1. The user selects a file in the **Upload Sites** panel.
2. The file is staged to S3 (`{S3_PREFIX}/site-upload-stage/`) under a
   time-limited token (12-hour TTL).
3. A Celery task (`tasks.process_site_upload`) parses and validates the
   geometries, converts them to GeoParquet format, and writes a
   `UserSiteUpload` record to the database.
4. On success the site set appears in the **My Sites** list and is ready for
   use in a task submission.

### Validation

The upload task checks:

- All features have valid polygon or multipolygon geometries in EPSG:4326.
- Required fields (`site_id`, `site_name`, `start_date`) are present and
  correctly typed.
- `start_date` (and optional `end_date`) parse as ISO 8601 dates.
- `site_id` values are unique within the file.

Validation errors are stored in the `UserSiteUpload.error` column and
displayed in the UI.

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
- 95 % confidence interval bounds (if the task was run with `n_replicates > 1`)
- Number of matched treatment pixels used
- Sampling fraction (< 1 if treatment was subsampled)

### Time series chart

An interactive Plotly line chart of annual avoided emissions (MgCO₂e) per
site, including the pre-intervention baseline period. Confidence intervals
are shown as a shaded ribbon when available.

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

The Admin panel is accessible only to users with `role = "admin"`. It has
three sections:

### Users

A table of all registered users. Admins can:

- Approve or reject pending registrations.
- Change a user's role.
- Deactivate accounts.

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
