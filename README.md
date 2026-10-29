# Avoided Emissions Analysis System

A multi-component system for running avoided emissions analyses using
propensity score matching to estimate counterfactual deforestation outcomes at
conservation sites.

## Architecture

```
avoided-emissions-web/
  gee_export/          Python scripts to export GEE covariate layers to GCS as COGs
  r-analysis/          Docker container for R-based avoided emissions matching
  webapp/              Dash web application (includes Alembic migrations)
  deploy/              CI/CD, Docker Compose, and CodeDeploy configuration
```

For in-depth technical documentation see **[docs/](docs/index.md)**.

## Components

### 1. GEE Covariate Export (`gee_export/`)

Python scripts using the Earth Engine Python API to export covariate rasters
as Cloud-Optimized GeoTIFFs (COGs) to Google Cloud Storage. Each covariate is
exported as an individual GEE batch task. Covariates include:

- **Climate**: precipitation, mean annual temperature
- **Terrain**: elevation, slope, aspect
- **Accessibility**: travel time to cities, friction surface
- **Demographics**: population count (2000–2020 quinquennial), population growth rate
- **Biomass/Carbon**: aboveground biomass, total biomass (above + belowground), soil organic carbon, irrecoverable carbon
- **Land use**: GLAD cropland extent (2003–2019 quinquennial), agro-ecological zone
- **Forest cover**: Hansen GFC annual forest cover fraction (2000–2025)

Administrative, ecoregion, and protected area reference layers are sourced
from PostGIS (geoBoundaries, WWF Ecoregions, WDPA) and rasterized to the
same grid. See [docs/covariates.md](docs/covariates.md) for the full list.

### 2. R Analysis Container (`r-analysis/`)

A Docker container running the avoided emissions propensity score matching
analysis. Supports:

- Arbitrary site polygons via GeoJSON or GeoPackage upload
- Configurable covariate selection from the standard set
- AWS Batch integration for parallel multi-site analysis
- Emissions calculation: biomass change to MgCO2e conversion

Pipeline implementation:

- **Step 1 (extract)**: Python — samples COG pixel values for treatment and control areas (`r-analysis/scripts/01_extract_covariates.py`)
- **Step 2 (match)**: R — propensity score (or Mahalanobis) matching per site (`r-analysis/scripts/02_perform_matching.R`)
- **Step 3 (summarize)**: R — computes forest loss and avoided CO₂e emissions from matched pairs (`r-analysis/scripts/03_summarize_results.R`)

See [docs/matching.md](docs/matching.md) for matching methodology and parameter reference,
and [docs/analysis-outputs.md](docs/analysis-outputs.md) for output file schemas.

### 3. Web Application (`webapp/`)

A Dash (Plotly) web application providing:

- User authentication with role-based access (admin/user)
- Site polygon upload (GeoJSON/GeoPackage)
- Task submission via the trends.earth API (dispatched to AWS Batch)
- Task status monitoring
- Results download and interactive visualization (plots, maps)
- Shareable read-only result links with configurable expiry
- Admin panel for triggering GEE covariate exports and managing asynchronous site uploads

The application is organized as a package with sub-modules:

```
webapp/
  app.py              Entry point — creates Dash app, Flask server, URL routing
  api_routes.py       Flask API blueprint (/api/*, /health)
  config.py           Config class reading from env vars
  auth.py             Flask-Login user/session helpers (Trends.Earth identities)
  auth_routes.py      /auth/login, /auth/callback, /auth/logout (OIDC flow)
  oidc.py             Trends.Earth OIDC client (PKCE, token verification)
  session_store.py    Redis-backed sessions holding Trends.Earth tokens
  celery_app.py       Celery factory with beat schedule and task routing
  cog_merge.py        Merge GEE tiles into single COGs via GDAL
  layer_config.py     Visualization styles for covariate COG map overlays
  trendsearth_client.py Client for the trends.earth API (user or service token)
  callbacks/          Dash interactive callback functions
  layouts/            Page layouts and AG Grid column definitions
  models/             SQLAlchemy model definitions (one module per domain)
  services/           Business logic (AWS Batch, GEE, S3, task management)
  tasks/              Celery background tasks
  tests/              Unit and integration tests
  scripts/            Utility scripts (COG distribution analysis, etc.)
  migrations/         Alembic migration versions
```

### 4. Database

PostgreSQL + PostGIS, managed by Alembic migrations (in `webapp/migrations/`).
Schema is created automatically on first startup via `alembic upgrade head`.

Model definitions live in `webapp/models/` (one file per domain):

- **Users**: none — users are Trends.Earth accounts, identified by their OIDC
  subject (`sub`); see [Sign-in and user access](#sign-in-and-user-access)
- **Tasking & results**: `AnalysisTask`, `TaskSite`, `TaskResult`, `TaskResultTotal`
- **Site uploads**: `UserSiteSet`, `UserSiteFeature`, `UserSiteUpload`
- **Covariates**: `Covariate`, `GeeExportMetadata`, `ReferenceLayerExport`, `CovariatePreset`, `MatchingSettingsPreset`
- **Sharing**: `TaskShareLink`
- **Reference vectors**: `GeoBoundaryADM0`, `GeoBoundaryADM1`, `GeoBoundaryADM2`, `Ecoregion`, `ProtectedArea`, `VectorImportMetadata`

### 5. Deployment (`deploy/`)

- Docker Compose for local development and production
- GitHub Actions CI/CD pipeline
- AWS CodeDeploy integration for EC2 deployment via Docker Swarm

## Site Input Format

Sites can be GeoJSON or GeoPackage files, or a ZIP/TAR archive containing
exactly one supported dataset (including Shapefile). Map source columns after
upload; only the intervention start date is required.
HTTP requests are limited to 800 MiB including form data. Archives allow up to
2,000 files, 512 MiB per member, 1 GiB expanded total and 200:1 compression.

| Field          | Type    | Description                              |
|----------------|---------|------------------------------------------|
| `site_id`      | string  | Optional display identifier; duplicates allowed |
| `site_name`    | string  | Optional human-readable name             |
| `start_date`   | date    | Required intervention start (YYYY-MM-DD) |
| `end_date`     | date    | Intervention end date (optional)         |

Geometries must be polygons or multipolygons. GeoJSON follows RFC 7946
(EPSG:4326); other datasets must declare their CRS and are transformed to
EPSG:4326. Invalid, nonfinite, or out-of-range coordinates are rejected after repair.
Use explicit dates, not numeric years or spreadsheet serial dates.
Omitted IDs and names are generated. Each imported feature receives an immutable
UUID used as the canonical analysis `site_id`; the original label is retained as
`display_site_id`, so duplicate user-supplied labels never combine distinct sites.

The site catalog lists all reusable site sets independently of recent import
history. Map previews show original polygons and load selected-site extents on
demand. After submission, **Site Preparation** in task details reports retained
and excluded sites, fragment counts, reference versions, and area reconciliation,
with downloads of the preparation JSON and split GeoParquet analysis units.
Splitting preserves all positive area and reference strata; overlapping reference
coverage fails explicitly. Results also provide a **Site ID / label mapping**
CSV. See [the preparation and audit guide](docs/webapp.md#conservative-analysis-preparation).

## Key Environment Variables

Copy `deploy/.env.example` to `.env` and fill in the values listed below.
See the example file for the full set of variables and their defaults.

| Variable | Required | Description |
|---|---|---|
| `TRENDSEARTH_SCRIPT_ID` | **Yes** (for task submission) | UUID of the avoided-emissions R analysis script registered on the trends.earth API. Obtain this by publishing the script with `trends publish` (see the trends.earth CLI docs) or from the API UI script list. Without this, task submission will fail. |
| `TRENDSEARTH_API_URL` | No | trends.earth API v1 endpoint. Defaults to `https://api.trends.earth/api/v1`. |
| `TRENDSEARTH_CLIENT_ID` | **Yes** (for polling) | Service-client ID used for background status polling of executions, result downloads and admin cancellation of other users' tasks. Create it with `webapp/scripts/create_service_client.py` using a Trends.Earth admin account. Task *submission* uses the signed-in user's own token. |
| `TRENDSEARTH_CLIENT_SECRET` | **Yes** (for polling) | Service-client secret for the above. |
| `S3_BUCKET` | **Yes** | S3 bucket for site uploads and analysis results. |
| `GCS_BUCKET` | **Yes** (for GEE exports) | GCS bucket where GEE covariate COGs are stored. |
| `GOOGLE_PROJECT_ID` | **Yes** (for GEE exports) | Google Cloud project registered for Earth Engine access. |
| `APP_URL` | **Yes** | Public URL of the web app (e.g. `https://app.avoided-emissions.org`). The OIDC redirect URI is `APP_URL/auth/callback` and the post-logout redirect URI is `APP_URL/`; both must be registered on the Trends.Earth OIDC client. Defaults to `http://localhost:8050`. |
| `OIDC_CLIENT_SECRET` | **Yes** | Secret of this app's confidential OIDC client on Trends.Earth. The app refuses to start without it unless `DEBUG` is set. |
| `OIDC_CLIENT_ID` | No | OIDC client ID. Defaults to `avoided-emissions-web` (production); staging uses `avoided-emissions-web-staging`. |
| `OIDC_ACCESS_AUDIENCE` | No | Audience of the access tokens issued to this app. Defaults to `avoided-emissions` (staging: `avoided-emissions-staging`). |
| `OIDC_APP_KEY` | No | Trends.Earth app key that access grants and roles are recorded under. Defaults to `avoided_emissions` (staging: `avoided_emissions_staging`). |
| `OIDC_ISSUER` | No | Trends.Earth OIDC issuer. Defaults to `https://api.trends.earth`. |

## Quick Start

```bash
# Copy environment template
cp deploy/.env.example .env

# Start development environment
docker compose -f deploy/docker-compose.develop.yml up --build

# Access the web app at http://localhost:8050
```

### Default Development Credentials

| Service   | Username / Email | Password      |
|-----------|------------------|---------------|
| Postgres  | `ae_user`        | `ae_password` |

### Sign-in and user access

Users sign in with their **Trends.Earth account** (OpenID Connect); there are
no separate Avoided Emissions passwords or registrations — analyses are submitted to
Trends.Earth with the signed-in user's own token.

- The first time someone signs in, Trends.Earth records a **pending** access
  request for this app and emails the app admins who can approve users (and
  any recipients configured for the app in the Trends.Earth API UI).
- App admins approve, revoke or change roles (`member` / `admin`) on the
  **Admin → Users** tab, or in the Trends.Earth API UI (**App Access** tab).
  Trends.Earth superadmins are always admins.
- Sessions are stored server-side in Redis; refresh tokens never reach the
  browser.

### Granting the first admin

No admin is seeded. In the Trends.Earth API UI, a Trends.Earth admin or
superadmin opens **App Access**, selects the Avoided Emissions app
(`avoided_emissions`, or `avoided_emissions_staging` for staging and local
development) and grants the first users the `admin` role. From then on, app
admins manage users from **Admin → Users** in this app.

For local development, register `http://localhost:8050/auth/callback` (and
`http://localhost:8050/` as a post-logout redirect URI) on the staging OIDC
client and put its secret in `OIDC_CLIENT_SECRET`.

> **Note:** Change the Postgres credentials in your `.env` file before
> deploying to any non-local environment.

### Testing

Unit tests live in `webapp/tests/unit/`. Run them inside the webapp container or
with the local venv active:

```bash
# Run the full test suite
python -m pytest webapp/tests/ -v

# Unit tests only
python -m pytest webapp/tests/unit/ -v
```

## Covariate Configuration

Users can customize which covariates are included in the matching analysis by
editing the covariate selection when submitting a task. The default set matches
the standard formula:

```
treatment ~ precip + temp + elev + slope + dist_cities +
  friction_surface + pop_2015 + pop_growth + total_biomass_2025
```

With exact matching on selected stratification variables (default:
`admin1`, `ecoregion`, `pa`).
For sites established after 2005, `defor_pre_intervention` (5-year
pre-establishment deforestation rate) is added automatically.

See [docs/matching.md](docs/matching.md) for the full parameter reference.

## Automated Match Quality Checks

When a task completes, the webapp runs a series of automated quality checks
on the matching results and displays warnings on the task detail page when
potential issues are detected.

### Checks performed

The checks are implemented in `webapp/callbacks/_match_quality.py` and use
the following thresholds:

#### 1. Matched pixel count per site

Low matched-pixel counts reduce statistical power and make site-level
estimates less reliable.

| Condition | Level |
|---|---|
| `n_matched_pixels < 50` | Critical |
| `n_matched_pixels < 200` | Caution |

#### 2. Covariate balance (Standardized Mean Difference)

After matching, the Standardized Mean Difference (SMD) for each covariate
should ideally be below 0.1 in absolute value (the conventional threshold
shown on the Love plot). The checks are run both at the aggregate level
(across all sites) and per-site:

| Condition | Level |
|---|---|
| Any covariate with \|SMD\| ≥ 0.25 | Critical — names the worst covariate |
| > 20 % of covariates with \|SMD\| > 0.1 | Caution |

### Adjusting thresholds

The threshold constants are defined at the top of `webapp/callbacks/_match_quality.py`:

```python
_SMD_CRITICAL = 0.25
_SMD_WARN = 0.1
_SMD_POOR_FRAC = 0.20
_MIN_PIXELS_CRITICAL = 50
_MIN_PIXELS_WARN = 200
```

Modify these values and restart the webapp to change the sensitivity of the
checks. No database migration or R-side changes are needed — the checks are
purely evaluated at display time from existing result outputs.

## License

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

This program is free software: you can redistribute it and/or modify it under
the terms of the **GNU General Public License v3.0** as published by the Free
Software Foundation.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the [GNU General Public License](https://www.gnu.org/licenses/gpl-3.0.en.html)
for more details.

See the [LICENSE](LICENSE) file for the full license text.
