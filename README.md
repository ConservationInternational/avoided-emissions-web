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

- **Step 0 (prep, optional)**: Pre-computes spatial buffers used to exclude nearby controls
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
  auth.py             Flask-Login + bcrypt authentication
  celery_app.py       Celery factory with beat schedule and task routing
  cog_merge.py        Merge GEE tiles into single COGs via GDAL
  layer_config.py     Visualization styles for covariate COG map overlays
  email_service.py    SparkPost transactional email (password reset)
  credential_store.py Fernet-encrypted credential storage
  trendsearth_client.py OAuth2 client for trends.earth API
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

- **Auth & users**: `User`, `TrendsEarthCredential`, `PasswordResetToken`, `RefreshToken`
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

Sites must be provided as GeoJSON or GeoPackage files with the following
required attributes:

| Field          | Type    | Description                              |
|----------------|---------|------------------------------------------|
| `site_id`      | string  | Unique site identifier                   |
| `site_name`    | string  | Human-readable site name                 |
| `start_date`   | date    | Intervention start date (YYYY-MM-DD)     |
| `end_date`     | date    | Intervention end date (optional)         |

Geometries must be valid polygons or multipolygons in EPSG:4326.

## Key Environment Variables

Copy `deploy/.env.example` to `.env` and fill in the values listed below.
See the example file for the full set of variables and their defaults.

| Variable | Required | Description |
|---|---|---|
| `TRENDSEARTH_SCRIPT_ID` | **Yes** (for task submission) | UUID of the avoided-emissions R analysis script registered on the trends.earth API. Obtain this by publishing the script with `trends publish` (see the trends.earth CLI docs) or from the API UI script list. Without this, task submission will fail. |
| `TRENDSEARTH_API_URL` | No | trends.earth API v1 endpoint. Defaults to `https://api.trends.earth/api/v1`. |
| `TRENDSEARTH_CLIENT_ID` | **Yes** (for polling) | OAuth2 client ID for background status polling of executions. |
| `TRENDSEARTH_CLIENT_SECRET` | **Yes** (for polling) | OAuth2 client secret for background status polling. |
| `S3_BUCKET` | **Yes** | S3 bucket for site uploads and analysis results. |
| `GCS_BUCKET` | **Yes** (for GEE exports) | GCS bucket where GEE covariate COGs are stored. |
| `GOOGLE_PROJECT_ID` | **Yes** (for GEE exports) | Google Cloud project registered for Earth Engine access. |
| `SPARKPOST_API_KEY` | **Yes** (for password reset emails) | SparkPost API key for transactional email (password resets). Without this, password reset emails are logged to the console instead of sent. |
| `APP_URL` | **Yes** (for password reset emails) | Public URL of the web app (e.g. `https://app.avoided-emissions.org`). Used to build password-reset links in emails. Defaults to `http://localhost:8050`. |
| `SPARKPOST_FROM_EMAIL` | No | Sender address for outgoing emails. Defaults to `noreply@avoided-emissions.org`. Must be a verified sending domain in SparkPost. |

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

### Creating the Admin User

No default admin user is seeded in the database. After starting the
development environment for the first time, create one by running:

```bash
docker compose -f deploy/docker-compose.develop.yml exec webapp python -c "
from auth import hash_password
from models import User, get_db
db = get_db()
db.add(User(
    email='admin@avoided-emissions.org',
    password_hash=hash_password('CHANGE_ME'),
    name='Administrator',
    role='admin',
    is_approved=True,
))
db.commit()
db.close()
"
```

Replace `admin@avoided-emissions.org` and `CHANGE_ME` with your preferred
email and a strong password.

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
