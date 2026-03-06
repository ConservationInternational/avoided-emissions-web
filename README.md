# Avoided Emissions Analysis System

A multi-component system for running avoided emissions analyses using
propensity score matching to estimate counterfactual deforestation outcomes at
conservation sites.

## Architecture

```
avoided-emissions-web/
  gee-export/          Python scripts to export GEE covariate layers to GCS as COGs
  r-analysis/          Docker container for R-based avoided emissions matching
  webapp/              Dash web application (includes Alembic migrations)
  deploy/              CI/CD, Docker Compose, and CodeDeploy configuration
```

## Components

### 1. GEE Covariate Export (`gee-export/`)

Python scripts using the Earth Engine Python API to export covariate rasters
as Cloud-Optimized GeoTIFFs (COGs) to Google Cloud Storage. Each covariate is
exported as an individual GEE batch task. Covariates include:

- **Climate**: precipitation, temperature
- **Terrain**: elevation, slope
- **Accessibility**: distance to cities, friction surface, crop suitability
- **Demographics**: population (2000, 2005, 2010, 2015, 2020), population growth
- **Biomass**: above + below ground biomass
- **Land cover**: ESA CCI 7-class land cover (2015)
- **Forest cover**: Hansen GFC annual forest cover (2000-2024)
- **Administrative**: GADM level-1 regions, ecoregions, protected areas

### 2. R Analysis Container (`r-analysis/`)

A Docker container running the avoided emissions propensity score matching
analysis. Supports:

- Arbitrary site polygons via GeoJSON or GeoPackage upload
- Configurable covariate selection from the standard set
- AWS Batch integration for parallel multi-site analysis
- Emissions calculation: biomass change to MgCO2e conversion

Pipeline implementation:

- **Step 1 (extract)**: Python (`r-analysis/scripts/01_extract_covariates.py`)
- **Step 2 (match)**: R (`r-analysis/scripts/02_perform_matching.R`)
- **Step 3 (summarize)**: R (`r-analysis/scripts/03_summarize_results.R`)

### 3. Web Application (`webapp/`)

A Dash (Plotly) web application providing:

- User authentication with role-based access (admin/user)
- Site polygon upload (GeoJSON/GeoPackage)
- Task submission via the trends.earth API (dispatched to AWS Batch)
- Task status monitoring
- Results download and interactive visualization (plots, maps)
- Admin panel for triggering GEE covariate exports

### 4. Database

PostgreSQL + PostGIS, managed by Alembic migrations (in `webapp/migrations/`).
Schema is created automatically on first startup via `alembic upgrade head`.

Core model definitions live in `webapp/models.py`.

Primary application models:

- **Auth & users**: `User`, `TrendsEarthCredential`, `PasswordResetToken`
- **Tasking & results**: `AnalysisTask`, `TaskSite`, `TaskResult`, `TaskResultTotal`
- **Site uploads**: `UserSiteSet`, `UserSiteFeature`
- **Covariates**: `Covariate`, `GeeExportMetadata`, `CovariatePreset`
- **Reference vectors**: `GeoBoundaryADM0`, `GeoBoundaryADM1`, `GeoBoundaryADM2`, `Ecoregion`, `ProtectedArea`, `VectorImportMetadata`

Database tracks:

- Users and roles
- Covariate export snapshots and merge status
- Analysis tasks and per-site run metadata
- Task results and metadata
- Uploaded user site sets and site features
- Imported vector reference-data provenance

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

## Covariate Configuration

Users can customize which covariates are included in the matching analysis by
editing the covariate selection when submitting a task. The default set matches
the standard formula:

```
treatment ~ lc_2015_agriculture + precip + temp + elev + slope +
  dist_cities + friction_surface + pop_2015 +
    pop_growth + total_biomass
```

With exact matching on selected stratification variables (default:
`admin0`, `admin1`, `admin2`, `ecoregion`, `pa`).
For sites established after 2005, `defor_pre_intervention` (5-year
pre-establishment deforestation rate) is added automatically.

## Automated Match Quality Checks

When a task completes, the webapp runs a series of automated quality checks
on the matching results and displays warnings on the task detail page when
potential issues are detected.

### Checks performed

The checks are implemented in `webapp/callbacks.py` (function
`_assess_match_quality`) and use the following thresholds:

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

The threshold constants are defined at the top of the quality-check section
in `webapp/callbacks.py`:

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
