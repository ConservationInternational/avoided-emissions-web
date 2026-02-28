# Avoided Emissions Analysis System

A multi-component system for running avoided emissions analyses using
propensity score matching to estimate counterfactual deforestation outcomes at
conservation sites.

## Architecture

```
avoided-emissions-web/
  gee-export/          Python scripts to export GEE covariate layers to GCS as COGs
  r-analysis/          Docker container for R-based avoided emissions matching
  webapp/              Dash web application for task management and visualization
  database/            PostgreSQL schema and initialization
  deploy/              CI/CD, Docker Compose, and CodeDeploy configuration
```

## Components

### 1. GEE Covariate Export (`gee-export/`)

Python scripts using the Earth Engine Python API to export covariate rasters
as Cloud-Optimized GeoTIFFs (COGs) to Google Cloud Storage. Each covariate is
exported as an individual GEE batch task. Covariates include:

- **Climate**: precipitation, temperature
- **Terrain**: elevation, slope
- **Accessibility**: distance to cities, distance to roads, crop suitability
- **Demographics**: population (2000, 2005, 2010, 2015, 2020), population growth
- **Biomass**: above + below ground biomass
- **Land cover**: ESA CCI 7-class land cover (2015)
- **Forest cover**: Hansen GFC annual forest cover (2000-2023)
- **Administrative**: GADM level-1 regions, ecoregions, protected areas

### 2. R Analysis Container (`r-analysis/`)

A Docker container running the avoided emissions propensity score matching
analysis. Supports:

- Arbitrary site polygons via GeoJSON or GeoPackage upload
- Configurable covariate selection from the standard set
- AWS Batch integration for parallel multi-site analysis
- Emissions calculation: biomass change to MgCO2e conversion

### 3. Web Application (`webapp/`)

A Dash (Plotly) web application providing:

- User authentication with role-based access (admin/user)
- Site polygon upload (GeoJSON/GeoPackage)
- Task submission to AWS Batch
- Task status monitoring
- Results download and interactive visualization (plots, maps)
- Admin panel for triggering GEE covariate exports

### 4. Database (`database/`)

PostgreSQL database tracking:

- Users and roles
- GEE covariate export tasks
- AWS Batch analysis tasks
- Task results and metadata

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
    dist_cities + dist_roads + crop_suitability + pop_2015 +
    pop_growth + total_biomass
```

With exact matching on `region`, `ecoregion`, and `pa` (protected area status).
For sites established after 2005, `defor_pre_intervention` (5-year
pre-establishment deforestation rate) is added automatically.
