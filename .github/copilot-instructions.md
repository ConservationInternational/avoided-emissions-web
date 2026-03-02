# Copilot Instructions for avoided-emissions-web

## Repository Overview

A multi-component system for running avoided deforestation emissions analyses using propensity score matching. It has three main parts: a **Dash web application** (Python), a **GEE covariate export** pipeline (Python/Earth Engine), and an **R analysis container** for the statistical matching. The database is PostgreSQL + PostGIS, managed by Alembic migrations. Background tasks use Celery with Redis.

- **Languages**: Python 3.13 (webapp, gee-export, extraction scripts), R 4.5 (matching/summarization)
- **Frameworks**: Dash/Plotly, Flask-Login, SQLAlchemy, Alembic, Celery, AG Grid
- **Runtime**: Docker Compose (PostGIS 18, Redis 7, Python 3.13-slim)
- **Size**: ~30 Python source files in `webapp/`, 6 in `gee-export/`, 6 in `r-analysis/scripts/`

## Project Structure

```
.env                          # Environment variables (gitignored, copy from deploy/.env.example)
appspec.yml                   # AWS CodeDeploy hooks
webapp/                       # Dash web application (main component)
  app.py                      # Entry point — creates Dash app, Flask server, URL routing
  config.py                   # Config class reading from env vars
  models.py                   # SQLAlchemy models (User, Covariate, AnalysisTask, TaskResult, etc.)
  auth.py                     # Flask-Login + bcrypt authentication
  callbacks.py                # All Dash interactive callbacks (~1500 lines)
  layouts.py                  # Page layouts and AG Grid column definitions (~1700 lines)
  services.py                 # Business logic: AWS Batch, GEE, S3, task management (~1000 lines)
  tasks.py                    # Celery background tasks (GEE polling, Batch polling, merges)
  celery_app.py               # Celery factory with beat schedule and task routing
  cog_merge.py                # Merge GEE tiles into single COGs via GDAL
  import_vector_data.py       # Import geoBoundaries/ecoregions/WDPA into PostGIS
  rasterize_vectors.py        # Rasterize PostGIS vectors to COGs aligned with GEE grid
  trendsearth_client.py       # OAuth2 client for trends.earth API
  credential_store.py         # Fernet-encrypted credential storage
  entrypoint.sh               # Runs `alembic upgrade head` then exec's CMD
  Dockerfile                  # Python 3.13-slim with GDAL, gunicorn
  requirements.txt            # pip dependencies (no pyproject.toml)
  alembic.ini                 # Alembic config (sqlalchemy.url set in env.py from Config)
  migrations/                 # Alembic migrations
    env.py                    # Uses Config.DATABASE_URL and models.Base.metadata
    versions/                 # Migration files (revision IDs are hex-like strings)
  scripts/
    create_service_client.py  # CLI to register OAuth2 client on trends.earth API
  assets/                     # CSS, JS (AG Grid renderers), images
gee-export/                   # GEE covariate export scripts
  config.py                   # Covariate definitions, grid params, GEE asset IDs
  tasks.py                    # GEE batch task management (start/check exports)
  export_covariates.py        # Click CLI for triggering exports
  derived_layers.py           # Custom GEE computations (slope, forest cover, etc.)
r-analysis/                   # R analysis Docker container
  Dockerfile                  # rocker/r-ver:4.5.2 with spatial R packages + Python
  entrypoint.sh               # Dispatches to extract/match/summarize steps
  configuration.json          # trends.earth script definition (published via CI)
  requirements.txt            # Python deps for extraction step (includes te_schemas)
  scripts/
    01_extract_covariates.py  # Python: extract pixel values from COGs
    02_perform_matching.R     # R: propensity score matching per site
    03_summarize_results.R    # R: compute avoided emissions from matches
    utils.R                   # R utility functions
    py_utils.py               # Python utility functions
  src/
    main.py                   # CLI entry point for the analysis container
    batch_runner.py           # AWS Batch integration (S3 param retrieval)
deploy/                       # Deployment configuration
  .env.example                # Template for .env — copy to repo root
  docker-compose.develop.yml  # Local dev: postgres, redis, webapp, worker, merge-worker, beat
  docker-compose.prod.yml     # Production compose
  docker-compose.staging.yml  # Staging compose
  codedeploy/                 # AWS CodeDeploy lifecycle scripts
```

## Build and Development

### Prerequisites
- Docker and Docker Compose
- Ruff (installed globally or via pip; version 0.14+ is used)
- R with the `lintr` package (`install.packages("lintr")`) for R script linting

### Environment Setup (always do this first)
```bash
cp deploy/.env.example .env
# Edit .env if needed — defaults work for local development
```

### Start Development Environment
```bash
docker compose -f deploy/docker-compose.develop.yml up --build
# Webapp available at http://localhost:8050
# Health check at http://localhost:8050/health
```
Services started: `postgres` (PostGIS 18), `redis`, `webapp`, `worker` (default queue), `merge-worker` (merge queue), `beat` (Celery scheduler). The webapp entrypoint automatically runs `alembic upgrade head` before starting.

### Creating an Admin User
No default admin is seeded. After the first startup:
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

### Code Quality (MANDATORY before submitting changes)

#### Python — Ruff
There is no `pyproject.toml` or `ruff.toml` — ruff uses its defaults. Always run from the repo root:
```bash
ruff check webapp/ gee-export/ r-analysis/scripts/
ruff format --check webapp/ gee-export/ r-analysis/scripts/
```
To auto-fix:
```bash
ruff format webapp/ gee-export/ r-analysis/scripts/
ruff check --fix webapp/ gee-export/ r-analysis/scripts/
```
Both `ruff check` and `ruff format --check` **must pass with zero errors** before any code change is submitted.

#### R — lintr
R scripts in `r-analysis/scripts/` are linted with [lintr](https://lintr.r-lib.org/). Configuration is in `.lintr` at the repo root. Always run from the repo root:
```bash
Rscript -e "lintr::lint_dir('r-analysis/scripts', pattern = '\\.[rR]$')"
```
`lintr::lint_dir` **must report zero lint issues** before any R code change is submitted. Key disabled linters (see `.lintr` for rationale): `indentation_linter`, `object_usage_linter`, `return_linter`, `commented_code_linter`, `object_name_linter`. Line length limit is 120 characters.

### Database Migrations
Migrations live in `webapp/migrations/versions/`. To create a new migration:
```bash
docker compose -f deploy/docker-compose.develop.yml exec webapp \
  alembic revision --autogenerate -m "description of change"
```
Migrations run automatically on webapp startup via `entrypoint.sh`.

### Testing
There is **no test suite** in this repository. Validate changes by:
1. Ensuring `ruff check` and `ruff format --check` pass (see above)
2. Starting the Docker Compose stack and verifying the health endpoint returns `ok`
3. Manually checking affected functionality in the web UI

## CI/CD Pipelines

### GitHub Actions Workflows
- **`.github/workflows/deploy-staging.yml`** — Triggers on push to `staging`/`develop` branches. Builds webapp + r-analysis Docker images, pushes to ECR, deploys via AWS CodeDeploy.
- **`.github/workflows/deploy-production.yml`** — Triggers on push to `master`/`main` branches. Same build-and-deploy flow targeting production.
- **`.github/workflows/publish-script.yml`** — Triggers on push to `main` when `r-analysis/` files change. Publishes the R analysis script to the trends.earth API using the trends.earth CLI.

There is **no CI linting or test workflow** — linting and formatting must be done locally before committing.

## Key Architecture Details

- **PYTHONPATH**: Set to `/app` in the webapp Dockerfile. All imports in `webapp/` are flat (e.g., `from models import User`, not `from webapp.models`).
- **Celery task routing**: Tasks named `tasks.run_cog_merge` and `tasks.rasterize_vectors` are routed to the `merge` queue; all others go to the default `celery` queue. The `worker` service handles the default queue; `merge-worker` handles the merge queue.
- **Celery beat schedule**: Polls GEE export status (60s), AWS Batch task status (30s), auto-merge unmerged covariates (120s).
- **Database**: PostgreSQL with PostGIS extensions (`uuid-ossp`, `postgis`). Session management is manual — callers of `get_db()` must close the session.
- **Environment variables**: `Config` class in `webapp/config.py` reads all settings from env vars with sensible defaults. `DATABASE_URL` is auto-constructed from `POSTGRES_*` vars if not set explicitly.
- **Vector data import**: On first webapp startup, `import_vector_data_task` is dispatched to the Celery worker to download and import geoBoundaries, ecoregions, and WDPA data into PostGIS. This chains to `rasterize_vectors_task`.
- **Grid alignment**: All covariates share a fixed 30 arc-second (~1 km) grid defined in `gee-export/config.py` (EPSG:4326, origin at 0°E/0°N). The rasterize step in `webapp/rasterize_vectors.py` must stay in sync with these constants.

## Trust These Instructions

These instructions reflect the current state of the repository. Only search for additional information if commands fail unexpectedly or if you need implementation details not covered here.
