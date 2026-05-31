# GEE Export

Python scripts for exporting covariate layers from Google Earth Engine to
Google Cloud Storage as Cloud-Optimized GeoTIFFs. See `config.py` for the
full list of covariates and their GEE sources.

## Usage

```bash
pip install -r requirements.txt

# Export all covariates
python export_covariates.py --bucket my-gcs-bucket --prefix covariates/

# Export a specific covariate
python export_covariates.py --bucket my-gcs-bucket --covariates precip temp elev

# Export by category
python export_covariates.py --bucket my-gcs-bucket --category climate

# List available covariates
python export_covariates.py --list

# Check status of active GEE tasks
python export_covariates.py --status

# Block until all submitted tasks finish
python export_covariates.py --bucket my-gcs-bucket --wait
```

## CLI Options

- `--bucket` GCS bucket for exports (required unless using `--list`/`--status`)
- `--prefix` object prefix within bucket (default from `DEFAULT_GCS_PREFIX`)
- `--covariates` one or more covariate names
- `--category` export only one covariate category
- `--list` print available covariates and exit
- `--status` list active Earth Engine batch tasks and exit
- `--wait` poll task state every 60 seconds until completion

## Authentication and Environment

The exporter initializes Earth Engine with optional environment overrides:

- `GOOGLE_PROJECT_ID` (passed to `ee.Initialize(project=...)`)
- `GEE_ENDPOINT` (optional custom EE API endpoint)
- `EE_SERVICE_ACCOUNT_JSON` (service-account JSON, plain JSON or base64-encoded)

If `EE_SERVICE_ACCOUNT_JSON` is not set, it uses your local EE auth context
(for example from `earthengine authenticate`).

## Covariate Source of Truth

Covariate names, categories, and default matching covariates are maintained in
`gee-export/config.py` (`COVARIATES`, `DEFAULT_MATCHING_COVARIATES`,
`EXACT_MATCHING_VARIABLES`).
