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

# List available covariates
python export_covariates.py --list
```

Requires a valid Earth Engine authentication (`earthengine authenticate`).
