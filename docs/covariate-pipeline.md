# Covariate Processing Pipeline

This document describes how covariate data moves from source datasets in
Google Earth Engine (GEE) through to the per-pixel extract files that the
matching algorithm consumes.

## Contents

- [Grid Definition](#grid-definition)
- [Stage 1 — GEE Export](#stage-1--gee-export)
- [Stage 2 — COG Tile Merge](#stage-2--cog-tile-merge)
- [Stage 3 — Reference Layer Rasterization](#stage-3--reference-layer-rasterization)
- [Stage 4 — Pixel Extraction](#stage-4--pixel-extraction)
- [S3 Layout for Covariates](#s3-layout-for-covariates)
- [Triggering a New Export](#triggering-a-new-export)

---

## Grid Definition

All covariates share a single fixed global grid:

| Property | Value |
|---|---|
| CRS | EPSG:4326 (WGS 84 geographic) |
| Resolution (1 km) | 30 arc-seconds (0.008333… degrees) |
| Resolution (250 m) | 7.5 arc-seconds |
| Grid origin | 0° E, 0° N |
| Global extent | −180° to +180° longitude, −90° to +90° latitude |

The origin and pixel size are chosen so that every pixel boundary aligns
exactly with integer multiples of 30 arc-seconds from the origin. This means
any two covariates can be stacked without resampling, and pixel indices are
globally consistent across all layers.

---

## Stage 1 — GEE Export

**Code**: `gee_export/export_covariates.py`, `gee_export/gee_config.py`,
`gee_export/derived_layers.py`

Each covariate is exported from GEE as a separate batch task. GEE cannot
write a single file covering the entire globe in one task, so each covariate
is split into a configurable grid of tiles (typically 10° × 10°). All tiles
for a covariate share the same pixel alignment and resolution.

### Export parameters per covariate (`gee_config.py`)

- **`asset_id`** — GEE asset path (e.g.
  `WORLDCLIM/V1/BIO`) or a derived layer tag (e.g. `"slope"`,
  `"total_biomass"`).
- **`band`** — Band name or index within the asset to export.
- **`resampling`** — `"mean"`, `"sum"`, or `"mode"`. Applied when
  aggregating from the native GEE scale to the 30 arc-second grid.
- **`derived`** — Boolean; if `True` the layer is computed via a function
  in `gee_export/derived_layers.py` rather than read directly from a GEE
  asset.

### Derived layers

Derived layers are computed inside GEE before export:

| Layer | Derivation |
|---|---|
| `slope`, `aspect` | Computed from SRTM elevation using `ee.Terrain.products()` |
| `pop_growth` | Annualised log-growth rate from WorldPop 2000 and 2020 counts |
| `friction_surface` | Road-proximity composite from OpenStreetMap friction surface data |
| `fc_YYYY` | Annual forest cover fraction derived from Hansen GFC cumulative loss layers; `fc_t = (treecover2000 > 0) ∧ (loss_year > t)` |
| `total_biomass_2025` | AGB from external asset × (1 + root:shoot ratio from Mokany et al. 2006), masked by Hansen loss to 2025 |

### Output destination

Tiles land in the Google Cloud Storage (GCS) bucket configured by
`GCS_BUCKET`:

```
gs://{GCS_BUCKET}/exports/{covariate_name}/{tile_id}.tif
```

The `GeeExportMetadata` database table records each GEE task ID, the
covariate it belongs to, the tile bounds, and the current GEE task status
(`READY`, `RUNNING`, `COMPLETED`, `FAILED`).

---

## Stage 2 — COG Tile Merge

**Code**: `webapp/cog_merge.py`  
**Celery queue**: `merge` (handled by the `merge-worker` container)

Once all tiles for a covariate have reached `COMPLETED` status in GEE, the
Celery beat task `tasks.auto_merge_unmerged_covariates` dispatches a merge
job. The merge job:

1. Downloads all tile GeoTIFFs from GCS to a temporary directory.
2. Creates a GDAL VRT (virtual raster) covering the full global extent.
3. Translates the VRT to a Cloud-Optimized GeoTIFF using `gdal_translate`
   with:
   - LZW compression with horizontal predictor
   - 512 × 512 internal tile size
   - Overview levels generated at 2×, 4×, 8×, 16×, 32× downsampling
4. Uploads the merged COG to S3:

```
s3://{S3_BUCKET}/{S3_PREFIX}/cog_1km/{covariate_name}.tif
```

or for 250 m exports:

```
s3://{S3_BUCKET}/{S3_PREFIX}/cog_250m/{covariate_name}.tif
```

5. Updates the `Covariate` database record (`merge_status = "ready"`,
   `s3_key` set).

The merge step is memory-intensive for large global rasters. It runs on
the dedicated `merge-worker` to avoid blocking the main worker queue.

---

## Stage 3 — Reference Layer Rasterization

**Code**: `webapp/rasterize_vectors.py`  
**Celery queue**: `merge`

Administrative boundaries, ecoregions, and protected areas are stored in
PostGIS (imported by `webapp/import_vector_data.py` on first startup). They
cannot be exported from GEE, so they are rasterized locally using GDAL.

### Rasterization process

For each reference layer (`admin0`, `admin1`, `admin2`, `ecoregion`, `pa`):

1. The PostGIS table is queried and written to a temporary GeoJSON / Parquet
   file.
2. `gdal_rasterize` burns the integer ID of each polygon into the output
   grid, using `mode` (most-common-value) resampling.
3. The rasterized COG is uploaded to S3 alongside the GEE-exported layers:

```
s3://{S3_BUCKET}/{S3_PREFIX}/cog_1km/{layer_name}.tif
```

4. A vector Parquet copy (for spatial joins in Python) is also exported:

```
s3://{S3_BUCKET}/{S3_PREFIX}/reference/{layer_name}.parquet
```

The Celery beat task `tasks.export_reference_layers_task` re-exports all
reference layer parquets on the 1st and 15th of each month to pick up
WDPA updates.

---

## Stage 4 — Pixel Extraction

**Code**: `r-analysis/scripts/01_extract_covariates.py`

This is Step 1 of the analysis pipeline, executed inside the `r-analysis`
Docker container on AWS Batch. Its inputs are the merged COG files on S3 and
the site polygons uploaded for the task.

### What it does

1. **Load sites** — reads the GeoParquet file from S3
   (`{S3_PREFIX}/sites/{task_id}/sites.parquet`).
2. **Define treatment pixels** — for each site polygon, rasterize the
   polygon boundary against the 1 km grid to identify all treatment cell
   indices.
3. **Define control candidate pixels** — for each site, sample a pool of
   control candidate pixels from within the same country (or configurable
   spatial extent), excluding pixels within `min_control_distance_km` of
   any treatment polygon.
4. **Extract covariate values** — for each treatment and control candidate
   pixel, open the relevant COG file (via GDAL with cloud range requests),
   read the pixel value at the cell index, and append it to a running
   data frame.
5. **Compute `defor_pre_intervention`** — for sites with `start_date` after
   2005, compute the 5-year pre-intervention mean annual deforestation rate
   from the `fc_YYYY` layers.
6. **Write outputs** to local disk, then upload to the intermediate S3
   prefix:

```
s3://{intermediate_s3_uri}/
  sites_processed.parquet       # site metadata + area_ha
  treatment_cell_key.parquet    # cell → site_id mapping for treatment pixels
  treatments_and_controls.parquet  # all pixels with covariate values
  formula.json                  # propensity score formula string
  site_id_key.csv               # integer id_numeric ↔ site_id mapping
  grid_metadata.json            # grid origin, resolution, CRS
```

### Covariate value extraction details

- All COG files are opened with GDAL using `/vsicurl/` or `/vsis3/` virtual
  file system drivers — no full file download is required unless the entire
  global raster is needed.
- For large sites with many treatment pixels, GEE tiles may be accessed via
  HTTP range requests to retrieve only the relevant overview level.
- `NoData` pixels (as defined in the COG header) are recorded as `NA` in
  the output data frame. Pixels where any selected covariate is `NA` are
  excluded from matching.

---

## S3 Layout for Covariates

```
s3://{S3_BUCKET}/{S3_PREFIX}/
  cog_1km/
    precip.tif
    temp.tif
    elev.tif
    slope.tif
    aspect.tif
    dist_cities.tif
    friction_surface.tif
    pop_2000.tif
    pop_2005.tif
    pop_2010.tif
    pop_2015.tif
    pop_2020.tif
    pop_growth.tif
    agb_2025.tif
    total_biomass_2025.tif
    soil_oc.tif
    irr_carbon_2024.tif
    aez.tif
    fc_2000.tif  …  fc_2025.tif   (26 files)
    cropland_2003.tif  …  cropland_2019.tif  (5 files)
    admin0.tif
    admin1.tif
    admin2.tif
    ecoregion.tif
    pa.tif
  cog_250m/
    (same file names at 250 m resolution, where available)
  reference/
    admin0.parquet
    admin1.parquet
    admin2.parquet
    ecoregion.parquet
    pa.parquet
```

---

## Triggering a New Export

### Via the Admin panel

1. Log in as an admin.
2. Navigate to **Admin → Covariates**.
3. Find the covariate and click **Trigger Export**.
4. Monitor GEE task progress in the status column. Once all tiles complete,
   the auto-merge task will merge them automatically within 2 minutes.

### Via the CLI

```bash
# Export a single covariate
python gee_export/export_covariates.py --covariate precip

# Export all covariates
python gee_export/export_covariates.py --all
```

Requires a valid `GOOGLE_APPLICATION_CREDENTIALS` JSON file and access to
the configured `GCS_BUCKET`.
