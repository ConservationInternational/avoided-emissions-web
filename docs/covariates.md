# Covariate Reference

All covariates are rasterized to a global **30 arc-second (~1 km)** grid in
**EPSG:4326**, with origin at 0° E, 0° N. The same grid is used by both the
1 km and 250 m resolution workflows; at 250 m resolution a 7.5 arc-second
sub-grid aligned to the same origin is used.

Covariates are exported from Google Earth Engine (GEE) as tiled GeoTIFFs,
merged into single Cloud-Optimized GeoTIFFs (COGs) on S3, and then used for
pixel extraction during analysis. See
[covariate-pipeline.md](covariate-pipeline.md) for the full processing flow.

## Contents

- [Default Matching Covariates](#default-matching-covariates)
- [All Available Covariates](#all-available-covariates)
  - [Climate](#climate)
  - [Terrain](#terrain)
  - [Accessibility](#accessibility)
  - [Demographics](#demographics)
  - [Biomass and Carbon](#biomass-and-carbon)
  - [Land Use](#land-use)
  - [Forest Cover](#forest-cover)
- [Reference / Exact-Match Layers](#reference--exact-match-layers)
- [Pre-Intervention Deforestation Rate](#pre-intervention-deforestation-rate)
- [Adding New Covariates](#adding-new-covariates)

---

## Default Matching Covariates

The following covariates are pre-selected when submitting a new task:

```
precip + temp + elev + slope + dist_cities + friction_surface +
  pop_2015 + pop_growth + total_biomass_2025
```

These provide a broad representation of the biophysical and socioeconomic
factors that influence both where conservation sites are established and
where deforestation occurs, making them suitable propensity score predictors.

---

## All Available Covariates

### Climate

| Variable | Source | Description | Units | Type |
|---|---|---|---|---|
| `precip` | WorldClim V1 BIO-12 | Mean annual precipitation | mm | Static |
| `temp` | WorldClim V1 BIO-01 | Mean annual temperature (×10 in raw data) | °C × 10 | Static |

### Terrain

| Variable | Source | Description | Units | Type |
|---|---|---|---|---|
| `elev` | USGS SRTM GL1 | Elevation above sea level | m | Static |
| `slope` | Derived from SRTM | Terrain slope | degrees | Static |
| `aspect` | Derived from SRTM | Terrain aspect (0 = flat, 1–360 = bearing from N) | degrees | Static |

### Accessibility

| Variable | Source | Description | Units | Type |
|---|---|---|---|---|
| `dist_cities` | MAP Accessibility to Cities v1.0 (2015) | Motorised travel time to nearest city of ≥ 50,000 people | minutes | 2015 snapshot |
| `friction_surface` | Derived layer | Friction surface (travel time per metre, road-proximity proxy) | min / m | Static |

### Demographics

| Variable | Source | Description | Units | Type |
|---|---|---|---|---|
| `pop_2000` | WorldPop GP 100 m | Population count aggregated to 1 km | count | 2000 |
| `pop_2005` | WorldPop GP 100 m | Population count aggregated to 1 km | count | 2005 |
| `pop_2010` | WorldPop GP 100 m | Population count aggregated to 1 km | count | 2010 |
| `pop_2015` | WorldPop GP 100 m | Population count aggregated to 1 km | count | 2015 |
| `pop_2020` | WorldPop GP 100 m | Population count aggregated to 1 km | count | 2020 |
| `pop_growth` | Derived from WorldPop | Annualised population growth rate 2000–2020 | ratio / year | 2000–2020 |

Population pixels are aggregated by **sum** (not mean) to the 1 km grid to
preserve total head-count semantics.

### Biomass and Carbon

| Variable | Source | Description | Units | Type |
|---|---|---|---|---|
| `agb_2025` | External (Hansen GFC loss-masked) | Aboveground live woody biomass, loss-masked to 2025 | Mg / ha | 2025 |
| `total_biomass_2025` | Derived — Mokany et al. root:shoot ratio applied to AGB | Total biomass (above + belowground), loss-masked to 2025 | Mg / ha | 2025 |
| `soil_oc` | SoilGrids ISRIC (ocs_0–30 cm mean) | Soil organic carbon stock, 0–30 cm depth | t / ha | Static |
| `irr_carbon_2024` | CI external asset (irrC v1a, 30 m, 2024) | Irrecoverable carbon (above + below ground + soil) | Mg C / ha | 2024 |

`total_biomass_2025` is the biomass layer used in the emissions calculation
(see [analysis-outputs.md](analysis-outputs.md#biomass-to-co2e-conversion)).
It is also required as a non-optional input regardless of covariate selection
and is always extracted during Step 1.

### Land Use

| Variable | Source | Description | Units | Epochs |
|---|---|---|---|---|
| `cropland_2003` | GLAD Potapov et al. 2021 | Cropland extent fraction | 0–1 | ~2003 |
| `cropland_2007` | GLAD Potapov et al. 2021 | Cropland extent fraction | 0–1 | ~2007 |
| `cropland_2011` | GLAD Potapov et al. 2021 | Cropland extent fraction | 0–1 | ~2011 |
| `cropland_2015` | GLAD Potapov et al. 2021 | Cropland extent fraction | 0–1 | ~2015 |
| `cropland_2019` | GLAD Potapov et al. 2021 | Cropland extent fraction | 0–1 | ~2019 |
| `aez` | ESA WorldCereal AEZ v100 | Agro-ecological zone ID | categorical integer | Static |

### Forest Cover

| Variable | Source | Description | Units | Years |
|---|---|---|---|---|
| `fc_2000` … `fc_2025` | Hansen GFC v1.13 (derived annually) | Annual forest cover fraction | 0–1 | 2000–2025 (26 layers) |

Forest cover layers are not used as covariates in the propensity score model
directly. They are used by the summarisation step to calculate annual
deforestation rates and biomass loss. All years from `fc_year_start` to
`fc_year_end` (configured per task, default 2000–2025) are always extracted.

---

## Reference / Exact-Match Layers

Reference layers are used **only** as exact-matching stratification variables
(they define groups within which treatment–control pairs must be drawn). They
are sourced from PostGIS vector tables, rasterized to the 1 km COG grid, and
stored alongside the GEE-exported covariates.

| Variable | Source | Description | Rasterization |
|---|---|---|---|
| `admin0` | geoBoundaries ADM0 | Country boundary identifier | mode |
| `admin1` | geoBoundaries ADM1 | Province / state identifier | mode |
| `admin2` | geoBoundaries ADM2 | District identifier | mode |
| `ecoregion` | WWF Terrestrial Ecoregions | Ecoregion identifier | mode |
| `pa` | WDPA (Protected Planet) | Binary protected area mask (1 = protected) | mode |

`ecoregion` and `pa` can also be added to the **covariate** list instead of
(or in addition to) the exact-match list.

Default exact-match variables: `admin1`, `ecoregion`, `pa`.

---

## Pre-Intervention Deforestation Rate

`defor_pre_intervention` is not a standalone covariate COG. It is computed
on-the-fly during Step 1 (extraction) as the mean annual forest loss rate
over the 5 years immediately before a site's `start_date`, using the annual
`fc_YYYY` layers. It is automatically added to the propensity score formula
for any site whose `start_date` is after 2005 (so that at least 5 years of
forest cover data are available).

---

## Additional Covariates (Available but Not Default-Selected)

The following covariates are available in the system but are not included in
the default matching formula. They can be selected by the user when submitting
a task:

| Variable | Notes |
|---|---|
| `pop_2000`, `pop_2005`, `pop_2010`, `pop_2020` | Alternative population year snapshots |
| `agb_2025` | AGB only (use `total_biomass_2025` for full biomass) |
| `soil_oc` | May improve matching in grassland / savanna systems |
| `irr_carbon_2024` | CI-specific irrecoverable carbon layer |
| `cropland_2003`–`cropland_2019` | Useful if agricultural pressure is a key deforestation driver |
| `aez` | Agro-ecological zone; consider using as exact-match variable instead of covariate |
| `aspect` | Usually not a strong deforestation predictor but useful for specific topographic analyses |
| `sdg_baseline` | SDG baseline indicator raster (site-specific, may not be globally available) |
| `sdg_status_2019` | SDG status 2019 indicator raster |
| `sdg_status_2023` | SDG status 2023 indicator raster |

---

## Adding New Covariates

1. Define the export parameters in `gee_export/gee_config.py` (GEE asset ID,
   resampling method, band selection).
2. Trigger the export via the Admin → Covariates panel or
   `gee_export/export_covariates.py`.
3. Once all GEE tiles are complete, the webapp auto-merges them into a single
   COG on S3.
4. Add a style entry in `webapp/layer_config.py` so the layer renders
   correctly on the map. Run
   `webapp/scripts/analyze_cog_distributions.py` to determine appropriate
   `min_value` / `max_value` and colour stops.
5. Add the covariate name to `ALL_COVARIATES` (and optionally
   `DEFAULT_COVARIATES`) in `webapp/layouts/common.py`.
