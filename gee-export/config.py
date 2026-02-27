"""Configuration for GEE covariate exports.

Defines all covariate layers, their GEE source assets, band names, export
parameters, and the default matching formula used in the avoided emissions
analysis.
"""

# Target export resolution in arc-degrees (approximately 1 km at the equator)
EXPORT_SCALE_METERS = 927.67  # ~1km at equator in meters
EXPORT_CRS = "EPSG:4326"

# Default GCS path prefix for exported COGs
DEFAULT_GCS_PREFIX = "avoided-emissions/covariates"

# Maximum number of pixels per export tile (GEE limit is ~1e9 per task).
# We tile the global extent into manageable chunks and export each tile as
# a separate GEE task.
MAX_PIXELS_PER_TASK = 1e9

# Full-globe export region
GLOBAL_REGION = {
    "type": "Polygon",
    "coordinates": [[
        [-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]
    ]]
}


# -- Covariate definitions ---------------------------------------------------
# Each entry maps a short covariate name to its GEE source and export config.
# "asset": the GEE asset ID
# "bands": list of band names to export (or None for single-band)
# "select": which band(s) to select from the asset (if renaming)
# "derived": if True, requires a custom function instead of a simple export
# "description": human-readable description
# "resample": aggregation method when resampling to 1km:
#     "mean"  - continuous values (elevation, temperature, fractions, rates)
#     "sum"   - additive counts (population counts, area in hectares)
#     "mode"  - categorical / ID values (ecoregion, biome, admin codes)

COVARIATES = {
    # Climate
    "precip": {
        "asset": "WORLDCLIM/V1/BIO",
        "select": ["bio12"],
        "description": "Annual precipitation (mm)",
        "category": "climate",
        "resample": "mean",
    },
    "temp": {
        "asset": "WORLDCLIM/V1/BIO",
        "select": ["bio01"],
        "description": "Annual mean temperature (C * 10)",
        "category": "climate",
        "resample": "mean",
    },

    # Terrain
    "elev": {
        "asset": "USGS/SRTMGL1_003",
        "select": ["elevation"],
        "description": "Elevation (m)",
        "category": "terrain",
        "resample": "mean",
    },
    "slope": {
        "asset": "USGS/SRTMGL1_003",
        "select": ["elevation"],
        "derived": "slope",
        "description": "Slope (degrees), derived from SRTM",
        "category": "terrain",
        "resample": "mean",
    },

    # Accessibility
    "dist_cities": {
        "asset": "projects/malariaatlasproject/assets/accessibility/accessibility_to_cities/2015_v1_0",
        "select": ["accessibility"],
        "description": "Travel time to nearest city (minutes)",
        "category": "accessibility",
        "resample": "mean",
    },
    "dist_roads": {
        "derived": "friction_surface",
        "description": "Travel friction surface (minutes/m), proxy for road proximity",
        "category": "accessibility",
        "resample": "mean",
    },
    "crop_suitability": {
        "derived": "cropland_fraction",
        "description": "Cropland fraction (0-100%), proxy for crop suitability",
        "category": "accessibility",
        "resample": "mean",
    },

    # Demographics
    "pop_2000": {
        "asset": "WorldPop/GP/100m/pop",
        "filter_year": 2000,
        "select": ["population"],
        "description": "Population count (2000)",
        "category": "demographics",
        "resample": "sum",
    },
    "pop_2005": {
        "asset": "WorldPop/GP/100m/pop",
        "filter_year": 2005,
        "select": ["population"],
        "description": "Population count (2005)",
        "category": "demographics",
        "resample": "sum",
    },
    "pop_2010": {
        "asset": "WorldPop/GP/100m/pop",
        "filter_year": 2010,
        "select": ["population"],
        "description": "Population count (2010)",
        "category": "demographics",
        "resample": "sum",
    },
    "pop_2015": {
        "asset": "WorldPop/GP/100m/pop",
        "filter_year": 2015,
        "select": ["population"],
        "description": "Population count (2015)",
        "category": "demographics",
        "resample": "sum",
    },
    "pop_2020": {
        "asset": "WorldPop/GP/100m/pop",
        "filter_year": 2020,
        "select": ["population"],
        "description": "Population count (2020)",
        "category": "demographics",
        "resample": "sum",
    },
    "pop_growth": {
        "derived": "pop_growth",
        "description": "Annualized population growth rate (2000-2020)",
        "category": "demographics",
        "resample": "mean",
    },

    # Biomass
    "total_biomass": {
        "derived": "total_biomass",
        "description": "Above + below ground biomass (Mg/ha)",
        "category": "biomass",
        "resample": "mean",
    },

    # Land cover (Copernicus 2015, reclassed to 7 categories, in hectares)
    "lc_2015_forest": {
        "derived": "lc_class",
        "lc_class": "forest",
        "description": "Forest land cover area (ha), Copernicus 2015",
        "category": "land_cover",
        "resample": "sum",
    },
    "lc_2015_grassland": {
        "derived": "lc_class",
        "lc_class": "grassland",
        "description": "Grassland land cover area (ha), Copernicus 2015",
        "category": "land_cover",
        "resample": "sum",
    },
    "lc_2015_agriculture": {
        "derived": "lc_class",
        "lc_class": "agriculture",
        "description": "Agriculture land cover area (ha), Copernicus 2015",
        "category": "land_cover",
        "resample": "sum",
    },
    "lc_2015_wetlands": {
        "derived": "lc_class",
        "lc_class": "wetlands",
        "description": "Wetlands land cover area (ha), Copernicus 2015",
        "category": "land_cover",
        "resample": "sum",
    },
    "lc_2015_artificial": {
        "derived": "lc_class",
        "lc_class": "artificial",
        "description": "Artificial surfaces area (ha), Copernicus 2015",
        "category": "land_cover",
        "resample": "sum",
    },
    "lc_2015_other": {
        "derived": "lc_class",
        "lc_class": "other",
        "description": "Other land cover area (ha), Copernicus 2015",
        "category": "land_cover",
        "resample": "sum",
    },
    "lc_2015_water": {
        "derived": "lc_class",
        "lc_class": "water",
        "description": "Water bodies area (ha), Copernicus 2015",
        "category": "land_cover",
        "resample": "sum",
    },

    # Ecological zones
    "ecoregion": {
        "asset": "RESOLVE/ECOREGIONS/2017",
        "select": ["ECO_ID"],
        "description": "WWF ecoregion ID",
        "category": "ecological",
        "resample": "mode",
    },
    "biome": {
        "asset": "RESOLVE/ECOREGIONS/2017",
        "select": ["BIOME_NUM"],
        "description": "WWF biome number",
        "category": "ecological",
        "resample": "mode",
    },

    # Protected areas
    "pa": {
        "asset": "WCMC/WDPA/current/polygons",
        "derived": "pa_binary",
        "description": "Protected area (binary: 1=protected, 0=not)",
        "category": "ecological",
        "resample": "mode",
    },

    # Administrative boundaries
    "region": {
        "asset": "FAO/GAUL/2015/level1",
        "select": ["ADM1_CODE"],
        "description": "GADM level-1 administrative region ID",
        "category": "administrative",
        "resample": "mode",
    },
}

# Forest cover layers: Hansen GFC annual cover by year (2000-2023)
for year in range(2000, 2024):
    COVARIATES[f"fc_{year}"] = {
        "derived": "hansen_fc",
        "year": year,
        "description": f"Hansen GFC forest cover fraction ({year})",
        "category": "forest_cover",
        "resample": "mean",
    }


# -- Matching formula (default) ----------------------------------------------
# This is the standard propensity score matching formula. Users can modify
# the covariate list when submitting analysis tasks.

DEFAULT_MATCHING_COVARIATES = [
    "lc_2015_agriculture",
    "precip",
    "temp",
    "elev",
    "slope",
    "dist_cities",
    "dist_roads",
    "crop_suitability",
    "pop_2015",
    "pop_growth",
    "total_biomass",
]

# These are used for exact matching (stratification), not propensity scores
EXACT_MATCHING_VARIABLES = ["region", "ecoregion", "pa"]

# ESA CCI land cover class mapping (raw value -> category)
ESA_LC_REMAP = {
    "forest": [50, 60, 61, 62, 70, 71, 72, 80, 81, 82, 90, 100, 160, 170],
    "grassland": [110, 120, 121, 122, 130, 140],
    "agriculture": [10, 11, 12, 20, 30, 40],
    "wetlands": [180],
    "artificial": [190],
    "other": [150, 151, 152, 153, 200, 201, 202],
    "water": [210],
}
