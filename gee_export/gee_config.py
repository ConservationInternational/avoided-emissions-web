"""Configuration for GEE covariate exports.

Defines all covariate layers, their GEE source assets, band names, export
parameters, and the default matching formula used in the avoided emissions
analysis.
"""

EXPORT_CRS = "EPSG:4326"

# ---------------------------------------------------------------------------
# Forest cover year range (Hansen GFC)
# ---------------------------------------------------------------------------
# Single source of truth for forest cover year boundaries.
# FC_YEAR_MIN: First year of available Hansen forest cover data
# FC_YEAR_MAX: Last year of available Hansen forest cover data
# These determine which fc_YYYY covariates are defined and exported.
FC_YEAR_MIN = 2000
FC_YEAR_MAX = 2025  # Updated to match Hansen GFC 2025 v1.13

# ---------------------------------------------------------------------------
# Resolution presets
# ---------------------------------------------------------------------------
# Each preset defines a pixel size in degrees and a GCS sub-prefix so that
# 1 km and 250 m covariates live side-by-side in the same bucket.  The
# ``RESOLUTIONS`` dict is keyed by the nominal resolution in metres.
#
# 1 km  →  30 arc-seconds  = 1/120°  ≈  927.67 m at the equator
# 250 m →  ~8 arc-seconds  = 1/480°  ≈  231.92 m at the equator

RESOLUTIONS = {
    1000: {
        "pixel_size_deg": 1 / 120,  # 30 arc-seconds
        "label": "1 km",
        "gcs_suffix": "_1km",  # e.g. avoided-emissions/covariates_1km
        "cog_suffix": "_1km",  # e.g. avoided-emissions/cog_1km
    },
    250: {
        "pixel_size_deg": 1 / 480,  # ~8 arc-seconds
        "label": "250 m",
        "gcs_suffix": "_250m",  # e.g. avoided-emissions/covariates_250m
        "cog_suffix": "_250m",  # e.g. avoided-emissions/cog_250m
    },
}

# Legacy COG prefix (no suffix) is treated as 1 km for backwards
# compatibility.  New exports always write to the resolution-specific
# prefixes above.
LEGACY_COG_SUFFIX = ""  # cog/{name}.tif → assumed 1 km

DEFAULT_RESOLUTION_M = 1000

# Pixel size in degrees.  30 arc-seconds = 1/120 of a degree ≈ 927.67 m at
# the equator.  Defined in degrees (the native unit of EPSG:4326) so the
# crsTransform can be stated exactly.
EXPORT_PIXEL_SIZE_DEG = RESOLUTIONS[DEFAULT_RESOLUTION_M]["pixel_size_deg"]

# Affine transform that locks every export to the same pixel grid.
#   [xScale, xShearing, xTranslation, yShearing, yScale, yTranslation]
# Origin at (0°E, 0°N) means pixel edges fall on exact multiples of
# 1/120° (including ±180° and ±90°), so all covariates share the same
# grid regardless of their native resolution.
EXPORT_CRS_TRANSFORM = [
    EXPORT_PIXEL_SIZE_DEG,  # xScale
    0,  # xShearing
    0,  # xTranslation  (origin 0° E)
    0,  # yShearing
    -EXPORT_PIXEL_SIZE_DEG,  # yScale  (negative = rows go south)
    0,  # yTranslation  (origin 0° N)
]

# Default GCS path prefix for exported COGs
DEFAULT_GCS_PREFIX = "avoided-emissions/covariates"


def get_crs_transform(resolution_m=DEFAULT_RESOLUTION_M):
    """Return the affine *crsTransform* list for a given resolution."""
    pix = RESOLUTIONS[resolution_m]["pixel_size_deg"]
    return [pix, 0, 0, 0, -pix, 0]


def get_gcs_prefix(base_prefix=DEFAULT_GCS_PREFIX, resolution_m=DEFAULT_RESOLUTION_M):
    """Return the GCS prefix for a given resolution."""
    suffix = RESOLUTIONS[resolution_m]["gcs_suffix"]
    return f"{base_prefix}{suffix}"


def get_cog_suffix(resolution_m=DEFAULT_RESOLUTION_M):
    """Return the S3 COG sub-folder suffix for a given resolution."""
    return RESOLUTIONS[resolution_m]["cog_suffix"]


# Maximum number of pixels allowed in a single Export.image task.
# The GEE default is 1e8 (100 million); it can be raised up to 1e13.
# At ~1 km resolution the global extent has ~933 million pixels, so we
# set this high enough to avoid a pixel-count error.  Note that GEE also
# enforces a hard 32,768-pixel limit per dimension.  At 30 arc-seconds
# the global width is ~43,200 px, which exceeds that limit, so GEE will
# always split the export into at least 2 tiles regardless of maxPixels.
#
# At 250 m (1/480°) the global grid is ~172,800 × 86,400 = ~14.93 billion
# pixels, so the limit must be ≥ 15e9.  GEE allows up to 1e13.
MAX_PIXELS_PER_TASK = 2e10

# Full-globe export region
GLOBAL_REGION = {
    "type": "Polygon",
    "coordinates": [[[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]],
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
    "aspect": {
        "asset": "USGS/SRTMGL1_003",
        "select": ["elevation"],
        "derived": "aspect",
        "description": "Aspect (degrees), directional orientation from SRTM",
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
    "friction_surface": {
        "derived": "friction_surface",
        "description": "Travel friction surface (minutes/m), proxy for road proximity",
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
    # Soil
    "soil_oc": {
        "asset": "projects/soilgrids-isric/ocs_mean",
        "select": ["ocs_0-30cm_mean"],
        "description": "Soil organic carbon stock, 0-30 cm (t/ha)",
        "category": "soil",
        "resample": "mean",
        "unmask_nodata": True,
    },
    # Carbon
    "irr_carbon_2024": {
        "asset": "projects/ci_external_assets/irrC/Update/Irrecoverable_Carbon_Total_v1a_30m_2024",
        "select": ["b1"],
        "description": "Irrecoverable carbon total, 2024 (Mg C/ha)",
        "category": "biomass",
        "resample": "mean",
        "unmask_nodata": True,
    },
}

# Agro-ecological zones
COVARIATES["aez"] = {
    "asset": "ESA/WorldCereal/AEZ/v100",
    "derived": "aez",
    "description": "ESA WorldCereal agro-ecological zone ID",
    "category": "ecological",
    "resample": "mode",
}

# Forest cover layers: Hansen GFC annual cover by year
# Uses FC_YEAR_MIN and FC_YEAR_MAX constants defined at top of file.
for year in range(FC_YEAR_MIN, FC_YEAR_MAX + 1):
    COVARIATES[f"fc_{year}"] = {
        "derived": "hansen_fc",
        "year": year,
        "description": f"Hansen GFC forest cover fraction ({year})",
        "category": "forest_cover",
        "resample": "mean",
    }

# GLAD Global Cropland Expansion Time-series (Potapov et al. 2021)
# Binary cropland maps at 30m for five epochs: 2003, 2007, 2011, 2015, 2019.
# At ~1km export the mean resampling yields cropland fraction (0-1).
for year in (2003, 2007, 2011, 2015, 2019):
    COVARIATES[f"cropland_{year}"] = {
        "derived": "glad_cropland",
        "year": year,
        "description": f"GLAD cropland extent fraction ({year})",
        "category": "cropland",
        "resample": "mean",
    }


# -- Matching formula (default) ----------------------------------------------
# This is the standard propensity score matching formula. Users can modify
# the covariate list when submitting analysis tasks.

DEFAULT_MATCHING_COVARIATES = [
    "precip",
    "temp",
    "elev",
    "slope",
    "dist_cities",
    "friction_surface",
    "pop_2015",
    "pop_growth",
    "total_biomass",
]

# These are used for exact matching (stratification), not propensity scores.
# Names must match the output_name values in webapp/rasterize_vectors.py.
EXACT_MATCHING_VARIABLES = ["admin0", "admin1", "admin2", "ecoregion", "pa"]
