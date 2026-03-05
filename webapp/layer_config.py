"""Visualization style configuration for covariate COG layers on the map.

Each style entry defines how a covariate raster should be rendered as a
WebGLTile layer in OpenLayers.  Styles are resolved in order:

1. Per-covariate override in ``COVARIATE_STYLES``
2. Category default in ``CATEGORY_STYLES``
3. Fallback ``DEFAULT_STYLE``

Style properties
----------------
type : str
    ``"continuous"`` — linear interpolation between color stops.
    ``"categorical"`` — discrete color per integer value.
    ``"diverging"`` — two-tailed ramp around a midpoint.
color_stops : list[list]
    Each entry is ``[normalized_position, r, g, b, a]`` for continuous /
    diverging types, or ``[class_value, r, g, b, a]`` for categorical.
    RGB values are 0-255; alpha is 0-1.
min_value / max_value : float
    Data range used to normalize pixel values into [0, 1] for the
    color ramp.  Ignored for categorical styles.
opacity : float
    Default layer opacity (0-1).
legend_label : str | None
    Override label shown in the layer switcher.  Falls back to the
    covariate description from ``gee-export/config.py``.
nodata_value : float | None
    Pixel value treated as transparent.  Defaults to 0 for most layers.
"""

# ── Fallback ────────────────────────────────────────────────────────────────

DEFAULT_STYLE = {
    "type": "continuous",
    "color_stops": [
        [0.0, 255, 255, 204, 1],
        [0.25, 161, 218, 180, 1],
        [0.50, 65, 182, 196, 1],
        [0.75, 34, 94, 168, 1],
        [1.0, 8, 29, 88, 1],
    ],
    "min_value": 0,
    "max_value": 1,
    "opacity": 0.7,
    "nodata_value": 0,
}

# ── Category defaults ───────────────────────────────────────────────────────

CATEGORY_STYLES = {
    "climate": {
        "type": "continuous",
        "color_stops": [
            [0.0, 255, 255, 178, 1],
            [0.25, 254, 204, 92, 1],
            [0.50, 253, 141, 60, 1],
            [0.75, 240, 59, 32, 1],
            [1.0, 128, 0, 38, 1],
        ],
        "opacity": 0.65,
        "nodata_value": 0,
    },
    "terrain": {
        "type": "continuous",
        "color_stops": [
            [0.0, 26, 110, 62, 1],
            [0.20, 123, 200, 123, 1],
            [0.40, 242, 232, 90, 1],
            [0.60, 196, 106, 52, 1],
            [0.80, 160, 80, 50, 1],
            [1.0, 255, 255, 255, 1],
        ],
        "opacity": 0.65,
        "nodata_value": 0,
    },
    "accessibility": {
        "type": "continuous",
        "color_stops": [
            [0.0, 0, 104, 55, 1],
            [0.25, 102, 194, 164, 1],
            [0.50, 254, 224, 139, 1],
            [0.75, 244, 109, 67, 1],
            [1.0, 165, 0, 38, 1],
        ],
        "opacity": 0.65,
        "nodata_value": 0,
    },
    "demographics": {
        "type": "continuous",
        "color_stops": [
            [0.0, 255, 245, 235, 1],
            [0.25, 253, 190, 133, 1],
            [0.50, 253, 141, 60, 1],
            [0.75, 217, 72, 1, 1],
            [1.0, 127, 39, 4, 1],
        ],
        "opacity": 0.65,
        "nodata_value": 0,
    },
    "biomass": {
        "type": "continuous",
        "color_stops": [
            [0.0, 255, 255, 229, 1],
            [0.25, 173, 221, 142, 1],
            [0.50, 49, 163, 84, 1],
            [0.75, 0, 109, 44, 1],
            [1.0, 0, 68, 27, 1],
        ],
        "opacity": 0.65,
        "nodata_value": 0,
    },
    "land_cover": {
        "type": "continuous",
        "color_stops": [
            [0.0, 255, 255, 204, 1],
            [0.25, 161, 218, 180, 1],
            [0.50, 65, 182, 196, 1],
            [0.75, 44, 127, 184, 1],
            [1.0, 37, 52, 148, 1],
        ],
        "opacity": 0.65,
        "nodata_value": 0,
    },
    "ecological": {
        "type": "categorical",
        "color_stops": [],
        "opacity": 0.55,
        "nodata_value": 0,
    },
    "forest_cover": {
        "type": "continuous",
        "color_stops": [
            [0.0, 255, 255, 229, 1],
            [0.25, 173, 221, 142, 1],
            [0.50, 49, 163, 84, 1],
            [0.75, 0, 109, 44, 1],
            [1.0, 0, 68, 27, 1],
        ],
        "min_value": 0,
        "max_value": 1,
        "opacity": 0.65,
        "nodata_value": 0,
    },
    "cropland": {
        "type": "continuous",
        "color_stops": [
            [0.0, 255, 255, 229, 1],
            [0.25, 247, 229, 140, 1],
            [0.50, 219, 173, 61, 1],
            [0.75, 163, 117, 25, 1],
            [1.0, 102, 69, 0, 1],
        ],
        "min_value": 0,
        "max_value": 1,
        "opacity": 0.65,
        "nodata_value": 0,
    },
}

# ── Per-covariate overrides ─────────────────────────────────────────────────

COVARIATE_STYLES = {
    "elev": {
        "type": "continuous",
        "min_value": -100,
        "max_value": 5000,
        "color_stops": [
            [0.0, 26, 110, 62, 1],
            [0.20, 123, 200, 123, 1],
            [0.40, 242, 232, 90, 1],
            [0.60, 196, 106, 52, 1],
            [0.80, 160, 80, 50, 1],
            [1.0, 255, 255, 255, 1],
        ],
        "opacity": 0.65,
    },
    "slope": {
        "type": "continuous",
        "min_value": 0,
        "max_value": 45,
        "opacity": 0.65,
    },
    "aspect": {
        "type": "continuous",
        "min_value": 0,
        "max_value": 360,
        "color_stops": [
            [0.0, 33, 102, 172, 1],
            [0.25, 146, 197, 222, 1],
            [0.50, 247, 247, 247, 1],
            [0.75, 244, 165, 130, 1],
            [1.0, 178, 24, 43, 1],
        ],
        "opacity": 0.55,
    },
    "precip": {
        "type": "continuous",
        "min_value": 0,
        "max_value": 3000,
    },
    "temp": {
        "type": "continuous",
        "min_value": -100,
        "max_value": 320,
    },
    "dist_cities": {
        "type": "continuous",
        "min_value": 0,
        "max_value": 2000,
    },
    "total_biomass": {
        "type": "continuous",
        "min_value": 0,
        "max_value": 400,
    },
    "aez": {
        "type": "categorical",
        "opacity": 0.55,
    },
}

# Population layers share the same scheme.
for _yr in (2000, 2005, 2010, 2015, 2020):
    COVARIATE_STYLES[f"pop_{_yr}"] = {
        "type": "continuous",
        "min_value": 0,
        "max_value": 50000,
    }

COVARIATE_STYLES["pop_growth"] = {
    "type": "diverging",
    "min_value": -0.05,
    "max_value": 0.10,
    "color_stops": [
        [0.0, 5, 48, 97, 1],
        [0.25, 67, 147, 195, 1],
        [0.50, 247, 247, 247, 1],
        [0.75, 214, 96, 77, 1],
        [1.0, 165, 0, 38, 1],
    ],
    "opacity": 0.65,
}

# Land cover hectare layers
for _lc in (
    "forest",
    "grassland",
    "agriculture",
    "wetlands",
    "artificial",
    "other",
    "water",
):
    COVARIATE_STYLES[f"lc_2015_{_lc}"] = {
        "type": "continuous",
        "min_value": 0,
        "max_value": 100,
    }


def get_style(covariate_name, category=None):
    """Resolve the visualization style for a covariate.

    Returns a merged dict of DEFAULT_STYLE ← CATEGORY_STYLES ← COVARIATE_STYLES.
    """
    style = dict(DEFAULT_STYLE)
    if category and category in CATEGORY_STYLES:
        style.update(
            {k: v for k, v in CATEGORY_STYLES[category].items() if v is not None}
        )
    if covariate_name in COVARIATE_STYLES:
        style.update(
            {k: v for k, v in COVARIATE_STYLES[covariate_name].items() if v is not None}
        )
    return style
