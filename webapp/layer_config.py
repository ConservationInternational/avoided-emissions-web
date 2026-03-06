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
    For right-skewed data, use non-linear stop positions so that more
    colour bandwidth is allocated to the densely-populated low end.
min_value / max_value : float
    Data range used to normalize pixel values into [0, 1] for the
    color ramp.  Ignored for categorical styles.  Use approx p2/p98
    from ``scripts/analyze_cog_distributions.py``.
opacity : float
    Default layer opacity (0-1).
legend_label : str | None
    Override label shown in the layer switcher.  Falls back to the
    covariate description from ``gee-export/config.py``.
nodata_value : float | None
    Pixel value treated as transparent.  Set to ``None`` when the
    GeoTIFF has no explicit nodata, or when zero is a valid data value.
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
    "nodata_value": None,
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
        "opacity": 1.0,
        "nodata_value": None,
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
        "opacity": 1.0,
        "nodata_value": None,
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
        "opacity": 1.0,
        "nodata_value": None,
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
        "opacity": 1.0,
        "nodata_value": None,
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
        "opacity": 1.0,
        "nodata_value": None,
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
        "opacity": 1.0,
        "nodata_value": None,
    },
    "ecological": {
        "type": "categorical",
        "color_stops": [],
        "opacity": 0.55,
        "nodata_value": None,
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
        "opacity": 1.0,
        "nodata_value": None,
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
        "opacity": 1.0,
        "nodata_value": None,
    },
}

# ── Per-covariate overrides ─────────────────────────────────────────────────
#
# Data ranges and stop positions are derived from the output of
# ``scripts/analyze_cog_distributions.py``.
#
# Right-skewed layers use non-linear stop positions so that the dense
# low-value region gets more colour bandwidth.  The pattern is:
#   [0, ...], [0.01–0.05, ...], [0.10–0.15, ...], [0.30, ...], [0.60, ...], [1.0, ...]
# This prevents 80%+ of the map from rendering as a single flat colour.

COVARIATE_STYLES = {
    # ── Terrain ──────────────────────────────────────────────────────────
    # elev: range [-415, 6609], p2=0, p50=282, p98=3739, 18.8% zeros
    "elev": {
        "type": "continuous",
        "min_value": 0,
        "max_value": 4000,
        "color_stops": [
            [0.0, 26, 110, 62, 1],
            [0.10, 86, 175, 86, 1],
            [0.25, 188, 220, 105, 1],
            [0.45, 242, 232, 90, 1],
            [0.65, 196, 106, 52, 1],
            [0.85, 160, 80, 50, 1],
            [1.0, 255, 255, 255, 1],
        ],
        "opacity": 1.0,
        "nodata_value": None,
    },
    # slope: range [0, 55], p2=0, p50=2.66, p98=28, 19.3% zeros
    # Right-skewed: most land is flat, steep slopes are rare.
    "slope": {
        "type": "continuous",
        "min_value": 0,
        "max_value": 30,
        "color_stops": [
            [0.0, 255, 255, 204, 1],
            [0.03, 217, 240, 163, 1],
            [0.10, 173, 221, 142, 1],
            [0.25, 120, 198, 121, 1],
            [0.50, 49, 163, 84, 1],
            [0.75, 0, 109, 44, 1],
            [1.0, 0, 68, 27, 1],
        ],
        "opacity": 1.0,
        "nodata_value": None,
    },
    # aspect: range [0, 326], p2=0, p50=142, p98=252, 30% zeros
    # Circular variable — N/S/E/W.  Fairly uniform when non-zero.
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
        "nodata_value": None,
    },
    # ── Climate ──────────────────────────────────────────────────────────
    # precip: range [0, 8960], p2=16, p50=490, p98=2762
    "precip": {
        "type": "continuous",
        "min_value": 0,
        "max_value": 3000,
        "color_stops": [
            [0.0, 255, 255, 204, 1],
            [0.05, 229, 245, 190, 1],
            [0.15, 161, 218, 180, 1],
            [0.35, 65, 182, 196, 1],
            [0.60, 34, 94, 168, 1],
            [1.0, 8, 29, 88, 1],
        ],
        "opacity": 1.0,
        "nodata_value": None,
    },
    # temp: range [-275, 314], p2=-201, p50=87, p98=281
    # Values are C*10.  Fairly symmetric.
    "temp": {
        "type": "diverging",
        "min_value": -200,
        "max_value": 300,
        "color_stops": [
            [0.0, 5, 48, 97, 1],
            [0.20, 67, 147, 195, 1],
            [0.40, 171, 217, 233, 1],
            [0.50, 247, 247, 247, 1],
            [0.60, 253, 174, 97, 1],
            [0.80, 244, 109, 67, 1],
            [1.0, 165, 0, 38, 1],
        ],
        "opacity": 1.0,
        "nodata_value": None,
    },
    # ── Accessibility ────────────────────────────────────────────────────
    # dist_cities: range [0, 25083], p2=10, p50=414, p98=11014
    # Heavily right-skewed.
    "dist_cities": {
        "type": "continuous",
        "min_value": 0,
        "max_value": 12000,
        "color_stops": [
            [0.0, 0, 104, 55, 1],
            [0.02, 26, 152, 80, 1],
            [0.05, 102, 194, 164, 1],
            [0.12, 171, 221, 164, 1],
            [0.30, 254, 224, 139, 1],
            [0.60, 244, 109, 67, 1],
            [1.0, 165, 0, 38, 1],
        ],
        "opacity": 1.0,
        "nodata_value": None,
    },
    # friction_surface: range [0, 1.21], p2=0, p50=0, p98=0.04
    # Extremely right-skewed — nearly all pixels are 0 or near-0.
    "friction_surface": {
        "type": "continuous",
        "min_value": 0,
        "max_value": 0.06,
        "color_stops": [
            [0.0, 0, 104, 55, 1],
            [0.05, 102, 194, 164, 1],
            [0.15, 171, 221, 164, 1],
            [0.35, 254, 224, 139, 1],
            [0.65, 244, 109, 67, 1],
            [1.0, 165, 0, 38, 1],
        ],
        "opacity": 1.0,
        "nodata_value": None,
    },
    # ── Biomass ──────────────────────────────────────────────────────────
    # total_biomass: range [0, 329], p2=0, p50=11.6, p98=~150
    # Right-skewed, 24% zeros, 82% nodata.
    "total_biomass": {
        "type": "continuous",
        "min_value": 0,
        "max_value": 170,
        "color_stops": [
            [0.0, 255, 255, 229, 1],
            [0.02, 247, 252, 196, 1],
            [0.08, 217, 240, 163, 1],
            [0.20, 173, 221, 142, 1],
            [0.40, 120, 198, 121, 1],
            [0.65, 49, 163, 84, 1],
            [0.85, 0, 109, 44, 1],
            [1.0, 0, 68, 27, 1],
        ],
        "opacity": 1.0,
        "nodata_value": None,
    },
    # ── Ecological ───────────────────────────────────────────────────────
    "aez": {
        "type": "categorical",
        "opacity": 0.55,
        "nodata_value": None,
    },
    # pa: binary (1 = protected, nodata = not protected)
    # 96.9% nodata — only protected areas have pixels.
    "pa": {
        "type": "categorical",
        "color_stops": [
            [1, 34, 139, 34, 0.6],
        ],
        "opacity": 0.60,
        "nodata_value": None,
    },
}

# ── Forest cover (fc_YYYY) ──────────────────────────────────────────────────
# range [0, 100], p2=0, p50=0, p98=84, 80.7% zeros
# Extremely right-skewed: most of the globe has 0% forest cover.
# Quantile-based stops concentrate colour in the 0–20% range where most
# variation occurs among forested pixels.
_fc_style = {
    "type": "continuous",
    "min_value": 0,
    "max_value": 100,
    "color_stops": [
        [0.0, 255, 255, 229, 0],
        [0.005, 255, 255, 229, 1],
        [0.02, 247, 252, 196, 1],
        [0.05, 217, 240, 163, 1],
        [0.12, 173, 221, 142, 1],
        [0.25, 120, 198, 121, 1],
        [0.50, 49, 163, 84, 1],
        [0.75, 0, 109, 44, 1],
        [1.0, 0, 68, 27, 1],
    ],
    "opacity": 1.0,
    "nodata_value": None,
}
for _yr in range(2000, 2025):
    COVARIATE_STYLES[f"fc_{_yr}"] = _fc_style

# ── Population (pop_YYYY) ───────────────────────────────────────────────────
# range [0, 886], p2=0, p50=0, p98=2.75, 30.5% zeros + 71.8% nodata
# Extremely right-skewed — median is 0, p99 is 5.6, but max is ~886.
# Most populated areas are in a tiny fraction of the range.
_pop_style = {
    "type": "continuous",
    "min_value": 0,
    "max_value": 10,
    "color_stops": [
        [0.0, 255, 255, 204, 0],
        [0.005, 255, 255, 204, 1],
        [0.02, 255, 237, 160, 1],
        [0.06, 254, 217, 118, 1],
        [0.15, 254, 178, 76, 1],
        [0.30, 253, 141, 60, 1],
        [0.55, 240, 59, 32, 1],
        [0.80, 189, 0, 38, 1],
        [1.0, 128, 0, 38, 1],
    ],
    "opacity": 1.0,
    "nodata_value": None,
}
for _yr in (2000, 2005, 2010, 2015, 2020):
    COVARIATE_STYLES[f"pop_{_yr}"] = _pop_style

# pop_growth: range [-0.28, 0.84], p2=-0.07, p50=0, p98=0.10
# Diverging around zero — negative = shrinking, positive = growing.
COVARIATE_STYLES["pop_growth"] = {
    "type": "diverging",
    "min_value": -0.10,
    "max_value": 0.12,
    "color_stops": [
        [0.0, 5, 48, 97, 1],
        [0.20, 67, 147, 195, 1],
        [0.40, 171, 217, 233, 1],
        [0.455, 247, 247, 247, 1],
        [0.60, 253, 174, 97, 1],
        [0.80, 214, 96, 77, 1],
        [1.0, 165, 0, 38, 1],
    ],
    "opacity": 1.0,
    "nodata_value": None,
}

# ── Land cover hectare layers ────────────────────────────────────────────────
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
        "nodata_value": None,
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
