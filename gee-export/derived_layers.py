"""Derived covariate layer builders for GEE.

Functions that construct derived ee.Image objects for covariates that cannot
be loaded as simple assets (e.g., slope from DEM, population growth rates,
Hansen forest cover by year, land cover class areas, total biomass, and
protected area masks).
"""

import ee

from config import EXPORT_SCALE_METERS


def build_slope():
    """Compute slope in degrees from the SRTM DEM."""
    dem = ee.Image("USGS/SRTMGL1_003").select("elevation")
    return ee.Terrain.slope(dem).rename("slope")


def build_pop_growth():
    """Compute annualized population growth rate between 2000 and 2020.

    Uses the WorldPop 100m population grids. Growth rate is calculated as
    the compound annual growth rate: ((pop_2020 / pop_2000) ^ (1/20)) - 1.
    Areas with zero population in 2000 are set to 0.
    """
    pop_2000 = (
        ee.ImageCollection("WorldPop/GP/100m/pop")
        .filter(ee.Filter.eq("year", 2000))
        .mosaic()
        .select("population")
        .rename("pop_2000")
    )
    pop_2020 = (
        ee.ImageCollection("WorldPop/GP/100m/pop")
        .filter(ee.Filter.eq("year", 2020))
        .mosaic()
        .select("population")
        .rename("pop_2020")
    )
    ratio = pop_2020.divide(pop_2000.where(pop_2000.eq(0), 1))
    growth = ratio.pow(1.0 / 20).subtract(1)
    # Zero out where population in 2000 was zero
    growth = growth.where(pop_2000.eq(0), 0)
    return growth.rename("pop_growth").toFloat()


def build_total_biomass():
    """Load baseline carbon/biomass density at 30m resolution.

    Uses the CI Geospatial Assets Baseline Carbon Biomass 2010 dataset
    at 30m resolution (v2).
    """
    biomass = ee.Image(
        "projects/ci_geospatial_assets/Baseline_Carbon_Biomass_2010_30m_v2"
    )
    return biomass.rename("total_biomass").toFloat()


def build_hansen_fc(year):
    """Build Hansen GFC forest cover fraction for a given year.

    Forest cover in 2000 is the treecover2000 band. For subsequent years,
    forest loss up to that year is subtracted. Forest gain is only available
    as a cumulative layer through 2012 and is added for years >= 2012.

    Returns forest cover as a percentage (0-100).
    """
    gfc = ee.Image("UMD/hansen/global_forest_change_2024_v1_12")
    tree_cover_2000 = gfc.select("treecover2000")

    if year == 2000:
        return tree_cover_2000.rename(f"fc_{year}").toFloat()

    # Loss year is encoded as years since 2000 (1 = 2001, etc.)
    loss_year = gfc.select("lossyear")
    # Cumulative loss through the target year
    years_since_2000 = year - 2000
    loss_mask = loss_year.gt(0).And(loss_year.lte(years_since_2000))
    fc = tree_cover_2000.where(loss_mask, 0)

    return fc.rename(f"fc_{year}").toFloat()


def build_lc_class(lc_class):
    """Build a land cover class area layer from Copernicus Global LC 2015.

    Reclassifies the Copernicus 100m land cover map (discrete_classification)
    to the target class, then computes the area of the class within each
    ~1km pixel. Returns area in hectares.
    """
    lc = ee.Image(
        "COPERNICUS/Landcover/100m/Proba-V-C3/Global/2015"
    ).select("discrete_classification")

    # Copernicus discrete classification values:
    #   0=Unknown, 20=Shrubs, 30=Herbaceous, 40=Cultivated,
    #   50=Urban, 60=Bare/sparse, 70=Snow/ice, 80=Water,
    #   90=Herbaceous wetland, 100=Moss/lichen,
    #   111=Closed forest evergreen needle, 112=Closed forest evergreen broad,
    #   113=Closed forest deciduous needle, 114=Closed forest deciduous broad,
    #   115=Closed forest mixed, 116=Closed forest unknown,
    #   121=Open forest evergreen needle, 122=Open forest evergreen broad,
    #   123=Open forest deciduous needle, 124=Open forest deciduous broad,
    #   125=Open forest mixed, 126=Open forest unknown, 200=Oceans
    COPERNICUS_LC_REMAP = {
        "forest": [111, 112, 113, 114, 115, 116, 121, 122, 123, 124, 125, 126],
        "grassland": [20, 30],
        "agriculture": [40],
        "wetlands": [90],
        "artificial": [50],
        "other": [60, 70, 100],
        "water": [80, 200],
    }

    class_values = COPERNICUS_LC_REMAP.get(lc_class, [])
    if not class_values:
        raise ValueError(f"Unknown land cover class: {lc_class}")

    # Create binary mask for this class
    mask = lc.eq(class_values[0])
    for val in class_values[1:]:
        mask = mask.Or(lc.eq(val))

    # Compute area fraction at target resolution. Each source pixel is 300m,
    # so within a ~1km target pixel there are roughly 9-12 source pixels.
    # We use reduceResolution to get the mean (fraction), then multiply
    # by the pixel area in hectares.
    pixel_area_ha = ee.Image.pixelArea().divide(10000)
    class_area = mask.multiply(pixel_area_ha)

    return class_area.rename(f"lc_2015_{lc_class}").toFloat()


def build_pa_binary():
    """Build a binary protected area layer from WDPA.

    Returns 1 where any WDPA polygon exists, 0 otherwise.
    """
    wdpa = ee.FeatureCollection("WCMC/WDPA/current/polygons")
    pa_image = (
        wdpa.reduceToImage(["WDPAID"], ee.Reducer.first())
        .gt(0)
        .unmask(0)
    )
    return pa_image.rename("pa").toInt()


def build_friction_surface():
    """Build a travel friction surface layer.

    Uses the Oxford MAP global friction surface (2019) which represents
    travel time cost (minutes per metre) across the landscape. Lower values
    indicate proximity to roads and other transport infrastructure.
    This serves as a proxy for distance to roads.
    """
    friction = ee.Image(
        "projects/malariaatlasproject/assets/accessibility/friction_surface/2019_v5_1"
    )
    return friction.rename("dist_roads").toFloat()


def build_cropland_fraction():
    """Build a cropland fraction layer from Copernicus Global LC 2015.

    Uses the 'crops-coverfraction' band from the Copernicus 100m land cover
    dataset, which gives the percentage of each pixel covered by cropland
    (0-100). This serves as a proxy for crop suitability.
    """
    cropland = ee.Image(
        "COPERNICUS/Landcover/100m/Proba-V-C3/Global/2015"
    ).select("crops-coverfraction")
    return cropland.rename("crop_suitability").toFloat()


def build_glad_cropland(year):
    """Build a GLAD cropland extent layer for a given epoch year.

    Uses the GLAD Global Cropland Expansion Time-series (Potapov et al.,
    2021, Nature Food) which provides binary cropland maps at 30m
    resolution for five epochs: 2003, 2007, 2011, 2015, and 2019.
    Cropland is defined as land used for annual and perennial herbaceous
    crops for human consumption, forage (including hay), and biofuel.

    At the ~1km export resolution the binary mask is resampled via mean
    aggregation, yielding the fraction of each pixel that is cropland
    (0.0-1.0).

    Returns:
        ee.Image with a single band named ``cropland_{year}``.
    """
    valid_years = (2003, 2007, 2011, 2015, 2019)
    if year not in valid_years:
        raise ValueError(
            f"GLAD cropland year must be one of {valid_years}, got {year}"
        )
    asset_id = f"users/potapovpeter/Global_cropland_{year}"
    cropland = ee.Image(asset_id)
    return cropland.rename(f"cropland_{year}").toFloat()


# geoBoundaries ADM1 properties to include in the CSV key
_GEOBOUNDARIES_KEY_PROPERTIES = [
    "shapeGroup",
    "shapeName",
    "shapeISO",
    "shapeID",
    "shapeType",
]


def _build_indexed_admin_fc():
    """Load the geoBoundaries ADM1 FeatureCollection with sequential IDs.

    Returns the FeatureCollection with an added 'region_id' property
    (1-based integer) on each feature.
    """
    fc = ee.FeatureCollection("WM/geoLab/geoBoundaries/600/ADM1")
    n = fc.size()
    indexed_fc = ee.FeatureCollection(
        fc.toList(n)
        .zip(ee.List.sequence(1, n))
        .map(
            lambda pair: ee.Feature(ee.List(pair).get(0)).set(
                "region_id", ee.List(pair).get(1)
            )
        )
    )
    return indexed_fc


def build_aez():
    """Build an agro-ecological zone ID raster from ESA WorldCereal AEZ.

    Uses the ESA WorldCereal Agro-Ecological Zones v100 FeatureCollection.
    Each feature has an ``aez_id`` property (integer) that uniquely identifies
    the agro-ecological zone.  The FeatureCollection is rasterized to produce
    a single-band integer image.

    See: https://developers.google.com/earth-engine/datasets/catalog/ESA_WorldCereal_AEZ_v100
    """
    aez_fc = ee.FeatureCollection("ESA/WorldCereal/AEZ/v100")
    aez_image = (
        aez_fc.reduceToImage(["aez_id"], ee.Reducer.first())
        .unmask(0)
        .rename("aez")
    )
    return aez_image.toInt()


def build_admin_region():
    """Build an administrative region ID raster from geoBoundaries ADM1.

    Loads the WM/geoLab/geoBoundaries/600/ADM1 FeatureCollection, assigns
    each feature a unique sequential integer ID, and rasterizes the result.
    Used for exact matching (stratification) in the avoided-emissions analysis.
    """
    indexed_fc = _build_indexed_admin_fc()

    region_image = (
        indexed_fc.reduceToImage(["region_id"], ee.Reducer.first())
        .unmask(0)
        .rename("region")
    )
    return region_image.toInt()


def fetch_admin_region_key():
    """Fetch the mapping between sequential region_id and geoBoundaries properties.

    Queries GEE for the indexed geoBoundaries ADM1 FeatureCollection and
    returns a list of dicts with region_id and the original feature attributes.

    Returns:
        list[dict]: Each dict has 'region_id' plus the geoBoundaries
        attribute columns (shapeGroup, shapeName, shapeISO, shapeID,
        shapeType).
    """
    indexed_fc = _build_indexed_admin_fc()

    # Select only the columns we need (drops geometry for faster transfer)
    keep_cols = ["region_id"] + _GEOBOUNDARIES_KEY_PROPERTIES
    key_fc = indexed_fc.select(keep_cols)

    features = key_fc.getInfo()["features"]
    rows = []
    for f in features:
        props = f["properties"]
        rows.append({col: props.get(col) for col in keep_cols})
    return rows


def get_derived_image(covariate_name, covariate_config):
    """Dispatch to the appropriate builder for a derived covariate.

    Args:
        covariate_name: The short name of the covariate.
        covariate_config: The config dict for this covariate from COVARIATES.

    Returns:
        An ee.Image with a single band named after the covariate.
    """
    derived_type = covariate_config.get("derived")

    if derived_type == "slope":
        return build_slope()
    elif derived_type == "pop_growth":
        return build_pop_growth()
    elif derived_type == "total_biomass":
        return build_total_biomass()
    elif derived_type == "hansen_fc":
        return build_hansen_fc(covariate_config["year"])
    elif derived_type == "lc_class":
        return build_lc_class(covariate_config["lc_class"])
    elif derived_type == "pa_binary":
        return build_pa_binary()
    elif derived_type == "friction_surface":
        return build_friction_surface()
    elif derived_type == "cropland_fraction":
        return build_cropland_fraction()
    elif derived_type == "glad_cropland":
        return build_glad_cropland(covariate_config["year"])
    elif derived_type == "aez":
        return build_aez()
    elif derived_type == "admin_region":
        return build_admin_region()
    else:
        raise ValueError(
            f"Unknown derived type '{derived_type}' for {covariate_name}"
        )
