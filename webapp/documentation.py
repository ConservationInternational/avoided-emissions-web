"""Public documentation content and scientific variable metadata (no live I/O)."""

import re
from dataclasses import dataclass
from pathlib import Path

from gee_export import gee_config

DOCS_DIR = Path(__file__).resolve().parents[1] / "docs"
PAGES = {
    "overview": ("How it works", "how-it-works.md"),
    "setup": ("Setting up an analysis", "analysis-setup.md"),
    "variables": ("Covariates and reference layers", "covariates.md"),
    "methodology": ("Detailed methodology", "matching.md"),
    "results": ("Interpreting results", "interpreting-results.md"),
    "limitations": ("Limitations and references", "limitations.md"),
    "outputs": ("Output file reference", "analysis-outputs.md"),
}
SETTING_SECTIONS = {
    "resolution-m": "resolution",
    "preset-selector": "presets",
    "covariate-selection": "covariates",
    "exact-match-selection": "exact-matching",
    "max-treatment-pixels": "max-treatment-pixels",
    "control-multiplier": "control-multiplier",
    "min-site-area-ha": "minimum-site-area",
    "min-glm-treatment-pixels": "minimum-glm-treatment-pixels",
    "caliper-width": "caliper-width",
    "max-controls-per-treatment": "controls-per-treatment",
    "min-control-distance-km": "minimum-control-distance",
    "matching-method": "matching-method",
    "shared-support-controls": "shared-support-controls",
    "separation-fallback-mahalanobis": "separation-fallback",
    "n-replicates": "replicates",
    "random-seed": "random-seed",
    "match-memory-gb": "matching-memory",
    "matching-job-queue": "worker-queue",
}


def heading_id(text: str) -> str:
    text = text.translate(str.maketrans("₂", "2"))
    return re.sub(r"[^\w\s-]", "", text.lower()).replace(" ", "-")


def load_document(page: str) -> str:
    """Load only allowlisted documents, resolving links for in-app navigation."""
    content = (DOCS_DIR / PAGES[page][1]).read_text(encoding="utf-8")
    if "{{" in content:
        from services import ANALYSIS_DEFAULTS, DEFAULT_MATCHING_JOB_QUEUE

        defaults = {
            **ANALYSIS_DEFAULTS,
            "matching_job_queue": DEFAULT_MATCHING_JOB_QUEUE,
        }
        content = re.sub(
            r"\{\{(\w+)\}\}",
            lambda match: str(defaults[match[1]]).lower(),
            content,
        )
    for slug, (_, filename) in PAGES.items():
        content = content.replace(f"]({filename}", f"](/docs/{slug}")
    content = content.replace(
        "](../webapp/documentation.py)",
        "](https://github.com/ConservationInternational/avoided-emissions-web/"
        "blob/main/webapp/documentation.py)",
    )
    return content


@dataclass(frozen=True)
class VariableMetadata:
    title: str
    category: str
    source: str
    url: str
    version: str
    period: str
    native_resolution: str
    units: str
    processing: str
    role: str = "Optional matching predictor"
    nodata: str = "Missing selected values exclude pixels from matching; valid zeros are retained."
    limitations: str = (
        "Use conditions preceding the intervention. A later snapshot can introduce "
        "post-intervention bias; matching does not remove unmeasured confounding."
    )


def variable_catalog() -> dict[str, VariableMetadata]:
    """Describe every configured layer, including derived and nonselectable inputs."""
    catalog = {
        "precip": VariableMetadata(
            "Annual precipitation",
            "Climate",
            "WorldClim BIO12",
            "https://developers.google.com/earth-engine/datasets/catalog/WORLDCLIM_V1_BIO",
            "V1",
            "1960-1990 climate normal",
            "30 arc-seconds",
            "mm/year",
            "Mean aggregation; no additional unit conversion.",
        ),
        "temp": VariableMetadata(
            "Annual mean temperature",
            "Climate",
            "WorldClim BIO01",
            "https://developers.google.com/earth-engine/datasets/catalog/WORLDCLIM_V1_BIO",
            "V1",
            "1960-1990 climate normal",
            "30 arc-seconds",
            "Degrees C x 10",
            "Mean aggregation; stored values retain the source factor of 10.",
        ),
        "dist_cities": VariableMetadata(
            "Travel time to cities",
            "Accessibility",
            "Weiss et al. accessibility to cities",
            "https://doi.org/10.1038/nature25181",
            "2015 v1.0",
            "2015",
            "Approximately 1 km",
            "Minutes",
            "Mean aggregation of motorised travel time to cities of at least 50,000 people.",
        ),
        "friction_surface": VariableMetadata(
            "Travel friction",
            "Accessibility",
            "Malaria Atlas Project friction surface",
            "https://developers.google.com/earth-engine/datasets/catalog/Oxford_MAP_friction_surface_2019",
            "2019 v5.1",
            "2019",
            "Approximately 1 km",
            "Minutes per metre",
            "Mean aggregation; a transport-cost proxy, not a measured distance to roads.",
        ),
        "pop_growth": VariableMetadata(
            "Annualised population growth",
            "Demographics",
            "WorldPop GP population grids",
            "https://developers.google.com/earth-engine/datasets/catalog/WorldPop_GP_100m_pop",
            "GP 100 m population collection",
            "2000-2020",
            "100 m",
            "Ratio per year",
            "((population_2020 / population_2000) ** (1/20)) - 1, then mean aggregation. "
            "Source pixels with zero population in 2000 are assigned growth zero.",
        ),
        "soil_oc": VariableMetadata(
            "Soil organic carbon",
            "Soil",
            "ISRIC SoilGrids",
            "https://www.isric.org/explore/soilgrids",
            "ocs_mean, ocs_0-30cm_mean",
            "Modelled stock; not an annual time series",
            "250 m",
            "Tonnes C/ha",
            "Mean stock over the 0-30 cm depth layer; exported missing values remain NoData.",
        ),
        "irr_carbon_2024": VariableMetadata(
            "Irrecoverable carbon",
            "Biomass and carbon",
            "CI irrecoverable carbon",
            "https://doi.org/10.1038/s41893-021-00803-6",
            "CI irrC v1a, 2024 update",
            "2024",
            "30 m",
            "Mg C/ha",
            "Mean of aboveground, belowground and soil irrecoverable carbon; "
            "not the biomass input used for emissions.",
        ),
        "aez": VariableMetadata(
            "Agro-ecological zone",
            "Ecological",
            "ESA WorldCereal",
            "https://developers.google.com/earth-engine/datasets/catalog/ESA_WorldCereal_AEZ_v100",
            "v100",
            "Static zoning",
            "Vector zones (rasterized at 1 km before aggregation)",
            "Integer zone identifier",
            "Rasterize aez_id, then mode aggregation. IDs are labels, not magnitudes; "
            "consult the source zone definitions.",
            nodata="Uncovered areas are assigned 0 by the source builder; 0 is not automatically missing.",
            limitations="The analysis does not automatically make every integer-coded layer a factor. "
            "Inspect the fitted formula and diagnostics before interpreting numeric zone-ID effects.",
        ),
    }
    for name in ("elev", "slope", "aspect"):
        title, units, processing = {
            "elev": ("Elevation", "Metres above sea level", "Mean SRTM elevation."),
            "slope": (
                "Terrain slope",
                "Degrees",
                "Derive slope from SRTM, then mean aggregation.",
            ),
            "aspect": (
                "Terrain aspect",
                "Degrees clockwise from north",
                (
                    "Derive aspect from SRTM, then arithmetic mean aggregation; "
                    "0 denotes flat terrain. Circular directions near north can average misleadingly."
                ),
            ),
        }[name]
        catalog[name] = VariableMetadata(
            title,
            "Terrain",
            "USGS SRTM GL1",
            "https://developers.google.com/earth-engine/datasets/catalog/USGS_SRTMGL1_003",
            "GL1 003",
            "February 2000 acquisition",
            "30 m",
            units,
            processing,
            limitations="SRTM has limited high-latitude coverage; terrain is treated as time-invariant.",
        )
    for name, config in gee_config.COVARIATES.items():
        if name.startswith("pop_") and "filter_year" in config:
            year = config["filter_year"]
            catalog[name] = VariableMetadata(
                f"Population count ({year})",
                "Demographics",
                "WorldPop GP population grids",
                "https://developers.google.com/earth-engine/datasets/catalog/WorldPop_GP_100m_pop",
                "GP 100 m population collection",
                str(year),
                "100 m",
                "People per analysis cell",
                "Sum aggregation, not mean, preserves head-count semantics.",
            )
        elif name.startswith("cropland_"):
            year = config["year"]
            catalog[name] = VariableMetadata(
                f"Cropland fraction ({year})",
                "Land use",
                "Potapov et al. GLAD cropland",
                "https://doi.org/10.1038/s43016-021-00429-z",
                "Global cropland time series, 2021",
                f"Epoch centred on {year}",
                "30 m",
                "Fraction (0-1)",
                "Mean aggregation of binary cropland extent; 0 is no cropland and 1 is all cropland.",
            )
        elif name.startswith("fc_"):
            year = config["year"]
            catalog[name] = VariableMetadata(
                f"Forest cover ({year})",
                "Forest cover",
                "Hansen et al. Global Forest Change",
                "https://developers.google.com/earth-engine/datasets/catalog/UMD_hansen_global_forest_change_2025_v1_13",
                "2025 v1.13",
                str(year),
                "Approximately 30 m",
                "Percent (0-100)",
                "Mean of treecover2000 with source pixels lost through this year set to zero. "
                "This is a loss-only series: forest gain and regrowth are not added.",
                role="Automatically extracted eligibility and outcome input; not a selectable predictor",
                nodata="Missing required forest years exclude pixels. Valid zero cover is retained, "
                "but zero forest at baseline makes a pixel ineligible.",
                limitations="Tree-cover loss is not necessarily permanent deforestation. "
                "No post-intervention forest-cover outcomes should be selected as predictors.",
            )
    for year in (2000, 2025):
        for prefix, title in (
            ("agb", "Aboveground biomass"),
            ("total_biomass", "Total biomass"),
        ):
            catalog[f"{prefix}_{year}"] = VariableMetadata(
                f"{title} ({year})",
                "Biomass and carbon",
                "WHRC/GFW aboveground live woody biomass stock 2000; "
                "Mokany et al. (2006) belowground estimate for total biomass",
                "https://data-api.globalforestwatch.org/dataset/"
                "whrc_aboveground_woody_biomass_stock_2000/v1.4",
                f"GFW v1.4 source; project-prepared {year} COG (verify task artifact)",
                "2000" if year == 2000 else "2000 biomass, loss-masked through 2025",
                (
                    "Approximately 30 m AGB source; total biomass computed on the 250 m grid"
                    if prefix == "total_biomass"
                    else "Approximately 30 m AGB source"
                ),
                "Mg dry biomass/ha of whole analysis cell",
                (
                    "AGB is mean-aggregated; total biomass = AGB + 0.489 * AGB**0.89 "
                    "(Mokany et al. 2006), computed at 250 m then averaged to 1 km. "
                    if prefix == "total_biomass"
                    else "Mean aggregation. "
                )
                + (
                    "Unmasked historical reference; no future Hansen loss mask."
                    if year == 2000
                    else "Hansen loss-masked through 2025."
                ),
                role=(
                    "Required historical emissions and eligibility input; also selectable as a predictor"
                    if (prefix, year) == ("total_biomass", 2000)
                    else "Optional matching predictor"
                ),
                nodata="Missing values are not zeros. The historical total-biomass COG uses -9999 "
                "for NoData; valid zero biomass is retained.",
                limitations=(
                    "The 2025 product reflects subsequent forest loss and can leak outcomes "
                    "into matching. It is not a replacement for the historical emissions input."
                    if year == 2025
                    else "Verify source version, coverage and whole-cell density semantics. "
                    "Biomass uncertainty is not captured by matching replicate ranges."
                ),
            )
    for level, title in enumerate(("Country", "Province / state", "District")):
        catalog[f"admin{level}"] = VariableMetadata(
            title,
            "Reference layers",
            f"geoBoundaries ADM{level}",
            "https://www.geoboundaries.org/",
            "Imported release; inspect preparation reference versions",
            "Imported boundary snapshot",
            "Vector polygons",
            "Integer boundary identifier",
            "Burn imported database IDs onto the aligned grid (pixel-centre rasterization), "
            "not numeric averaging. Look up names using the reference key.",
            role="Exact-match constraint only",
            nodata="Outside reference coverage is NoData; uncovered site area is reported by preparation.",
            limitations="Administrative boundaries and identifiers change across releases. "
            "More restrictive geography can leave too few controls.",
        )
    catalog["ecoregion"] = VariableMetadata(
        "Ecoregion",
        "Reference layers",
        "RESOLVE terrestrial ecoregions",
        "https://ecoregions.appspot.com/",
        "Imported RESOLVE release",
        "Static ecological classification",
        "Vector polygons",
        "Integer eco_id",
        "Burn eco_id onto the aligned grid; source key maps IDs to names, biome and realm.",
        role="Exact-match constraint OR matching predictor, never both",
        limitations="Eco-ID magnitudes are not ecological distances. Exact matching enforces equality; "
        "as a predictor the pipeline treats ecoregion as categorical.",
    )
    catalog["pa"] = VariableMetadata(
        "Protected-area status",
        "Reference layers",
        "WDPA / Protected Planet",
        "https://www.protectedplanet.net/",
        "Imported WDPA snapshot",
        "Snapshot, not designation history",
        "Vector polygons",
        "Binary: 1 protected; 0 otherwise",
        "Rasterize only IUCN categories Ia, Ib, II, III and IV with value 1; background is 0.",
        role="Exact-match constraint OR matching predictor, never both",
        nodata="Unprotected background 0 is valid, not missing.",
        limitations="Exact matching can compare protected treatment to other protected controls. "
        "Category V, VI, unclassified areas and historical status are not represented as strict protection.",
    )
    for name, year, band in (
        ("sdg_baseline", "2000-2015", 1),
        ("sdg_status_2019", "2019", 9),
        ("sdg_status_2023", "2023", 14),
    ):
        catalog[name] = VariableMetadata(
            f"SDG 15.3.1 {'baseline' if name == 'sdg_baseline' else 'status'} ({year})",
            "Land degradation",
            "Trends.Earth UNCCD reporting product",
            "https://docs.trends.earth/en/latest/for_users/features/unccdreporting.html",
            "Public 2000-2023 multiband COG (2016-2023 reporting directory)",
            (
                "2000-2015 baseline"
                if name == "sdg_baseline"
                else f"Status in {year} versus the 2000-2015 baseline"
            ),
            "0.0022457882102988 degrees (approximately 250 m at the equator)",
            (
                "Classes: -1 degraded, 0 stable, 1 improved"
                if name == "sdg_baseline"
                else "1 persistent degradation; 2 recent degradation; 3 baseline degradation; "
                "4 stable; 5 baseline improvement; 6 recent improvement; 7 persistent improvement"
            ),
            f"Extract source band {band}, then mode resampling to the analysis grid.",
            nodata="Source -32768 is NoData; a baseline value of 0 is valid.",
            limitations="These are categorical codes, not a continuous degradation scale. "
            "Verify encoding in the fitted model; the pipeline does not automatically factor SDG codes. "
            "Periods overlapping or following intervention risk post-intervention bias.",
        )
    catalog["defor_pre_intervention"] = VariableMetadata(
        "Pre-intervention deforestation",
        "Derived",
        "Derived from Hansen forest cover",
        "https://doi.org/10.1126/science.1244693",
        "Same GFC version as extracted forest cover",
        "start_year - 6 through start_year - 1",
        "Analysis grid",
        "Percent relative forest-cover change",
        "100 * (cover_end - cover_start) / cover_start; forest loss is negative. "
        "Added automatically for sites starting in 2006 or later.",
        role="Automatically added matching predictor; no standalone COG",
        limitations="Ends before intervention; measures a five-year interval between endpoints, "
        "not an annual rate. Historical trends do not guarantee parallel future trends.",
    )
    missing = set(gee_config.COVARIATES) - catalog.keys()
    if missing:
        raise ValueError(f"Scientific documentation is missing for: {sorted(missing)}")
    return catalog


VARIABLES = variable_catalog()
