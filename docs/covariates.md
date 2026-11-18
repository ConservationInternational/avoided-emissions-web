# Covariates and reference layers

## Finding metadata

In the app, the searchable entries below describe every supported matching
covariate, exact-match layer, forest-cover outcome, and derived
pre-intervention deforestation input. Search by identifier, readable name,
category, or source.

Each entry provides source and citation, version, period, native and analysis
resolution, units/codes, processing, role, missing-data behavior, and
limitations. Scientific metadata is maintained in the
[variable catalog](../webapp/documentation.py), shared by these pages and
contextual help, rather than duplicated in separately maintained tables.

Documentation lists supported datasets, not current export status. Submission
and map selectors use deployed-layer availability at the requested resolution.
An entry here does not guarantee that its COG is ready.

## Variable catalog

<!-- variable-catalog -->

## Grids and missing values

Analysis uses an EPSG:4326 grid aligned to origin 0 degrees longitude and
latitude. Nominal 1 km cells are 30 arc-seconds; nominal 250 m cells are
7.5 arc-seconds. Physical cell area varies with latitude. Finer output pixels
do not increase the information content of coarser native data.

Continuous GEE layers generally use mean aggregation; population counts use
sum and categorical zones use mode. Reference polygons are rasterized onto
the aligned grid, not averaged as numeric identifiers. SDG classes use mode
resampling. Consult each entry for exceptions.

Missing required covariates or outcomes exclude pixels from matching.
Zero is a valid observation for many inputs, including no cropland, no
population, and unprotected background. A zero value is not automatically
missing merely because a raster's metadata uses zero as its sentinel.
Treatment pixels without forest at the site's baseline are ineligible.

## Choosing predictors

The initial matching selection emphasizes climate, terrain, accessibility,
and population. Select variables that plausibly explain intervention
placement and forest-loss risk, measured before intervention where possible.

Static snapshots are not automatically pre-intervention. Population growth
spans 2000-2020, accessibility has 2015/2019 snapshots, and SDG products
summarize particular reporting periods. Review their timing for each site.
Loss-masked 2025 biomass can encode intervention-period outcomes and is not
the required historical emissions input.

Numerically encoded classes are not physical quantities. Ecoregion is
explicitly treated as categorical in the model; do not assume every
integer-coded SDG or AEZ variable receives that treatment.

## Reference and control layers

Country, province/state, and district are exact-match constraints.
Ecoregion and protected-area status can each be a predictor or an exact-match
constraint, never both simultaneously.

Exact matching on `pa` can compare protected treatment with other protected
areas. The WDPA raster includes IUCN categories Ia, Ib, II, III, and IV;
it is a snapshot, not historical designation status.
Removing `pa` from exact matching permits unprotected controls but does not
require them.

Candidate controls pass geography and eligibility checks. Matched controls
are the subset selected by the matching algorithm, with matched-set weights.
Neither group should be confused with the reference layers defining strata.

## Automatic outcome and derived inputs

Hansen forest-cover layers define baseline forest, pre-intervention forest
change, and annual loss. They are not selectable propensity-score predictors.
The loss-only series retains year-2000 tree cover until a source pixel's
recorded loss, then sets it to zero; regrowth is not added.

Unmasked `total_biomass_2000` and `fc_2000` are required for historical
emissions. There is no fallback to 2025 biomass.
`defor_pre_intervention` is computed automatically when the forest record
reaches from `start_year - 6` to `start_year - 1`. It is relative cover
change over five years, negative for loss, not an annual rate.

## Generating historical biomass inputs

The upstream preparation script is
`CI_Impacts/KPI_Code/total-biomass-prep/total_biomass.py`.
Its existing 2025 mode remains the default. For the historical reference, run
each stage with `--year 2000` in its configured preparation environment:

```bash
python total_biomass.py process-tile --year 2000 --tile-index <index>
python total_biomass.py merge --year 2000
python total_biomass.py calc-biomass --year 2000
```

Run process-tile for every source tile before merging and calculating biomass.
The year-2000 mode never downloads or applies a Hansen future-loss mask.
It writes separate `agb_2000.tif` and `total_biomass_2000.tif` COGs in both
`cog_1km/` and `cog_250m/`, without overwriting the 2025 products.

Before deploying:

1. Verify complete tile coverage, grid alignment, whole-cell Mg/ha units,
   valid zeros versus NoData, and retention of biomass in subsequently
   deforested areas. Historical total biomass uses -9999 for NoData.
2. Register/sync external covariates and rebuild the analysis image.
3. Recompute old analyses; loss-masked emissions cannot be corrected by a
   global multiplier.
4. Inspect actual COG distributions before adding map styles.

## Adding new covariates

1. Define source/export parameters in `gee_export/gee_config.py`, or the
   appropriate SDG/reference ingestion definitions.
2. Add reviewed scientific metadata to `webapp/documentation.py`.
   Coverage tests require metadata for every configured layer.
3. Export, merge, or rasterize COGs at the desired resolution.
4. Inspect distributions and configure map styles in `webapp/layer_config.py`.
5. Update default selections only when scientifically appropriate.
   The submission selector discovers ready layers dynamically; adding a
   name only to a static UI list does not make a layer available.
