# Step 1: Extract covariate values for treatment sites and control regions.
#
# Loads covariate rasters from GCS, loads site polygons, identifies treatment
# pixels (within sites) and control pixels (same GADM region), and saves
# the extracted values for use in the matching step.
#
# Input:
#   - Task config JSON (--config)
#   - Site polygons (GeoJSON or GeoPackage)
#   - Covariate COGs on GCS
#
# Output:
#   - {output_dir}/sites_processed.rds       : Cleaned site geometries
#   - {output_dir}/treatment_cell_key.rds    : Treatment pixel cell IDs by site
#   - {output_dir}/treatments_and_controls.rds : All pixel covariate values
#   - {output_dir}/formula.rds               : Matching formula object
#   - {output_dir}/site_id_key.csv           : Site ID mapping table

library(sf)
library(terra)
library(tidyverse)
library(exactextractr)
library(fasterize)
library(units)
library(foreach)

source("/app/scripts/utils.R")
rollbar_init()

with_rollbar({

config <- parse_config()
message("Step 1: Extracting covariates")
message("  Config: ", toJSON(config, auto_unbox = TRUE, pretty = FALSE))

# Load sites
sites <- load_sites(config$sites_file)
message("  Loaded ", nrow(sites), " sites")

# Filter to terrestrial sites over minimum area
sites <- filter(sites, area_ha >= config$min_site_area_ha)
message("  After area filter: ", nrow(sites), " sites")

if (nrow(sites) == 0) {
    stop("No sites remaining after filtering. Check minimum area threshold.")
}

# Save site ID key
sites %>%
    as_tibble() %>%
    select(site_id, id_numeric, site_name, start_year, end_year, area_ha) %>%
    write_csv(file.path(config$output_dir, "site_id_key.csv"))

# Save processed sites
saveRDS(sites, file.path(config$output_dir, "sites_processed.rds"))

# Build the full list of layers to load: matching covariates + exact match
# variables + forest cover years
all_layers <- c(
    config$covariates,
    config$exact_match_vars,
    paste0("fc_", config$fc_years)
)
message("  Loading ", length(all_layers), " covariate layers from GCS")

# Load covariate rasters
d <- build_covariate_vrt(
    gcs_bucket = config$gcs_bucket,
    gcs_prefix = config$gcs_prefix,
    covariate_names = all_layers
)
message("  Covariate stack dimensions: ", paste(dim(d), collapse = " x "))

# Load GADM regions (exported as a covariate layer)
if (!("region" %in% names(d))) {
    stop("'region' layer must be included in covariate exports")
}

# Save the matching formula
f <- build_matching_formula(config$covariates)
saveRDS(f, file.path(config$output_dir, "formula.rds"))
message("  Formula: ", deparse(f))

# Convert terra SpatRaster to raster package objects for exactextractr
d_raster <- raster::stack(raster::brick(sources(d)))
names(d_raster) <- names(d)

# Extract treatment cell keys (which cells belong to which site)
message("  Extracting treatment cell keys...")
treatment_key <- exact_extract(
    d_raster$region,
    sites,
    include_cell = TRUE,
    include_cols = c("id_numeric", "site_id"),
    force_df = TRUE
) %>%
    bind_rows() %>%
    rename(region = value) %>%
    filter(!is.na(region))

# Compute pixel areas
treatment_key$area_ha <- calc_pixel_area_ha(
    y = exact_extract(
        d_raster[[1]], sites,
        include_cell = TRUE, include_xy = TRUE,
        force_df = TRUE
    ) %>% bind_rows() %>% pull(y) %>% unique(),
    yres = raster::yres(d_raster),
    xres = raster::xres(d_raster)
)

saveRDS(treatment_key, file.path(config$output_dir, "treatment_cell_key.rds"))
message("  Treatment cells: ", nrow(treatment_key))

# Extract covariate values for all regions containing treatment pixels
treatment_regions <- unique(treatment_key$region)
message("  Extracting covariates for ", length(treatment_regions), " regions")

# For large regions, split into quadrants to manage memory
regions_from_raster <- unique(treatment_key$region)

covariate_values <- exact_extract(
    d_raster,
    sites %>% st_buffer(2),  # Buffer to include surrounding control area
    include_cell = TRUE,
    force_df = TRUE
) %>%
    bind_rows() %>%
    filter(coverage_fraction >= 0.99) %>%
    select(-coverage_fraction) %>%
    distinct()

message("  Total covariate values extracted: ", nrow(covariate_values), " pixels")

saveRDS(
    covariate_values,
    file.path(config$output_dir, "treatments_and_controls.rds")
)

message("Step 1 complete.")

}, step_name = "01_extract_covariates")
