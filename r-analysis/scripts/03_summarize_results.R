# Step 3: Summarize matching results into avoided emissions estimates.
#
# Reads all per-site match files, computes forest cover trajectories for
# matched treatment-control pairs, and calculates avoided emissions in
# MgCO2e using the standard biomass-to-carbon-to-CO2e conversion.
#
# Emissions calculation:
#   forest_frac_remaining = forest_at_year_end / forest_at_year_start
#   biomass_at_year_end = total_biomass * forest_frac_remaining
#   C_change = diff(biomass_at_year_end) * 0.5   (biomass -> carbon)
#   Emissions_MgCO2e = C_change * -3.67           (carbon -> CO2e)
#   Avoided = control_emissions - treatment_emissions
#
# Output:
#   - {output_dir}/results_by_site_year.csv    : Per-site per-year results
#   - {output_dir}/results_by_site_total.csv   : Per-site totals
#   - {output_dir}/results_pixel_level.csv     : Pixel-level detail
#   - {output_dir}/results_summary.json        : Global summary

library(tidyverse)
library(foreach)
library(jsonlite)
library(arrow)

source("/app/scripts/utils.R")
rollbar_init()

with_rollbar({

config <- parse_config()
message("Step 3: Summarizing results")

# Load site metadata (Parquet from Python step 1)
sites <- read_parquet(file.path(config$output_dir, "sites_processed.parquet")) %>%
    as_tibble()

# Load all match files
match_files <- list.files(config$matches_dir, pattern = "^m_[0-9]+\\.rds$",
                          full.names = TRUE)

# Load failure markers written by step 2 (tryCatch) or by the Python
# wrapper (OOM-killed subprocess)
failure_files <- list.files(config$matches_dir,
                            pattern = "^failed_.*\\.json$",
                            full.names = TRUE)
failed_sites <- lapply(failure_files, function(fp) {
    fromJSON(fp)
})
n_failed <- length(failed_sites)

# Build and write failed-sites table (always emitted)
failed_sites_table <- if (length(failed_sites) > 0) {
    bind_rows(lapply(seq_along(failed_sites), function(i) {
        fs <- failed_sites[[i]]
        tibble(
            id_numeric = as.integer(fs$id_numeric %||% NA),
            site_id = as.character(fs$site_id %||% NA),
            error = as.character(fs$error %||% "Unknown error"),
            timestamp = as.character(fs$timestamp %||% NA),
            array_index = as.integer(fs$array_index %||% NA),
            failure_marker_file = basename(failure_files[[i]])
        )
    }))
} else {
    tibble(
        id_numeric = integer(),
        site_id = character(),
        error = character(),
        timestamp = character(),
        array_index = integer(),
        failure_marker_file = character()
    )
}
write_csv(
    failed_sites_table,
    file.path(config$output_dir, "results_failed_sites.csv")
)

if (length(match_files) == 0 && n_failed == 0) {
    stop("No match files and no failure markers found. Run step 2 first.")
}
if (n_failed > 0) {
    message("  WARNING: ", n_failed, " site(s) failed matching")
    for (fs in failed_sites) {
        message("    - site id_numeric=", fs$id_numeric %||% "?",
                " (", fs$site_id %||% "unknown", "): ", fs$error)
    }
}
message("  Found ", length(match_files), " match files")

# Forest cover year columns
fc_cols <- paste0("fc_", config$fc_years)

if (length(match_files) > 0) {
    required_match_cols <- c(
        "cell", "site_id", "id_numeric", "area_ha", "treatment",
        "sampled_fraction", "total_biomass", "match_group"
    )

    # Process in chunks
    m_processed <- foreach(f = match_files, .combine = bind_rows) %do% {
        m <- readRDS(f)
        missing_cols <- setdiff(required_match_cols, names(m))
        if (length(missing_cols) > 0) {
            stop(
                paste0(
                    "Match file ", basename(f),
                    " is missing required columns: ",
                    paste(missing_cols, collapse = ", "),
                    ". Re-run steps 1 and 2 to regenerate match files."
                )
            )
        }

        m %>%
            select(cell, site_id, id_numeric, area_ha, treatment,
                   sampled_fraction, total_biomass, match_group,
                   all_of(fc_cols[fc_cols %in% names(m)])) %>%
            left_join(
                sites %>% select(site_id, start_year, end_year),
                by = "site_id"
            ) %>%
            pivot_longer(
                cols = starts_with("fc_"),
                names_to = "year",
                values_to = "forest_at_year_end"
            ) %>%
            mutate(year = as.integer(str_replace(year, "fc_", ""))) %>%
            group_by(site_id, cell, treatment) %>%
            filter(between(year, start_year[1] - 1, end_year[1])) %>%
            # Convert forest cover fraction to hectares
            mutate(
                forest_at_year_end = forest_at_year_end / 100 * area_ha
            ) %>%
            arrange(cell, year) %>%
            mutate(
                forest_change_ha = c(NA, diff(forest_at_year_end)),
                forest_frac_remaining =
                    forest_at_year_end / forest_at_year_end[1],
                biomass_at_year_end =
                    total_biomass * forest_frac_remaining,
                # Biomass to carbon (* 0.5), then carbon to CO2e (* -3.67)
                C_change = c(NA, diff(biomass_at_year_end)) * 0.5,
                Emissions_MgCO2e = C_change * -3.67
            ) %>%
            # Drop the year before start (only needed for initial fc)
            filter(between(year, start_year[1], end_year[1])) %>%
            as_tibble()
    }

    message("  Processed ", nrow(m_processed), " pixel-year records")

    # Per-site sampling table (includes indicator for subsampled sites)
    sampling_by_site <- m_processed %>%
        distinct(id_numeric, site_id, sampled_fraction) %>%
        mutate(
            sampled_percent = sampled_fraction * 100,
            was_subsampled = sampled_fraction < 1
        ) %>%
        arrange(site_id)

    write_csv(
        sampling_by_site,
        file.path(config$output_dir, "results_sampling_by_site.csv")
    )

    # Save pixel-level results
    m_processed %>%
        select(cell, site_id, year, treatment, sampled_fraction,
               match_group, forest_at_year_end, forest_change_ha,
               Emissions_MgCO2e) %>%
        write_csv(file.path(config$output_dir, "results_pixel_level.csv"))

    # Summarize by site and year
    results_by_year <- m_processed %>%
        group_by(match_group, site_id, year) %>%
        summarise(
            cell = cell[treatment],
            forest_loss_avoided_ha =
                abs(forest_change_ha[!treatment]) -
                abs(forest_change_ha[treatment]),
            emissions_avoided_mgco2e =
                abs(Emissions_MgCO2e[!treatment]) -
                abs(Emissions_MgCO2e[treatment]),
            .groups = "drop"
        ) %>%
        group_by(site_id, year) %>%
        summarise(
            forest_loss_avoided_ha =
                sum(forest_loss_avoided_ha, na.rm = TRUE),
            emissions_avoided_mgco2e =
                sum(emissions_avoided_mgco2e, na.rm = TRUE),
            n_matched_pixels = n(),
            .groups = "drop"
        ) %>%
        left_join(
            m_processed %>%
                distinct(site_id, sampled_fraction),
            by = "site_id"
        ) %>%
        mutate(
            # Scale up for sampled sites
            forest_loss_avoided_ha =
                forest_loss_avoided_ha / sampled_fraction,
            emissions_avoided_mgco2e =
                emissions_avoided_mgco2e / sampled_fraction
        )

    results_by_year %>%
        left_join(
            sites %>% select(site_id, site_name),
            by = "site_id"
        ) %>%
        write_csv(file.path(config$output_dir,
                            "results_by_site_year.csv"))

    message("  Per-site per-year results: ",
            nrow(results_by_year), " rows")

    # Summarize totals by site
    results_total <- results_by_year %>%
        group_by(site_id) %>%
        summarise(
            forest_loss_avoided_ha =
                sum(forest_loss_avoided_ha, na.rm = TRUE),
            emissions_avoided_mgco2e =
                sum(emissions_avoided_mgco2e, na.rm = TRUE),
            n_matched_pixels = max(n_matched_pixels),
            sampled_fraction = sampled_fraction[1],
            first_year = min(year),
            last_year = max(year),
            n_years = n(),
            .groups = "drop"
        ) %>%
        left_join(
            sites %>% select(site_id, site_name, area_ha),
            by = "site_id"
        )

    results_total %>%
        write_csv(file.path(config$output_dir,
                            "results_by_site_total.csv"))

    message("  Per-site totals: ", nrow(results_total), " sites")
} else {
    # No successful matches — produce empty result files
    message("  No match files — all sites failed or had no matches")
    results_by_year <- tibble(
        site_id = character(),
        year = integer(),
        forest_loss_avoided_ha = numeric(),
        emissions_avoided_mgco2e = numeric(),
        n_matched_pixels = integer(),
        sampled_fraction = numeric()
    )
    results_total <- tibble(
        site_id = character(),
        site_name = character(),
        forest_loss_avoided_ha = numeric(),
        emissions_avoided_mgco2e = numeric(),
        n_matched_pixels = integer(),
        sampled_fraction = numeric(),
        first_year = integer(),
        last_year = integer(),
        n_years = integer(),
        area_ha = numeric()
    )
    write_csv(results_by_year,
              file.path(config$output_dir, "results_by_site_year.csv"))
    write_csv(results_total,
              file.path(config$output_dir, "results_by_site_total.csv"))

    write_csv(
        tibble(
            id_numeric = integer(),
            site_id = character(),
            sampled_fraction = numeric(),
            sampled_percent = numeric(),
            was_subsampled = logical()
        ),
        file.path(config$output_dir, "results_sampling_by_site.csv")
    )
}

# Global summary
# Build failed sites list for the summary
failed_sites_summary <- lapply(failed_sites, function(fs) {
    list(
        id_numeric = fs$id_numeric,
        site_id = fs$site_id,
        error = fs$error
    )
})

subsampled_sites_summary <- if (exists("sampling_by_site")) {
    ss <- sampling_by_site %>%
        filter(was_subsampled) %>%
        transmute(
            id_numeric = id_numeric,
            site_id = site_id,
            sampled_fraction = sampled_fraction,
            sampled_percent = sampled_percent
        )
    if (nrow(ss) == 0) {
        list()
    } else {
        split(ss, seq_len(nrow(ss))) %>%
            lapply(function(row) as.list(row[1, ]))
    }
} else {
    list()
}

summary_data <- list(
    task_id = config$task_id,
    n_sites = nrow(results_total),
    n_failed_sites = n_failed,
    total_emissions_avoided_mgco2e = sum(
        results_total$emissions_avoided_mgco2e, na.rm = TRUE
    ),
    total_forest_loss_avoided_ha = sum(
        results_total$forest_loss_avoided_ha, na.rm = TRUE
    ),
    total_area_ha = sum(results_total$area_ha, na.rm = TRUE),
    year_range = if (nrow(results_by_year) > 0) {
        list(
            min = min(results_by_year$year),
            max = max(results_by_year$year)
        )
    } else {
        list(min = NA, max = NA)
    },
    sites = results_total %>%
        select(site_id, site_name, emissions_avoided_mgco2e,
               forest_loss_avoided_ha, area_ha, n_years) %>%
        as.list(),
    failed_sites = failed_sites_summary,
    subsampled_sites = subsampled_sites_summary
)

write_json(
    summary_data,
    file.path(config$output_dir, "results_summary.json"),
    auto_unbox = TRUE, pretty = TRUE
)

message("Step 3 complete. Results written to: ", config$output_dir)

}, step_name = "03_summarize_results")
