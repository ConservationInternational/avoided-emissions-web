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
#   - {output_dir}/results_match_covariates.csv: Matched pixel covariate values
#   - {output_dir}/results_balance.csv         : SMD balance statistics (Love plot)
#   - {output_dir}/results_propensity_scores.csv: Propensity scores (QQ plot)

library(tidyverse)
library(foreach)
library(jsonlite)
library(arrow)

source("/app/scripts/utils.R")
rollbar_init()

with_rollbar({

config <- parse_config()
message("Step 3: Summarizing results")
RANDOM_SEED <- if (is.null(config$random_seed)) {
    NULL
} else {
    as.integer(config$random_seed)
}

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
        # Look up site_name from sites table if not in failure marker
        site_name <- fs$site_name
        if (is.null(site_name) && !is.null(fs$id_numeric)) {
            site_row <- filter(sites, id_numeric == as.integer(fs$id_numeric))
            if (nrow(site_row) > 0 && "site_name" %in% names(site_row)) {
                site_name <- site_row$site_name[1]
            }
        }
        tibble(
            id_numeric = as.integer(fs$id_numeric %||% NA),
            site_id = as.character(fs$site_id %||% NA),
            site_name = as.character(site_name %||% NA),
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
        site_name = character(),
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
fc_year_min <- min(config$fc_years)

# Number of pre-intervention years to include in results (for plotting
# treatment-vs-control deforestation baselines).  The actual range is
# clamped to the available fc data (earliest year is fc_year_min + 1,
# since we lose one year computing the diff).
PRE_INTERVENTION_YEARS <- 5

if (length(match_files) > 0) {
    required_match_cols <- c(
        "cell", "site_id", "id_numeric", "area_ha", "treatment",
        "sampled_fraction", "total_biomass", "match_group", "match_weight"
    )

    # --- Extract matched-pixel covariate data for match-quality assessment ---
    # Read the formula to identify which covariates the user selected.
    formula_path <- file.path(config$output_dir, "formula.json")
    covariate_cols <- character(0)
    if (file.exists(formula_path)) {
        formula_json <- fromJSON(formula_path)
        formula_rhs <- formula_json$rhs
        if (is.null(formula_rhs)) {
            # Parse covariates from the formula string as a fallback
            fstr <- formula_json$formula_str
            if (!is.null(fstr)) {
                rhs_str <- trimws(sub("^.*~", "", fstr))
                formula_rhs <- trimws(strsplit(rhs_str, "\\+")[[1]])
            }
        }
        covariate_cols <- formula_rhs
    }

    # Collect covariate values for all matched treatment & control pixels
    match_cov_data <- foreach(f = match_files, .combine = bind_rows) %do% {
        m <- readRDS(f)
        # Determine which covariate columns are present in this match file
        available_covs <- intersect(covariate_cols, names(m))
        # Also include defor_pre_intervention if present (added dynamically)
        if ("defor_pre_intervention" %in% names(m) &&
            !"defor_pre_intervention" %in% available_covs) {
            available_covs <- c(available_covs, "defor_pre_intervention")
        }
        id_cols <- c("cell", "site_id", "treatment", "match_group",
                    "match_weight")
        keep_cols <- intersect(c(id_cols, available_covs), names(m))
        m %>% select(all_of(keep_cols)) %>% as_tibble()
    }

    if (nrow(match_cov_data) > 0) {
        write_csv(
            match_cov_data,
            file.path(config$output_dir, "results_match_covariates.csv")
        )
        message("  Match quality data: ", nrow(match_cov_data),
                " rows, ", length(covariate_cols), " covariates")
    } else {
        # Write empty file with expected columns
        empty_cov <- tibble(
            cell = integer(),
            site_id = character(),
            treatment = logical(),
            match_group = character(),
            match_weight = numeric()
        )
        write_csv(
            empty_cov,
            file.path(config$output_dir, "results_match_covariates.csv")
        )
    }

    # --- Balance statistics (SMD) for Love plot ----------------------------
    # Compute the Standardized Mean Difference (SMD) for each covariate,
    # both per-site and aggregated across all sites.  The SMD is defined
    # as (mean_treatment - mean_control) / pooled_sd.
    all_covs_for_balance <- if (nrow(match_cov_data) > 0) {
        setdiff(names(match_cov_data),
                c("cell", "site_id", "treatment", "match_group",
                  "match_weight"))
    } else {
        character(0)
    }

    if (length(all_covs_for_balance) > 0 && nrow(match_cov_data) > 0) {
        compute_smd <- function(df, cov) {
            # Use match_weight for weighted statistics so that 1:k
            # matching is correctly reflected in the balance check.
            t_mask <- df$treatment
            t_vals <- df[[cov]][t_mask]
            c_vals <- df[[cov]][!t_mask]
            t_wts <- df$match_weight[t_mask]
            c_wts <- df$match_weight[!t_mask]
            ok_t <- !is.na(t_vals)
            ok_c <- !is.na(c_vals)
            t_vals <- t_vals[ok_t]
            t_wts <- t_wts[ok_t]
            c_vals <- c_vals[ok_c]
            c_wts <- c_wts[ok_c]
            if (length(t_vals) < 2 || length(c_vals) < 2) {
                return(tibble(
                    covariate = cov, mean_treatment = NA_real_,
                    mean_control = NA_real_, pooled_sd = NA_real_,
                    smd = NA_real_
                ))
            }
            m_t <- weighted.mean(t_vals, t_wts)
            m_c <- weighted.mean(c_vals, c_wts)
            # Weighted variance (reliability weights)
            wvar <- function(x, w) {
                sum(w * (x - weighted.mean(x, w))^2) / sum(w)
            }
            sd_t <- sqrt(wvar(t_vals, t_wts))
            sd_c <- sqrt(wvar(c_vals, c_wts))
            pooled <- sqrt((sd_t^2 + sd_c^2) / 2)
            smd_val <- if (pooled > 0) (m_t - m_c) / pooled else 0
            tibble(
                covariate = cov, mean_treatment = m_t,
                mean_control = m_c, pooled_sd = pooled,
                smd = smd_val
            )
        }

        # Per-site balance
        balance_by_site <- match_cov_data %>%
            group_by(site_id) %>%
            group_modify(~ {
                bind_rows(lapply(all_covs_for_balance,
                                 function(cv) compute_smd(.x, cv)))
            }) %>%
            ungroup()

        # Aggregate balance across all sites
        balance_agg <- bind_rows(
            lapply(all_covs_for_balance,
                   function(cv) compute_smd(match_cov_data, cv))
        ) %>% mutate(site_id = "__all__")

        balance_table <- bind_rows(balance_agg, balance_by_site)
        write_csv(
            balance_table,
            file.path(config$output_dir, "results_balance.csv")
        )
        message("  Balance statistics: ", nrow(balance_table),
                " rows (", length(all_covs_for_balance), " covariates)")
    } else {
        write_csv(
            tibble(
                site_id = character(), covariate = character(),
                mean_treatment = numeric(), mean_control = numeric(),
                pooled_sd = numeric(), smd = numeric()
            ),
            file.path(config$output_dir, "results_balance.csv")
        )
    }

    # --- Propensity scores for QQ plot -------------------------------------
    # Collect propensity scores from all match files (saved in step 2).
    pscore_data <- foreach(f = match_files, .combine = bind_rows) %do% {
        m <- readRDS(f)
        if ("pscore" %in% names(m)) {
            m %>%
                select(cell, site_id, treatment, match_group,
                       match_weight, pscore) %>%
                as_tibble()
        } else {
            tibble(
                cell = integer(), site_id = character(),
                treatment = logical(), match_group = character(),
                match_weight = numeric(), pscore = numeric()
            )
        }
    }

    if (nrow(pscore_data) > 0) {
        write_csv(
            pscore_data,
            file.path(config$output_dir, "results_propensity_scores.csv")
        )
        message("  Propensity scores: ", nrow(pscore_data), " rows")
    } else {
        write_csv(
            tibble(
                cell = integer(), site_id = character(),
                treatment = logical(), match_group = character(),
                match_weight = numeric(), pscore = numeric()
            ),
            file.path(config$output_dir, "results_propensity_scores.csv")
        )
    }

    # --- Pre-computed match quality summary (for web UI) -------------------
    # The web UI previously loaded the full pixel-level CSVs to render
    # histograms and QQ plots, which caused out-of-memory kills for large
    # jobs.  Instead we pre-compute the aggregated data needed for the
    # plots here (where all data is already in memory on the Batch
    # worker) and save a small JSON summary.
    N_HIST_BINS <- 40L
    N_QQ_POINTS <- 500L

    mq_summary <- list(
        summary_stats = list(),
        histograms = list(),
        qq_quantiles = list(),
        covariate_cols = all_covs_for_balance
    )

    if (nrow(match_cov_data) > 0 && length(all_covs_for_balance) > 0) {
        # -- Summary stats per site and aggregate --------------------------
        mq_summary$summary_stats[["__all__"]] <- list(
            n_treatment = sum(match_cov_data$treatment),
            n_control = sum(!match_cov_data$treatment),
            n_sites = length(unique(match_cov_data$site_id))
        )
        for (sid in unique(match_cov_data$site_id)) {
            site_mask <- match_cov_data$site_id == sid
            mq_summary$summary_stats[[as.character(sid)]] <- list(
                n_treatment = sum(match_cov_data$treatment[site_mask]),
                n_control = sum(!match_cov_data$treatment[site_mask])
            )
        }

        # -- Histogram bins per covariate ----------------------------------
        compute_histogram <- function(df, cov, n_bins = N_HIST_BINS) {
            vals <- df[[cov]]
            t_mask <- df$treatment
            t_vals <- vals[t_mask & !is.na(vals)]
            c_vals <- vals[!t_mask & !is.na(vals)]
            all_vals <- vals[!is.na(vals)]
            if (length(all_vals) < 2 || diff(range(all_vals)) == 0) {
                return(NULL)
            }
            brks <- seq(min(all_vals), max(all_vals),
                        length.out = n_bins + 1)
            t_h <- hist(t_vals, breaks = brks, plot = FALSE)
            c_h <- hist(c_vals, breaks = brks, plot = FALSE)
            t_tot <- sum(t_h$counts)
            c_tot <- sum(c_h$counts)
            list(
                bin_edges = as.numeric(brks),
                treatment_pct = if (t_tot > 0) {
                    as.numeric(t_h$counts / t_tot * 100)
                } else {
                    rep(0, n_bins)
                },
                control_pct = if (c_tot > 0) {
                    as.numeric(c_h$counts / c_tot * 100)
                } else {
                    rep(0, n_bins)
                }
            )
        }

        # Aggregate histograms
        agg_hists <- list()
        for (cov in all_covs_for_balance) {
            h <- compute_histogram(match_cov_data, cov)
            if (!is.null(h)) agg_hists[[cov]] <- h
        }
        mq_summary$histograms[["__all__"]] <- agg_hists

        # Per-site histograms
        for (sid in unique(match_cov_data$site_id)) {
            site_data <- match_cov_data[match_cov_data$site_id == sid, ]
            site_hists <- list()
            for (cov in all_covs_for_balance) {
                h <- compute_histogram(site_data, cov)
                if (!is.null(h)) site_hists[[cov]] <- h
            }
            mq_summary$histograms[[as.character(sid)]] <- site_hists
        }
    }

    # -- QQ quantiles from propensity scores -------------------------------
    if (nrow(pscore_data) > 0 && "pscore" %in% names(pscore_data)) {
        compute_qq <- function(df, n_points = N_QQ_POINTS) {
            t_sc <- sort(df$pscore[df$treatment & !is.na(df$pscore)])
            c_sc <- sort(df$pscore[!df$treatment & !is.na(df$pscore)])
            if (length(t_sc) < 2 || length(c_sc) < 2) return(NULL)
            probs <- seq(0, 1, length.out = n_points)
            list(
                quantiles = as.numeric(probs),
                treatment_values = as.numeric(quantile(t_sc, probs)),
                control_values = as.numeric(quantile(c_sc, probs))
            )
        }

        qq_agg <- compute_qq(pscore_data)
        if (!is.null(qq_agg)) {
            mq_summary$qq_quantiles[["__all__"]] <- qq_agg
        }
        for (sid in unique(pscore_data$site_id)) {
            qq_s <- compute_qq(
                pscore_data[pscore_data$site_id == sid, ]
            )
            if (!is.null(qq_s)) {
                mq_summary$qq_quantiles[[as.character(sid)]] <- qq_s
            }
        }
    }

    write_json(
        mq_summary,
        file.path(config$output_dir,
                  "results_match_quality_summary.json"),
        auto_unbox = TRUE
    )
    message("  Match quality summary: written")

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
                   match_weight,
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
            # Include PRE_INTERVENTION_YEARS before start for baseline
            # plotting; need one extra year for the diff() baseline.
            filter(between(
                year,
                max(fc_year_min,
                    start_year[1] - PRE_INTERVENTION_YEARS - 1),
                end_year[1]
            )) %>%
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
            # Drop the earliest year (only needed for diff() baseline)
            filter(between(
                year,
                max(fc_year_min + 1,
                    start_year[1] - PRE_INTERVENTION_YEARS),
                end_year[1]
            )) %>%
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
               match_group, match_weight, forest_at_year_end,
               forest_change_ha, Emissions_MgCO2e) %>%
        write_csv(file.path(config$output_dir, "results_pixel_level.csv"))

    # Summarize by site and year.
    # Aggregation is done per matched set using match_weight so that it
    # remains correct for both pair matching and full matching (where a
    # set may contain multiple treated pixels).
    results_by_match_year <- m_processed %>%
        group_by(match_group, site_id, year) %>%
        summarise(
            treatment_defor_ha = sum(
                abs(forest_change_ha[treatment]) *
                    match_weight[treatment],
                na.rm = TRUE
            ),
            control_defor_ha = sum(
                abs(forest_change_ha[!treatment]) *
                    match_weight[!treatment],
                na.rm = TRUE
            ),
            treatment_emissions_mgco2e = sum(
                abs(Emissions_MgCO2e[treatment]) *
                    match_weight[treatment],
                na.rm = TRUE
            ),
            control_emissions_mgco2e = sum(
                abs(Emissions_MgCO2e[!treatment]) *
                    match_weight[!treatment],
                na.rm = TRUE
            ),
            n_treated_pixels = sum(treatment),
            .groups = "drop"
        ) %>%
        mutate(
            forest_loss_avoided_ha =
                control_defor_ha - treatment_defor_ha,
            emissions_avoided_mgco2e =
                control_emissions_mgco2e - treatment_emissions_mgco2e
        )

    results_by_year <- results_by_match_year %>%
        group_by(site_id, year) %>%
        summarise(
            treatment_defor_ha = sum(treatment_defor_ha, na.rm = TRUE),
            control_defor_ha = sum(control_defor_ha, na.rm = TRUE),
            forest_loss_avoided_ha =
                sum(forest_loss_avoided_ha, na.rm = TRUE),
            treatment_emissions_mgco2e =
                sum(treatment_emissions_mgco2e, na.rm = TRUE),
            control_emissions_mgco2e =
                sum(control_emissions_mgco2e, na.rm = TRUE),
            emissions_avoided_mgco2e =
                sum(emissions_avoided_mgco2e, na.rm = TRUE),
            n_matched_pixels = sum(n_treated_pixels, na.rm = TRUE),
            .groups = "drop"
        ) %>%
        left_join(
            m_processed %>%
                distinct(site_id, sampled_fraction),
            by = "site_id"
        ) %>%
        mutate(
            # Scale up for sampled sites
            treatment_defor_ha =
                treatment_defor_ha / sampled_fraction,
            control_defor_ha =
                control_defor_ha / sampled_fraction,
            forest_loss_avoided_ha =
                forest_loss_avoided_ha / sampled_fraction,
            treatment_emissions_mgco2e =
                treatment_emissions_mgco2e / sampled_fraction,
            control_emissions_mgco2e =
                control_emissions_mgco2e / sampled_fraction,
            emissions_avoided_mgco2e =
                emissions_avoided_mgco2e / sampled_fraction
        )

    results_by_year %>%
        left_join(
            sites %>% select(site_id, site_name, start_year),
            by = "site_id"
        ) %>%
        mutate(
            is_pre_intervention = year < start_year
        ) %>%
        select(-start_year) %>%
        write_csv(file.path(config$output_dir,
                            "results_by_site_year.csv"))

    message("  Per-site per-year results: ",
            nrow(results_by_year), " rows")

    # Summarize totals by site (intervention period only)
    results_total <- results_by_year %>%
        left_join(
            sites %>% select(site_id, start_year),
            by = "site_id"
        ) %>%
        filter(year >= start_year) %>%
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
        treatment_defor_ha = numeric(),
        control_defor_ha = numeric(),
        forest_loss_avoided_ha = numeric(),
        treatment_emissions_mgco2e = numeric(),
        control_emissions_mgco2e = numeric(),
        emissions_avoided_mgco2e = numeric(),
        n_matched_pixels = integer(),
        sampled_fraction = numeric(),
        site_name = character(),
        is_pre_intervention = logical()
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

    # Empty match covariates file
    write_csv(
        tibble(
            cell = integer(),
            site_id = character(),
            treatment = logical(),
            match_group = character(),
            match_weight = numeric()
        ),
        file.path(config$output_dir, "results_match_covariates.csv")
    )

    # Empty balance file
    write_csv(
        tibble(
            site_id = character(), covariate = character(),
            mean_treatment = numeric(), mean_control = numeric(),
            pooled_sd = numeric(), smd = numeric()
        ),
        file.path(config$output_dir, "results_balance.csv")
    )

    # Empty propensity scores file
    write_csv(
        tibble(
            cell = integer(), site_id = character(),
            treatment = logical(), match_group = character(),
            match_weight = numeric(), pscore = numeric()
        ),
        file.path(config$output_dir, "results_propensity_scores.csv")
    )

    # Empty match quality summary
    write_json(
        list(
            summary_stats = list(),
            histograms = list(),
            qq_quantiles = list(),
            covariate_cols = character(0)
        ),
        file.path(config$output_dir,
                  "results_match_quality_summary.json"),
        auto_unbox = TRUE
    )
}

# Global summary
# Build failed sites list for the summary
failed_sites_summary <- lapply(failed_sites, function(fs) {
    # Look up site_name from the sites table if not in the failure marker
    site_name <- fs$site_name
    if (is.null(site_name) && !is.null(fs$id_numeric)) {
        site_row <- filter(sites, id_numeric == as.integer(fs$id_numeric))
        if (nrow(site_row) > 0 && "site_name" %in% names(site_row)) {
            site_name <- site_row$site_name[1]
        }
    }
    list(
        id_numeric = fs$id_numeric,
        site_id = fs$site_id,
        site_name = site_name,
        error = fs$error
    )
})

subsampled_sites_summary <- if (exists("sampling_by_site")) {
    sites_lookup <- sites %>%
        select(id_numeric, site_name)
    ss <- sampling_by_site %>%
        filter(was_subsampled) %>%
        left_join(sites_lookup, by = "id_numeric") %>%
        transmute(
            id_numeric = id_numeric,
            site_id = site_id,
            site_name = site_name,
            sampled_fraction = sampled_fraction,
            sampled_percent = sampled_percent
        )
    if (nrow(ss) == 0) {
        list()
    } else {
        unname(lapply(seq_len(nrow(ss)), function(i) as.list(ss[i, ])))
    }
} else {
    list()
}

summary_data <- list(
    task_id = config$task_id,
    n_sites = nrow(results_total),
    n_failed_sites = n_failed,
    random_seed = RANDOM_SEED,
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
