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
#   - {output_dir}/results_by_site_year.csv              : Per-site per-year results
#   - {output_dir}/results_by_site_total.csv             : Per-site totals
#   - {output_dir}/results_pixel_year_emissions.csv      : Per-pixel per-year detail
#   - {output_dir}/results_summary.json                  : Global summary
#   - {output_dir}/results_pixel_covariates.csv          : Matched pixel covariate values
#   - {output_dir}/results_covariate_balance.csv         : SMD balance statistics (Love plot)
#   - {output_dir}/results_propensity_scores.csv         : Propensity scores (QQ plot)
#   - {output_dir}/results_pixel_locations.csv           : Matched pixel lon/lat for map

library(tidyverse)
library(foreach)
library(jsonlite)
library(arrow)

source("/app/scripts/utils.R")
rollbar_init()

with_rollbar({

config <- parse_config()
message("Step 3: Summarizing results")
step3_timer <- proc.time()
RANDOM_SEED <- if (is.null(config$random_seed)) {
    NULL
} else {
    as.integer(config$random_seed)
}

N_REPLICATES <- if (is.null(config$n_replicates)) {
    1L
} else {
    as.integer(config$n_replicates)
}

# Load site metadata (Parquet from Python step 1)
sites <- read_parquet(file.path(config$output_dir, "sites_processed.parquet")) %>%
    as_tibble()

# Load match files.
# When N_REPLICATES > 1 files are named m_{id}_rep{k}.rds;
# when N_REPLICATES == 1 files use the original m_{id}.rds naming.
if (N_REPLICATES > 1L) {
    match_files_all <- list.files(
        config$matches_dir,
        pattern = "^m_[0-9]+(_rep[0-9]+)?\\.rds$",
        full.names = TRUE
    )
    # Use replicate-1 files for match-quality assessment
    match_files <- list.files(
        config$matches_dir,
        pattern = "^m_[0-9]+_rep1\\.rds$",
        full.names = TRUE
    )
    # Backward compat: if old-style files exist, treat them as rep 1
    if (length(match_files) == 0) {
        match_files <- list.files(
            config$matches_dir,
            pattern = "^m_[0-9]+\\.rds$",
            full.names = TRUE
        )
    }
} else {
    match_files_all <- list.files(
        config$matches_dir,
        pattern = "^m_[0-9]+\\.rds$",
        full.names = TRUE
    )
    match_files <- match_files_all
}

# Load failure markers written by step 2 (tryCatch) or by the Python
# wrapper (OOM-killed subprocess).  When using spot instances, a job may
# fail on one attempt but succeed after retry.  In that case we have both
# a failure marker AND a success file — exclude failures where a
# corresponding match file exists.
failure_files_all <- list.files(config$matches_dir,
                                pattern = "^failed_.*\\.json$",
                                full.names = TRUE)
# Get basenames of all success files (m_*.rds) for quick lookup
success_basenames <- basename(match_files_all)

# Filter out failure markers where the job succeeded on retry
failure_files <- Filter(function(fp) {
    # Parse failed_{id_numeric}_rep{k}.json or failed_{id_numeric}.json
    fname <- basename(fp)
    # Try pattern with replicate first: failed_1_rep5.json -> m_1_rep5.rds
    m <- regmatches(fname, regexec("^failed_(\\d+)_rep(\\d+)\\.json$", fname))[[1]]
    if (length(m) == 3) {
        id_numeric <- m[2]
        rep_k <- m[3]
        success_name <- sprintf("m_%s_rep%s.rds", id_numeric, rep_k)
        return(!success_name %in% success_basenames)
    }
    # Try pattern without replicate: failed_1.json -> m_1.rds
    m <- regmatches(fname, regexec("^failed_(\\d+)\\.json$", fname))[[1]]
    if (length(m) == 2) {
        id_numeric <- m[2]
        success_name <- sprintf("m_%s.rds", id_numeric)
        return(!success_name %in% success_basenames)
    }
    # Unknown pattern (e.g. failed_array_5.json) - keep it as a failure
    TRUE
}, failure_files_all)

failed_sites <- lapply(failure_files, function(fp) {
    fromJSON(fp)
})
n_failed <- length(failed_sites)
n_stale_failures <- length(failure_files_all) - length(failure_files)
if (n_stale_failures > 0) {
    message("  INFO: Ignored ", n_stale_failures, " stale failure marker(s) ",
            "from retried jobs that succeeded")
}

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

if (length(match_files_all) == 0 && n_failed == 0) {
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
load_elapsed <- (proc.time() - step3_timer)["elapsed"]
message("  [TIMING] Match file discovery: ", round(load_elapsed, 1), "s")

# Forest cover year columns
fc_cols <- paste0("fc_", config$fc_years)
fc_year_min <- min(config$fc_years)
fc_year_max <- max(config$fc_years)

# Number of pre-intervention years to include in results (for plotting
# treatment-vs-control deforestation baselines).  The actual range is
# clamped to the available fc data (earliest year is fc_year_min + 1,
# since we lose one year computing the diff).
PRE_INTERVENTION_YEARS <- 5

if (length(match_files_all) > 0) {
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
            file.path(config$output_dir, "results_pixel_covariates.csv")
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
            file.path(config$output_dir, "results_pixel_covariates.csv")
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
            file.path(config$output_dir, "results_covariate_balance.csv")
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
            file.path(config$output_dir, "results_covariate_balance.csv")
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

    # -- Read total treatment / control counts from match files -----------
    # Each match RDS file carries per-site pre-matching counts that were
    # recorded during step 2:
    #   sampled_fraction  = n_treatment_sampled / n_treatment_total
    #   n_control_sampled = controls fed into the matching algorithm
    #   n_control_pool    = controls available before subsampling
    # We also read the treatment_cell_key for total treatment counts.
    treatment_key_path <- file.path(
        config$output_dir, "treatment_cell_key.parquet"
    )
    total_treatment_by_site <- list()
    if (file.exists(treatment_key_path)) {
        tk <- read_parquet(treatment_key_path) %>% as_tibble()
        tk_counts <- tk %>%
            count(site_id, name = "n") %>%
            mutate(site_id = as.character(site_id))
        for (i in seq_len(nrow(tk_counts))) {
            total_treatment_by_site[[tk_counts$site_id[i]]] <-
                tk_counts$n[i]
        }
    }

    # Aggregate n_control_sampled and n_control_pool from match files
    control_sampled_by_site <- list()
    control_pool_by_site <- list()
    for (f in match_files) {
        m_tmp <- readRDS(f)
        sid_str <- as.character(m_tmp$site_id[1])
        if ("n_control_sampled" %in% names(m_tmp)) {
            control_sampled_by_site[[sid_str]] <-
                m_tmp$n_control_sampled[1]
        }
        if ("n_control_pool" %in% names(m_tmp)) {
            control_pool_by_site[[sid_str]] <-
                m_tmp$n_control_pool[1]
        }
    }

    if (nrow(match_cov_data) > 0 && length(all_covs_for_balance) > 0) {
        # -- Summary stats per site and aggregate --------------------------
        n_treatment_total_all <- sum(
            unlist(total_treatment_by_site), na.rm = TRUE
        )
        n_control_sampled_all <- sum(
            unlist(control_sampled_by_site), na.rm = TRUE
        )
        n_control_pool_all <- sum(
            unlist(control_pool_by_site), na.rm = TRUE
        )
        mq_summary$summary_stats[["__all__"]] <- list(
            n_treatment = sum(match_cov_data$treatment),
            n_control = sum(!match_cov_data$treatment),
            n_sites = length(unique(match_cov_data$site_id)),
            n_treatment_total = n_treatment_total_all,
            n_control_sampled = if (n_control_sampled_all > 0) {
                n_control_sampled_all
            } else {
                NULL
            },
            n_control_pool = if (n_control_pool_all > 0) {
                n_control_pool_all
            } else {
                NULL
            }
        )
        for (sid in unique(match_cov_data$site_id)) {
            site_mask <- match_cov_data$site_id == sid
            sid_str <- as.character(sid)
            mq_summary$summary_stats[[sid_str]] <- list(
                n_treatment = sum(match_cov_data$treatment[site_mask]),
                n_control = sum(!match_cov_data$treatment[site_mask]),
                n_treatment_total = total_treatment_by_site[[sid_str]],
                n_control_sampled =
                    control_sampled_by_site[[sid_str]],
                n_control_pool =
                    control_pool_by_site[[sid_str]]
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
    # Group match files by replicate for CI computation
    if (N_REPLICATES > 1L) {
        rep_nums <- as.integer(
            str_match(basename(match_files_all),
                      "_rep([0-9]+)\\.rds$")[, 2]
        )
        # Backward compat: old-style m_ID.rds files treated as rep 1
        rep_nums[is.na(rep_nums)] <- 1L
        match_file_groups <- split(match_files_all, rep_nums)
    } else {
        match_file_groups <- list("1" = match_files_all)
    }

    process_match_files <- function(mfiles) {
        foreach(f = mfiles, .combine = bind_rows) %do% {
            m <- readRDS(f)
            missing_cols <- setdiff(required_match_cols, names(m))
            if (length(missing_cols) > 0) {
                stop(
                    paste0(
                        "Match file ", basename(f),
                        " is missing required columns: ",
                        paste(missing_cols, collapse = ", "),
                        ". Re-run steps 1 and 2."
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
                mutate(
                    year = as.integer(str_replace(year, "fc_", ""))
                ) %>%
                group_by(site_id, cell, treatment) %>%
                filter(between(
                    year,
                    max(fc_year_min,
                        start_year[1] - PRE_INTERVENTION_YEARS - 1),
                    fc_year_max
                )) %>%
                mutate(
                    forest_at_year_end =
                        forest_at_year_end / 100 * area_ha
                ) %>%
                arrange(cell, year) %>%
                mutate(
                    forest_change_ha =
                        c(NA, diff(forest_at_year_end)),
                    forest_frac_remaining =
                        forest_at_year_end / forest_at_year_end[1],
                    biomass_at_year_end =
                        total_biomass * forest_frac_remaining,
                    C_change =
                        c(NA, diff(biomass_at_year_end)) * 0.5,
                    Emissions_MgCO2e = C_change * -3.67
                ) %>%
                filter(between(
                    year,
                    max(fc_year_min + 1,
                        start_year[1] - PRE_INTERVENTION_YEARS),
                    fc_year_max
                )) %>%
                as_tibble()
        }
    }

    aggregate_to_site_year <- function(m_proc) {
        by_match <- m_proc %>%
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
                    control_emissions_mgco2e -
                    treatment_emissions_mgco2e
            )

        by_match %>%
            group_by(site_id, year) %>%
            summarise(
                treatment_defor_ha =
                    sum(treatment_defor_ha, na.rm = TRUE),
                control_defor_ha =
                    sum(control_defor_ha, na.rm = TRUE),
                forest_loss_avoided_ha =
                    sum(forest_loss_avoided_ha, na.rm = TRUE),
                treatment_emissions_mgco2e =
                    sum(treatment_emissions_mgco2e, na.rm = TRUE),
                control_emissions_mgco2e =
                    sum(control_emissions_mgco2e, na.rm = TRUE),
                emissions_avoided_mgco2e =
                    sum(emissions_avoided_mgco2e, na.rm = TRUE),
                n_matched_pixels =
                    sum(n_treated_pixels, na.rm = TRUE),
                .groups = "drop"
            ) %>%
            left_join(
                m_proc %>% distinct(site_id, sampled_fraction),
                by = "site_id"
            ) %>%
            mutate(
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
    }

    # Process each replicate and collect site-year results
    all_rep_results <- list()
    m_processed_rep1 <- NULL
    rep_loop_timer <- proc.time()

    for (rep_name in names(match_file_groups)) {
        rep_files <- match_file_groups[[rep_name]]
        if (length(rep_files) == 0) next
        m_proc <- process_match_files(rep_files)
        if (nrow(m_proc) == 0) next

        if (is.null(m_processed_rep1)) {
            m_processed_rep1 <- m_proc
        }

        rep_by_year <- aggregate_to_site_year(m_proc)
        all_rep_results[[rep_name]] <- rep_by_year

        message("    Replicate ", rep_name, ": ",
                nrow(m_proc), " pixel-year records -> ",
                nrow(rep_by_year), " site-year rows")

        # Free memory between replicates
        if (rep_name != names(match_file_groups)[1]) {
            rm(m_proc)
            gc()
        }
    }

    # Use rep-1 processed data for pixel-level output and sampling table
    m_processed <- m_processed_rep1

    rep_loop_elapsed <- (proc.time() - rep_loop_timer)["elapsed"]
    message(
        "  [TIMING] Replicate processing: ",
        round(rep_loop_elapsed, 1), "s for ",
        length(all_rep_results), " replicate(s)"
    )

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

    # Save pixel-level results (rep 1 only to keep output manageable)
    m_processed %>%
        select(cell, site_id, year, treatment, sampled_fraction,
               match_group, match_weight, forest_at_year_end,
               forest_change_ha, Emissions_MgCO2e) %>%
        write_csv(file.path(
            config$output_dir, "results_pixel_year_emissions.csv"
        ))

    # Combine replicate results and compute CIs
    ci_metrics <- c(
        "treatment_defor_ha", "control_defor_ha",
        "forest_loss_avoided_ha",
        "treatment_emissions_mgco2e",
        "control_emissions_mgco2e",
        "emissions_avoided_mgco2e"
    )

    if (length(all_rep_results) > 1) {
        stacked <- bind_rows(all_rep_results, .id = "replicate")

        # Mean of each metric across replicates
        results_by_year <- stacked %>%
            group_by(site_id, year) %>%
            summarise(
                across(
                    all_of(ci_metrics),
                    list(
                        mean = ~ mean(.x, na.rm = TRUE),
                        ci_lower = ~ quantile(.x, 0.025,
                                              na.rm = TRUE),
                        ci_upper = ~ quantile(.x, 0.975,
                                              na.rm = TRUE)
                    ),
                    .names = "{.col}__{.fn}"
                ),
                n_matched_pixels =
                    as.integer(round(mean(n_matched_pixels,
                                         na.rm = TRUE))),
                sampled_fraction = sampled_fraction[1],
                n_replicates_available = n(),
                .groups = "drop"
            ) %>%
            rename_with(
                ~ str_replace(.x, "__mean$", ""),
                ends_with("__mean")
            ) %>%
            rename_with(
                ~ str_replace(.x, "__", "_"),
                matches("__ci_(lower|upper)$")
            )
    } else {
        results_by_year <- all_rep_results[[1]]
    }

    results_by_year %>%
        left_join(
            sites %>% select(site_id, site_name, start_year, end_year),
            by = "site_id"
        ) %>%
        mutate(
            is_pre_intervention = year < start_year,
            is_post_intervention = year > end_year
        ) %>%
        select(-start_year, -end_year) %>%
        write_csv(file.path(config$output_dir,
                            "results_by_site_year.csv"))

    message("  Per-site per-year results: ",
            nrow(results_by_year), " rows")

    # Summarize totals by site (intervention period only)
    results_total <- results_by_year %>%
        left_join(
            sites %>% select(site_id, start_year, end_year),
            by = "site_id"
        ) %>%
        filter(year >= start_year, year <= end_year) %>%
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
        ) %>%
        mutate(
            n_treatment_pixels = as.integer(sapply(
                as.character(site_id),
                function(sid) {
                    total_treatment_by_site[[sid]] %||% NA_integer_
                }
            ))
        )

    # For multi-replicate runs, also compute CI on totals
    if (length(all_rep_results) > 1) {
        rep_totals <- bind_rows(all_rep_results, .id = "replicate") %>%
            left_join(
                sites %>% select(site_id, start_year, end_year),
                by = "site_id"
            ) %>%
            filter(year >= start_year, year <= end_year) %>%
            group_by(replicate, site_id) %>%
            summarise(
                forest_loss_avoided_ha =
                    sum(forest_loss_avoided_ha, na.rm = TRUE),
                emissions_avoided_mgco2e =
                    sum(emissions_avoided_mgco2e, na.rm = TRUE),
                .groups = "drop"
            )

        total_cis <- rep_totals %>%
            group_by(site_id) %>%
            summarise(
                forest_loss_avoided_ha_ci_lower =
                    quantile(forest_loss_avoided_ha, 0.025,
                             na.rm = TRUE),
                forest_loss_avoided_ha_ci_upper =
                    quantile(forest_loss_avoided_ha, 0.975,
                             na.rm = TRUE),
                emissions_avoided_mgco2e_ci_lower =
                    quantile(emissions_avoided_mgco2e, 0.025,
                             na.rm = TRUE),
                emissions_avoided_mgco2e_ci_upper =
                    quantile(emissions_avoided_mgco2e, 0.975,
                             na.rm = TRUE),
                .groups = "drop"
            )

        results_total <- results_total %>%
            left_join(total_cis, by = "site_id")
    }

    results_total %>%
        write_csv(file.path(config$output_dir,
                            "results_by_site_total.csv"))

    message("  Per-site totals: ", nrow(results_total), " sites")

    # --- Matched pixel locations for map visualisation --------------------
    # Read the grid metadata saved by the extract step and convert cell
    # indices to geographic coordinates so the webapp can plot treatment
    # and control pixels on the map.
    grid_meta_path <- file.path(config$output_dir, "grid_metadata.json")
    if (file.exists(grid_meta_path) && nrow(match_cov_data) > 0) {
        grid_meta <- fromJSON(grid_meta_path)
        grid_width <- grid_meta$width
        # Affine transform: [a, b, c, d, e, f]
        # lon = c + col * a + 0.5 * a  (pixel centre)
        # lat = f + row * e + 0.5 * e  (pixel centre)
        tf <- grid_meta$transform
        tf_a <- tf[1]  # pixel width (positive)
        tf_c <- tf[3]  # x origin (left edge)
        tf_e <- tf[5]  # pixel height (negative)
        tf_f <- tf[6]  # y origin (top edge)

        pixel_locations <- match_cov_data %>%
            distinct(cell, site_id, treatment, match_group) %>%
            mutate(
                row = cell %/% grid_width,
                col = cell %% grid_width,
                lon = tf_c + (col + 0.5) * tf_a,
                lat = tf_f + (row + 0.5) * tf_e
            ) %>%
            select(cell, site_id, treatment, match_group, lon, lat)

        write_csv(
            pixel_locations,
            file.path(config$output_dir,
                      "results_pixel_locations.csv")
        )
        message("  Matched pixel locations: ",
                nrow(pixel_locations), " pixels")
    } else {
        # Write empty file
        write_csv(
            tibble(
                cell = integer(), site_id = character(),
                treatment = logical(), match_group = character(),
                lon = numeric(), lat = numeric()
            ),
            file.path(config$output_dir,
                      "results_pixel_locations.csv")
        )
    }
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
        area_ha = numeric(),
        n_treatment_pixels = integer()
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
        file.path(config$output_dir, "results_pixel_covariates.csv")
    )

    # Empty balance file
    write_csv(
        tibble(
            site_id = character(), covariate = character(),
            mean_treatment = numeric(), mean_control = numeric(),
            pooled_sd = numeric(), smd = numeric()
        ),
        file.path(config$output_dir, "results_covariate_balance.csv")
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

    # Empty matched pixel locations
    write_csv(
        tibble(
            cell = integer(), site_id = character(),
            treatment = logical(), match_group = character(),
            lon = numeric(), lat = numeric()
        ),
        file.path(config$output_dir, "results_pixel_locations.csv")
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
    entry <- list(
        id_numeric = fs$id_numeric,
        site_id = fs$site_id,
        site_name = site_name,
        error = fs$error
    )
    # Include separation diagnostics when present
    if (!is.null(fs$separation_warnings) &&
        length(fs$separation_warnings) > 0) {
        entry$separation_warnings <- fs$separation_warnings
    }
    # Include group diagnostics when present
    if (!is.null(fs$group_diagnostics) &&
        length(fs$group_diagnostics) > 0) {
        entry$group_diagnostics <- fs$group_diagnostics
    }
    entry
})

# Aggregate per-group matching diagnostics from diagnostic JSON files
# written alongside each match RDS by step 2.
diag_files <- list.files(
    config$matches_dir,
    pattern = "_diagnostics\\.json$",
    full.names = TRUE
)
all_group_diagnostics <- list()
for (df in diag_files) {
    tryCatch({
        gd <- fromJSON(df, simplifyVector = FALSE)
        all_group_diagnostics <- c(all_group_diagnostics, gd)
    }, error = function(e) {
        message("  Warning: could not read diagnostics file: ", df)
    })
}
if (length(all_group_diagnostics) > 0) {
    message(
        "  Loaded ", length(all_group_diagnostics),
        " group diagnostics from ", length(diag_files), " file(s)"
    )
}

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
    n_replicates = N_REPLICATES,
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
    subsampled_sites = subsampled_sites_summary,
    group_diagnostics = all_group_diagnostics
)

write_json(
    summary_data,
    file.path(config$output_dir, "results_summary.json"),
    auto_unbox = TRUE, pretty = TRUE
)

step3_elapsed <- (proc.time() - step3_timer)["elapsed"]
message("[TIMING] Step 3 total: ", round(step3_elapsed, 1), "s")
message("Step 3 complete. Results written to: ", config$output_dir)

}, step_name = "03_summarize_results")
