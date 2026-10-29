# Step 3: Summarize matching results into avoided emissions estimates.
#
# Reads each match file once, computes annual forest loss and emissions for
# the matched pixels (see pixel_outcomes() in stats_functions.R), and
# aggregates within each replicate.  Means and replicate ranges (2.5% / 97.5%
# quantiles across replicates) are then computed separately at every reported
# scope -- site-year, site total, and portfolio -- because quantiles of sums
# are not sums of quantiles.
#
# An exact-match stratum of a site fragment is included only when it was
# matched in every replicate; fragments with no such stratum are reported as
# failed.
#
# Output (in {output_dir}):
#   results_by_site_year.csv, results_by_site_total.csv, results_summary.json,
#   results_pixel_year_emissions.csv (replicate 1), results_pixel_covariates.csv,
#   results_covariate_balance.csv, results_propensity_scores.csv,
#   results_pixel_locations.csv, results_match_quality_summary.json,
#   results_sampling_by_site.csv, results_failed_sites.csv,
#   na_exclusion_summary.csv

library(arrow, warn.conflicts = FALSE)
library(jsonlite)

scripts_dir <- Sys.getenv("R_SCRIPTS_DIR", "/app/scripts")
source(file.path(scripts_dir, "utils.R"))
source(file.path(scripts_dir, "stats_functions.R"))
rollbar_init()

with_rollbar({

config <- parse_config()
message("Step 3: Summarizing results")
step3_timer <- proc.time()

RANDOM_SEED <- if (is.null(config$random_seed)) NULL else as.integer(config$random_seed)
N_REPLICATES <- if (is.null(config$n_replicates)) 1L else as.integer(config$n_replicates)
FC_YEARS <- as.integer(config$fc_years)
FC_YEAR_MIN <- min(FC_YEARS)
FC_YEAR_MAX <- max(FC_YEARS)
GROUP_MAPPING <- fromJSON(config$config_path, simplifyVector = FALSE)$exact_match_group_mapping
BOUNDS_LABEL <- "Range across matching replicates"
LOW_COVERAGE <- 0.8
out_path <- function(name) file.path(config$output_dir, name)

formula_json <- fromJSON(out_path("formula.json"))
f <- as.formula(formula_json$formula_str)
CATEGORICAL <- factor_vars(f)
COVARIATES <- c(setdiff(all.vars(f), "treatment"), "defor_pre_intervention")

fragments <- read_parquet(
    out_path("sites_processed.parquet"),
    col_select = any_of(c("site_id", "display_site_id", "site_name", "id_numeric", "sub_site_index",
                         "start_year", "end_year", "area_ha"))
) %>%
    as_tibble() %>%
    mutate(site_id = as.character(site_id))
if (!"display_site_id" %in% names(fragments)) fragments$display_site_id <- fragments$site_id
site_pixels <- read_parquet(out_path("treatment_cell_key.parquet")) %>%
    count(id_numeric, name = "n_site_pixels")
fragments <- fragments %>%
    left_join(site_pixels, by = "id_numeric") %>%
    mutate(n_site_pixels = coalesce(n_site_pixels, 0L))
sites <- fragments %>%
    group_by(site_id) %>%
    summarise(
        display_site_id = first(display_site_id),
        site_name = first(site_name), start_year = first(start_year),
        end_year = first(end_year), area_ha = sum(area_ha),
        n_site_pixels = sum(n_site_pixels), .groups = "drop"
    )
write_csv(select(sites, site_id, display_site_id, site_name), out_path("site_identity.csv"))


# -- Work reconciliation -----------------------------------------------------

match_paths <- list.files(config$matches_dir, "^m_[0-9]+_rep[0-9]+\\.rds$", full.names = TRUE)
matched_files <- tibble(
    path = match_paths,
    id_numeric = as.integer(sub("^m_([0-9]+)_rep.*$", "\\1", basename(match_paths))),
    replicate = as.integer(sub("^.*_rep([0-9]+)\\.rds$", "\\1", basename(match_paths)))
) %>%
    group_by(id_numeric) %>%
    filter(n_distinct(replicate) == N_REPLICATES) %>%
    ungroup()


# -- Read each match file once -----------------------------------------------

stratum_rows <- list()
strata <- list()
count_rows <- list()
cov_rows <- list()
diagnostics <- list()
detail_path <- out_path("results_pixel_year_emissions.csv")
if (file.exists(detail_path)) file.remove(detail_path)

for (i in seq_len(nrow(matched_files))) {
    file_info <- matched_files[i, ]
    frag <- filter(fragments, id_numeric == file_info$id_numeric)
    m <- readRDS(file_info$path)
    outcomes <- pixel_outcomes(m, frag$start_year, FC_YEAR_MIN, FC_YEAR_MAX)
    ids <- tibble(id_numeric = file_info$id_numeric, site_id = frag$site_id,
                  replicate = file_info$replicate)
    stratum_rows[[i]] <- bind_cols(ids, aggregate_outcomes(m, outcomes))
    # Eligible pixels per matched stratum (sampling_weight = n_eligible / n_matched).
    strata[[i]] <- bind_cols(ids, m %>%
        filter(treatment) %>%
        group_by(group) %>%
        summarise(n_matched = n(), n_eligible = sampling_weight[1] * n(), .groups = "drop"))
    count_rows[[i]] <- bind_cols(ids, tibble(
        n_eligible = m$n_eligible[1], n_sampled = m$n_sampled[1],
        n_control_pool = m$n_control_pool[1], n_control_sampled = m$n_control_sampled[1]
    ))
    if (file_info$replicate == 1L) {
        n_years <- length(outcomes$years)
        write_csv(
            tibble(
                cell = rep(m$cell, times = n_years),
                id_numeric = file_info$id_numeric,
                site_id = frag$site_id,
                group = rep(m$group, times = n_years),
                year = rep(outcomes$years, each = nrow(m)),
                treatment = rep(m$treatment, times = n_years),
                match_group = rep(m$match_group, times = n_years),
                match_weight = rep(m$match_weight, times = n_years),
                sampling_weight = rep(m$sampling_weight, times = n_years),
                forest_loss_ha = as.vector(outcomes$loss_ha),
                emissions_mgco2e = as.vector(outcomes$emissions)
            ),
            detail_path, append = file.exists(detail_path)
        )
        cov_rows[[length(cov_rows) + 1]] <- m %>%
            select(cell, group, treatment, match_group, match_weight, pscore,
                   any_of(COVARIATES)) %>%
            mutate(id_numeric = file_info$id_numeric, site_id = frag$site_id, .before = 1)
        diagnostics <- c(diagnostics, attr(m, "group_diagnostics"))
    }
}
if (!file.exists(detail_path)) {
    write_csv(tibble(cell = integer(), site_id = character(), year = integer()), detail_path)
}

# A stratum is included only if it was matched in every replicate, so that all
# replicates estimate the same population.  Otherwise a stratum missing from
# some replicates would count as zero there, biasing the mean and the range.
strata <- bind_rows(
    tibble(id_numeric = integer(), site_id = character(), replicate = integer(),
           group = character(), n_matched = integer(), n_eligible = numeric()),
    strata
)
complete_strata <- strata %>%
    count(id_numeric, group) %>%
    filter(n == N_REPLICATES) %>%
    select(id_numeric, group)
n_inconsistent_strata <- nrow(distinct(strata, id_numeric, group)) - nrow(complete_strata)
strata <- semi_join(strata, complete_strata, by = c("id_numeric", "group"))
included_ids <- unique(complete_strata$id_numeric)

if (n_inconsistent_strata > 0) {
    filtered_detail <- tempfile("pixel_year_", tmpdir = config$output_dir, fileext = ".csv")
    read_csv_chunked(
        detail_path,
        SideEffectChunkCallback$new(function(chunk, pos) {
            write_csv(
                semi_join(chunk, complete_strata, by = c("id_numeric", "group")),
                filtered_detail, append = file.exists(filtered_detail)
            )
        }),
        chunk_size = 100000L, progress = FALSE,
        col_types = cols(
            .default = col_double(), site_id = col_character(), group = col_character(),
            match_group = col_character(), treatment = col_logical()
        )
    )
    if (!file.rename(filtered_detail, detail_path)) stop("Could not replace filtered pixel-year output")
}


# -- Work reconciliation -----------------------------------------------------

# Failure markers for outputs that were later produced (e.g. a retried spot
# job) are ignored.  Worker-level markers (failed_array_*.json) are kept.
markers <- lapply(
    list.files(config$matches_dir, "^failed_.*\\.json$", full.names = TRUE),
    fromJSON, simplifyVector = FALSE
)
markers <- Filter(function(mk) {
    is.null(mk$match_file) || !file.exists(file.path(config$matches_dir, mk$match_file))
}, markers)
fragment_markers <- Filter(function(mk) !is.null(mk$id_numeric), markers)
worker_failures <- Filter(function(mk) is.null(mk$id_numeric), markers)

mapped_keys <- if (is.null(GROUP_MAPPING)) {
    NULL
} else {
    vapply(unlist(GROUP_MAPPING, recursive = FALSE),
           function(m) paste(m[[1]], m[[2]], sep = "\r"), character(1))
}
failure_reason <- function(id, site_id, sub_site_index) {
    mk <- Filter(function(x) identical(as.integer(x$id_numeric), as.integer(id)), fragment_markers)
    if (length(mk) > 0) return(mk[[1]]$error)
    if (id %in% matched_files$id_numeric) {
        return("No exact-match stratum was matched in every replicate")
    }
    if (!is.null(mapped_keys) && !paste(site_id, sub_site_index, sep = "\r") %in% mapped_keys) {
        return("Site has no exact-match region assignment and was not matched")
    }
    if (length(worker_failures) > 0) {
        return(paste("No match output; worker failure:", worker_failures[[1]]$error))
    }
    "No match output for one or more replicates"
}
# Fragments without any pixel (e.g. slivers smaller than a pixel) have nothing
# to estimate and are not failures.
failed_fragments <- filter(fragments, !id_numeric %in% included_ids, n_site_pixels > 0)
failed_fragments$error <- vapply(seq_len(nrow(failed_fragments)), function(i) {
    failure_reason(failed_fragments$id_numeric[i], failed_fragments$site_id[i],
                   failed_fragments$sub_site_index[i])
}, character(1))
write_csv(
    select(failed_fragments, id_numeric, site_id, site_name, sub_site_index, error),
    out_path("results_failed_sites.csv")
)
n_failed_sites <- n_distinct(failed_fragments$site_id)
message("  ", length(included_ids), " fragment(s) included; ", nrow(failed_fragments),
        " fragment(s) in ", n_failed_sites, " site(s) failed; ", n_inconsistent_strata,
        " stratum/strata excluded for not matching in every replicate")


# -- Estimates ---------------------------------------------------------------

metric_cols <- c(
    "sample_treatment_defor_ha", "sample_control_defor_ha",
    "sample_treatment_emissions_mgco2e", "sample_control_emissions_mgco2e",
    "extrapolated_treatment_defor_ha", "extrapolated_control_defor_ha",
    "extrapolated_treatment_emissions_mgco2e", "extrapolated_control_emissions_mgco2e",
    "sample_forest_loss_avoided_ha", "sample_emissions_avoided_mgco2e",
    "extrapolated_forest_loss_avoided_ha", "extrapolated_emissions_avoided_mgco2e"
)

# Strata and fragments are summed to sites within each replicate.
empty_metrics <- as_tibble(setNames(rep(list(numeric()), 8), metric_cols[1:8]))
site_year_rep <- bind_rows(
    tibble(id_numeric = integer(), group = character(), replicate = integer(),
           site_id = character(), year = integer()),
    empty_metrics, stratum_rows
) %>%
    semi_join(complete_strata, by = c("id_numeric", "group")) %>%
    group_by(replicate, site_id, year) %>%
    summarise(across(any_of(metric_cols), sum), .groups = "drop") %>%
    add_avoided()

site_counts <- bind_rows(
    tibble(id_numeric = integer(), site_id = character(), replicate = integer(),
           n_eligible = numeric(), n_sampled = numeric(), n_control_pool = numeric(),
           n_control_sampled = numeric()),
    count_rows
) %>%
    filter(id_numeric %in% included_ids) %>%
    left_join(
        strata %>%
            group_by(id_numeric, replicate) %>%
            summarise(n_matched = sum(n_matched), n_represented = sum(n_eligible),
                      .groups = "drop"),
        by = c("id_numeric", "replicate")
    ) %>%
    group_by(site_id) %>%
    summarise(
        n_sample_pixels = as.integer(round(sum(n_matched) / N_REPLICATES)),
        n_eligible_pixels = sum(n_eligible) / N_REPLICATES,
        n_represented_pixels = sum(n_represented) / N_REPLICATES,
        sampled_fraction = sum(n_sampled) / sum(n_eligible),
        match_rate = sum(n_matched) / sum(n_sampled),
        n_control_pool = sum(n_control_pool) / N_REPLICATES,
        n_control_sampled = sum(n_control_sampled) / N_REPLICATES,
        .groups = "drop"
    ) %>%
    left_join(select(sites, site_id, n_site_pixels), by = "site_id") %>%
    # Share of the site's pixels that the estimates represent.
    mutate(coverage_fraction = n_represented_pixels / n_site_pixels)

results_by_year <- (if (nrow(site_year_rep) > 0) {
    summarise_replicates(site_year_rep, c("site_id", "year"), metric_cols)
} else {
    tibble(site_id = character(), year = integer())
}) %>%
    inner_join(select(sites, site_id, site_name, start_year, end_year), by = "site_id") %>%
    left_join(select(site_counts, site_id, n_sample_pixels, sampled_fraction), by = "site_id") %>%
    mutate(
        is_pre_intervention = year < start_year,
        is_post_intervention = year > end_year
    ) %>%
    select(-start_year, -end_year)
write_csv(results_by_year, out_path("results_by_site_year.csv"))

intervention_rep <- site_year_rep %>%
    inner_join(select(sites, site_id, start_year, end_year), by = "site_id") %>%
    filter(year >= start_year, year <= end_year)
site_total_rep <- intervention_rep %>%
    group_by(replicate, site_id) %>%
    summarise(across(all_of(metric_cols), sum), .groups = "drop")
results_total <- (if (nrow(site_total_rep) > 0) {
    summarise_replicates(site_total_rep, "site_id", metric_cols)
} else {
    tibble(site_id = character())
}) %>%
    inner_join(
        intervention_rep %>%
            filter(replicate == 1L) %>%
            group_by(site_id) %>%
            summarise(first_year = min(year), last_year = max(year),
                      n_years = n_distinct(year), .groups = "drop"),
        by = "site_id"
    ) %>%
    inner_join(select(sites, site_id, site_name, area_ha), by = "site_id") %>%
    inner_join(site_counts, by = "site_id") %>%
    rename(n_treatment_pixels = n_site_pixels)
write_csv(results_total, out_path("results_by_site_total.csv"))
message("  Per-site results: ", nrow(results_by_year), " site-years, ",
        nrow(results_total), " sites")

# Portfolio series are summed within each replicate, then summarised.  The
# names match the fields plotted by the web app.
portfolio_cols <- c(
    treatment_defor_ha = "extrapolated_treatment_defor_ha",
    control_defor_ha = "extrapolated_control_defor_ha",
    treatment_emissions_mgco2e = "extrapolated_treatment_emissions_mgco2e",
    control_emissions_mgco2e = "extrapolated_control_emissions_mgco2e",
    forest_loss_avoided_ha = "extrapolated_forest_loss_avoided_ha",
    emissions_avoided_mgco2e = "extrapolated_emissions_avoided_mgco2e"
)
portfolio_yearly <- list()
portfolio_total <- list()
if (nrow(site_year_rep) > 0) {
    yearly_rep <- site_year_rep %>%
        group_by(replicate, year) %>%
        summarise(across(all_of(unname(portfolio_cols)), sum), n_sites = n(), .groups = "drop") %>%
        rename(all_of(portfolio_cols)) %>%
        arrange(replicate, year) %>%
        group_by(replicate) %>%
        mutate(
            cum_treatment_defor_ha = cumsum(treatment_defor_ha),
            cum_control_defor_ha = cumsum(control_defor_ha)
        ) %>%
        ungroup()
    yearly_metrics <- c(names(portfolio_cols), "cum_treatment_defor_ha", "cum_control_defor_ha")
    portfolio_yearly <- summarise_replicates(
        select(yearly_rep, -n_sites), "year", yearly_metrics
    ) %>%
        inner_join(distinct(yearly_rep, year, n_sites), by = "year")
}
if (nrow(site_total_rep) > 0) {
    portfolio_total <- site_total_rep %>%
        group_by(replicate) %>%
        summarise(across(all_of(metric_cols), sum), .groups = "drop") %>%
        summarise_replicates(character(0), metric_cols) %>%
        as.list()
}


# -- Match quality -----------------------------------------------------------

cov_data <- bind_rows(
    tibble(id_numeric = integer(), site_id = character(), cell = numeric(),
           group = character(), treatment = logical(), match_group = character(),
           match_weight = numeric(), pscore = numeric()),
    cov_rows
) %>%
    semi_join(complete_strata, by = c("id_numeric", "group")) %>%
    select(-id_numeric, -group)
covariates <- intersect(COVARIATES, names(cov_data))
write_csv(select(cov_data, -pscore), out_path("results_pixel_covariates.csv"))
write_csv(
    select(cov_data, cell, site_id, treatment, match_group, match_weight, pscore),
    out_path("results_propensity_scores.csv")
)

balance <- bind_rows(
    balance_table(cov_data, covariates, CATEGORICAL) %>% mutate(site_id = "__all__"),
    cov_data %>%
        group_by(site_id) %>%
        group_modify(~ balance_table(.x, covariates, CATEGORICAL)) %>%
        ungroup()
)
if (nrow(balance) == 0) {
    balance <- tibble(site_id = character(), covariate = character(), smd = numeric())
}
write_csv(balance, out_path("results_covariate_balance.csv"))

numeric_covariates <- setdiff(covariates, CATEGORICAL)
scope_quality <- function(df) {
    ps <- df[!is.na(df$pscore), ]
    qq <- NULL
    if (sum(ps$treatment) >= 2 && sum(!ps$treatment) >= 2) {
        probs <- seq(0, 1, length.out = 500)
        qq <- list(
            quantiles = probs,
            treatment_values = weighted_quantiles(
                ps$pscore[ps$treatment], ps$match_weight[ps$treatment], probs
            ),
            control_values = weighted_quantiles(
                ps$pscore[!ps$treatment], ps$match_weight[!ps$treatment], probs
            )
        )
    }
    hists <- list()
    for (cov in numeric_covariates) {
        h <- weighted_histogram(df[[cov]], df$treatment, df$match_weight)
        if (!is.null(h)) hists[[cov]] <- h
    }
    list(histograms = hists, qq = qq)
}
mq <- list(summary_stats = list(), histograms = list(), qq_quantiles = list(),
           covariate_cols = covariates)
if (nrow(cov_data) > 0) {
    scopes <- c("__all__", unique(cov_data$site_id))
    for (scope in scopes) {
        df <- if (scope == "__all__") cov_data else cov_data[cov_data$site_id == scope, ]
        counts <- if (scope == "__all__") {
            site_counts
        } else {
            site_counts[site_counts$site_id == scope, ]
        }
        mq$summary_stats[[scope]] <- list(
            n_treatment = sum(df$treatment), n_control = sum(!df$treatment),
            n_sites = n_distinct(df$site_id),
            n_treatment_total = sum(counts$n_site_pixels),
            n_control_sampled = round(sum(counts$n_control_sampled)),
            n_control_pool = round(sum(counts$n_control_pool))
        )
        q <- scope_quality(df)
        mq$histograms[[scope]] <- q$histograms
        if (!is.null(q$qq)) mq$qq_quantiles[[scope]] <- q$qq
    }
}
write_json(mq, out_path("results_match_quality_summary.json"), auto_unbox = TRUE)

gm <- fromJSON(out_path("grid_metadata.json"))
tf <- gm$transform
cov_data %>%
    transmute(
        cell, site_id, treatment, match_group,
        lon = tf[3] + (cell %% gm$width + 0.5) * tf[1],
        lat = tf[6] + (cell %/% gm$width + 0.5) * tf[5]
    ) %>%
    write_csv(out_path("results_pixel_locations.csv"))


# -- Diagnostics and summary -------------------------------------------------

sampling_by_site <- site_counts %>%
    transmute(site_id, sampled_fraction, sampled_percent = sampled_fraction * 100,
              was_subsampled = sampled_fraction < 1)
write_csv(sampling_by_site, out_path("results_sampling_by_site.csv"))

as_records <- function(df) unname(lapply(seq_len(nrow(df)), function(i) as.list(df[i, ])))

na_files <- list.files(config$matches_dir, "_na_exclusion\\.json$", full.names = TRUE)
na_summary <- NULL
if (length(na_files) > 0) {
    na_df <- bind_rows(lapply(na_files, function(path) {
        x <- fromJSON(path)
        x$variables_with_na <- paste(
            names(x$variables_with_na), unlist(x$variables_with_na), sep = "=", collapse = "; "
        )
        as_tibble(x)
    }))
    write_csv(na_df, out_path("na_exclusion_summary.csv"))
    substantial <- filter(na_df, substantial_exclusion)
    na_summary <- list(
        n_sites_with_exclusions = sum(na_df$n_dropped > 0),
        n_sites_with_substantial_exclusions = nrow(substantial),
        avg_pct_excluded_overall = round(mean(na_df$pct_excluded_overall), 2),
        avg_pct_treatment_excluded = round(mean(na_df$pct_treatment_excluded), 2),
        avg_pct_control_excluded = round(mean(na_df$pct_control_excluded), 2),
        max_pct_excluded_overall = round(max(na_df$pct_excluded_overall), 2),
        sites_with_substantial_exclusions = as_records(select(
            substantial, site_id, site_name, pct_excluded_overall,
            pct_treatment_excluded, pct_control_excluded
        ))
    )
}

site_names <- function(ids) {
    filter(sites, site_id %in% ids) %>% transmute(site_id, site_name) %>% as_records()
}
warnings <- list()
if (N_REPLICATES > 1L) {
    warnings[[length(warnings) + 1]] <- list(
        code = "replicate_range",
        message = paste0(
            "Bounds are the ", BOUNDS_LABEL, " (2.5% to 97.5% of ", N_REPLICATES,
            " replicates). They reflect variability from pixel sampling and ",
            "matching only, not a confidence interval for the avoided emissions.",
            if (N_REPLICATES < 50L) " Fewer than 50 replicates were used, so the bounds are imprecise." else ""
        )
    )
}
no_baseline <- filter(sites, start_year - PRE_INTERVENTION_YEARS - 1L < FC_YEAR_MIN)
if (nrow(no_baseline) > 0) {
    warnings[[length(warnings) + 1]] <- list(
        code = "no_baseline_deforestation",
        message = paste0(
            nrow(no_baseline), " site(s) start before ", FC_YEAR_MIN + PRE_INTERVENTION_YEARS + 1L,
            ", so pre-intervention deforestation could not be used as a matching covariate."
        ),
        affected_sites = site_names(no_baseline$site_id)
    )
}
low_coverage <- filter(site_counts, coverage_fraction < LOW_COVERAGE | match_rate < LOW_COVERAGE)
if (nrow(low_coverage) > 0) {
    warnings[[length(warnings) + 1]] <- list(
        code = "low_coverage",
        message = paste0(
            nrow(low_coverage), " site(s) have less than ", LOW_COVERAGE * 100,
            "% of their pixels represented by the estimates (forested, complete data, ",
            "in an exact-match stratum with matches) or less than ", LOW_COVERAGE * 100,
            "% of sampled pixels matched. Totals cover represented pixels only and ",
            "assume unmatched pixels in matched strata resemble matched ones."
        ),
        affected_sites = site_names(low_coverage$site_id)
    )
}

if (n_inconsistent_strata > 0) {
    warnings[[length(warnings) + 1]] <- list(
        code = "inconsistent_strata",
        message = paste0(
            n_inconsistent_strata, " exact-match stratum/strata were matched in only some ",
            "replicates and were excluded from the estimates, so their pixels are not represented."
        )
    )
}

failed_summary <- as_records(select(failed_fragments, id_numeric, site_id, site_name,
                                     sub_site_index, error))
subsampled <- sampling_by_site %>%
    filter(was_subsampled) %>%
    inner_join(select(sites, site_id, site_name), by = "site_id") %>%
    select(site_id, site_name, sampled_fraction, sampled_percent)

total_value <- function(name) if (length(portfolio_total) > 0) portfolio_total[[name]] else 0
summary_data <- list(
    task_id = config$task_id,
    resolution_m = config$resolution_m,
    n_sites = nrow(results_total),
    n_failed_sites = n_failed_sites,
    random_seed = RANDOM_SEED,
    n_replicates = N_REPLICATES,
    biomass_reference_year = 2000L,
    bounds_label = BOUNDS_LABEL,
    r_analysis_git_sha = Sys.getenv("CODE_VERSION", unset = NA_character_),
    extrapolated_total_emissions_avoided_mgco2e = total_value("extrapolated_emissions_avoided_mgco2e"),
    extrapolated_total_forest_loss_avoided_ha = total_value("extrapolated_forest_loss_avoided_ha"),
    sample_total_emissions_avoided_mgco2e = total_value("sample_emissions_avoided_mgco2e"),
    sample_total_forest_loss_avoided_ha = total_value("sample_forest_loss_avoided_ha"),
    total_area_ha = sum(results_total$area_ha),
    year_range = if (nrow(results_by_year) > 0) {
        list(min = min(results_by_year$year), max = max(results_by_year$year))
    } else {
        list(min = NA, max = NA)
    },
    failed_sites = failed_summary,
    worker_failures = lapply(worker_failures, function(x) x[c("array_index", "error")]),
    subsampled_sites = as_records(subsampled),
    na_exclusion_summary = na_summary,
    methodology_warnings = warnings,
    group_diagnostics = c(diagnostics, unlist(lapply(fragment_markers, function(x) x$group_diagnostics),
                                              recursive = FALSE)),
    portfolio = list(
        yearly = if (length(portfolio_yearly) > 0) as_records(portfolio_yearly) else list(),
        total = portfolio_total
    )
)
write_json(summary_data, out_path("results_summary.json"), auto_unbox = TRUE,
           pretty = TRUE, null = "null", na = "null", digits = NA)

message("[TIMING] Step 3 total: ", round((proc.time() - step3_timer)["elapsed"], 1), "s")
message("Step 3 complete. Results written to: ", config$output_dir)

}, step_name = "03_summarize_results")
