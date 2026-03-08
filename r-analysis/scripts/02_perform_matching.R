# Step 2: Propensity score matching for avoided emissions analysis.
#
# For each site, matches treatment pixels (within the site) to control pixels
# (outside the site, within the precomputed matching extent) using propensity
# scores estimated via logistic regression or Mahalanobis distance.
#
# When run on AWS Batch as an array job, each array element processes one site
# (specified by --site-id or AWS_BATCH_JOB_ARRAY_INDEX).
#
# Input:
#   - {output_dir}/sites_processed.parquet
#   - {output_dir}/treatment_cell_key.parquet
#   - {output_dir}/treatments_and_controls.parquet
#   - {output_dir}/formula.json
#
# Output:
#   - {matches_dir}/m_{id_numeric}.rds : Matched pairs for each site

library(dtplyr)
library(dplyr, warn.conflicts = FALSE)
library(tidyverse)
library(foreach)
library(optmatch)
library(lubridate)
library(arrow)
library(jsonlite)

source("/app/scripts/utils.R")
rollbar_init()

with_rollbar({

options("optmatch_max_problem_size" = Inf)

config <- parse_config()
message("Step 2: Propensity score matching")

MAX_TREATMENT <- config$max_treatment_pixels
CONTROL_MULTIPLIER <- config$control_multiplier
MIN_GLM <- config$min_glm_treatment_pixels
CALIPER_WIDTH <- config$caliper_width
# 0 means no upper limit (full matching); positive integer caps controls
MAX_CONTROLS <- config$max_controls_per_treatment
RANDOM_SEED <- if (is.null(config$random_seed)) {
    NULL
} else {
    as.integer(config$random_seed)
}

# Exact-match variables read from config (e.g. admin1, ecoregion, pa)
EXACT_MATCH_VARS <- config$exact_match_vars

# Load data — step 1 now outputs Parquet (from the Python rewrite)
sites <- read_parquet(file.path(config$output_dir, "sites_processed.parquet")) %>%
    as_tibble()
# Reconstruct sf geometry from the WKB column written by GeoPandas
if ("geometry" %in% names(sites)) {
    sites <- st_as_sf(sites, wkt = "geometry", crs = 4326)
}
treatment_key <- read_parquet(file.path(config$output_dir, "treatment_cell_key.parquet"))
base_data <- read_parquet(file.path(config$output_dir, "treatments_and_controls.parquet"))
all_site_ids <- unique(treatment_key$id_numeric)
all_treatment_cells <- unique(treatment_key$cell)

# Load formula from JSON
formula_json <- fromJSON(file.path(config$output_dir, "formula.json"))
f <- as.formula(formula_json$formula_str)

# Determine which site(s) to process
if (!is.null(config$site_id)) {
    # Process a specific site
    target_site <- filter(sites, site_id == config$site_id)
    if (nrow(target_site) == 0) {
        stop(paste("Site not found:", config$site_id))
    }
    site_ids <- target_site$id_numeric
} else {
    # Check for AWS Batch array index
    array_index <- Sys.getenv("AWS_BATCH_JOB_ARRAY_INDEX", "")
    if (array_index != "") {
        idx <- as.integer(array_index) + 1  # AWS uses 0-based indexing
        site_ids <- all_site_ids[idx]
        batch_site_id <- filter(sites, id_numeric == site_ids)$site_id[1]
        message("  AWS Batch array index: ", array_index,
                " -> site_id ", batch_site_id)
    } else {
        # Process all sites sequentially
        site_ids <- all_site_ids
    }
}


get_matches <- function(d, dists) {
    # Attempt matching and return matched pairs with weights.
    # Returns empty data.frame if matching fails.
    #
    # MAX_CONTROLS controls the matching strategy:
    #   0 -> full matching with variable ratios
    #   k -> fixed k:1 matching via pairmatch
    #
    # Controls within each matched set are weighted so that total control
    # weight equals the number of treated units in that set. Treatment
    # pixels get weight = 1.
    subdim_works <- tryCatch(
        is.data.frame(subdim(dists)),
        error = function(e) FALSE
    )
    if (subdim_works) {
        if (MAX_CONTROLS > 0) {
            # Assigns exactly controls per treatment when feasible;
            # treatments without enough eligible controls are left unmatched.
            m <- pairmatch(dists, controls = MAX_CONTROLS, data = d)
        } else {
            # Full matching — variable ratios, all units matched
            m <- fullmatch(dists, min.controls = 1,
                           max.controls = Inf, data = d)
        }
        d$match_group <- as.character(m)
        d <- d[matched(m), ]

        # Weights within matched sets:
        # treatment units carry weight 1 each; control weights are scaled so
        # the total control weight in a set equals the number of treated units.
        # This supports both pair matching (1:k) and full matching with
        # potentially multiple treated units per matched set.
        d$match_weight <- 1
        group_counts <- d %>%
            group_by(match_group) %>%
            summarise(
                n_treated = sum(treatment),
                n_controls = sum(!treatment),
                .groups = "drop"
            )
        ctrl_idx <- which(!d$treatment)
        ctrl_groups <- d$match_group[ctrl_idx]
        matched_counts <- group_counts[match(ctrl_groups, group_counts$match_group), ]
        d$match_weight[ctrl_idx] <-
            matched_counts$n_treated / matched_counts$n_controls
    } else {
        d <- data.frame()
    }
    return(d)
}


match_site <- function(d, f) {
    # Run propensity score matching within each exact-match group.
    # Propensity scores (from GLM) are stored in a ``pscore`` column on
    # the returned data.frame.  Groups too small for GLM get NA scores.
    m <- foreach(this_group = unique(d$group), .combine = foreach_rbind) %do% {
        this_d <- filter(d, group == this_group)

        # Drop rows with NA in any formula variable so that glm() and
        # predict() operate on the same set of rows (glm uses na.omit
        # by default, which silently drops incomplete cases and causes
        # a length mismatch when assigning predictions back).
        formula_vars <- all.vars(f)
        complete <- complete.cases(this_d[, formula_vars, drop = FALSE])
        n_dropped_na <- sum(!complete)
        if (n_dropped_na > 0) {
            this_d <- this_d[complete, ]
            message(
                "    Dropped ", n_dropped_na,
                " rows with NA covariates in group ", this_group
            )
        }

        n_treatment <- sum(this_d$treatment)

        if (n_treatment < 1) {
            return(NULL)
        } else if (n_treatment < MIN_GLM) {
            # Too few treatment pixels for GLM; use Mahalanobis distance
            dists <- match_on(f, data = this_d)
            if (CALIPER_WIDTH > 0) {
                dists <- dists + caliper(dists, width = CALIPER_WIDTH)
            }
            this_d$pscore <- NA_real_
        } else {
            # Estimate propensity scores with logistic regression
            model <- glm(f, data = this_d, family = binomial())
            this_d$pscore <- predict(model, type = "response")
            dists <- match_on(model, data = this_d)
            if (CALIPER_WIDTH > 0) {
                dists <- dists + caliper(dists, width = CALIPER_WIDTH)
            }
        }
        return(get_matches(this_d, dists))
    }

    if (is.null(m) || nrow(m) == 0) {
        return(NULL)
    }
    return(m)
}

n_failed <- 0L
required_match_cols <- c(
    "cell", "site_id", "id_numeric", "area_ha", "treatment",
    "sampled_fraction", "total_biomass", "match_group", "match_weight"
)

for (this_id in site_ids) {
    site <- filter(sites, id_numeric == this_id)
    this_site_id <- site$site_id[1]
    this_site_name <- if ("site_name" %in% names(site)) site$site_name[1] else NA_character_
    this_batch_index <- match(this_id, all_site_ids) - 1L
    match_path <- file.path(config$matches_dir, paste0("m_", this_id, ".rds"))
    failure_path <- file.path(config$matches_dir,
                              paste0("failed_", this_id, ".json"))

    if (file.exists(match_path)) {
        existing_ok <- tryCatch({
            existing <- readRDS(match_path)
            missing_cols <- setdiff(required_match_cols, names(existing))
            if (length(missing_cols) > 0) {
                message(
                    "  Existing match file for site_id ", this_site_id,
                    " is missing columns: ",
                    paste(missing_cols, collapse = ", "),
                    "; regenerating"
                )
                FALSE
            } else {
                TRUE
            }
        }, error = function(e) {
            message(
                "  Existing match file for site_id ", this_site_id,
                " is unreadable (", conditionMessage(e), "); regenerating"
            )
            FALSE
        })

        if (existing_ok) {
            message("  Skipping site_id ", this_site_id,
                    " (batch_index=", this_batch_index,
                    "): already processed")
            next
        }

        unlink(match_path, force = TRUE)
    }
    message("  Processing site_id ", this_site_id,
            " (batch_index=", this_batch_index, ")")

    # Deterministic per-site sampling for reproducibility across reruns.
    if (!is.null(RANDOM_SEED)) {
        set.seed(RANDOM_SEED + as.integer(this_id))
    }

    # Wrap per-site matching in tryCatch so that a failure in one site
    # (e.g. memory allocation error in optmatch) does not abort the
    # entire job.  A failure marker JSON is written instead.
    ok <- tryCatch({
        # Get treatment cell IDs for this site
        treatment_cells <- filter(treatment_key, id_numeric == this_id)
        n_treatment_total <- nrow(treatment_cells)

        if (n_treatment_total == 0) {
            message("  Skipping: no treatment cells")
            failure_info <- list(
                id_numeric = this_id,
                site_id = this_site_id,
                site_name = this_site_name,
                error = "No treatment cells found for site",
                timestamp = format(
                    Sys.time(), "%Y-%m-%dT%H:%M:%SZ"
                ),
                array_index = this_batch_index
            )
            write_json(
                failure_info, failure_path,
                auto_unbox = TRUE, pretty = TRUE
            )
            TRUE
        } else {
            # All candidate pixels (treatment + controls) are spatially
            # constrained to the matching extent computed in the webapp
            site_treatment_cells <- treatment_cells$cell
            vals <- base_data %>%
                mutate(treatment = cell %in% site_treatment_cells) %>%
                filter(treatment | !(cell %in% all_treatment_cells))

            # Remove pixels with NA in exact-match grouping variables
            n_before <- nrow(vals)
            vals <- vals %>%
                filter(if_all(all_of(EXACT_MATCH_VARS), ~ !is.na(.)))
            n_dropped <- n_before - nrow(vals)
            if (n_dropped > 0) {
                message("  Filtered ", n_dropped,
                        " pixels with missing group data")
            }

            # Filter to groups present in both treatment and control
            vals <- filter_groups(vals, EXACT_MATCH_VARS)

            # Record control pool size before subsampling
            n_control_pool_site <- sum(!vals$treatment)

            # Sample to manageable sizes
            sample_sizes <- vals %>% count(treatment, group)
            vals <- bind_rows(
                filter(vals, treatment) %>%
                    group_by(group) %>%
                    sample_n(min(MAX_TREATMENT, n())),
                filter(vals, !treatment) %>%
                    group_by(this_group = group) %>%
                    sample_n(min(
                        CONTROL_MULTIPLIER * filter(
                            sample_sizes, treatment == TRUE,
                            group == this_group[1]
                        )$n,
                        n()
                    ))
            ) %>%
                ungroup() %>%
                select(-any_of("this_group"))

            # Add pre-intervention deforestation for sites >= 2005
            estab_year <- site$start_year
            this_f <- f

            if (estab_year >= 2005) {
                fc_init_name <- paste0("fc_", estab_year - 5)
                fc_final_name <- paste0("fc_", estab_year)

                if (fc_init_name %in% names(vals) &&
                    fc_final_name %in% names(vals)) {
                    init_fc <- vals[[fc_init_name]]
                    final_fc <- vals[[fc_final_name]]
                    vals$defor_pre_intervention <-
                        ((final_fc - init_fc) / init_fc) * 100
                    vals$defor_pre_intervention[init_fc == 0] <- 0
                    vals <- filter(vals, .data[[fc_init_name]] != 0)
                    vals <- filter_groups(vals, EXACT_MATCH_VARS)
                    this_f <- update(this_f, ~ . + defor_pre_intervention)
                }
            }

            n_treatment_final <- sum(vals$treatment)
            n_control_final <- sum(!vals$treatment)
            message("  Treatment pixels: ", n_treatment_final,
                    ", Control pixels: ", n_control_final)

            if (n_treatment_final == 0) {
                message("  No treatment pixels remaining after filtering")
                failure_info <- list(
                    id_numeric = this_id,
                    site_id = this_site_id,
                    site_name = this_site_name,
                    error = "No treatment pixels remaining after filtering",
                    timestamp = format(
                        Sys.time(), "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    array_index = this_batch_index
                )
                write_json(
                    failure_info, failure_path,
                    auto_unbox = TRUE, pretty = TRUE
                )
                TRUE
            } else {
                # Run matching
                m <- match_site(vals, this_f)

                if (is.null(m)) {
                    message("  No matches found")
                    # Write a failure marker so the summarize step
                    # knows this site was processed but produced no
                    # matches (e.g. caliper too tight, perfect
                    # separation in propensity scores).
                    failure_info <- list(
                        id_numeric = this_id,
                        site_id = this_site_id,
                        site_name = this_site_name,
                        error = paste0(
                            "No matches found (treatment=",
                            n_treatment_final,
                            ", control=", n_control_final, ")"
                        ),
                        timestamp = format(
                            Sys.time(), "%Y-%m-%dT%H:%M:%SZ"
                        ),
                        array_index = this_batch_index
                    )
                    write_json(
                        failure_info, failure_path,
                        auto_unbox = TRUE, pretty = TRUE
                    )
                    message(
                        "  No-match marker written to ",
                        failure_path
                    )
                } else {
                    m$id_numeric <- this_id
                    m$site_id <- site$site_id
                    m$sampled_fraction <- n_treatment_final / n_treatment_total
                    m$n_control_sampled <- n_control_final
                    m$n_control_pool <- n_control_pool_site
                    saveRDS(m, match_path)
                    message("  Saved ", nrow(m), " matched rows")
                }
                TRUE
            }
        }
    }, error = function(e) {
        msg <- conditionMessage(e)
        message("  ERROR processing site_id ", this_site_id,
            " (batch_index=", this_batch_index,
            "): ", msg)
        failure_info <- list(
            id_numeric = this_id,
            site_id = this_site_id,
            site_name = this_site_name,
            error = msg,
            timestamp = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")
        )
        write_json(failure_info, failure_path,
                    auto_unbox = TRUE, pretty = TRUE)
        message("  Failure marker written to ", failure_path)
        rollbar_report_error(msg)
        FALSE
    })

    if (!ok) n_failed <- n_failed + 1L
}

if (n_failed > 0L) {
    message("WARNING: ", n_failed, " site(s) failed matching ",
            "(failure markers written)")
}

message("Step 2 complete.")

}, step_name = "02_perform_matching")
