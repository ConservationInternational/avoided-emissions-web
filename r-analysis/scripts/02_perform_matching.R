# Step 2: Propensity score matching for avoided emissions analysis.
#
# For each site, matches treatment pixels (within the site) to control pixels
# (outside the site, same GADM region) using propensity scores estimated via
# logistic regression or Mahalanobis distance.
#
# When run on AWS Batch as an array job, each array element processes one site
# (specified by --site-id or AWS_BATCH_JOB_ARRAY_INDEX).
#
# Input:
#   - {output_dir}/sites_processed.rds
#   - {output_dir}/treatment_cell_key.rds
#   - {output_dir}/treatments_and_controls.rds
#   - {output_dir}/formula.rds
#
# Output:
#   - {matches_dir}/m_{id_numeric}.rds : Matched pairs for each site

library(dtplyr)
library(dplyr, warn.conflicts = FALSE)
library(tidyverse)
library(foreach)
library(optmatch)
library(lubridate)

source("/app/scripts/utils.R")
rollbar_init()

with_rollbar({

options("optmatch_max_problem_size" = Inf)

config <- parse_config()
message("Step 2: Propensity score matching")

MAX_TREATMENT <- config$max_treatment_pixels
CONTROL_MULTIPLIER <- config$control_multiplier
MIN_GLM <- config$min_glm_treatment_pixels

# Load data
sites <- readRDS(file.path(config$output_dir, "sites_processed.rds")) %>%
    as_tibble()
treatment_key <- readRDS(file.path(config$output_dir, "treatment_cell_key.rds"))
base_data <- readRDS(file.path(config$output_dir, "treatments_and_controls.rds"))
f <- readRDS(file.path(config$output_dir, "formula.rds"))

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
        site_ids <- unique(treatment_key$id_numeric)[idx]
        message("  AWS Batch array index: ", array_index, " -> site ", site_ids)
    } else {
        # Process all sites sequentially
        site_ids <- unique(treatment_key$id_numeric)
    }
}


get_matches <- function(d, dists) {
    # Attempt full matching (1:1) and return matched pairs.
    # Returns empty data.frame if matching fails.
    subdim_works <- tryCatch(
        is.data.frame(subdim(dists)),
        error = function(e) FALSE
    )
    if (subdim_works) {
        m <- fullmatch(dists, min.controls = 1, max.controls = 1, data = d)
        d$match_group <- as.character(m)
        d <- d[matched(m), ]
        # Label match groups by treatment cell ID
        match_pos <- match(
            d$match_group[!d$treatment],
            d$match_group[d$treatment]
        )
        d$match_group[!d$treatment] <- d$cell[d$treatment][match_pos]
        d$match_group[d$treatment] <- d$cell[d$treatment]
    } else {
        d <- data.frame()
    }
    return(d)
}


match_site <- function(d, f) {
    # Run propensity score matching within each exact-match group.
    m <- foreach(this_group = unique(d$group), .combine = foreach_rbind) %do% {
        this_d <- filter(d, group == this_group)
        n_treatment <- sum(this_d$treatment)

        if (n_treatment < 1) {
            return(NULL)
        } else if (n_treatment < MIN_GLM) {
            # Too few treatment pixels for GLM; use Mahalanobis distance
            dists <- match_on(f, data = this_d)
        } else {
            # Estimate propensity scores with logistic regression
            model <- glm(f, data = this_d, family = binomial())
            dists <- match_on(model, data = this_d)
        }
        return(get_matches(this_d, dists))
    }

    if (is.null(m) || nrow(m) == 0) {
        return(NULL)
    }
    return(m)
}


set.seed(31)

for (this_id in site_ids) {
    site <- filter(sites, id_numeric == this_id)
    match_path <- file.path(config$matches_dir, paste0("m_", this_id, ".rds"))

    if (file.exists(match_path)) {
        message("  Skipping site ", this_id, " (", site$site_id, "): already processed")
        next
    }
    message("  Processing site ", this_id, " (", site$site_id, ")")

    # Get treatment cell IDs for this site
    treatment_cells <- filter(treatment_key, id_numeric == this_id, !is.na(region))
    n_treatment_total <- nrow(treatment_cells)

    if (n_treatment_total == 0) {
        message("  Skipping: no treatment cells")
        next
    }

    # Get all pixels in the treatment site's regions
    vals <- filter(base_data, region %in% unique(treatment_cells$region))
    vals <- vals %>%
        full_join(
            treatment_cells %>% select(cell) %>% mutate(treatment = TRUE),
            by = "cell"
        )
    vals$treatment <- as.logical(vals$treatment)
    vals$treatment[is.na(vals$treatment)] <- FALSE

    # Exclude control pixels that fall within any other site
    treatment_vals <- filter(vals, cell %in% treatment_cells$cell)
    control_vals <- filter(vals, !(cell %in% treatment_key$cell))
    vals <- bind_rows(treatment_vals, control_vals)

    # Remove pixels with NA in grouping variables
    n_before <- nrow(vals)
    vals <- filter(vals, !is.na(region), !is.na(ecoregion), !is.na(pa))
    n_dropped <- n_before - nrow(vals)
    if (n_dropped > 0) {
        message("  Filtered ", n_dropped, " pixels with missing group data")
    }

    # Filter to groups present in both treatment and control
    vals <- filter_groups(vals)

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

    vals <- filter_groups(vals)

    # Add pre-intervention deforestation for sites established >= 2005
    estab_year <- site$start_year
    this_f <- f

    if (estab_year >= 2005) {
        fc_init_name <- paste0("fc_", estab_year - 5)
        fc_final_name <- paste0("fc_", estab_year)

        if (fc_init_name %in% names(vals) && fc_final_name %in% names(vals)) {
            init_fc <- vals[[fc_init_name]]
            final_fc <- vals[[fc_final_name]]
            vals$defor_pre_intervention <- ((final_fc - init_fc) / init_fc) * 100
            vals$defor_pre_intervention[init_fc == 0] <- 0
            # Remove pixels with zero initial forest
            vals <- filter(vals, .data[[fc_init_name]] != 0)
            vals <- filter_groups(vals)
            this_f <- update(this_f, ~ . + defor_pre_intervention)
        }
    }

    n_treatment_final <- sum(vals$treatment)
    n_control_final <- sum(!vals$treatment)
    message("  Treatment pixels: ", n_treatment_final,
            ", Control pixels: ", n_control_final)

    if (n_treatment_final == 0) {
        message("  No treatment pixels remaining after filtering")
        next
    }

    # Run matching
    m <- match_site(vals, this_f)

    if (is.null(m)) {
        message("  No matches found")
    } else {
        m$id_numeric <- this_id
        m$site_id <- site$site_id
        m$sampled_fraction <- n_treatment_final / n_treatment_total
        saveRDS(m, match_path)
        message("  Saved ", nrow(m), " matched rows")
    }
}

message("Step 2 complete.")

}, step_name = "02_perform_matching")
