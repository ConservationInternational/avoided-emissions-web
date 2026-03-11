# Step 2: Propensity score matching for avoided emissions analysis.
#
# For each site, matches treatment pixels (within the site) to control pixels
# (outside the site, within the precomputed matching extent) using propensity
# scores estimated via logistic regression or Mahalanobis distance.
#
# When run on AWS Batch as an array job, each array element processes one
# (site, replicate) combination.  The composite array index is decoded as:
#   site_index      = floor(array_index / N_REPLICATES)
#   replicate_index = (array_index %% N_REPLICATES) + 1
# When N_REPLICATES == 1 the mapping is identical to the old behaviour
# (one element per site).
#
# Input:
#   - {output_dir}/sites_processed.parquet
#   - {output_dir}/treatment_cell_key.parquet
#   - {output_dir}/treatments_and_controls.parquet
#   - {output_dir}/formula.json
#
# Output:
#   - {matches_dir}/m_{id_numeric}[_rep{k}].rds : Matched pairs for each site

library(dplyr, warn.conflicts = FALSE)
library(foreach)
library(optmatch)
library(arrow)
library(jsonlite)

source("/app/scripts/utils.R")
rollbar_init()

with_rollbar({

options("optmatch_max_problem_size" = Inf)

config <- parse_config()
message("Step 2: Propensity score matching")
step2_timer <- proc.time()

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

# Number of times to repeat the matching (with different random samples)
# to construct confidence intervals. Default 1 (single run, no CIs).
N_REPLICATES <- if (is.null(config$n_replicates)) {
    1L
} else {
    as.integer(config$n_replicates)
}

# When TRUE and GLM separation is detected, fall back to Mahalanobis
# distance matching instead of failing outright.  Default FALSE so
# users must opt in via the webapp.
SEPARATION_FALLBACK <- isTRUE(
    config$separation_fallback_mahalanobis
)

# Minimum distance (km) from treatment polygons for control pixels.
# Controls whose centroids fall within this buffer are excluded.
# 0 disables the distance exclusion.
MIN_CONTROL_DISTANCE_KM <- if (is.null(config$min_control_distance_km)) {
    10
} else {
    as.numeric(config$min_control_distance_km)
}

# Exact-match variables read from config (e.g. admin1, ecoregion, pa)
EXACT_MATCH_VARS <- config$exact_match_vars

# Load data — step 1 now outputs Parquet (from the Python rewrite)
sites <- read_parquet(file.path(config$output_dir, "sites_processed.parquet")) %>%
    as_tibble()
# Reconstruct sf geometry from the WKB column written by GeoPandas.
# Temporarily disable S2 so that st_as_sf() does not reject geometries
# with degenerate edges or self-intersections under spherical rules.
# After repairing with st_make_valid(), S2 is re-enabled.
if ("geometry" %in% names(sites)) {
    s2_was_on <- sf_use_s2()
    sf_use_s2(FALSE)
    sites <- st_as_sf(sites, wkt = "geometry", crs = 4326)
    sites <- st_make_valid(sites)
    sf_use_s2(s2_was_on)
}

# Load grid metadata for cell-index → lon/lat conversion (needed for
# the minimum control distance exclusion).
grid_meta <- fromJSON(file.path(config$output_dir, "grid_metadata.json"))

cell_to_lonlat <- function(cells, gm) {
    # Convert flat cell indices to pixel-centre lon/lat using the
    # affine transform stored in grid_metadata.json.
    # transform = [a, b, c, d, e, f] where:
    #   x = c + col * a  (a = pixel width)
    #   y = f + row * e  (e = pixel height, negative for north-up)
    tf <- gm$transform
    w <- gm$width
    rows <- cells %/% w
    cols <- cells %% w
    data.frame(
        lon = tf[3] + (cols + 0.5) * tf[1],
        lat = tf[6] + (rows + 0.5) * tf[5]
    )
}
treatment_key <- read_parquet(file.path(config$output_dir, "treatment_cell_key.parquet"))
# Keep parquet on disk as an Arrow dataset — avoids materialising the entire
# Per-site subsets are filtered in Arrow and collected just-in-time.
base_dataset <- open_dataset(
    file.path(config$output_dir, "treatments_and_controls.parquet"),
    format = "parquet"
)
all_site_ids <- unique(treatment_key$id_numeric)
all_treatment_cells <- unique(treatment_key$cell)

# Pre-compute the buffer around ALL treatment sites for the minimum
# control distance exclusion.  Controls for any site must be at least
# MIN_CONTROL_DISTANCE_KM away from every treatment polygon.
if (MIN_CONTROL_DISTANCE_KM > 0) {
    if (!is.null(config$sites_exclusion_buffer)) {
        # Use the pre-computed buffer from PostGIS (geography-based,
        # avoids S2 edge-crossing issues in R).
        buf_json <- toJSON(config$sites_exclusion_buffer, auto_unbox = TRUE)
        s2_state <- sf_use_s2()
        sf_use_s2(FALSE)
        all_sites_buffer <- st_make_valid(
            st_set_crs(st_as_sfc(buf_json, GeoJSON = TRUE), 4326)
        )
        sf_use_s2(s2_state)
        message(
            "  Distance exclusion buffer loaded from config: ",
            MIN_CONTROL_DISTANCE_KM, " km around all ",
            nrow(sites), " site(s)"
        )
    } else {
        # Fallback: compute buffer locally.  The webapp normally
        # pre-computes this in PostGIS; this path only runs for
        # manual/legacy invocations.
        warning("sites_exclusion_buffer not in config; computing locally")
        all_sites_buffer <- st_buffer(
            st_union(st_geometry(sites)),
            dist = units::set_units(MIN_CONTROL_DISTANCE_KM, "km")
        )
    }
} else {
    all_sites_buffer <- NULL
}

# Load formula from JSON
formula_json <- fromJSON(file.path(config$output_dir, "formula.json"))
f <- as.formula(formula_json$formula_str)

# Determine which site(s) and replicate(s) to process.
# BATCH_REPLICATE will be set to a single integer when the array index
# specifies a specific replicate; NULL means "run all replicates".
BATCH_REPLICATE <- NULL

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
        ai <- as.integer(array_index)
        n_sites <- length(all_site_ids)
        if (N_REPLICATES > 1L) {
            # Composite index: site_index * N_REPLICATES + replicate_index
            site_idx_0 <- ai %/% N_REPLICATES  # 0-based site index
            rep_idx_0 <- ai %% N_REPLICATES    # 0-based replicate index
            BATCH_REPLICATE <- rep_idx_0 + 1L   # 1-based
            site_ids <- all_site_ids[site_idx_0 + 1L]
            batch_site_id <- filter(
                sites, id_numeric == site_ids
            )$site_id[1]
            message(
                "  AWS Batch array index: ", array_index,
                " -> site_id ", batch_site_id,
                ", replicate ", BATCH_REPLICATE, "/", N_REPLICATES
            )
        } else {
            site_ids <- all_site_ids[ai + 1L]
            batch_site_id <- filter(
                sites, id_numeric == site_ids
            )$site_id[1]
            message(
                "  AWS Batch array index: ", array_index,
                " -> site_id ", batch_site_id
            )
        }
    } else {
        # Process all sites sequentially
        site_ids <- all_site_ids
    }
}


check_separation <- function(d, f) {
    # Detect quasi-complete separation in the data before fitting GLM.
    # Returns a list with:
    #   separated    : logical — TRUE if any covariate is problematic
    #   details      : character vector describing each problem
    #
    # Checks two things:
    # 1. Factor levels that appear only in treatment or only in control
    # 2. Continuous variables with zero overlap between groups
    formula_vars <- all.vars(f)
    rhs_vars <- setdiff(formula_vars, "treatment")
    problems <- character(0)

    for (v in rhs_vars) {
        if (!v %in% names(d)) next
        col <- d[[v]]
        treat_vals <- col[d$treatment]
        ctrl_vals <- col[!d$treatment]

        if (is.factor(col) || is.character(col)) {
            treat_levels <- unique(as.character(treat_vals))
            ctrl_levels <- unique(as.character(ctrl_vals))
            only_treat <- setdiff(treat_levels, ctrl_levels)
            only_ctrl <- setdiff(ctrl_levels, treat_levels)
            if (length(only_treat) > 0) {
                problems <- c(problems, paste0(
                    v, ": ", length(only_treat),
                    " level(s) only in treatment (",
                    paste(head(only_treat, 5), collapse = ", "),
                    if (length(only_treat) > 5) ", ..." else "",
                    ")"
                ))
            }
            if (length(only_ctrl) > 0) {
                problems <- c(problems, paste0(
                    v, ": ", length(only_ctrl),
                    " level(s) only in control (",
                    paste(head(only_ctrl, 5), collapse = ", "),
                    if (length(only_ctrl) > 5) ", ..." else "",
                    ")"
                ))
            }
        } else if (is.numeric(col)) {
            t_range <- range(treat_vals, na.rm = TRUE)
            c_range <- range(ctrl_vals, na.rm = TRUE)
            if (t_range[2] < c_range[1] || c_range[2] < t_range[1]) {
                problems <- c(problems, paste0(
                    v, ": no overlap (treatment [",
                    round(t_range[1], 3), ", ", round(t_range[2], 3),
                    "], control [",
                    round(c_range[1], 3), ", ", round(c_range[2], 3),
                    "])"
                ))
            }
        }
    }

    list(separated = length(problems) > 0, details = problems)
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
    #
    # Returns a list with:
    #   result              : data.frame of matched rows (or NULL)
    #   separation_warnings : list of per-group separation diagnostics
    sep_warnings <- list()

    m <- foreach(this_group = unique(d$group), .combine = foreach_rbind) %do% {
        this_d <- filter(d, group == this_group)
        n_treatment_grp <- sum(this_d$treatment)
        n_control_grp <- sum(!this_d$treatment)

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
            message("    Group ", this_group, ": 0 treatment pixels, skipping")
            return(NULL)
        } else if (n_treatment < MIN_GLM) {
            # Too few treatment pixels for GLM; use Mahalanobis distance
            dists <- match_on(f, data = this_d)
            if (CALIPER_WIDTH > 0) {
                dists <- dists + caliper(dists, width = CALIPER_WIDTH)
            }
            this_d$pscore <- NA_real_
        } else {
            # Check for quasi-complete separation before fitting GLM
            sep <- check_separation(this_d, f)
            if (sep$separated) {
                for (d_msg in sep$details) {
                    message("    Separation detected in group ",
                            this_group, ": ", d_msg)
                }
                sep_warnings[[as.character(this_group)]] <<- sep$details

                if (SEPARATION_FALLBACK) {
                    message("    Falling back to Mahalanobis distance ",
                            "(separation_fallback_mahalanobis=true)")
                    dists <- match_on(f, data = this_d)
                    if (CALIPER_WIDTH > 0) {
                        dists <- dists + caliper(dists, width = CALIPER_WIDTH)
                    }
                    this_d$pscore <- NA_real_
                    grp_fb <- get_matches(this_d, dists)
                    n_fb <- if (is.data.frame(grp_fb)) {
                        sum(grp_fb$treatment)
                    } else {
                        0L
                    }
                    message("    Group ", this_group,
                            " (fallback): T=", n_treatment_grp,
                            " C=", n_control_grp,
                            " -> ", n_fb, " matched")
                    return(grp_fb)
                }
            }

            # Estimate propensity scores with logistic regression
            model <- glm(f, data = this_d, family = binomial())
            this_d$pscore <- predict(model, type = "response")
            dists <- match_on(model, data = this_d)
            if (CALIPER_WIDTH > 0) {
                dists <- dists + caliper(dists, width = CALIPER_WIDTH)
            }
        }
        grp_result <- get_matches(this_d, dists)
        n_matched <- if (is.data.frame(grp_result)) {
            sum(grp_result$treatment)
        } else {
            0L
        }
        if (n_matched == 0) {
            message("    Group ", this_group, ": T=", n_treatment_grp,
                    " C=", n_control_grp, " -> 0 matches")
        } else {
            message("    Group ", this_group, ": T=", n_treatment_grp,
                    " C=", n_control_grp, " -> ", n_matched, " matched")
        }
        return(grp_result)
    }

    if (is.null(m) || nrow(m) == 0) {
        return(list(result = NULL, separation_warnings = sep_warnings))
    }
    return(list(result = m, separation_warnings = sep_warnings))
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
    # Which replicates should this node process?
    replicate_range <- if (!is.null(BATCH_REPLICATE)) {
        BATCH_REPLICATE  # single replicate when parallelised
    } else {
        seq_len(N_REPLICATES)
    }

    # Build replicate-specific match file paths.
    # When N_REPLICATES == 1, use the original naming for backward compat.
    if (N_REPLICATES > 1L) {
        rep_match_paths <- file.path(
            config$matches_dir,
            paste0("m_", this_id, "_rep", seq_len(N_REPLICATES), ".rds")
        )
        # Paths for only the replicates this node will process
        node_match_paths <- file.path(
            config$matches_dir,
            paste0("m_", this_id, "_rep", replicate_range, ".rds")
        )
    } else {
        rep_match_paths <- file.path(
            config$matches_dir, paste0("m_", this_id, ".rds")
        )
        node_match_paths <- rep_match_paths
    }
    # Include replicate in failure marker name when parallelised,
    # so each array child writes a distinct file.
    if (!is.null(BATCH_REPLICATE)) {
        failure_path <- file.path(
            config$matches_dir,
            paste0("failed_", this_id, "_rep", BATCH_REPLICATE, ".json")
        )
    } else {
        failure_path <- file.path(
            config$matches_dir, paste0("failed_", this_id, ".json")
        )
    }

    # Check if the replicates this node is responsible for already exist
    node_reps_done <- all(file.exists(node_match_paths))
    if (node_reps_done) {
        existing_ok <- tryCatch({
            existing <- readRDS(node_match_paths[1])
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

        for (rp in node_match_paths) unlink(rp, force = TRUE)
    }
    rep_label <- if (!is.null(BATCH_REPLICATE)) {
        paste0("replicate ", BATCH_REPLICATE, "/", N_REPLICATES)
    } else {
        paste0("replicates=", N_REPLICATES)
    }
    message("  Processing site_id ", this_site_id,
            " (batch_index=", this_batch_index,
            ", ", rep_label, ")")

    # Wrap per-site matching in tryCatch so that a failure in one site
    # (e.g. memory allocation error in optmatch) does not abort the
    # entire job.  A failure marker JSON is written instead.
    ok <- tryCatch({
        site_timer <- proc.time()

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
            # constrained to the matching extent computed in the webapp.
            # Filter in Arrow to avoid materialising the full parquet.
            site_treatment_cells <- treatment_cells$cell
            vals <- base_dataset %>%
                filter(cell %in% site_treatment_cells |
                       !(cell %in% all_treatment_cells)) %>%
                collect() %>%
                mutate(treatment = cell %in% site_treatment_cells)

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

            # Exclude control pixels too close to ANY treatment polygon
            if (MIN_CONTROL_DISTANCE_KM > 0) {
                control_mask <- !vals$treatment
                n_ctrl_before <- sum(control_mask)
                if (n_ctrl_before > 0) {
                    coords <- cell_to_lonlat(
                        vals$cell[control_mask], grid_meta
                    )
                    ctrl_pts <- st_as_sf(
                        coords,
                        coords = c("lon", "lat"), crs = 4326
                    )
                    too_close <- lengths(
                        st_intersects(ctrl_pts, all_sites_buffer)
                    ) > 0
                    n_excluded <- sum(too_close)
                    if (n_excluded > 0) {
                        exclude_rows <- which(control_mask)[too_close]
                        vals <- vals[-exclude_rows, ]
                        message(
                            "  Excluded ", n_excluded,
                            " control pixels within ",
                            MIN_CONTROL_DISTANCE_KM,
                            " km of treatment polygons"
                        )
                    }
                }
                # Re-filter groups after distance exclusion
                vals <- filter_groups(vals, EXACT_MATCH_VARS)
            }

            # Record control pool size before subsampling
            n_control_pool_site <- sum(!vals$treatment)

            data_elapsed <- (proc.time() - site_timer)["elapsed"]
            message(
                "    [TIMING] Data loading & filtering: ",
                round(data_elapsed, 1), "s",
                " (T=", sum(vals$treatment),
                ", C=", n_control_pool_site, ")"
            )

            # Determine formula update for pre-intervention deforestation
            # (deterministic — computed once, applied inside each replicate)
            estab_year <- site$start_year
            this_f <- f
            add_defor_pre <- FALSE
            fc_init_name <- NULL
            fc_final_name <- NULL

            if (estab_year >= 2005) {
                fc_init_name <- paste0("fc_", estab_year - 5)
                fc_final_name <- paste0("fc_", estab_year)
                if (fc_init_name %in% names(vals) &&
                    fc_final_name %in% names(vals)) {
                    add_defor_pre <- TRUE
                    this_f <- update(this_f, ~ . + defor_pre_intervention)
                }
            }

            # Save unsampled data for replicate re-use
            vals_base <- vals

            # ---- Replicate loop ----
            for (rep_k in replicate_range) {
                rep_timer <- proc.time()
                match_path_k <- rep_match_paths[rep_k]

                if (file.exists(match_path_k)) {
                    message("    Replicate ", rep_k,
                            "/", N_REPLICATES, ": already exists, skipping")
                    next
                }

                # Deterministic per-site, per-replicate seed
                if (!is.null(RANDOM_SEED)) {
                    if (N_REPLICATES > 1L) {
                        set.seed(
                            RANDOM_SEED + as.integer(this_id) * 1000L + rep_k
                        )
                    } else {
                        set.seed(RANDOM_SEED + as.integer(this_id))
                    }
                }

                if (N_REPLICATES > 1L) {
                    message("    Replicate ", rep_k, "/", N_REPLICATES)
                }

                vals <- vals_base

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
                if (add_defor_pre) {
                    init_fc <- vals[[fc_init_name]]
                    final_fc <- vals[[fc_final_name]]
                    vals$defor_pre_intervention <-
                        ((final_fc - init_fc) / init_fc) * 100
                    vals$defor_pre_intervention[init_fc == 0] <- 0
                    vals <- filter(vals, .data[[fc_init_name]] != 0)
                    vals <- filter_groups(vals, EXACT_MATCH_VARS)
                }

                n_treatment_final <- sum(vals$treatment)
                n_control_final <- sum(!vals$treatment)
                message("    Treatment pixels: ", n_treatment_final,
                        ", Control pixels: ", n_control_final)

                if (n_treatment_final == 0) {
                    message("    No treatment pixels remaining after filtering")
                    failure_info <- list(
                        id_numeric = this_id,
                        site_id = this_site_id,
                        site_name = this_site_name,
                        replicate = rep_k,
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
                } else {
                    # Run matching
                    match_timer <- proc.time()
                    match_result <- match_site(vals, this_f)
                    match_elapsed <- (proc.time() - match_timer)["elapsed"]
                    m <- match_result$result
                    sep_warnings <- match_result$separation_warnings
                    message(
                        "    [TIMING] Matching: ",
                        round(match_elapsed, 1), "s"
                    )

                    if (is.null(m)) {
                        message("    No matches found")
                        error_msg <- paste0(
                            "No matches found (treatment=",
                            n_treatment_final,
                            ", control=", n_control_final, ")"
                        )
                        if (length(sep_warnings) > 0) {
                            error_msg <- paste0(
                                error_msg,
                                "; separation detected in ",
                                length(sep_warnings), " group(s)"
                            )
                        }
                        failure_info <- list(
                            id_numeric = this_id,
                            site_id = this_site_id,
                            site_name = this_site_name,
                            replicate = rep_k,
                            error = error_msg,
                            separation_warnings = sep_warnings,
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
                            "    No-match marker written to ",
                            failure_path
                        )
                    } else {
                        m$id_numeric <- this_id
                        m$site_id <- site$site_id
                        m$sampled_fraction <-
                            n_treatment_final / n_treatment_total
                        m$n_control_sampled <- n_control_final
                        m$n_control_pool <- n_control_pool_site
                        m$replicate <- rep_k
                        if (length(sep_warnings) > 0) {
                            attr(m, "separation_warnings") <-
                                sep_warnings
                        }
                        saveRDS(m, match_path_k)
                        message("    Saved ", nrow(m), " matched rows")
                        if (length(sep_warnings) > 0) {
                            message(
                                "    WARNING: separation detected in ",
                                length(sep_warnings),
                                " group(s) but matching succeeded"
                            )
                        }
                    }
                }
                rep_elapsed <- (proc.time() - rep_timer)["elapsed"]
                message(
                    "    [TIMING] Replicate ", rep_k,
                    " total: ", round(rep_elapsed, 1), "s"
                )
            }
            # ---- End replicate loop ----
            TRUE
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
    # Free per-site temporaries and reclaim memory before next site
    rm(list = intersect(
        c("vals", "vals_base", "m", "treatment_cells",
          "site_treatment_cells", "sample_sizes"),
        ls()
    ))
    gc()
}

if (n_failed > 0L) {
    message("WARNING: ", n_failed, " site(s) failed matching ",
            "(failure markers written)")
}

step2_elapsed <- (proc.time() - step2_timer)["elapsed"]
message("[TIMING] Step 2 total: ", round(step2_elapsed, 1), "s")
message("Step 2 complete.")

}, step_name = "02_perform_matching")
