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
library(MatchIt)
library(arrow)
library(jsonlite)

source("/app/scripts/utils.R")
rollbar_init()

with_rollbar({

options("optmatch_max_problem_size" = Inf)
# Use the RELAX-IV network flow solver (via rrelaxiv) which is 2-5x
# faster than the default LEMON solver for large matching problems.
if (requireNamespace("rrelaxiv", quietly = TRUE)) {
    options("optmatch_solver" = "RELAX-IV")
    message("Using RELAX-IV solver")
}

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

# When the treatment-to-control pixel ratio exceeds this value, the
# logistic GLM produces unreliable propensity scores (intercept bias
# pushes all predictions away from 0.5, collapsing score overlap).
# Fall back to Mahalanobis distance matching unconditionally.
# This typically occurs when the control pool is exhausted in a sparse
# exact-match stratum (e.g. T=503 vs C=50 in a small ecoregion unit).
IMBALANCE_RATIO_THRESHOLD <- 2.0

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

# Matching method: "optimal" uses optmatch (slower, globally optimal)
# "nearest" uses MatchIt nearest-neighbour (faster, greedy)
MATCHING_METHOD <- if (is.null(config$matching_method)) {
    "optimal"
} else {
    config$matching_method
}
message("Matching method: ", MATCHING_METHOD)

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

# Check for cross-site grouping mode
GROUP_BY_EXACT_MATCHES <- isTRUE(config$group_by_exact_matches)
BATCH_GROUP_SITES <- isTRUE(config$batch_group_sites)
GROUP_MAPPING <- NULL
if (BATCH_GROUP_SITES || GROUP_BY_EXACT_MATCHES) {
    group_file <- file.path(config$output_dir, "exact_match_groups.json")
    if (file.exists(group_file)) {
        GROUP_MAPPING <- fromJSON(group_file, simplifyVector = FALSE)
        message(
            "  Cross-site grouping enabled: ",
            length(GROUP_MAPPING), " exact-match groups"
        )
    } else {
        warning("Group mapping file not found: exact_match_groups.json")
        GROUP_BY_EXACT_MATCHES <- FALSE
        BATCH_GROUP_SITES <- FALSE
    }
}

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

        if ((BATCH_GROUP_SITES || GROUP_BY_EXACT_MATCHES) && !is.null(GROUP_MAPPING)) {
            # Cross-site grouping: array index maps to (group_id, replicate)
            group_ids <- as.integer(names(GROUP_MAPPING))
            n_groups <- length(group_ids)

            if (N_REPLICATES > 1L) {
                group_idx_0 <- ai %/% N_REPLICATES  # 0-based
                rep_idx_0 <- ai %% N_REPLICATES
                BATCH_REPLICATE <- rep_idx_0 + 1L
                # Validate array index is within bounds
                if (group_idx_0 >= n_groups) {
                    stop("AWS Batch array index ", ai, " out of bounds for ",
                         n_groups, " groups × ", N_REPLICATES, " replicates ",
                         "(valid range: 0-", (n_groups * N_REPLICATES - 1), ")")
                }
                group_id <- group_ids[group_idx_0 + 1L]
            } else {
                # Validate array index is within bounds
                if (ai >= n_groups) {
                    stop("AWS Batch array index ", ai, " out of bounds for ",
                         n_groups, " groups (valid range: 0-", (n_groups - 1), ")")
                }
                group_id <- group_ids[ai + 1L]
            }

            # Load all sites in this group
            group_members <- GROUP_MAPPING[[as.character(group_id)]]
            # group_members is list of [site_id, sub_site_index]
            group_site_ids <- sapply(group_members, function(x) x[[1]])
            group_sub_indices <- sapply(group_members, function(x) x[[2]])

            # Get numeric IDs for all sites in group
            site_ids <- sites %>%
                filter(site_id %in% group_site_ids) %>%
                pull(id_numeric) %>%
                unique()

            message(
                "  AWS Batch array index: ", array_index,
                " -> group ", group_id,
                " (", length(site_ids), " sites",
                if (!is.null(BATCH_REPLICATE)) paste0(", replicate ", BATCH_REPLICATE, "/", N_REPLICATES) else "",
                ")"
            )
        } else {
            # Standard per-site mode
            n_sites <- length(all_site_ids)
            if (N_REPLICATES > 1L) {
                # Composite index: site_index * N_REPLICATES + replicate_index
                site_idx_0 <- ai %/% N_REPLICATES  # 0-based site index
                rep_idx_0 <- ai %% N_REPLICATES    # 0-based replicate index
                BATCH_REPLICATE <- rep_idx_0 + 1L   # 1-based
                # Validate array index is within bounds
                if (site_idx_0 >= n_sites) {
                    stop("AWS Batch array index ", ai, " out of bounds for ",
                         n_sites, " sites × ", N_REPLICATES, " replicates ",
                         "(valid range: 0-", (n_sites * N_REPLICATES - 1), ")")
                }
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
                # Validate array index is within bounds
                if (ai >= n_sites) {
                    stop("AWS Batch array index ", ai, " out of bounds for ",
                         n_sites, " sites (valid range: 0-", (n_sites - 1), ")")
                }
                site_ids <- all_site_ids[ai + 1L]
                batch_site_id <- filter(
                    sites, id_numeric == site_ids
                )$site_id[1]
                message(
                    "  AWS Batch array index: ", array_index,
                    " -> site_id ", batch_site_id
                )
            }
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
    #   sep_vars     : character vector of variable names that caused
    #                  separation (used to build a reduced formula for
    #                  the Mahalanobis fallback)
    #
    # Checks three things:
    # 1. Factor levels that appear only in treatment or only in control
    # 2. Continuous variables with zero overlap between groups
    # 3. Low-cardinality numeric variables (e.g. binary 0/1) where
    #    specific values appear exclusively in one group — this catches
    #    cases like pa=1 appearing only in treatment, which causes
    #    quasi-complete separation even though the ranges overlap at 0.
    formula_vars <- all.vars(f)
    rhs_vars <- setdiff(formula_vars, "treatment")
    problems <- character(0)
    problem_vars <- character(0)

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
                problem_vars <- c(problem_vars, v)
                problems <- c(problems, paste0(
                    v, ": ", length(only_treat),
                    " level(s) only in treatment (",
                    paste(head(only_treat, 5), collapse = ", "),
                    if (length(only_treat) > 5) ", ..." else "",
                    ")"
                ))
            }
            if (length(only_ctrl) > 0) {
                problem_vars <- c(problem_vars, v)
                problems <- c(problems, paste0(
                    v, ": ", length(only_ctrl),
                    " level(s) only in control (",
                    paste(head(only_ctrl, 5), collapse = ", "),
                    if (length(only_ctrl) > 5) ", ..." else "",
                    ")"
                ))
            }
        } else if (is.numeric(col)) {
            # For numeric variables, only check for complete range separation
            # (no overlap between treatment and control ranges)
            t_range <- range(treat_vals, na.rm = TRUE)
            c_range <- range(ctrl_vals, na.rm = TRUE)
            if (t_range[2] < c_range[1] || c_range[2] < t_range[1]) {
                problem_vars <- c(problem_vars, v)
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

    list(
        separated = length(problems) > 0,
        details = problems,
        sep_vars = unique(problem_vars)
    )
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
    #   group_diagnostics   : list of per-group match outcome summaries
    sep_warnings <- list()
    group_diags <- list()
    fn_env <- environment()  # explicit ref for use inside foreach

    m <- foreach(this_group = unique(d$group), .combine = foreach_rbind) %do% {
        this_d <- filter(d, group == this_group)
        n_treatment_grp <- sum(this_d$treatment)
        n_control_grp <- sum(!this_d$treatment)

        # NA removal now happens before sampling (upstream) to preserve
        # sampling weight accuracy. This function assumes clean data.

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
                sw <- get("sep_warnings", envir = fn_env)
                sw[[as.character(this_group)]] <- sep$details
                assign("sep_warnings", sw, envir = fn_env)

                if (SEPARATION_FALLBACK) {
                    message("    Falling back to Mahalanobis distance ",
                            "(separation_fallback_mahalanobis=true)")
                    # Build a reduced formula excluding the variables
                    # that caused separation so they don't dominate the
                    # Mahalanobis distance.  If that would remove ALL
                    # RHS variables, fall back to the full formula.
                    rhs_vars <- setdiff(all.vars(f), "treatment")
                    reduced_vars <- setdiff(rhs_vars, sep$sep_vars)
                    fb_formula <- if (length(reduced_vars) > 0) {
                        as.formula(
                            paste("treatment ~",
                                  paste(reduced_vars, collapse = " + "))
                        )
                    } else {
                        f
                    }
                    dists <- match_on(fb_formula, data = this_d)
                    # Skip caliper for Mahalanobis fallback — the
                    # separated variable is excluded from the distance
                    # but would make a caliper unreliable. This is
                    # consistent with the MatchIt path, which also
                    # drops the caliper for Mahalanobis distance.
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
        # Prefix match_group with exact-match group ID to ensure uniqueness
        # across groups within the site.  Without this prefix, different
        # exact-match groups can produce overlapping match_group values
        # (e.g., "1.1", "1.2") which would be incorrectly combined in step 3
        # when aggregating by (match_group, site_id, year).
        if (is.data.frame(grp_result) && nrow(grp_result) > 0) {
            grp_result$match_group <- paste(
                this_group, grp_result$match_group, sep = "_"
            )
        }
        if (n_matched == 0) {
            message("    Group ", this_group, ": T=", n_treatment_grp,
                    " C=", n_control_grp, " -> 0 matches")
        } else {
            message("    Group ", this_group, ": T=", n_treatment_grp,
                    " C=", n_control_grp, " -> ", n_matched, " matched")
        }

        # Record per-group diagnostics for structured output
        gd <- get("group_diags", envir = fn_env)
        sep_cur <- get("sep_warnings", envir = fn_env)
        gd[[length(gd) + 1L]] <- list(
            group = as.character(this_group),
            n_treatment = n_treatment_grp,
            n_control = n_control_grp,
            n_matched = n_matched,
            separation_detected = as.character(this_group) %in%
                names(sep_cur),
            separation_details = sep_cur[[as.character(this_group)]]
        )
        assign("group_diags", gd, envir = fn_env)

        return(grp_result)
    }

    if (is.null(m) || nrow(m) == 0) {
        return(list(
            result = NULL,
            separation_warnings = sep_warnings,
            group_diagnostics = group_diags
        ))
    }
    return(list(
        result = m,
        separation_warnings = sep_warnings,
        group_diagnostics = group_diags
    ))
}


match_site_matchit <- function(d, f, precomputed_scores = NULL) {
    # Match treatment to control pixels using MatchIt nearest-neighbour
    # matching.  This is a greedy (non-optimal) algorithm that is
    # substantially faster than optmatch.
    #
    # Exact matching on EXACT_MATCH_VARS is handled natively by MatchIt
    # via the `exact` parameter, so no external group loop is needed.
    #
    # Returns a list with:
    #   result              : data.frame of matched rows (or NULL)
    #   separation_warnings : list of per-group separation diagnostics
    sep_warnings <- list()

    # Check separation before fitting
    sep <- check_separation(d, f)
    if (sep$separated) {
        for (d_msg in sep$details) {
            message("    Separation detected: ", d_msg)
        }
        sep_warnings[["global"]] <- sep$details
        if (!SEPARATION_FALLBACK) {
            # Without fallback, MatchIt may still succeed since nearest-
            # neighbour is more forgiving than GLM-based optimal matching.
            # Log the warning but proceed.
            message("    Proceeding with MatchIt despite separation")
        }
    }

    # NA removal now happens before sampling (upstream) to preserve
    # sampling weight accuracy. This function assumes clean data.

    n_treatment <- sum(d$treatment)
    n_control <- sum(!d$treatment)
    if (n_treatment < 1) {
        return(list(
            result = NULL, separation_warnings = sep_warnings,
            group_diagnostics = list()
        ))
    }

    # Build exact-match formula from EXACT_MATCH_VARS that are present
    exact_vars_present <- intersect(EXACT_MATCH_VARS, names(d))
    exact_formula <- if (length(exact_vars_present) > 0) {
        as.formula(paste("~", paste(exact_vars_present, collapse = " + ")))
    } else {
        NULL
    }

    # Determine distance method.
    # When a named numeric vector of pre-computed propensity scores is
    # supplied, skip GLM fitting entirely and pass the scores to MatchIt.
    use_precomputed <- !is.null(precomputed_scores)
    if (use_precomputed) {
        cell_scores <- precomputed_scores[as.character(d$cell)]
        if (anyNA(cell_scores)) {
            message(
                "    WARNING: ", sum(is.na(cell_scores)),
                " cells missing from pre-computed scores;",
                " falling back to per-site GLM"
            )
            use_precomputed <- FALSE
        }
    }
    use_mahalanobis <- (
        (!use_precomputed && n_treatment < MIN_GLM) ||
        (sep$separated && SEPARATION_FALLBACK) ||
        (n_control > 0 && n_treatment / n_control > IMBALANCE_RATIO_THRESHOLD)
    )
    # Separation and imbalance fallbacks must override pre-computed group
    # scores: the group GLM may be unreliable in those edge cases
    # (degenerate scores from T >> C cause the caliper to reject all matches;
    # perfect separation means the group model didn't fit a valid GLM anyway).
    if (use_mahalanobis && use_precomputed) {
        reason <- if (sep$separated && SEPARATION_FALLBACK) {
            "separation detected"
        } else {
            paste0(
                "T/C ratio ", round(n_treatment / n_control, 1),
                " > threshold ", IMBALANCE_RATIO_THRESHOLD
            )
        }
        message(
            "    Discarding pre-computed group scores (", reason,
            "); falling back to Mahalanobis distance"
        )
        use_precomputed <- FALSE
    }
    distance_method <- if (use_precomputed) "precomputed" else
                       if (use_mahalanobis) "mahalanobis" else "glm"

    # When falling back to Mahalanobis due to separation, exclude the
    # separated variables so they don't dominate the distance.
    # When using pre-computed scores, pass the base formula f — MatchIt
    # uses it only to identify the treatment column; no GLM is fitted.
    mi_formula <- f
    if (!use_precomputed && use_mahalanobis &&
        sep$separated && length(sep$sep_vars) > 0) {
        rhs_vars <- setdiff(all.vars(f), "treatment")
        reduced_vars <- setdiff(rhs_vars, sep$sep_vars)
        if (length(reduced_vars) > 0) {
            mi_formula <- as.formula(
                paste("treatment ~", paste(reduced_vars, collapse = " + "))
            )
        }
    }

    # Determine ratio
    ratio_val <- if (MAX_CONTROLS > 0) MAX_CONTROLS else NA

    # Run MatchIt
    mi <- tryCatch({
        matchit(
            mi_formula, data = d,
            method = "nearest",
            distance = if (use_precomputed) cell_scores else distance_method,
            exact = exact_formula,
            ratio = if (!is.na(ratio_val)) ratio_val else 1L,
            caliper = if (CALIPER_WIDTH > 0 &&
                          distance_method != "mahalanobis") {
                CALIPER_WIDTH
            } else {
                NULL
            },
            std.caliper = TRUE,
            replace = FALSE
        )
    }, error = function(e) {
        message("    MatchIt error: ", conditionMessage(e))
        rollbar_report_error(conditionMessage(e))
        NULL
    })

    if (is.null(mi)) {
        return(list(
            result = NULL, separation_warnings = sep_warnings,
            group_diagnostics = list(list(
                group = "__all__",
                n_treatment = n_treatment,
                n_control = n_control,
                n_matched = 0L,
                separation_detected = sep$separated,
                separation_details = if (sep$separated) sep$details
            ))
        ))
    }

    # Extract matched data
    md <- match.data(mi)
    if (nrow(md) == 0) {
        return(list(
            result = NULL, separation_warnings = sep_warnings,
            group_diagnostics = list(list(
                group = "__all__",
                n_treatment = n_treatment,
                n_control = n_control,
                n_matched = 0L,
                separation_detected = sep$separated,
                separation_details = if (sep$separated) sep$details
            ))
        ))
    }

    # Map MatchIt output to the same schema as optmatch output
    md$match_group <- as.character(md$subclass)
    md$match_weight <- md$weights

    # Prefix match_group with exact-match stratum for consistency with optmatch.
    # Both paths create prefixes from interaction(exact_vars), ensuring unique
    # match_group IDs across different strata.
    if (length(exact_vars_present) > 0) {
        exact_group <- interaction(md[exact_vars_present], drop = TRUE)
        md$match_group <- paste(exact_group, md$match_group, sep = "_")
    }

    # Add propensity scores
    if (distance_method %in% c("glm", "precomputed")) {
        md$pscore <- md$distance
    } else {
        md$pscore <- NA_real_
    }

    # Drop MatchIt-specific columns
    md <- md %>% select(-any_of(c("distance", "weights", "subclass")))

    n_matched <- sum(md$treatment)
    message("    MatchIt: T=", n_treatment,
            " C=", n_control,
            " -> ", n_matched, " matched")

    # Build per-group diagnostics from MatchIt exact-match subgroups
    matchit_diags <- list()
    if (length(exact_vars_present) > 0) {
        md_all <- d  # full data before matching
        md_all$group <- interaction(
            md_all[exact_vars_present], drop = TRUE
        )
        md_matched <- md
        md_matched$group <- interaction(
            md_matched[exact_vars_present], drop = TRUE
        )
        for (g in levels(md_all$group)) {
            gd <- md_all[md_all$group == g, ]
            gm <- md_matched[md_matched$group == g, ]
            n_t <- sum(gd$treatment)
            n_c <- sum(!gd$treatment)
            n_m <- if (nrow(gm) > 0) sum(gm$treatment) else 0L
            matchit_diags[[length(matchit_diags) + 1L]] <- list(
                group = as.character(g),
                n_treatment = n_t,
                n_control = n_c,
                n_matched = n_m,
                separation_detected = sep$separated,
                separation_details = if (sep$separated) sep$details
            )
        }
    } else {
        matchit_diags <- list(list(
            group = "__all__",
            n_treatment = n_treatment,
            n_control = n_control,
            n_matched = n_matched,
            separation_detected = sep$separated,
            separation_details = if (sep$separated) sep$details
        ))
    }

    return(list(
        result = md, separation_warnings = sep_warnings,
        group_diagnostics = matchit_diags
    ))
}

n_failed <- 0L
required_match_cols <- c(
    "cell", "site_id", "id_numeric", "area_ha", "treatment",
    "sampled_fraction", "total_biomass_2025", "match_group", "match_weight"
)

# ======================================================================
# Group-level pre-computation (cross-site mode only)
#
# When multiple sites share an exact-match group, a single Arrow scan,
# one distance exclusion, and one propensity model are computed here.
# The per-site loop then works entirely from this in-memory cache,
# eliminating repeated I/O and repeated GLM fitting.
# ======================================================================
group_cache <- NULL
if (GROUP_BY_EXACT_MATCHES && length(site_ids) > 1L) {
    cache_timer <- proc.time()
    message("Pre-computing group cache for ", length(site_ids), " sites ...")

    gc_treatment_cells <- filter(treatment_key, id_numeric %in% site_ids)$cell

    # Single Arrow scan: all group treatment pixels + all eligible controls
    gc_raw <- base_dataset %>%
        filter(cell %in% gc_treatment_cells | !(cell %in% all_treatment_cells)) %>%
        collect() %>%
        mutate(treatment = cell %in% gc_treatment_cells)

    # Remove pixels missing any exact-match grouping variable, then drop
    # strata that lack both treatment and control representation.
    gc_raw <- gc_raw %>%
        filter(if_all(all_of(EXACT_MATCH_VARS), ~ !is.na(.)))
    gc_raw <- filter_groups(gc_raw, EXACT_MATCH_VARS)

    # Spatial distance exclusion — performed once for all sites in the group
    if (MIN_CONTROL_DISTANCE_KM > 0) {
        ctrl_mask <- !gc_raw$treatment
        n_ctrl <- sum(ctrl_mask)
        if (n_ctrl > 0) {
            coords <- cell_to_lonlat(gc_raw$cell[ctrl_mask], grid_meta)
            ctrl_pts <- st_as_sf(
                coords, coords = c("lon", "lat"), crs = 4326
            )
            too_close <- lengths(st_intersects(ctrl_pts, all_sites_buffer)) > 0
            n_excluded <- sum(too_close)
            if (n_excluded > 0) {
                gc_raw <- gc_raw[-which(ctrl_mask)[too_close], ]
                message(
                    "  Group cache: excluded ", n_excluded, " control pixels",
                    " within ", MIN_CONTROL_DISTANCE_KM, " km of treatment"
                )
            }
        }
        # Re-filter groups after distance exclusion
        gc_raw <- filter_groups(gc_raw, EXACT_MATCH_VARS)
    }

    # Fit ONE propensity model on the pooled group data (base formula f).
    # These scores are reused for every site, avoiding N repeated GLM fits.
    gc_complete <- gc_raw[
        complete.cases(gc_raw[, all.vars(f), drop = FALSE]), , drop = FALSE
    ]
    gc_propensity_scores <- NULL
    if (sum(gc_complete$treatment) >= MIN_GLM) {
        gc_sep <- check_separation(gc_complete, f)
        if (!gc_sep$separated) {
            gc_glm <- tryCatch(
                glm(f, data = gc_complete, family = binomial),
                error = function(e) {
                    message(
                        "  Group GLM failed: ", conditionMessage(e),
                        "; each site will fit its own model"
                    )
                    NULL
                }
            )
            if (!is.null(gc_glm)) {
                raw_scores <- predict(gc_glm, newdata = gc_raw, type = "response")
                gc_propensity_scores <- setNames(
                    raw_scores, as.character(gc_raw$cell)
                )
            }
        } else {
            message(
                "  Group GLM skipped (separation detected);",
                " each site will fit its own model"
            )
        }
    } else {
        message(
            "  Group GLM skipped (only ", sum(gc_complete$treatment),
            " treatment pixels < MIN_GLM=", MIN_GLM, ");",
            " each site will fit its own model"
        )
    }

    cache_elapsed <- round((proc.time() - cache_timer)["elapsed"], 1)
    message(
        "Group cache ready: ",
        sum(gc_raw$treatment), " treatment / ",
        sum(!gc_raw$treatment), " control pixels; ",
        if (!is.null(gc_propensity_scores)) {
            "shared propensity model fitted"
        } else {
            "no shared model (per-site GLM will be used)"
        },
        " (", cache_elapsed, "s)"
    )

    group_cache <- list(
        raw               = gc_raw,
        treatment_cells   = gc_treatment_cells,
        propensity_scores = gc_propensity_scores
    )
}

for (this_id in site_ids) {
    site <- filter(sites, id_numeric == this_id)
    this_site_id <- site$site_id[1]
    this_site_name <- if ("site_name" %in% names(site)) site$site_name[1] else NA_character_
    this_batch_index <- match(this_id, all_site_ids) - 1L

    # Determine sub_site_index if in cross-site grouping mode
    this_sub_site_index <- 0L
    if ((BATCH_GROUP_SITES || GROUP_BY_EXACT_MATCHES) && !is.null(GROUP_MAPPING)) {
        # Find this site in the group mapping
        for (gid in names(GROUP_MAPPING)) {
            group_members <- GROUP_MAPPING[[gid]]
            for (member in group_members) {
                if (member[[1]] == this_site_id) {
                    this_sub_site_index <- as.integer(member[[2]])
                    break
                }
            }
            if (this_sub_site_index > 0L) break
        }
    }

    # Which replicates should this node process?
    replicate_range <- if (!is.null(BATCH_REPLICATE)) {
        BATCH_REPLICATE  # single replicate when parallelised
    } else {
        seq_len(N_REPLICATES)
    }

    # Build replicate-specific match file paths.
    # Include sub_site_index when cross-site grouping is enabled.
    # When N_REPLICATES == 1, use the original naming for backward compat.
    base_filename <- if (this_sub_site_index > 0L) {
        paste0("m_", this_id, "_", this_sub_site_index)
    } else {
        paste0("m_", this_id)
    }

    if (N_REPLICATES > 1L) {
        rep_match_paths <- file.path(
            config$matches_dir,
            paste0(base_filename, "_rep", seq_len(N_REPLICATES), ".rds")
        )
        # Paths for only the replicates this node will process
        node_match_paths <- file.path(
            config$matches_dir,
            paste0(base_filename, "_rep", replicate_range, ".rds")
        )
    } else {
        rep_match_paths <- file.path(
            config$matches_dir, paste0(base_filename, ".rds")
        )
        node_match_paths <- rep_match_paths
    }
    # Include replicate in failure marker name when parallelised,
    # so each array child writes a distinct file.
    if (!is.null(BATCH_REPLICATE)) {
        failure_path <- file.path(
            config$matches_dir,
            paste0("failed_", base_filename, "_rep", BATCH_REPLICATE, ".json")
        )
    } else {
        failure_path <- file.path(
            config$matches_dir, paste0("failed_", base_filename, ".json")
        )
    }

    rep_label <- if (!is.null(BATCH_REPLICATE)) {
        paste0("replicate ", BATCH_REPLICATE, "/", N_REPLICATES)
    } else {
        paste0("replicates=", N_REPLICATES)
    }

    site_label <- if (this_sub_site_index > 0L) {
        paste0(this_site_id, " (sub-site ", this_sub_site_index, ")")
    } else {
        this_site_id
    }

    message("  Processing site_id ", site_label,
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
                sub_site_index = this_sub_site_index,
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
            # Load treatment and control pixels for matching.
            site_treatment_cells <- treatment_cells$cell

            if (!is.null(group_cache)) {
                # ---- Cross-site: derive data from the pre-loaded group cache ----
                # All Arrow I/O and spatial distance exclusion were done once
                # before the site loop.  Here we only slice the in-memory cache.
                group_treatment_cells <- group_cache$treatment_cells

                data_timer <- proc.time()
                # Build site-specific dataset: current site's treatment pixels
                # plus all controls.  Exclude other sites' treatment pixels so
                # that filter_groups() only sees groups relevant to THIS site,
                # preventing control pool contamination.
                vals <- group_cache$raw %>%
                    filter(
                        cell %in% site_treatment_cells |
                        !(cell %in% group_treatment_cells)
                    ) %>%
                    mutate(treatment = cell %in% site_treatment_cells)
                vals <- filter_groups(vals, EXACT_MATCH_VARS)

                # Other sites' treatment pixels — added back after sampling for
                # shared propensity modeling (via pre-computed group scores)
                other_sites_treatment <- group_cache$raw %>%
                    filter(
                        cell %in% group_treatment_cells &
                        !(cell %in% site_treatment_cells)
                    ) %>%
                    mutate(treatment = TRUE)

                n_control_pool_site <- sum(!vals$treatment)
                data_elapsed <- round(
                    (proc.time() - data_timer)["elapsed"], 2
                )
                message(
                    "    [TIMING] Group cache derivation: ",
                    data_elapsed, "s",
                    " (T=", sum(vals$treatment),
                    ", C=", n_control_pool_site, ")"
                )
            } else {
                # ---- Standard mode: Arrow scan + distance exclusion ----
                group_treatment_cells <- treatment_cells$cell
                other_sites_treatment <- NULL

                arrow_timer <- proc.time()
                vals <- base_dataset %>%
                    filter(cell %in% group_treatment_cells |
                           !(cell %in% all_treatment_cells)) %>%
                    collect() %>%
                    mutate(treatment = cell %in% site_treatment_cells)
                arrow_elapsed <- (proc.time() - arrow_timer)["elapsed"]
                message(
                    "    [TIMING] Arrow dataset filter & collect: ",
                    round(arrow_elapsed, 2), "s (", nrow(vals), " rows)"
                )

                # Remove pixels with NA in exact-match grouping variables
                n_before <- nrow(vals)
                vals <- vals %>%
                    filter(if_all(all_of(EXACT_MATCH_VARS), ~ !is.na(.)))
                n_dropped <- n_before - nrow(vals)
                if (n_dropped > 0) {
                    message("  Filtered ", n_dropped,
                            " pixels with missing group data")
                }

                # Filter to groups present in both treatment and control.
                vals <- filter_groups(vals, EXACT_MATCH_VARS)

                # Exclude control pixels too close to ANY treatment polygon
                if (MIN_CONTROL_DISTANCE_KM > 0) {
                    control_mask <- !vals$treatment
                    n_ctrl_before <- sum(control_mask)
                    if (n_ctrl_before > 0) {
                        # Coordinate conversion
                        coord_timer <- proc.time()
                        coords <- cell_to_lonlat(
                            vals$cell[control_mask], grid_meta
                        )
                        ctrl_pts <- st_as_sf(
                            coords,
                            coords = c("lon", "lat"), crs = 4326
                        )
                        coord_elapsed <- (proc.time() - coord_timer)["elapsed"]
                        message(
                            "    [TIMING] Coordinate conversion: ",
                            round(coord_elapsed, 2), "s for ", n_ctrl_before,
                            " pixels"
                        )

                        # Spatial intersection - likely bottleneck
                        intersect_timer <- proc.time()
                        too_close <- lengths(
                            st_intersects(ctrl_pts, all_sites_buffer)
                        ) > 0
                        intersect_elapsed <- (proc.time() - intersect_timer)["elapsed"]

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
                        message(
                            "    [TIMING] Spatial intersection: ",
                            round(intersect_elapsed, 2), "s (",
                            round(n_ctrl_before / intersect_elapsed, 0),
                            " pixels/sec)"
                        )
                    }
                    # Re-filter groups after distance exclusion
                    refilter_timer <- proc.time()
                    vals <- filter_groups(vals, EXACT_MATCH_VARS)
                    refilter_elapsed <- (proc.time() - refilter_timer)["elapsed"]
                    message(
                        "    [TIMING] Group re-filtering: ",
                        round(refilter_elapsed, 2), "s"
                    )
                }

                n_control_pool_site <- sum(!vals$treatment)
                data_elapsed <- (proc.time() - site_timer)["elapsed"]
                message(
                    "    [TIMING] Data loading & filtering: ",
                    round(data_elapsed, 1), "s",
                    " (T=", sum(vals$treatment),
                    ", C=", n_control_pool_site, ")"
                )
            }

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

            # Pre-compute defor_pre_intervention for cross-site treatment pixels.
            # Only needed when falling back to per-site GLM (i.e. no shared
            # propensity scores from the group cache).
            if (!is.null(other_sites_treatment) && add_defor_pre &&
                is.null(group_cache)) {
                other_sites_treatment <- other_sites_treatment %>%
                    mutate(
                        defor_pre_intervention = if_else(
                            .data[[fc_init_name]] > 0,
                            (.data[[fc_final_name]] - .data[[fc_init_name]]) /
                                .data[[fc_init_name]] * 100,
                            NA_real_
                        )
                    )
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

                # Deterministic per-site, per-replicate seed using bitwise XOR
                # to avoid collision risks from simple addition
                if (!is.null(RANDOM_SEED)) {
                    if (N_REPLICATES > 1L) {
                        # Use bit shifting to pack site and replicate IDs
                        combined_seed <- bitwXor(
                            RANDOM_SEED,
                            bitwXor(
                                bitwShiftL(as.integer(this_id) %% 65536L, 10L),
                                as.integer(rep_k)
                            )
                        )
                        set.seed(combined_seed)
                    } else {
                        set.seed(bitwXor(RANDOM_SEED, as.integer(this_id)))
                    }
                }

                if (N_REPLICATES > 1L) {
                    message("    Replicate ", rep_k, "/", N_REPLICATES)
                }

                vals <- vals_base

                # ----------------------------------------------------------------
                # Add pre-intervention deforestation
                # ----------------------------------------------------------------
                if (add_defor_pre) {
                    defor_timer <- proc.time()
                    init_fc <- vals[[fc_init_name]]
                    final_fc <- vals[[fc_final_name]]
                    # Set to NA when initial forest cover is 0 (undefined rate of change)
                    # These pixels are excluded in the NA removal step below.
                    vals$defor_pre_intervention <- if_else(
                        init_fc > 0,
                        ((final_fc - init_fc) / init_fc) * 100,
                        NA_real_
                    )
                    defor_elapsed <- (proc.time() - defor_timer)["elapsed"]
                    message(
                        "    [TIMING] Pre-intervention defor calculation: ",
                        round(defor_elapsed, 2), "s"
                    )
                }

                # ----------------------------------------------------------------
                # Drop NA covariates BEFORE sampling to preserve weight accuracy
                # ----------------------------------------------------------------
                # Sampling weights become inaccurate if NAs are dropped after
                # sampling. Remove incomplete cases first, then calculate weights.
                formula_vars <- all.vars(this_f)
                complete <- complete.cases(vals[, formula_vars, drop = FALSE])
                n_dropped_na <- sum(!complete)
                n_before_na_drop <- nrow(vals)
                n_treatment_before_na <- sum(vals$treatment)
                n_control_before_na <- sum(!vals$treatment)

                if (n_dropped_na > 0) {
                    na_counts <- vapply(
                        formula_vars,
                        function(v) sum(is.na(vals[[v]])),
                        integer(1)
                    )
                    na_vars <- na_counts[na_counts > 0]

                    # Calculate exclusion percentages
                    pct_excluded_overall <- (n_dropped_na / n_before_na_drop) * 100
                    n_treatment_dropped <- sum(vals$treatment[!complete])
                    n_control_dropped <- sum(!vals$treatment[!complete])
                    pct_treatment_excluded <- (n_treatment_dropped / n_treatment_before_na) * 100
                    pct_control_excluded <- (n_control_dropped / n_control_before_na) * 100

                    message(
                        "    Dropped ", n_dropped_na,
                        " rows with NA covariates before sampling (",
                        sprintf("%.1f%%", pct_excluded_overall),
                        "). NA counts: ",
                        paste(names(na_vars), na_vars, sep = "=", collapse = ", ")
                    )

                    # Flag substantial exclusions
                    if (pct_excluded_overall > 10 || pct_treatment_excluded > 10) {
                        message(
                            "    WARNING: Substantial NA exclusion detected! ",
                            "Treatment: ", sprintf("%.1f%%", pct_treatment_excluded),
                            ", Control: ", sprintf("%.1f%%", pct_control_excluded),
                            ". Results may not be representative of full population."
                        )
                    }

                    # Write NA exclusion metrics to JSON
                    na_exclusion_metrics <- list(
                        id_numeric = this_id,
                        site_id = this_site_id,
                        site_name = this_site_name,
                        sub_site_index = this_sub_site_index,
                        replicate = rep_k,
                        n_before_exclusion = n_before_na_drop,
                        n_dropped = n_dropped_na,
                        pct_excluded_overall = round(pct_excluded_overall, 2),
                        n_treatment_before = n_treatment_before_na,
                        n_treatment_dropped = n_treatment_dropped,
                        pct_treatment_excluded = round(pct_treatment_excluded, 2),
                        n_control_before = n_control_before_na,
                        n_control_dropped = n_control_dropped,
                        pct_control_excluded = round(pct_control_excluded, 2),
                        variables_with_na = as.list(na_vars),
                        substantial_exclusion = pct_excluded_overall > 10 || pct_treatment_excluded > 10,
                        timestamp = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
                        array_index = this_batch_index
                    )

                    na_exclusion_path <- file.path(
                        config$matches_dir,
                        paste0(base_filename, "_rep", rep_k, "_na_exclusion.json")
                    )
                    write_json(
                        na_exclusion_metrics, na_exclusion_path,
                        auto_unbox = TRUE, pretty = TRUE
                    )

                    vals <- vals[complete, ]
                    # Re-filter groups after NA removal
                    vals <- filter_groups(vals, EXACT_MATCH_VARS)
                } else {
                    # No NAs dropped - still write metrics file for consistency
                    na_exclusion_metrics <- list(
                        id_numeric = this_id,
                        site_id = this_site_id,
                        site_name = this_site_name,
                        sub_site_index = this_sub_site_index,
                        replicate = rep_k,
                        n_before_exclusion = n_before_na_drop,
                        n_dropped = 0,
                        pct_excluded_overall = 0,
                        n_treatment_before = n_treatment_before_na,
                        n_treatment_dropped = 0,
                        pct_treatment_excluded = 0,
                        n_control_before = n_control_before_na,
                        n_control_dropped = 0,
                        pct_control_excluded = 0,
                        variables_with_na = list(),
                        substantial_exclusion = FALSE,
                        timestamp = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
                        array_index = this_batch_index
                    )

                    na_exclusion_path <- file.path(
                        config$matches_dir,
                        paste0(base_filename, "_rep", rep_k, "_na_exclusion.json")
                    )
                    write_json(
                        na_exclusion_metrics, na_exclusion_path,
                        auto_unbox = TRUE, pretty = TRUE
                    )
                }

                # ----------------------------------------------------------------
                # Stratified sampling with per-group population weights
                # ----------------------------------------------------------------
                # Compute per-group population sizes before sampling so we can
                # track the sampling weight for each group.  This enables proper
                # inverse-probability weighting in step 3 when groups have
                # different sampling rates.
                sampling_timer <- proc.time()

                # Treatment population sizes per group
                treatment_pop <- vals %>%
                    filter(treatment) %>%
                    count(group, name = "pop_size")

                # Sample treatment pixels: min(MAX_TREATMENT, n) per group
                treatment_sampled <- filter(vals, treatment) %>%
                    left_join(treatment_pop, by = "group") %>%
                    group_by(group) %>%
                    sample_n(min(MAX_TREATMENT, n())) %>%
                    mutate(
                        sample_size = n(),
                        sampling_weight = pop_size / sample_size
                    ) %>%
                    ungroup() %>%
                    select(-pop_size, -sample_size)

                # Build lookup of sampled treatment counts per group for control sampling
                treatment_sample_counts <- treatment_sampled %>%
                    count(group, name = "n_treatment_sampled")

                # Sample control pixels: CONTROL_MULTIPLIER * sampled treatment per group
                control_sampled <- filter(vals, !treatment) %>%
                    left_join(treatment_sample_counts, by = "group") %>%
                    filter(!is.na(n_treatment_sampled)) %>%
                    group_by(group) %>%
                    sample_n(min(
                        CONTROL_MULTIPLIER * n_treatment_sampled[1],
                        n()
                    )) %>%
                    ungroup() %>%
                    select(-n_treatment_sampled)

                vals <- bind_rows(treatment_sampled, control_sampled)

                sampling_elapsed <- (proc.time() - sampling_timer)["elapsed"]
                message(
                    "    [TIMING] Stratified sampling: ",
                    round(sampling_elapsed, 2), "s"
                )

                # Note: pre-intervention deforestation was calculated earlier
                # (before NA filtering) to ensure it's available for complete.cases()

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
                    # Add cross-site treatment pixels for propensity modeling.
                    # Restrict to groups that survived the full filtering pipeline.
                    if (!is.null(other_sites_treatment) &&
                        nrow(other_sites_treatment) > 0) {
                        valid_groups <- as.character(unique(vals$group))
                        other_filtered <- other_sites_treatment %>%
                            filter(if_all(all_of(EXACT_MATCH_VARS), ~ !is.na(.))) %>%
                            mutate(group = as.character(
                                interaction(across(all_of(EXACT_MATCH_VARS)), drop = TRUE)
                            )) %>%
                            filter(group %in% valid_groups)

                        if (nrow(other_filtered) > 0) {
                            vals <- bind_rows(vals, other_filtered)
                            message(
                                "    Added ", nrow(other_filtered),
                                " cross-site treatment pixels for propensity modeling"
                            )
                        }
                    }

                    # Run matching
                    match_timer <- proc.time()
                    if (MATCHING_METHOD == "nearest") {
                        match_result <- match_site_matchit(
                            vals, this_f,
                            precomputed_scores = if (!is.null(group_cache)) {
                                group_cache$propensity_scores
                            } else {
                                NULL
                            }
                        )
                    } else {
                        match_result <- match_site(vals, this_f)
                    }
                    match_elapsed <- (proc.time() - match_timer)["elapsed"]
                    m <- match_result$result
                    sep_warnings <- match_result$separation_warnings
                    grp_diags <- match_result$group_diagnostics
                    message(
                        "    [TIMING] Matching: ",
                        round(match_elapsed, 1), "s"
                    )

                    # Write per-group matching diagnostics to JSON
                    if (length(grp_diags) > 0) {
                        diag_path <- sub(
                            "\\.rds$", "_diagnostics.json",
                            match_path_k
                        )
                        write_json(
                            grp_diags, diag_path,
                            auto_unbox = TRUE, pretty = TRUE
                        )
                    }

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
                            sub_site_index = this_sub_site_index,
                            replicate = rep_k,
                            error = error_msg,
                            separation_warnings = sep_warnings,
                            group_diagnostics = grp_diags,
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
                        # In cross-site grouping mode, the match result contains
                        # matches for ALL sites in the group.  Filter to keep only
                        # matches for THIS site's treatment pixels.
                        if (GROUP_BY_EXACT_MATCHES && length(site_ids) > 1L) {
                            n_before_filter <- nrow(m)
                            m <- m %>% filter(
                                !treatment | cell %in% site_treatment_cells
                            )
                            n_after_filter <- nrow(m)
                            if (n_before_filter != n_after_filter) {
                                message(
                                    "    Filtered group matches: ",
                                    n_before_filter, " -> ", n_after_filter,
                                    " rows (keeping only this site's pixels)"
                                )
                            }
                        }

                        m$id_numeric <- this_id
                        m$site_id <- site$site_id
                        m$sub_site_index <- this_sub_site_index
                        m$sampled_fraction <-
                            n_treatment_final / n_treatment_total
                        m$n_control_sampled <- n_control_final
                        m$n_control_pool <- n_control_pool_site
                        m$replicate <- rep_k
                        if (length(sep_warnings) > 0) {
                            attr(m, "separation_warnings") <-
                                sep_warnings
                        }
                        if (length(grp_diags) > 0) {
                            attr(m, "group_diagnostics") <-
                                grp_diags
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

if (n_failed > 0L) {
    message("WARNING: ", n_failed, " site(s) failed matching ",
            "(failure markers written)")
}

step2_elapsed <- (proc.time() - step2_timer)["elapsed"]
message("[TIMING] Step 2 total: ", round(step2_elapsed, 1), "s")
message("Step 2 complete.")

}, step_name = "02_perform_matching")
