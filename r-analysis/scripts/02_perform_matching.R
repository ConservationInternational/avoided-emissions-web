# Step 2: Propensity score matching for avoided emissions analysis.
#
# Each site fragment (id_numeric) is matched independently.  Within every
# exact-match stratum, sampled treatment pixels are matched to control pixels
# outside all sites using a per-stratum logistic propensity model, or
# Mahalanobis distance when the stratum is too small or separated.
#
# On AWS Batch each array element processes one batch of match units (the
# exact-match groups in config$exact_match_group_mapping, or sites when no
# mapping is given) for one replicate:
#   batch_index     = floor(array_index / N_REPLICATES)
#   replicate_index = (array_index %% N_REPLICATES) + 1
#
# Output, per site fragment {id} and replicate {k}:
#   - {matches_dir}/m_{id}_rep{k}.rds          matched pixels
#   - {matches_dir}/failed_{id}_rep{k}.json    failure marker
#   - {matches_dir}/m_{id}_na_exclusion.json   missing-data exclusions

library(dplyr, warn.conflicts = FALSE)
library(optmatch)
library(MatchIt)
library(arrow, warn.conflicts = FALSE)
library(jsonlite)

scripts_dir <- Sys.getenv("R_SCRIPTS_DIR", "/app/scripts")
source(file.path(scripts_dir, "utils.R"))
source(file.path(scripts_dir, "stats_functions.R"))
rollbar_init()

with_rollbar({

options("optmatch_max_problem_size" = Inf)
# RELAX-IV (via rrelaxiv) is 2-5x faster than the default LEMON solver.
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
# 0 means full matching (optimal method only); k > 0 means k:1 matching
MAX_CONTROLS <- config$max_controls_per_treatment
RANDOM_SEED <- if (is.null(config$random_seed)) NULL else as.integer(config$random_seed)
N_REPLICATES <- if (is.null(config$n_replicates)) 1L else as.integer(config$n_replicates)
SEPARATION_FALLBACK <- isTRUE(config$separation_fallback_mahalanobis)
MIN_CONTROL_DISTANCE_KM <- if (is.null(config$min_control_distance_km)) {
    10
} else {
    as.numeric(config$min_control_distance_km)
}
EXACT_MATCH_VARS <- config$exact_match_vars
MATCHING_METHOD <- if (is.null(config$matching_method)) "optimal" else config$matching_method
MATCH_BATCH_SIZE <- if (is.null(config$match_batch_size)) 1L else as.integer(config$match_batch_size)
FC_YEARS <- as.integer(config$fc_years)
FC_COLS <- paste0("fc_", FC_YEARS)
GROUP_MAPPING <- fromJSON(config$config_path, simplifyVector = FALSE)$exact_match_group_mapping

if (length(EXACT_MATCH_VARS) == 0) {
    stop("At least one exact-match variable is required")
}
if (MATCHING_METHOD == "nearest" && MAX_CONTROLS == 0) {
    stop("Full matching (max_controls_per_treatment = 0) requires matching_method = 'optimal'")
}
message("Matching method: ", MATCHING_METHOD)

sites <- read_parquet(
    file.path(config$output_dir, "sites_processed.parquet"),
    col_select = c("site_id", "site_name", "id_numeric", "sub_site_index", "start_year")
) %>% as_tibble()

grid_meta <- fromJSON(file.path(config$output_dir, "grid_metadata.json"))

cell_to_lonlat <- function(cells, gm) {
    # Pixel-centre lon/lat from the affine transform [a, b, c, d, e, f]:
    #   x = c + col * a,  y = f + row * e
    tf <- gm$transform
    rows <- cells %/% gm$width
    cols <- cells %% gm$width
    data.frame(
        lon = tf[3] + (cols + 0.5) * tf[1],
        lat = tf[6] + (rows + 0.5) * tf[5]
    )
}

treatment_key <- read_parquet(file.path(config$output_dir, "treatment_cell_key.parquet"))
treatment_table <- read_parquet(file.path(config$output_dir, "treatment_pixels.parquet")) %>%
    as_tibble()
# Control pixels are a hive-partitioned Arrow dataset, streamed from S3 in
# pipeline mode or read from local disk in all-in-one mode.
control_pixels_uri <- if (!is.null(config$control_pixels_s3_uri)) {
    config$control_pixels_s3_uri
} else {
    file.path(config$output_dir, "control_pixels")
}
control_dataset <- open_dataset(control_pixels_uri)

# Buffer around all sites; controls inside it are excluded.
all_sites_buffer <- NULL
if (MIN_CONTROL_DISTANCE_KM > 0) {
    buffer_geojson <- config$sites_exclusion_buffer
    if (is.null(buffer_geojson)) {
        buffer_geojson <- fromJSON(
            file.path(config$output_dir, "prep_summary.json")
        )$sites_exclusion_buffer
    }
    if (is.null(buffer_geojson)) {
        stop("sites_exclusion_buffer is required when min_control_distance_km > 0")
    }
    s2_state <- sf_use_s2()
    sf_use_s2(FALSE)
    all_sites_buffer <- st_make_valid(st_set_crs(
        st_as_sfc(as.character(toJSON(buffer_geojson, auto_unbox = TRUE)), GeoJSON = TRUE),
        4326
    ))
    sf_use_s2(s2_state)
}

formula_json <- fromJSON(file.path(config$output_dir, "formula.json"))
f <- as.formula(formula_json$formula_str)


# -- Work selection ----------------------------------------------------------

select_work <- function() {
    # Returns the site fragments and replicates this process should handle.
    all_reps <- seq_len(N_REPLICATES)
    mapped <- if (is.null(GROUP_MAPPING)) {
        sites
    } else {
        keys <- vapply(unlist(GROUP_MAPPING, recursive = FALSE),
                       function(m) paste(m[[1]], m[[2]], sep = "\r"), character(1))
        filter(sites, paste(site_id, sub_site_index, sep = "\r") %in% keys)
    }
    if (!is.null(config$site_id)) {
        return(list(sites = filter(mapped, site_id == config$site_id), replicates = all_reps))
    }
    array_index <- Sys.getenv("AWS_BATCH_JOB_ARRAY_INDEX", "")
    if (array_index == "") {
        return(list(sites = mapped, replicates = all_reps))
    }
    ai <- as.integer(array_index)
    batch_idx <- ai %/% N_REPLICATES
    replicate <- ai %% N_REPLICATES + 1L
    units <- if (is.null(GROUP_MAPPING)) {
        as.character(sort(sites$id_numeric))
    } else {
        names(GROUP_MAPPING)
    }
    in_batch <- (seq_along(units) - 1L) %/% MATCH_BATCH_SIZE == batch_idx
    message("Array index ", ai, ": batch ", batch_idx + 1L, ", replicate ",
            replicate, ", ", sum(in_batch), " unit(s)")
    if (is.null(GROUP_MAPPING)) {
        selected <- filter(sites, id_numeric %in% as.integer(units[in_batch]))
    } else {
        members <- unlist(GROUP_MAPPING[in_batch], recursive = FALSE)
        keys <- vapply(members, function(m) paste(m[[1]], m[[2]], sep = "\r"), character(1))
        selected <- filter(sites, paste(site_id, sub_site_index, sep = "\r") %in% keys)
    }
    list(sites = selected, replicates = replicate)
}


# -- Data loading ------------------------------------------------------------

load_worker_pixels <- function(site_rows) {
    # Treatment pixels for every site in this batch plus the candidate
    # controls for their exact-match strata, loaded once per worker.  Pixels
    # missing an exact-match value are dropped and the distance exclusion is
    # applied to controls here because it does not depend on the site.
    needed <- unique(c(
        "cell", "area_ha", EXACT_MATCH_VARS, setdiff(all.vars(f), "treatment"),
        "total_biomass_2025", FC_COLS
    ))
    tk <- treatment_key %>%
        filter(id_numeric %in% site_rows$id_numeric) %>%
        select(cell, id_numeric)
    # The extract step encodes missing partition values as -1.
    partition_var <- EXACT_MATCH_VARS[1]
    t_rows <- treatment_table %>%
        filter(cell %in% tk$cell) %>%
        select(all_of(needed)) %>%
        mutate(across(all_of(partition_var), ~ if_else(.x == -1L, NA, .x))) %>%
        inner_join(tk, by = "cell")

    query <- control_dataset %>% select(all_of(needed))
    for (v in EXACT_MATCH_VARS) {
        present <- unique(t_rows[[v]][!is.na(t_rows[[v]])])
        query <- query %>% filter(!!sym(v) %in% !!present)
    }
    timer <- proc.time()
    c_rows <- query %>% collect() %>% as_tibble()
    message("  Loaded ", nrow(t_rows), " treatment and ", nrow(c_rows),
            " candidate control pixels in ",
            round((proc.time() - timer)["elapsed"], 1), "s")

    pixels <- bind_rows(t_rows, c_rows) %>%
        mutate(treatment = !is.na(id_numeric)) %>%
        filter(if_all(all_of(EXACT_MATCH_VARS), ~ !is.na(.x)))

    if (!is.null(all_sites_buffer)) {
        ctrl <- which(!pixels$treatment)
        points <- st_as_sf(
            cell_to_lonlat(pixels$cell[ctrl], grid_meta),
            coords = c("lon", "lat"), crs = 4326
        )
        # The buffer is validated under planar (GEOS) rules, so test with S2
        # off; at 1 km resolution the difference is sub-pixel.
        s2_state <- sf_use_s2()
        sf_use_s2(FALSE)
        too_close <- lengths(st_intersects(points, all_sites_buffer)) > 0
        sf_use_s2(s2_state)
        if (any(too_close)) pixels <- pixels[-ctrl[too_close], ]
        message("  Excluded ", sum(too_close), " control pixels within ",
                MIN_CONTROL_DISTANCE_KM, " km of any site")
    }
    pixels
}


# -- Matching ----------------------------------------------------------------

run_matcher <- function(gd, method, mf, lp) {
    # Match one stratum.  `lp` is the propensity linear predictor (NULL for
    # Mahalanobis).  Both backends apply the same caliper: CALIPER_WIDTH
    # standard deviations of the linear predictor, propensity matching only.
    caliper <- if (method == "glm" && CALIPER_WIDTH > 0) CALIPER_WIDTH * sd(lp) else NULL
    if (MATCHING_METHOD == "nearest") {
        mi <- matchit(
            mf, data = gd, method = "nearest",
            distance = if (method == "glm") lp else "mahalanobis",
            ratio = MAX_CONTROLS, replace = FALSE,
            caliper = caliper, std.caliper = FALSE
        )
        md <- match_data(mi, data = gd)
        if (nrow(md) == 0) return(NULL)
        md$match_group <- as.character(md$subclass)
        md <- md[, setdiff(names(md), c("distance", "weights", "subclass"))]
        return(set_match_weights(md))
    }
    dists <- if (method == "glm") {
        # Passing the caliper here builds a sparse distance matrix.
        match_on(setNames(lp, rownames(gd)), z = gd$treatment, caliper = caliper)
    } else {
        match_on(mf, data = gd)
    }
    m <- if (MAX_CONTROLS > 0) {
        pairmatch(dists, controls = MAX_CONTROLS, data = gd, remove.unmatchables = TRUE)
    } else {
        fullmatch(dists, min.controls = 1, max.controls = Inf, data = gd)
    }
    if (all(is.na(m))) {
        stop("no feasible optimal match within the caliper for the requested ratio")
    }
    gd$match_group <- as.character(m)
    gd <- gd[matched(m), , drop = FALSE]
    if (nrow(gd) == 0) NULL else set_match_weights(gd)
}


match_stratum <- function(gd, f) {
    # Choose the distance for one exact-match stratum and match it.
    rownames(gd) <- NULL
    method <- "glm"
    mf <- f
    lp <- NULL
    details <- NULL
    if (sum(gd$treatment) < MIN_GLM) {
        method <- "mahalanobis"
    } else {
        sep <- check_separation(gd, f)
        if (!sep$separated) {
            fit <- fit_propensity(f, gd)
            if (fit$separated) {
                sep <- list(
                    separated = TRUE, sep_vars = character(0),
                    details = "GLM did not converge or fitted probabilities of 0 or 1"
                )
            } else {
                lp <- predict(fit$model, type = "link")
            }
        }
        if (sep$separated) {
            details <- sep$details
            if (!SEPARATION_FALLBACK) {
                return(list(result = NULL, diag = list(method = "none", separation_details = details)))
            }
            method <- "mahalanobis"
            mf <- drop_formula_vars(f, sep$sep_vars)
        }
    }
    gd$pscore <- if (is.null(lp)) NA_real_ else plogis(lp)
    error <- NULL
    result <- tryCatch(
        suppressWarnings(run_matcher(gd, method, mf, lp)),
        error = function(e) {
            error <<- conditionMessage(e)
            message("    Matching failed: ", error)
            NULL
        }
    )
    list(result = result, diag = list(method = method, separation_details = details, error = error))
}


match_strata <- function(d, f) {
    d <- as.data.frame(d)
    d$group <- as.character(d$group)
    results <- list()
    diags <- list()
    for (g in unique(d$group)) {
        gd <- d[d$group == g, , drop = FALSE]
        out <- match_stratum(gd, f)
        n_matched <- if (is.null(out$result)) 0L else sum(out$result$treatment)
        message("    Stratum ", g, ": T=", sum(gd$treatment), " C=", sum(!gd$treatment),
                " -> ", n_matched, " matched (", out$diag$method, ")")
        diags[[length(diags) + 1]] <- c(
            list(group = g, n_treatment = sum(gd$treatment),
                 n_control = sum(!gd$treatment), n_matched = n_matched),
            out$diag
        )
        if (!is.null(out$result)) {
            # Prefix with the stratum so set IDs are unique across strata.
            out$result$match_group <- paste(g, out$result$match_group, sep = "_")
            results[[length(results) + 1]] <- out$result
        }
    }
    list(result = if (length(results) > 0) bind_rows(results) else NULL, diagnostics = diags)
}


# -- Per-site processing -----------------------------------------------------

match_file <- function(id, k) paste0("m_", id, "_rep", k, ".rds")

write_failure <- function(site, k, error, reason = "error", diagnostics = NULL) {
    write_json(
        list(
            id_numeric = site$id_numeric, site_id = site$site_id,
            site_name = site$site_name, replicate = k, error = error,
            reason = reason, match_file = match_file(site$id_numeric, k),
            group_diagnostics = diagnostics
        ),
        file.path(config$matches_dir, paste0("failed_", site$id_numeric, "_rep", k, ".json")),
        auto_unbox = TRUE, pretty = TRUE, null = "null"
    )
}

write_na_exclusion <- function(site, vals, complete, required) {
    na_counts <- vapply(required, function(v) sum(is.na(vals[[v]])), integer(1))
    pct <- function(x, n) if (n > 0) round(x / n * 100, 2) else 0
    n_t <- sum(vals$treatment)
    n_c <- sum(!vals$treatment)
    dropped_t <- sum(vals$treatment & !complete)
    dropped_c <- sum(!vals$treatment & !complete)
    write_json(
        list(
            id_numeric = site$id_numeric, site_id = site$site_id,
            site_name = site$site_name, sub_site_index = site$sub_site_index,
            n_before_exclusion = nrow(vals), n_dropped = sum(!complete),
            pct_excluded_overall = pct(sum(!complete), nrow(vals)),
            n_treatment_before = n_t, n_treatment_dropped = dropped_t,
            pct_treatment_excluded = pct(dropped_t, n_t),
            n_control_before = n_c, n_control_dropped = dropped_c,
            pct_control_excluded = pct(dropped_c, n_c),
            variables_with_na = as.list(na_counts[na_counts > 0]),
            substantial_exclusion = pct(sum(!complete), nrow(vals)) > 10 ||
                pct(dropped_t, n_t) > 10
        ),
        file.path(config$matches_dir, paste0("m_", site$id_numeric, "_na_exclusion.json")),
        auto_unbox = TRUE, pretty = TRUE
    )
}

replicate_seed <- function(id, k) {
    bitwXor(RANDOM_SEED, bitwXor(bitwShiftL(as.integer(id) %% 65536L, 10L), as.integer(k)))
}

process_site <- function(site, pixels, replicates) {
    this_id <- site$id_numeric
    todo <- replicates[!file.exists(file.path(config$matches_dir, match_file(this_id, replicates)))]
    if (length(todo) == 0) return(invisible())
    message("  Site ", site$site_id, " (fragment ", site$sub_site_index,
            ", id ", this_id, "), replicate(s) ", paste(todo, collapse = ","))

    n_site_pixels <- sum(treatment_key$id_numeric == this_id)
    vals <- filter(pixels, !treatment | id_numeric == this_id)

    this_f <- f
    defor <- defor_pre_intervention(vals, site$start_year, FC_YEARS)
    if (!is.null(defor)) {
        vals$defor_pre_intervention <- defor
        this_f <- update(f, ~ . + defor_pre_intervention)
    }

    # Eligible pixels: complete covariates and outcome inputs, and forest at
    # the site's baseline year (pixels without forest cannot lose any).
    required <- unique(c(setdiff(all.vars(this_f), "treatment"), "total_biomass_2025", FC_COLS))
    complete <- complete.cases(vals[required])
    write_na_exclusion(site, vals, complete, required)
    baseline <- paste0("fc_", baseline_year(site$start_year, min(FC_YEARS)))
    vals <- vals[complete & vals[[baseline]] > 0, ]
    vals <- filter_groups(vals, EXACT_MATCH_VARS)
    if (sum(vals$treatment) == 0 || sum(!vals$treatment) == 0) {
        msg <- paste0("No overlapping exact-match strata between eligible treatment and ",
                      "control pixels (T=", sum(vals$treatment), ", C=", sum(!vals$treatment), ")")
        for (k in todo) write_failure(site, k, msg, reason = "no_overlap")
        return(invisible())
    }
    eligible <- vals %>%
        filter(treatment) %>%
        count(group, name = "n_eligible") %>%
        mutate(group = as.character(group))
    keep_cols <- unique(c(
        "cell", "treatment", "group", "match_group", "match_weight", "pscore",
        "area_ha", "total_biomass_2025", FC_COLS, setdiff(all.vars(this_f), "treatment")
    ))

    for (k in todo) {
        if (!is.null(RANDOM_SEED)) set.seed(replicate_seed(this_id, k))
        sampled <- sample_strata(vals, MAX_TREATMENT, CONTROL_MULTIPLIER)
        res <- match_strata(sampled, this_f)
        if (is.null(res$result)) {
            write_failure(site, k, paste0(
                "No matches found (treatment=", sum(sampled$treatment),
                ", control=", sum(!sampled$treatment), ")"
            ), reason = "no_matches", diagnostics = res$diagnostics)
            next
        }
        # Stratum weights scale matched treatment pixels (and their controls)
        # to all eligible treatment pixels in the stratum.
        weights <- res$result %>%
            filter(treatment) %>%
            count(group, name = "n_matched") %>%
            inner_join(eligible, by = "group") %>%
            mutate(sampling_weight = n_eligible / n_matched)
        m <- res$result[, intersect(keep_cols, names(res$result))] %>%
            as_tibble() %>%
            inner_join(select(weights, group, sampling_weight), by = "group") %>%
            mutate(
                id_numeric = this_id, site_id = site$site_id, replicate = k,
                n_site_pixels = n_site_pixels,
                n_eligible = sum(eligible$n_eligible),
                n_sampled = sum(sampled$treatment),
                n_matched = sum(treatment),
                n_control_pool = sum(!vals$treatment),
                n_control_sampled = sum(!sampled$treatment)
            )
        attr(m, "group_diagnostics") <- res$diagnostics
        saveRDS(m, file.path(config$matches_dir, match_file(this_id, k)))
        message("    Replicate ", k, ": ", sum(m$treatment), " of ",
                sum(sampled$treatment), " sampled treatment pixels matched")
    }
}


# -- Main --------------------------------------------------------------------

work <- select_work()
message("Processing ", nrow(work$sites), " site fragment(s)")
if (nrow(work$sites) > 0) {
    pixels <- load_worker_pixels(work$sites)
    for (i in seq_len(nrow(work$sites))) {
        site <- work$sites[i, ]
        tryCatch(
            process_site(site, pixels, work$replicates),
            error = function(e) {
                msg <- conditionMessage(e)
                message("  ERROR for site ", site$site_id, ": ", msg)
                rollbar_report_error(msg)
                done <- file.exists(file.path(
                    config$matches_dir, match_file(site$id_numeric, work$replicates)
                ))
                for (k in work$replicates[!done]) write_failure(site, k, msg)
            }
        )
        gc()
    }
}

message("[TIMING] Step 2 total: ", round((proc.time() - step2_timer)["elapsed"], 1), "s")
message("Step 2 complete.")

}, step_name = "02_perform_matching")
