# Step 2: Propensity score matching for avoided emissions analysis.
#
# Each site fragment (id_numeric) is matched independently.  Within every
# exact-match stratum, sampled treatment pixels are matched to control pixels
# outside all sites using a per-stratum logistic propensity model, or
# Mahalanobis distance when the stratum is too small or separated.
#
# On AWS Batch each array element processes one match unit (an entry of
# config$exact_match_group_mapping, which the webapp packs to at most
# match_batch_size site fragments, or one site when no mapping is given) for
# one block of up to REPLICATE_BLOCK_SIZE replicates:
#   n_blocks    = ceiling(N_REPLICATES / REPLICATE_BLOCK_SIZE)
#   unit_index  = floor(array_index / n_blocks)
#   block_index = array_index %% n_blocks
#
# Output, one pair of files per batch (array element):
#   - {matches_dir}/mb_{batch}.parquet  matched pixels of every fragment and
#                                       replicate (id_numeric, replicate columns)
#   - {matches_dir}/mb_{batch}.json     match diagnostics, failure markers and
#                                       missing-data exclusions; written last,
#                                       so its presence marks a complete batch

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
REPLICATE_BLOCK_SIZE <- if (is.null(config$replicate_block_size)) {
    1L
} else {
    min(as.integer(config$replicate_block_size), N_REPLICATES)
}
N_BLOCKS <- as.integer(ceiling(N_REPLICATES / REPLICATE_BLOCK_SIZE))
SEPARATION_FALLBACK <- isTRUE(config$separation_fallback_mahalanobis)
EXACT_MATCH_VARS <- config$exact_match_vars
MATCHING_METHOD <- if (is.null(config$matching_method)) "optimal" else config$matching_method
FC_YEARS <- as.integer(config$fc_years)
FC_COLS <- unique(c("fc_2000", paste0("fc_", FC_YEARS)))
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

treatment_key_dataset <- open_dataset(file.path(config$output_dir, "treatment_cell_key.parquet"))
treatment_dataset <- open_dataset(file.path(config$output_dir, "treatment_pixels.parquet"))
# Control pixels are a hive-partitioned Arrow dataset, streamed from S3 in
# pipeline mode or read from local disk in all-in-one mode.
control_pixels_uri <- if (!is.null(config$control_pixels_s3_uri)) {
    config$control_pixels_s3_uri
} else {
    file.path(config$output_dir, "control_pixels")
}
control_dataset <- open_dataset(control_pixels_uri)

formula_json <- fromJSON(file.path(config$output_dir, "formula.json"))
f <- as.formula(formula_json$formula_str)


# -- Work selection ----------------------------------------------------------

select_work <- function() {
    # Returns the site fragments and replicates this process should handle,
    # and a batch name unique within the task for its output files.
    all_reps <- seq_len(N_REPLICATES)
    mapped <- if (is.null(GROUP_MAPPING)) {
        sites
    } else {
        keys <- vapply(unlist(GROUP_MAPPING, recursive = FALSE),
                       function(m) paste(m[[1]], m[[2]], sep = "\r"), character(1))
        filter(sites, paste(site_id, sub_site_index, sep = "\r") %in% keys)
    }
    if (!is.null(config$site_id)) {
        return(list(sites = filter(mapped, site_id == config$site_id), replicates = all_reps,
                    batch = paste0("site_", gsub("[^A-Za-z0-9_-]", "_", config$site_id))))
    }
    array_index <- Sys.getenv("AWS_BATCH_JOB_ARRAY_INDEX", "")
    if (array_index == "") {
        return(list(sites = mapped, replicates = all_reps, batch = "all"))
    }
    ai <- as.integer(array_index)
    unit <- ai %/% N_BLOCKS + 1L
    block <- ai %% N_BLOCKS
    replicates <- seq(block * REPLICATE_BLOCK_SIZE + 1L,
                      min((block + 1L) * REPLICATE_BLOCK_SIZE, N_REPLICATES))
    message("Array index ", ai, ": unit ", unit, ", replicate(s) ",
            min(replicates), "-", max(replicates))
    if (is.null(GROUP_MAPPING)) {
        selected <- filter(sites, id_numeric == sort(sites$id_numeric)[unit])
        unit_name <- paste0("s", selected$id_numeric[1])
    } else if (unit <= length(GROUP_MAPPING)) {
        keys <- vapply(GROUP_MAPPING[[unit]], function(m) paste(m[[1]], m[[2]], sep = "\r"), character(1))
        selected <- filter(sites, paste(site_id, sub_site_index, sep = "\r") %in% keys)
        # Unit names are unique across match chunks; positions are not.
        unit_name <- if (is.null(names(GROUP_MAPPING))) unit else names(GROUP_MAPPING)[unit]
    } else {
        selected <- sites[0, ]
        unit_name <- paste0("none", unit)
    }
    list(sites = selected, replicates = replicates,
         batch = paste0("u", unit_name, "_b", block + 1L))
}


# -- Data loading ------------------------------------------------------------

load_worker_pixels <- function(site_rows) {
    # Treatment pixels for every site in this unit plus the candidate controls
    # for their exact-match strata, loaded once per worker.  Pixels missing an
    # exact-match value are dropped.  Controls near any site were already
    # excluded by the extract step.
    needed <- unique(c(
        "cell", "area_ha", EXACT_MATCH_VARS, setdiff(all.vars(f), "treatment"),
        "total_biomass_2000", FC_COLS
    ))
    ids <- site_rows$id_numeric
    tk <- treatment_key_dataset %>%
        filter(id_numeric %in% ids) %>%
        select(cell, id_numeric) %>%
        collect()
    cells <- unique(tk$cell)
    # The extract step encodes missing partition values as -1.
    partition_var <- EXACT_MATCH_VARS[1]
    t_rows <- treatment_dataset %>%
        filter(cell %in% cells) %>%
        select(all_of(needed)) %>%
        collect() %>%
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
    pixels$group <- interaction(pixels[EXACT_MATCH_VARS], drop = TRUE)
    list(pixels = pixels, site_pixels = count(tk, id_numeric, name = "n"))
}


# -- Matching ----------------------------------------------------------------

run_matcher <- function(gd, method, mf, lp) {
    # Match one stratum on the distance `lp` (a propensity linear predictor),
    # or on Mahalanobis distance when `lp` is NULL.  Both backends apply the
    # same caliper: CALIPER_WIDTH standard deviations of the linear predictor,
    # for propensity matching only.
    caliper <- if (method == "glm" && CALIPER_WIDTH > 0) CALIPER_WIDTH * sd(lp) else NULL
    if (MATCHING_METHOD == "nearest") {
        mi <- matchit(
            mf, data = gd, method = "nearest",
            distance = if (is.null(lp)) "mahalanobis" else lp,
            ratio = MAX_CONTROLS, replace = FALSE,
            caliper = caliper, std.caliper = FALSE
        )
        md <- match_data(mi, data = gd)
        if (nrow(md) == 0) return(NULL)
        md$match_group <- as.character(md$subclass)
        md <- md[, setdiff(names(md), c("distance", "weights", "subclass"))]
        return(set_match_weights(md))
    }
    dists <- if (is.null(lp)) {
        match_on(mf, data = gd)
    } else {
        # Passing the caliper here builds a sparse distance matrix.
        match_on(setNames(lp, rownames(gd)), z = gd$treatment, caliper = caliper)
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
    mf <- drop_constant_terms(f, gd)
    method <- "glm"
    lp <- NULL
    details <- NULL
    if (length(attr(terms(mf), "term.labels")) == 0) {
        # No covariate varies: pixels are alike on observables, so every
        # pairing is equally good.
        method <- "constant"
        lp <- rep(0, nrow(gd))
    } else if (sum(gd$treatment) < MIN_GLM) {
        method <- "mahalanobis"
    } else {
        sep <- check_separation(gd, mf)
        if (!sep$separated) {
            fit <- fit_propensity(mf, gd)
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
            mf <- drop_formula_vars(mf, sep$sep_vars)
        }
    }
    gd$pscore <- if (method == "glm") plogis(lp) else NA_real_
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

# Results of this batch, written together at the end.
batch_out <- new.env()
batch_out$matches <- list()
batch_out$failures <- list()
batch_out$diagnostics <- list()
batch_out$na_exclusions <- list()
append_to <- function(name, x) batch_out[[name]][[length(batch_out[[name]]) + 1L]] <- x

record_failure <- function(site, k, error, reason = "error", diagnostics = NULL,
                           n_forest_pixels = NA_integer_) {
    append_to("failures", list(
        id_numeric = site$id_numeric, site_id = site$site_id,
        site_name = site$site_name, replicate = k, error = error,
        reason = reason, n_forest_pixels = n_forest_pixels,
        group_diagnostics = diagnostics
    ))
}

record_na_exclusion <- function(site, vals, complete, required) {
    na_counts <- vapply(required, function(v) sum(is.na(vals[[v]])), integer(1))
    pct <- function(x, n) if (n > 0) round(x / n * 100, 2) else 0
    n_t <- sum(vals$treatment)
    n_c <- sum(!vals$treatment)
    dropped_t <- sum(vals$treatment & !complete)
    dropped_c <- sum(!vals$treatment & !complete)
    append_to("na_exclusions", list(
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
    ))
}

write_batch <- function(batch) {
    # The Parquet file is written before the JSON sidecar, each via a
    # temporary file, so a complete sidecar implies complete matches.
    stem <- file.path(config$matches_dir, paste0("mb_", batch))
    matches_file <- NULL
    if (length(batch_out$matches) > 0) {
        matches_file <- paste0(basename(stem), ".parquet")
        tmp <- paste0(stem, ".parquet.tmp")
        write_parquet(bind_rows(batch_out$matches), tmp)
        if (!file.rename(tmp, paste0(stem, ".parquet"))) stop("Could not write ", stem, ".parquet")
    }
    tmp <- paste0(stem, ".json.tmp")
    write_json(
        list(batch = batch, matches_file = matches_file, failures = batch_out$failures,
             diagnostics = batch_out$diagnostics, na_exclusions = batch_out$na_exclusions),
        tmp, auto_unbox = TRUE, null = "null", na = "null", digits = NA
    )
    if (!file.rename(tmp, paste0(stem, ".json"))) stop("Could not write ", stem, ".json")
}

replicate_seed <- function(id, k) {
    bitwXor(RANDOM_SEED, bitwXor(bitwShiftL(as.integer(id) %% 65536L, 10L), as.integer(k)))
}

process_site <- function(site, pixels, n_site_pixels, replicates) {
    this_id <- site$id_numeric
    message("  Site ", site$site_id, " (fragment ", site$sub_site_index,
            ", id ", this_id, "), replicate(s) ", paste(range(replicates), collapse = "-"))

    # This site's treatment pixels and the controls in its strata only.
    is_site <- pixels$treatment & pixels$id_numeric %in% this_id
    vals <- pixels[is_site | (!pixels$treatment & pixels$group %in% pixels$group[is_site]), ]

    this_f <- f
    defor <- defor_pre_intervention(vals, site$start_year, FC_YEARS)
    if (!is.null(defor)) {
        vals$defor_pre_intervention <- defor
        this_f <- update(f, ~ . + defor_pre_intervention)
    }

    # Eligible pixels: complete covariates and outcome inputs, and forest at
    # the site's baseline year (pixels without forest cannot lose any).
    required <- unique(c(setdiff(all.vars(this_f), "treatment"), "total_biomass_2000", FC_COLS))
    complete <- complete.cases(vals[required])
    record_na_exclusion(site, vals, complete, required)
    baseline <- paste0("fc_", baseline_year(site$start_year, min(FC_YEARS)))
    forest <- !is.na(vals[[baseline]]) & vals[[baseline]] > 0
    n_forest_pixels <- sum(vals$treatment & forest)
    if (n_forest_pixels == 0) {
        for (k in replicates) {
            record_failure(site, k, paste0("No forest at the ", baseline, " baseline"),
                           reason = "no_eligible_treatment", n_forest_pixels = 0L)
        }
        return(invisible())
    }
    vals <- vals[complete & forest, ]
    vals <- filter_groups(vals, EXACT_MATCH_VARS)
    if (sum(vals$treatment) == 0 || sum(!vals$treatment) == 0) {
        msg <- paste0("No overlapping exact-match strata between eligible treatment and ",
                      "control pixels (T=", sum(vals$treatment), ", C=", sum(!vals$treatment), ")")
        for (k in replicates) {
            record_failure(site, k, msg, reason = "no_overlap", n_forest_pixels = n_forest_pixels)
        }
        return(invisible())
    }
    eligible <- vals %>%
        filter(treatment) %>%
        count(group, name = "n_eligible") %>%
        mutate(group = as.character(group))
    keep_cols <- unique(c(
        "cell", "treatment", "group", "match_group", "match_weight", "pscore",
        "area_ha", "total_biomass_2000", FC_COLS, setdiff(all.vars(this_f), "treatment")
    ))

    for (k in replicates) {
        if (!is.null(RANDOM_SEED)) set.seed(replicate_seed(this_id, k))
        sampled <- sample_strata(vals, MAX_TREATMENT, CONTROL_MULTIPLIER)
        res <- match_strata(sampled, this_f)
        if (is.null(res$result)) {
            record_failure(site, k, paste0(
                "No matches found (treatment=", sum(sampled$treatment),
                ", control=", sum(!sampled$treatment), ")"
            ), reason = "no_matches", diagnostics = res$diagnostics,
            n_forest_pixels = n_forest_pixels)
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
                n_forest_pixels = n_forest_pixels,
                n_eligible = sum(eligible$n_eligible),
                n_sampled = sum(sampled$treatment),
                n_matched = sum(treatment),
                n_control_pool = sum(!vals$treatment),
                n_control_sampled = sum(!sampled$treatment)
            )
        append_to("matches", m)
        append_to("diagnostics", list(id_numeric = this_id, replicate = k,
                                      group_diagnostics = res$diagnostics))
        message("    Replicate ", k, ": ", sum(m$treatment), " of ",
                sum(sampled$treatment), " sampled treatment pixels matched")
    }
}


# -- Main --------------------------------------------------------------------

work <- select_work()
message("Processing ", nrow(work$sites), " site fragment(s) as batch ", work$batch)
if (file.exists(file.path(config$matches_dir, paste0("mb_", work$batch, ".json")))) {
    message("Batch ", work$batch, " is already complete; skipping")
} else {
    if (nrow(work$sites) > 0) {
        loaded <- load_worker_pixels(work$sites)
        for (i in seq_len(nrow(work$sites))) {
            site <- work$sites[i, ]
            n_site_pixels <- sum(loaded$site_pixels$n[loaded$site_pixels$id_numeric == site$id_numeric])
            n_before <- length(batch_out$matches)
            n_diag_before <- length(batch_out$diagnostics)
            tryCatch(
                process_site(site, loaded$pixels, n_site_pixels, work$replicates),
                error = function(e) {
                    msg <- conditionMessage(e)
                    message("  ERROR for site ", site$site_id, ": ", msg)
                    rollbar_report_error(msg)
                    # Drop partial results so the fragment fails as a whole.
                    batch_out$matches <- batch_out$matches[seq_len(n_before)]
                    batch_out$diagnostics <- batch_out$diagnostics[seq_len(n_diag_before)]
                    for (k in work$replicates) record_failure(site, k, msg)
                }
            )
            gc()
        }
    }
    write_batch(work$batch)
}

message("[TIMING] Step 2 total: ", round((proc.time() - step2_timer)["elapsed"], 1), "s")
message("Step 2 complete.")

}, step_name = "02_perform_matching")
