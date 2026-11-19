# Step 2: Propensity score matching for avoided emissions analysis.
#
# Each site fragment (id_numeric) is matched independently.  Within every
# exact-match stratum, sampled treatment pixels are matched to control pixels
# outside all sites using a per-stratum logistic propensity model, or
# Mahalanobis distance for small strata (and explicit separation fallback).
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
#   - {matches_dir}/mb_{batch}.json     match diagnostics, failure markers,
#                                       missing-data exclusions and the census
#                                       of observed treatment outcomes by cell;
#                                       written last, so its presence marks a
#                                       complete batch

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
CHECKPOINT_MODE <- isTRUE(config$match_checkpoints)
MATCHING_POLICY <- if (isTRUE(config$shared_support_controls)) "shared_support_v2" else "legacy"
if ((!is.null(config$matching_policy) && !identical(config$matching_policy, MATCHING_POLICY)) ||
    (!is.null(config$diagnostics_schema_version) &&
        config$diagnostics_schema_version != MATCH_DIAGNOSTICS_SCHEMA_VERSION)) {
    stop("Incompatible configured matching policy or diagnostic schema")
}
soft_deadline <- if (is.null(config$match_soft_seconds)) Inf else config$match_soft_seconds
yield_requested <- FALSE
# Solve times for one stratum vary by roughly +/-40% between replicates, so
# projections reserve this multiple of the slowest observed duration.
DEADLINE_MARGIN <- 1.5
progress <- new.env()
progress$last_item_seconds <- 0
progress$items_done <- 0L
# Slowest observed seconds per fragment/replicate, keyed by id_numeric. Seeded
# from earlier tiers so a recovery task can guard its first replicate.
progress$item_seconds <- unlist(config$match_work$item_seconds)
if (is.null(progress$item_seconds)) progress$item_seconds <- numeric(0)
progress$stratum_seconds <- numeric(0)

checkpoint_publish <- function(paths) {
    if (is.null(config$checkpoint_uri)) return(invisible(NULL))
    worker <- file.path(scripts_dir, "../nextflow_worker.py")
    if (!file.exists(worker)) worker <- file.path(scripts_dir, "../src/nextflow_worker.py")
    status <- system2(
        Sys.getenv("AE_PYTHON", "python"),
        c(shQuote(worker), "commit",
          "--config", shQuote(config$config_path), "--files", shQuote(paths))
    )
    if (status != 0) stop("Could not persist match checkpoint")
}

checkpoint_path <- function(name) file.path(config$matches_dir, name)
item_name <- function(id, k) paste0("f", id, "_r", k)
validated_items <- new.env(parent = emptyenv())
item_complete <- function(id, k) {
    item <- item_name(id, k)
    path <- checkpoint_path(paste0("mb_", item, ".json"))
    if (!file.exists(path)) return(FALSE)
    if (!exists(item, envir = validated_items, inherits = FALSE)) {
        validate_sidecar(path, id, k)
        assign(item, TRUE, envir = validated_items)
    }
    TRUE
}
step2_elapsed <- function() unname((proc.time() - step2_timer)["elapsed"])
should_yield <- function() step2_elapsed() >= soft_deadline
# The first item of a task always runs: deferring it cannot make progress.
would_overrun <- function(seconds) {
    progress$items_done > 0 && step2_elapsed() + seconds > soft_deadline
}
fragment_estimate <- function(id) unname(progress$item_seconds[as.character(id)])
record_item_seconds <- function(id, timer) {
    seconds <- unname((proc.time() - timer)["elapsed"])
    key <- as.character(id)
    previous <- progress$item_seconds[key]
    progress$item_seconds[key] <- if (is.na(previous)) seconds else max(previous, seconds)
    progress$last_item_seconds <- seconds
    progress$items_done <- progress$items_done + 1L
}

if (length(EXACT_MATCH_VARS) == 0) {
    stop("At least one exact-match variable is required")
}
if (MATCHING_METHOD == "nearest" && MAX_CONTROLS == 0) {
    stop("Full matching (max_controls_per_treatment = 0) requires matching_method = 'optimal'")
}
message("Matching method: ", MATCHING_METHOD)

sites_path <- file.path(config$output_dir, "sites_processed.parquet")
sites <- read_parquet(
    sites_path, col_select = tidyselect::any_of(c(
        "site_id", "site_name", "id_numeric", "sub_site_index", "start_year", "ae_atoms"
    ))
) %>% as_tibble()

treatment_key_dataset <- open_dataset(file.path(config$output_dir, "treatment_cell_key.parquet"))
atom_enabled <- "ae_atoms" %in% names(sites) && any(!is.na(sites$ae_atoms))
key_has_atoms <- "atomic_id" %in% treatment_key_dataset$schema$names
if (atom_enabled != key_has_atoms) stop("Atom declarations and treatment key schema disagree")
atom_declarations <- list()
atom_run_id <- NULL
atom_count_path <- file.path(config$output_dir, "ae_atom_counts.parquet")
atom_count_md5 <- NULL
atom_extraction_counts <- NULL
atoms_with_keys <- character(0)
if (atom_enabled) {
    if (anyNA(sites$ae_atoms)) stop("Missing ae_atoms declarations")
    atom_declarations <- lapply(unique(sites$ae_atoms), function(x) {
        parse_json(x, simplifyVector = FALSE, bigint_as_char = TRUE)
    })
    names(atom_declarations) <- unique(sites$ae_atoms)
    runs <- unique(vapply(atom_declarations, function(x) x$run_id, character(1)))
    if (length(runs) != 1L) stop("Inconsistent ae_atoms run IDs")
    atom_run_id <- runs[[1]]
    if (file.exists(atom_count_path)) {
        atom_count_md5 <- unname(tools::md5sum(atom_count_path))
        atom_extraction_counts <- read_parquet(atom_count_path) %>%
            mutate(atomic_id = as.character(atomic_id))
        atoms_with_keys <- atom_extraction_counts$atomic_id[atom_extraction_counts$n_site_pixels > 0]
    } else {
        atoms_with_keys <- treatment_key_dataset %>%
            distinct(atomic_id) %>% collect() %>% pull(atomic_id) %>% as.character()
    }
}
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
format_formula <- function(f) paste(deparse(f, width.cutoff = 500L), collapse = "\n")
task_id <- if (is.null(config$task_id)) "" else as.character(config$task_id)
code_files <- c("02_perform_matching.R", "stats_functions.R", "utils.R")
code_md5 <- as.list(setNames(unname(tools::md5sum(file.path(scripts_dir, code_files))), code_files))
atom_schema_version <- if (atom_enabled) ATOM_RESULTS_SCHEMA_VERSION else NULL
# Atom inputs are sampled by atom cell; other inputs keep one cell per stratum.
MIN_CELL <- if (atom_enabled) MIN_CELL_SAMPLE else 0L
CELL_COLS <- if (atom_enabled) c("group", "atomic_id") else "group"
compatibility <- list(
    diagnostics_schema_version = MATCH_DIAGNOSTICS_SCHEMA_VERSION,
    matching_policy = MATCHING_POLICY, task_id = task_id, atom_run_id = atom_run_id,
    atom_results_schema_version = atom_schema_version,
    estimator_version = ESTIMATOR_VERSION,
    atom_key_type = if (atom_enabled) {
        treatment_key_dataset$schema$GetFieldByName("atomic_id")$type$ToString()
    } else NULL,
    atom_counts_md5 = atom_count_md5,
    atom_sites_md5 = if (atom_enabled) unname(tools::md5sum(sites_path)) else NULL,
    atom_key_md5 = if (atom_enabled) {
        unname(tools::md5sum(file.path(config$output_dir, "treatment_cell_key.parquet")))
    } else NULL,
    matching_config = list(
        original_formula = format_formula(f), exact_match_vars = as.list(EXACT_MATCH_VARS),
        forest_cover_years = as.list(FC_YEARS), max_treatment = MAX_TREATMENT,
        control_multiplier = CONTROL_MULTIPLIER, min_glm = MIN_GLM, ratio = MAX_CONTROLS,
        caliper_width = CALIPER_WIDTH, matching_method = MATCHING_METHOD,
        separation_fallback = SEPARATION_FALLBACK, random_seed = RANDOM_SEED,
        min_cell_sample = MIN_CELL
    ),
    code_md5 = code_md5
)
package_versions <- as.list(sapply(c("stats", "dplyr", "arrow", "MatchIt", "optmatch",
                                     if (requireNamespace("rrelaxiv", quietly = TRUE)) "rrelaxiv"),
                                   function(p) as.character(packageVersion(p))))

same_matching_contract <- function(saved, expected) {
    # Code fingerprints are provenance; statistical and input contracts gate reuse.
    saved$code_md5 <- expected$code_md5 <- NULL
    identical(saved, expected)
}

validate_sidecar <- function(path, id = NULL, k = NULL) {
    saved <- read_json(path)
    # Compare JSON-normalized contracts without dropping explicit null fields.
    expected <- parse_json(toJSON(compatibility, auto_unbox = TRUE, null = "null", digits = NA))
    if (!same_matching_contract(saved$compatibility, expected) ||
        !identical(saved$diagnostics_schema_version, expected$diagnostics_schema_version) ||
        !identical(saved$matching_policy, MATCHING_POLICY)) {
        stop("Incompatible matching sidecar: ", basename(path))
    }
    records <- c(saved$diagnostics, saved$failures)
    if (!is.null(id) && (length(records) != 1L || !isTRUE(records[[1]]$id_numeric == id) ||
                       !isTRUE(records[[1]]$replicate == k))) {
        stop("Invalid matching item identity: ", basename(path))
    }
    if (!is.null(saved$matches_file) && !file.exists(checkpoint_path(saved$matches_file))) {
        stop("Missing matches for sidecar: ", basename(path))
    }
    if (!identical(unlist(saved$expected_attempt_keys),
                   unlist(lapply(records, `[[`, "expected_attempt_keys")))) {
        stop("Invalid sidecar attempt roster: ", path)
    }
    for (entry in records) {
        if (!length(entry$group_diagnostics)) stop("Missing terminal stratum diagnostics: ", path)
        keys <- vapply(entry$group_diagnostics, function(d) d$attempt_key, character(1))
        groups <- vapply(entry$group_diagnostics, function(d) d$group, character(1))
        if (anyDuplicated(keys) || any(!nzchar(keys)) ||
            !setequal(keys, unlist(entry$expected_attempt_keys)) ||
            !setequal(groups, unlist(entry$expected_groups))) {
            stop("Invalid matching attempt keys: ", path)
        }
        replays <- Filter(Negate(is.null), lapply(entry$group_diagnostics, function(d) {
            if (!identical(d$diagnostics_schema_version, expected$diagnostics_schema_version) ||
                !identical(d$matching_policy, MATCHING_POLICY) ||
                !identical(d$atom_run_id, atom_run_id) ||
                !identical(d$task_id, task_id) || !isTRUE(d$id_numeric == entry$id_numeric) ||
                !isTRUE(d$replicate == entry$replicate)) stop("Invalid stratum diagnostic: ", path)
            if (isTRUE(d$glm_failed) && is.null(d$replay)) stop("Missing GLM replay: ", path)
            d$replay
        }))
        if (length(replays)) {
            ref <- replays[[1]]
            if (!all(vapply(replays, function(r) {
                identical(r$sample_file, ref$sample_file) && identical(r$sample_md5, ref$sample_md5)
            }, logical(1)))) stop("Conflicting replay samples: ", path)
            dep <- checkpoint_path(paste0(item_name(entry$id_numeric, entry$replicate), "_sample.rds"))
            if (!identical(basename(dep), ref$sample_file) || !file.exists(dep) ||
                !identical(unname(tools::md5sum(dep)), ref$sample_md5)) {
                stop("Missing or corrupt GLM replay sample: ", dep)
            }
        }
    }
    invisible(TRUE)
}


# -- Work selection ----------------------------------------------------------

select_work <- function() {
    # Returns the site fragments and replicates this process should handle,
    # and a batch name unique within the task for its output files.
    all_reps <- seq_len(N_REPLICATES)
    if (!is.null(config$match_work)) {
        return(list(
            sites = filter(sites, id_numeric %in% config$match_work$ids),
            replicates = as.integer(config$match_work$replicates),
            batch = "checkpointed"
        ))
    }
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
        select(cell, id_numeric, any_of("atomic_id")) %>%
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
    missing_extracted <- if (nrow(t_rows) == nrow(tk)) tk[0, ] else {
        anti_join(tk, select(t_rows, cell, id_numeric), by = c("cell", "id_numeric"))
    }

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

    t_rows$treatment <- rep(TRUE, nrow(t_rows))
    pixels <- bind_rows(t_rows, c_rows) %>%
        mutate(treatment = !is.na(id_numeric)) %>%
        filter(if_all(all_of(EXACT_MATCH_VARS), ~ !is.na(.x)))
    pixels$group <- interaction(pixels[EXACT_MATCH_VARS], drop = TRUE)
    list(pixels = pixels,
         missing_exact = t_rows %>% filter(if_any(all_of(EXACT_MATCH_VARS), is.na)),
         missing_extracted = mutate(missing_extracted, treatment = TRUE),
         site_pixels = count(tk, id_numeric, name = "n"))
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


get_rng <- function() get0(".Random.seed", envir = .GlobalEnv, inherits = FALSE)
restore_rng <- function(rng) {
    if (!is.null(rng)) assign(".Random.seed", rng, envir = .GlobalEnv)
}

match_stratum <- function(gd, f) {
    # Choose the distance for one exact-match stratum and match it.
    rownames(gd) <- NULL
    mf <- drop_constant_terms(f, gd)
    method <- "glm"
    lp <- NULL
    diag <- list(
        method = method, distance_method = method, separation_details = NULL, error = NULL,
        model_formula = format_formula(mf),
        removed_constant_terms = as.list(setdiff(attr(terms(f), "term.labels"),
                                                  attr(terms(mf), "term.labels"))),
        sampled_covariates = covariate_diagnostics(gd, f), separation = NULL, glm = NULL,
        matcher_warnings = list(), matcher_error = NULL,
        stage = "matching", cause = "matched"
    )
    replay_rng <- NULL
    if (length(attr(terms(mf), "term.labels")) == 0) {
        # No covariate varies: pixels are alike on observables, so every
        # pairing is equally good.
        method <- "constant"
        lp <- rep(0, nrow(gd))
    } else if (sum(gd$treatment) < MIN_GLM) {
        method <- "mahalanobis"
    } else {
        sep <- check_separation(gd, mf)
        diag$separation <- sep
        diag$separation$sep_vars <- as.list(sep$sep_vars)
        if (MATCHING_POLICY == "shared_support_v2" &&
            sep$separated && length(sep$sep_vars)) {
            boundary <- restrict_boundary_support(gd, mf, sep$sep_vars)
            diag$boundary_support <- list(
                variables = as.list(sep$sep_vars),
                constraints = boundary$constraints,
                n_controls_before = boundary$n_controls_before,
                n_controls_retained = boundary$n_controls_retained
            )
            diag$removed_separating_terms <- as.list(sep$sep_vars)
            gd <- boundary$data
            mf <- boundary$formula
            if (boundary$n_controls_retained == 0L) {
                diag$cause <- "shared_support_exhausted"
                diag$stage <- "support"
                diag$method <- "none"
                return(list(result = NULL, diag = diag, replay_rng = replay_rng))
            }
            sep_after_boundary <- check_separation(gd, mf)
            diag$separation_after_boundary <- sep_after_boundary
            sep <- sep_after_boundary
        }
        if (!sep$separated) {
            rng <- get_rng()
            fit <- fit_propensity(mf, gd)
            diag$glm <- c(list(cause = fit$cause), fit$diagnostics)
            if (!is.null(fit$cause)) {
                replay_rng <- rng
                diag$cause <- fit$cause
                diag$stage <- "glm"
                sep <- list(
                    separated = TRUE, sep_vars = character(0),
                    details = if (!is.null(fit$diagnostics$error)) {
                        fit$diagnostics$error$message
                    } else "GLM did not converge or fitted treatment probabilities of 0 or 1"
                )
            } else {
                lp <- fit$lp
            }
        } else {
            if (is.null(diag$cause) || identical(diag$cause, "matched")) {
                diag$cause <- "pre_fit_separation"
                diag$stage <- "separation"
            }
        }
        if (sep$separated) {
            diag$separation_details <- sep$details
            if (!SEPARATION_FALLBACK || identical(diag$cause, "glm_error")) {
                diag$method <- "none"
                return(list(result = NULL, diag = diag, replay_rng = replay_rng))
            }
            method <- "mahalanobis"
            mf <- drop_formula_vars(mf, sep$sep_vars)
        }
    }
    gd$pscore <- if (method == "glm") plogis(lp) else NA_real_
    captured <- capture_match_conditions(run_matcher(gd, method, mf, lp))
    diag$method <- method
    diag$distance_method <- method
    diag$matcher_formula <- format_formula(mf)
    diag$matcher_warnings <- captured$warnings
    diag$matcher_error <- captured$error
    diag$error <- captured$error$message
    diag$cause <- if (!is.null(captured$error)) {
        "matcher_error"
    } else if (is.null(captured$value)) {
        "matcher_no_matches"
    } else if (length(captured$warnings)) {
        "matcher_warning"
    } else "matched"
    diag$stage <- "matching"
    diag$effective_caliper <- if (method == "glm" && CALIPER_WIDTH > 0) CALIPER_WIDTH * sd(lp) else NULL
    list(result = captured$value, diag = diag, replay_rng = replay_rng)
}


match_strata <- function(d, f, groups, sample_path, saved_sample) {
    d <- as.data.frame(d)
    d$group <- as.character(d$group)
    results <- list()
    diags <- list()
    sampled_groups <- split(seq_len(nrow(d)), d$group)
    execution_order <- c(unique(d$group), setdiff(names(groups), unique(d$group)))
    sample_md5 <- NULL
    for (g in execution_order) {
        group_diag <- groups[[g]]
        indices <- sampled_groups[[g]]
        gd <- d[indices, , drop = FALSE]
        stratum_key <- paste(progress$current_id, g)
        stratum_path <- if (CHECKPOINT_MODE) {
            checkpoint_path(paste0(progress$current_item, "_g", match(g, unique(d$group)), ".rds"))
        } else {
            NULL
        }
        timer <- proc.time()
        if (is.null(group_diag$cause) && (!any(gd$treatment) || !any(!gd$treatment))) {
            group_diag$cause <- "sample_no_overlap"
            group_diag$stage <- "sampling"
        }
        if (!is.null(group_diag$cause)) {
            out <- list(result = NULL, diag = list(method = "none", distance_method = "none"))
        } else if (CHECKPOINT_MODE && should_yield()) {
            yield_requested <<- TRUE
            return(NULL)
        } else if (!is.null(stratum_path) && file.exists(stratum_path)) {
            saved <- readRDS(stratum_path)
            if (is.null(sample_md5)) sample_md5 <- unname(tools::md5sum(sample_path))
            if (!identical(saved$group, g) ||
                !same_matching_contract(saved$compatibility, compatibility) ||
                !identical(saved$attempt_key, group_diag$attempt_key) ||
                !identical(saved$sample_md5, sample_md5)) {
                stop("Incompatible stratum checkpoint: ", stratum_path)
            }
            out <- saved$out
            restore_rng(saved$rng)
        } else {
            est <- progress$stratum_seconds[stratum_key]
            if (CHECKPOINT_MODE && !is.na(est) && would_overrun(DEADLINE_MARGIN * est)) {
                message("    Yielding before stratum ", g, " (previous solve ",
                        round(est), "s would exceed the deadline)")
                yield_requested <<- TRUE
                return(NULL)
            }
            message("    Starting stratum ", g)
            out <- match_stratum(gd, f)
            seconds <- unname((proc.time() - timer)["elapsed"])
            progress$stratum_seconds[stratum_key] <- max(seconds, est, na.rm = TRUE)
            if (!is.null(stratum_path)) {
                if (is.null(sample_md5)) sample_md5 <- unname(tools::md5sum(sample_path))
                write_checkpoint_rds(list(
                    group = g, out = out, rng = get_rng(), compatibility = compatibility,
                    attempt_key = group_diag$attempt_key, sample_md5 = sample_md5
                ), stratum_path)
            }
        }
        if (!is.null(out$diag$glm$cause)) {
            if (!file.exists(sample_path)) write_checkpoint_rds(saved_sample, sample_path)
            if (is.null(sample_md5)) sample_md5 <- unname(tools::md5sum(sample_path))
            if (is.na(sample_md5)) stop("Could not checksum required replay sample: ", sample_path)
            out$diag$replay <- list(
                sample_file = basename(sample_path), sample_md5 = sample_md5, group = g,
                attempt_key = group_diag$attempt_key, rng = as.list(out$replay_rng),
                diagnostics_schema_version = MATCH_DIAGNOSTICS_SCHEMA_VERSION,
                matching_policy = MATCHING_POLICY, atom_run_id = atom_run_id,
                formula = out$diag$model_formula, analysis_image = config$analysis_image
            )
        }
        group_diag <- utils::modifyList(group_diag, out$diag, keep.null = TRUE)
        group_diag <- finish_group(group_diag, gd, out$result)
        message("MATCH_ATTEMPT ", toJSON(list(
            attempt_key = group_diag$attempt_key, cause = group_diag$cause,
            glm_cause = group_diag[["glm"]]$cause,
            n_glm_warnings = length(group_diag[["glm"]]$warnings),
            n_matcher_warnings = length(group_diag$matcher_warnings),
            sampled = group_diag$counts$sampled,
            matched = group_diag$counts$matched, replay = group_diag$replay$sample_file,
            seconds = round((proc.time() - timer)["elapsed"], 2)
        ), auto_unbox = TRUE, null = "null"))
        diags[[length(diags) + 1]] <- group_diag
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
batch_out$census <- list()
append_to <- function(name, x) batch_out[[name]][[length(batch_out[[name]]) + 1L]] <- x

attempt_keys <- function(records) lapply(records, function(d) d$attempt_key)

record_failure <- function(site, k, error, reason = "error", diagnostics = NULL,
                           n_forest_pixels = NA_integer_, roster = list()) {
    append_to("failures", list(
        id_numeric = site$id_numeric, site_id = site$site_id,
        site_name = site$site_name, replicate = k, error = error,
        reason = reason, n_forest_pixels = n_forest_pixels,
        expected_attempt_keys = unname(attempt_keys(roster)),
        expected_groups = unname(lapply(roster, `[[`, "group")),
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
        list(batch = batch, matches_file = matches_file,
             diagnostics_schema_version = MATCH_DIAGNOSTICS_SCHEMA_VERSION,
             matching_policy = MATCHING_POLICY, atom_run_id = atom_run_id,
             compatibility = compatibility,
             expected_attempt_keys = c(list(), unlist(lapply(
                 c(batch_out$diagnostics, batch_out$failures),
                 `[[`, "expected_attempt_keys"), recursive = FALSE)),
             failures = batch_out$failures,
             diagnostics = batch_out$diagnostics, na_exclusions = batch_out$na_exclusions,
             treatment_census = batch_out$census),
        tmp, auto_unbox = TRUE, null = "null", na = "null", digits = NA
    )
    if (!file.rename(tmp, paste0(stem, ".json"))) stop("Could not write ", stem, ".json")
}

begin_item <- function(na_exclusions) {
    batch_out$matches <- list()
    batch_out$failures <- list()
    batch_out$diagnostics <- list()
    batch_out$na_exclusions <- na_exclusions
    batch_out$census <- list()
}

commit_item <- function(id, k) {
    batch <- item_name(id, k)
    write_batch(batch)
    stem <- checkpoint_path(paste0("mb_", batch))
    paths <- c(paste0(stem, ".parquet"), paste0(stem, ".json"))
    checkpoint_publish(paths[file.exists(paths)])
}

write_checkpoint_rds <- function(data, path) {
    tmp <- paste0(path, ".tmp")
    saveRDS(data, tmp)
    if (!file.rename(tmp, path)) stop("Could not persist matching evidence: ", path)
    checkpoint_publish(path)
}

replicate_seed <- function(id, k) {
    bitwXor(RANDOM_SEED, bitwXor(bitwShiftL(as.integer(id) %% 65536L, 10L), as.integer(k)))
}

count_arms <- function(d) {
    if (is.null(d)) return(list(treatment = 0L, control = 0L))
    list(treatment = sum(d$treatment), control = sum(!d$treatment))
}
count_atoms <- function(d, ids) {
    if (!length(ids)) return(integer())
    tabulate(match(as.character(d[["atomic_id"]][d$treatment]), ids), nbins = length(ids))
}

describe_pool <- function(raw, forest, eligible, pre, post, group, kind, this_f,
                          support = NULL, declared_ids = character()) {
    frames <- list(input = raw, baseline_forest = forest, eligible = eligible,
                   pre_support = pre, post_support = post)
    ids <- unique(c(as.character(raw[["atomic_id"]][raw$treatment]), declared_ids))
    atom_stages <- lapply(frames, count_atoms, ids = ids)
    cause <- switch(kind,
        missing_exact = "missing_exact_match_values",
        no_grid = "no_grid_pixels",
        missing_extraction = "missing_extracted_treatment",
        exact = if (!any(forest$treatment)) {
            "no_baseline_forest"
        } else if (!any(eligible$treatment)) {
            "no_complete_treatment"
        } else if (!any(pre$treatment) || !any(!pre$treatment)) {
            "no_exact_overlap"
        } else if (!any(!post$treatment)) "shared_support_exhausted"
    )
    exact <- setNames(lapply(EXACT_MATCH_VARS, function(v) {
        if (nrow(raw) && v %in% names(raw)) raw[[v]][[1]] else NA
    }), EXACT_MATCH_VARS)
    list(
        group = group, exact_match_values = exact, group_kind = kind,
        counts = lapply(frames, count_arms),
        atoms = lapply(seq_along(ids), function(i) {
            counts <- lapply(atom_stages, `[[`, i)
            names(counts) <- paste0("n_", names(counts))
            c(list(atomic_id = ids[[i]]), counts)
        }),
        stage = if (identical(cause, "shared_support_exhausted")) "support" else "eligibility",
        cause = cause, original_formula = format_formula(f), site_formula = format_formula(this_f),
        model_formula = NULL, matcher_formula = NULL, removed_constant_terms = list(),
        glm = NULL, separation = NULL, matcher_warnings = list(), matcher_error = NULL,
        matching_method = MATCHING_METHOD, requested_ratio = MAX_CONTROLS,
        caliper_width = CALIPER_WIDTH, control_multiplier = CONTROL_MULTIPLIER,
        max_treatment_pixels = MAX_TREATMENT, min_glm_treatment_pixels = MIN_GLM,
        shared_support = list(
            constraints = if (is.null(support)) list() else support$constraints,
            n_treatment = sum(pre$treatment), n_controls_before = sum(!pre$treatment),
            n_controls_retained = sum(!post$treatment),
            n_controls_removed = sum(!pre$treatment) - sum(!post$treatment)
        )
    )
}

finish_group <- function(record, sampled, matched) {
    record$counts$sampled <- count_arms(sampled)
    record$counts$matched <- count_arms(matched)
    record$n_treatment <- record$counts$sampled$treatment
    record$n_control <- record$counts$sampled$control
    record$n_matched <- record$counts$matched$treatment
    record$status <- if (record$n_matched > 0) "matched" else "unmatched"
    record$reason_code <- record$cause
    record$separation_variables <- if (is.null(record$separation$sep_vars)) {
        list()
    } else record$separation$sep_vars
    record$glm_failed <- !is.null(record[["glm"]]$cause)
    record$control_shortage <- list(
        n_below_sampling_ceiling = max(0, record$n_treatment * CONTROL_MULTIPLIER - record$n_control),
        n_below_requested_ratio = if (MAX_CONTROLS > 0) {
            max(0, record$n_treatment * MAX_CONTROLS - record$n_control)
        } else NULL
    )
    ids <- vapply(record$atoms, function(atom) atom$atomic_id, character(1))
    s <- count_atoms(sampled, ids)
    m <- count_atoms(matched, ids)
    record$atoms <- lapply(seq_along(ids), function(i) {
        a <- record$atoms[[i]]
        a$n_sampled <- s[[i]]
        a$n_matched <- m[[i]]
        a$cause <- if (m[[i]] > 0L) {
            "matched"
        } else if (s[[i]] > 0L) {
            if (record$status == "matched") "sampled_but_unmatched" else record$cause
        } else if (record$stage %in% c("eligibility", "support") || a$n_eligible == 0L) {
            if (a$n_eligible == 0L && record$group_kind == "exact") {
                if (a$n_baseline_forest == 0L) "no_baseline_forest" else "no_complete_treatment"
            } else record$cause
        } else "not_sampled"
        a
    })
    record
}

attempt_roster <- function(groups, site, k, sample_path) {
    lapply(groups, function(d) {
        d$task_id <- task_id
        d$site_id <- site$site_id
        d$id_numeric <- site$id_numeric
        d$sub_site_index <- site$sub_site_index
        d$replicate <- k
        d["atom_run_id"] <- list(atom_run_id)
        d["atom_results_schema_version"] <- list(atom_schema_version)
        d$diagnostics_schema_version <- MATCH_DIAGNOSTICS_SCHEMA_VERSION
        d$matching_policy <- MATCHING_POLICY
        d$code_md5 <- code_md5
        d["analysis_image"] <- list(config$analysis_image)
        d$replicate_seed <- if (!is.null(RANDOM_SEED)) replicate_seed(site$id_numeric, k) else NULL
        d$sample_id <- basename(sample_path)
        d$attempt_key <- as.character(toJSON(list(
            task_id, site$site_id, site$id_numeric, site$sub_site_index, k,
            d$group_kind, d$exact_match_values
        ), auto_unbox = TRUE, null = "null", na = "null", digits = NA))
        d
    })
}

process_site <- function(site, pixels, missing_exact, missing_extracted, n_site_pixels, replicates) {
    this_id <- site$id_numeric
    message("  Site ", site$site_id, " (fragment ", site$sub_site_index,
            ", id ", this_id, "), replicate(s) ", paste(range(replicates), collapse = "-"))

    # This site's treatment pixels and the controls in its strata only.
    is_site <- pixels$treatment & pixels$id_numeric %in% this_id
    vals <- pixels[is_site | (!pixels$treatment & pixels$group %in% pixels$group[is_site]), ]
    missing_exact <- missing_exact[missing_exact$id_numeric == this_id, , drop = FALSE]
    missing_extracted <- missing_extracted[missing_extracted$id_numeric == this_id, , drop = FALSE]
    declaration <- if (atom_enabled) atom_declarations[[site$ae_atoms]] else NULL
    declared_ids <- vapply(declaration$atoms, function(atom) as.character(atom$atomic_id),
                           character(1))
    parent_atom_counts <- if (!is.null(atom_extraction_counts)) {
        filter(atom_extraction_counts, site_id == site$site_id)
    } else NULL
    if (atom_enabled) {
        extracted_ids <- c(as.character(vals$atomic_id[vals$treatment]),
                           as.character(missing_exact$atomic_id),
                           as.character(missing_extracted$atomic_id))
        if (anyNA(extracted_ids)) stop("Missing atom IDs in treatment keys for fragment ", this_id)
        if (!all(extracted_ids %in% declared_ids)) stop("Undeclared treatment atom for fragment ", this_id)
    }

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
    na_exclusions <- tail(batch_out$na_exclusions, 1)
    baseline <- paste0("fc_", baseline_year(site$start_year, min(FC_YEARS)))
    forest <- !is.na(vals[[baseline]]) & vals[[baseline]] > 0
    n_forest_pixels <- sum(vals$treatment & forest)
    raw <- vals
    vals <- filter_groups(raw[complete & forest, ], EXACT_MATCH_VARS)
    no_overlap <- !any(vals$treatment) || !any(!vals$treatment)
    eligible <- vals %>%
        filter(treatment) %>%
        mutate(group = as.character(group)) %>%
        count(across(all_of(CELL_COLS)), name = "n_eligible")
    # Construct the roster independently of sampling, preserving input order in
    # the sampler (including legacy seeded results).
    pre_rows <- split(seq_len(nrow(vals)), vals$group, drop = TRUE)
    raw_rows <- split(seq_len(nrow(raw)), raw$group, drop = TRUE)
    retain <- rep(TRUE, nrow(vals))
    groups <- list()
    for (g in names(raw_rows)) {
        ix <- raw_rows[[g]]
        before <- vals[pre_rows[[g]], ]
        support <-         if (MATCHING_POLICY == "shared_support_v2" && nrow(before) > 0L) {
            restrict_shared_support(before, this_f)
        } else NULL
        after <- if (is.null(support)) before else support$data
        if (!is.null(support)) {
            retain[pre_rows[[g]]] <- support$retained_rows & support$n_controls_retained > 0L
        }
        groups[[g]] <- describe_pool(raw[ix, ], raw[ix[forest[ix]], ],
                                     raw[ix[forest[ix] & complete[ix]], ],
                                     before, after, g, "exact", this_f, support)
    }
    empty <- raw[0, ]
    if (nrow(missing_exact)) {
        bins <- missing_exact %>% group_by(across(all_of(EXACT_MATCH_VARS))) %>% group_rows()
        for (rows in bins) {
            miss <- missing_exact[rows, ]
            forest_miss <- miss[!is.na(miss[[baseline]]) & miss[[baseline]] > 0, ]
            g <- paste0("missing_exact:", length(groups) + 1L)
            while (g %in% names(groups)) g <- paste0(":", g)
            groups[[g]] <- describe_pool(miss, forest_miss, empty, empty, empty, g,
                                         "missing_exact", this_f)
        }
    }
    if (nrow(missing_extracted)) {
        g <- "missing_extraction"
        while (g %in% names(groups)) g <- paste0(":", g)
        groups[[g]] <- describe_pool(missing_extracted, empty, empty, empty, empty, g,
                                     "missing_extraction", this_f)
    }
    zero_grid <- if (atom_enabled && this_id == min(sites$id_numeric[sites$site_id == site$site_id])) {
        setdiff(declared_ids, atoms_with_keys)
    } else character(0)
    if (!length(groups)) {
        kind <- if (n_site_pixels > 0L) "missing_extraction" else "no_grid"
        groups[[kind]] <- describe_pool(empty, empty, empty, empty, empty, kind, kind,
                                        this_f, declared_ids = if (kind == "no_grid") zero_grid)
    }
    # Zero-grid atoms have no exact-stratum membership; keep them visible in
    # one inventory-only record on the parent's first fragment.
    if (length(zero_grid) &&
        !any(vapply(groups, function(g) g$group_kind == "no_grid", logical(1)))) {
        g <- "no_grid"
        while (g %in% names(groups)) g <- paste0(":", g)
        groups[[g]] <- describe_pool(empty, empty, empty, empty, empty, g, "no_grid",
                                     this_f, declared_ids = zero_grid)
    }
    vals <- vals[retain, ]
    # Observed outcomes of every eligible treatment pixel, by cell.  Invalid
    # outcome inputs are reported to the summary, which excludes the site.
    census <- tryCatch(
        c(list(id_numeric = this_id, site_id = site$site_id),
          treatment_census(vals, site$start_year, min(FC_YEARS), max(FC_YEARS))),
        invalid_pixel_outcomes = function(e) {
            list(id_numeric = this_id, site_id = site$site_id, error = conditionMessage(e))
        }
    )
    census_pending <- TRUE
    keep_cols <- unique(c(
        "cell", "atomic_id", "treatment", "group", "match_group", "match_weight", "pscore",
        "area_ha", "total_biomass_2000", FC_COLS, setdiff(all.vars(this_f), "treatment")
    ))

    for (k in replicates) {
        if (CHECKPOINT_MODE) {
            if (item_complete(this_id, k)) next
            if (should_yield()) {
                yield_requested <<- TRUE
                return(invisible(NULL))
            }
            # Defer the rest of an expensive fragment rather than start a solve
            # that cannot finish, or hold this task for replicates that will not
            # fit. Cheaper fragments in the batch continue; repack spreads the
            # deferred replicates across recovery tasks.
            est <- fragment_estimate(this_id)
            remaining <- sum(!vapply(replicates[replicates >= k],
                                     function(r) item_complete(this_id, r), logical(1)))
            # Unmeasured fragments keep the previous item's duration as a guard.
            projected <- if (is.na(est)) {
                progress$last_item_seconds
            } else {
                max(DEADLINE_MARGIN * est, est * remaining)
            }
            if (would_overrun(projected)) {
                message("    Deferring ", remaining, " replicate(s) of fragment ", this_id,
                        " (projected ", round(projected), "s)")
                return(invisible(NULL))
            }
            begin_item(na_exclusions)
        }
        # One copy per worker suffices: the census does not vary by replicate.
        if (census_pending) {
            append_to("census", census)
            census_pending <- FALSE
        }
        item_timer <- proc.time()
        if (!is.null(RANDOM_SEED)) set.seed(replicate_seed(this_id, k))
        progress$current_item <- item_name(this_id, k)
        progress$current_id <- this_id
        sample_path <- checkpoint_path(paste0(progress$current_item, "_sample.rds"))
        roster <- attempt_roster(groups, site, k, sample_path)
        expected <- attempt_keys(roster)
        if (!no_overlap && file.exists(sample_path)) {
            saved_sample <- readRDS(sample_path)
            if (!same_matching_contract(saved_sample$compatibility, compatibility) ||
                !identical(saved_sample$item, progress$current_item) ||
                !identical(saved_sample$formula, format_formula(this_f)) ||
                !identical(saved_sample$expected_attempt_keys, expected)) {
                stop("Incompatible sample checkpoint: ", sample_path)
            }
            sampled <- saved_sample$data
            restore_rng(saved_sample$rng)
        } else {
            sampled <- sample_strata(vals, MAX_TREATMENT, CONTROL_MULTIPLIER, MIN_CELL)
            saved_sample <- list(
                data = sampled, rng = get_rng(), compatibility = compatibility,
                item = progress$current_item, expected_attempt_keys = expected,
                formula = format_formula(this_f),
                provenance = list(
                    task_id = task_id, site_id = site$site_id, id_numeric = this_id,
                    sub_site_index = site$sub_site_index, replicate = k,
                    matching_config = compatibility$matching_config, code_md5 = code_md5,
                    analysis_image = config$analysis_image, r_version = R.version.string,
                    package_versions = package_versions, rng_kind = RNGkind(),
                    locale_messages = Sys.getlocale("LC_MESSAGES"),
                    model_options = options()[c("contrasts", "na.action")],
                    diagnostics_schema_version = MATCH_DIAGNOSTICS_SCHEMA_VERSION,
                    matching_policy = MATCHING_POLICY, atom_run_id = atom_run_id,
                    atom_results_schema_version = atom_schema_version,
                    ae_atoms = declaration, extraction_atom_counts = parent_atom_counts,
                    extraction_atom_count_file = if (!is.null(atom_count_md5)) basename(atom_count_path),
                    extraction_atom_count_md5 = atom_count_md5
                )
            )
            if (CHECKPOINT_MODE && nrow(sampled)) {
                write_checkpoint_rds(saved_sample, sample_path)
            }
        }
        res <- match_strata(sampled, this_f, roster, sample_path, saved_sample)
        if (yield_requested) return(invisible(NULL))
        if (is.null(res$result)) {
            msg <- paste0(
                "No matches found (treatment=", sum(sampled$treatment),
                ", control=", sum(!sampled$treatment), ")"
            )
            reason <- "no_matches"
            if (n_forest_pixels == 0L) {
                msg <- paste0("No forest at the ", baseline, " baseline")
                reason <- "no_eligible_treatment"
            } else if (no_overlap) {
                msg <- paste0("No overlapping exact-match strata between eligible treatment ",
                              "and control pixels (T=0, C=0)")
                reason <- "no_overlap"
            }
            record_failure(site, k, msg, reason = reason, diagnostics = res$diagnostics,
                           n_forest_pixels = n_forest_pixels, roster = roster)
            if (CHECKPOINT_MODE) {
                commit_item(this_id, k)
                record_item_seconds(this_id, item_timer)
            }
            next
        }
        # Cell weights scale matched treatment pixels (and their controls) to
        # all eligible treatment pixels of the cell in this replicate.  The
        # summary replaces them once inclusion is known across replicates.
        weights <- res$result %>%
            filter(treatment) %>%
            count(across(all_of(CELL_COLS)), name = "n_matched") %>%
            inner_join(eligible, by = CELL_COLS) %>%
            inner_join(
                sampled %>%
                    filter(treatment) %>%
                    mutate(group = as.character(group)) %>%
                    count(across(all_of(CELL_COLS)), name = "n_sampled"),
                by = CELL_COLS
            ) %>%
            mutate(sampling_weight = n_eligible / n_matched)
        m <- res$result[, intersect(keep_cols, names(res$result))] %>%
            as_tibble() %>%
            apply_cell_weights(weights)
        # Treatment rows carry their cell's eligible and sampled counts, from
        # which the summary decides inclusion and the final expansion factor.
        cell_index <- match(cell_key(m, CELL_COLS), cell_key(weights, CELL_COLS))
        m <- m %>%
            mutate(
                cell_n_eligible = if_else(treatment, weights$n_eligible[cell_index], NA_integer_),
                cell_n_sampled = if_else(treatment, weights$n_sampled[cell_index], NA_integer_),
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
        append_to("diagnostics", list(id_numeric = this_id, replicate = k, site_id = site$site_id,
                                      expected_attempt_keys = unname(expected),
                                      expected_groups = unname(lapply(roster, `[[`, "group")),
                                      group_diagnostics = res$diagnostics))
        message("    Replicate ", k, ": ", sum(m$treatment), " of ",
                sum(sampled$treatment), " sampled treatment pixels matched")
        if (CHECKPOINT_MODE) {
            commit_item(this_id, k)
            record_item_seconds(this_id, item_timer)
        }
    }
}


# -- Main --------------------------------------------------------------------

work <- select_work()
message("Processing ", nrow(work$sites), " site fragment(s) as batch ", work$batch)
batch_path <- checkpoint_path(paste0("mb_", work$batch, ".json"))
if (!CHECKPOINT_MODE && file.exists(batch_path)) {
    validate_sidecar(batch_path)
    message("Batch ", work$batch, " is already complete; skipping")
} else {
    if (nrow(work$sites) > 0) {
        loaded <- load_worker_pixels(work$sites)
        for (i in seq_len(nrow(work$sites))) {
            site <- work$sites[i, ]
            already_complete <- CHECKPOINT_MODE &&
                all(vapply(work$replicates, function(k) item_complete(site$id_numeric, k), logical(1)))
            if (already_complete) {
                next
            }
            n_site_pixels <- sum(loaded$site_pixels$n[loaded$site_pixels$id_numeric == site$id_numeric])
            # Analytical errors are isolated at the model/matcher. Persistence
            # or input failures must instead leave the work incomplete.
            process_site(site, loaded$pixels, loaded$missing_exact, loaded$missing_extracted,
                         n_site_pixels, work$replicates)
            gc()
            if (yield_requested) break
        }
    }
    if (!CHECKPOINT_MODE) write_batch(work$batch)
}

if (CHECKPOINT_MODE) {
    unfinished <- list()
    for (id in work$sites$id_numeric) {
        for (k in work$replicates) {
            if (!item_complete(id, k)) unfinished[[length(unfinished) + 1L]] <- list(id = id, replicate = k)
        }
    }
    write_json(unfinished, file.path(config$data_dir, "unfinished.json"), auto_unbox = TRUE)
    write_json(as.list(progress$item_seconds), file.path(config$data_dir, "item_seconds.json"),
               auto_unbox = TRUE, digits = NA)
}
message("[TIMING] Step 2 total: ", round((proc.time() - step2_timer)["elapsed"], 1), "s")
message("Step 2 complete.")

}, step_name = "02_perform_matching")
