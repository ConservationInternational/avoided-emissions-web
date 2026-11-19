# Focused step-2 integration tests; no summary or spatial extraction required.
for (pkg in c("arrow", "optmatch", "MatchIt", "tidyverse", "httr")) {
    if (!requireNamespace(pkg, quietly = TRUE)) testthat::skip(paste("package not installed:", pkg))
}
library(arrow, warn.conflicts = FALSE)
library(jsonlite)

support_scripts <- normalizePath(Sys.getenv("R_SCRIPTS_DIR", "../scripts"))
source(file.path(support_scripts, "stats_functions.R"))

write_support_fixture <- function(path, treatment, controls, atom_ids, formula) {
    out <- file.path(path, "output")
    dir.create(out, recursive = TRUE)
    sites <- tibble(site_id = "A", site_name = "A", id_numeric = 1L, sub_site_index = 0L,
                    start_year = 2014L)
    keys <- select(treatment, cell, any_of("atomic_id")) %>% mutate(id_numeric = 1L)
    if (length(atom_ids)) {
        declarations <- toJSON(list(run_id = "kpi-run", atoms = lapply(atom_ids, function(id) {
            list(atomic_id = id)
        })), auto_unbox = TRUE)
        sites$ae_atoms <- as.character(declarations)
        write_parquet(count(keys, atomic_id, name = "n_site_pixels") %>%
                          mutate(site_id = "A", n_forest_pixels = n_site_pixels),
                      file.path(out, "ae_atom_counts.parquet"))
    }
    write_parquet(sites, file.path(out, "sites_processed.parquet"))
    write_parquet(keys, file.path(out, "treatment_cell_key.parquet"))
    covariates <- function(d) d %>%
        mutate(area_ha = 1, fc_2000 = 100, total_biomass_2000 = 100,
               fc_2008 = 100, fc_2013 = 100 - loss)
    write_parquet(covariates(select(treatment, -any_of("atomic_id"))),
                  file.path(out, "treatment_pixels.parquet"))
    write_dataset(covariates(controls), file.path(out, "control_pixels"), partitioning = "admin")
    write_json(list(formula_str = formula), file.path(out, "formula.json"), auto_unbox = TRUE)
    list(data_dir = path, task_id = "test-task", fc_years = c(2008, 2013),
         exact_match_vars = list("admin"), max_treatment_pixels = 10, control_multiplier = 10,
         min_glm_treatment_pixels = 2, caliper_width = 0, max_controls_per_treatment = 1,
         matching_method = "nearest", shared_support_controls = TRUE, n_replicates = 2,
         random_seed = 12)
}

run_support_fixture <- function(config) {
    cfg <- file.path(config$data_dir, "config.json")
    write_json(config, cfg, auto_unbox = TRUE)
    suppressWarnings(system2("Rscript", c(file.path(support_scripts, "02_perform_matching.R"), "--config", cfg),
        env = c(paste0("R_SCRIPTS_DIR=", support_scripts), "AWS_BATCH_JOB_ARRAY_INDEX=",
                paste0("R_LIBS=", paste(.libPaths(), collapse = ":"))),
        stdout = TRUE, stderr = TRUE))
}

expect_support_success <- function(config) {
    log <- run_support_fixture(config)
    expect_null(attr(log, "status"), info = paste(tail(log, 30), collapse = "\n"))
    invisible(log)
}

test_that("non-atom matching records diagnostics without reading an absent atom column", {
    path <- tempfile("matching_nonatom_")
    on.exit(unlink(path, recursive = TRUE))
    t <- tibble(cell = 1:3, admin = c(1, 1, 2), loss = 0, x = 0)
    c <- tibble(cell = 101:102, admin = 1, loss = 0, x = 0)
    config <- write_support_fixture(path, t, c, NULL, "treatment ~ x")
    config$shared_support_controls <- NULL
    config$n_replicates <- 1
    log <- expect_support_success(config)
    expect_false(any(grepl("Unknown or uninitialised column: `atomic_id`", log, fixed = TRUE)))
    out <- read_json(file.path(path, "output", "matches", "mb_all.json"))
    expect_equal(out$matching_policy, "shared_support_v2")
    expect_equal(lapply(out$diagnostics[[1]]$group_diagnostics, `[[`, "atoms"),
                 list(list(), list()))
})

test_that("shared support narrows controls before either matcher without rescaling treatment", {
    t <- tibble(cell = 1:6, admin = rep(1:3, each = 2), cropland = 0, loss = 0,
                atomic_id = rep(c("9223372036854775801", "2"), 3))
    c <- tibble(cell = 101:106, admin = c(1, 1, 1, 1, 2, 2),
                cropland = c(0, 0, 1e-15, 0, 0, 1), loss = c(0, 0, 0, 2, 2, 0))
    for (method in c("nearest", "optimal", "legacy")) {
        path <- tempfile("matching_support_")
        on.exit(unlink(path, recursive = TRUE), add = TRUE)
        config <- write_support_fixture(path, t, c, c(unique(t$atomic_id), "99"),
                                        "treatment ~ cropland")
        config$shared_support_controls <- method != "legacy"
        if (method == "optimal") config$matching_method <- method
        expect_support_success(config)
        out <- file.path(path, "output", "matches")
        sidecar <- read_json(file.path(out, "mb_all.json"))
        items <- if (method == "legacy") sidecar$failures else sidecar$diagnostics
        expect_length(items, 2)
        for (item in items) {
            groups <- setNames(item$group_diagnostics, vapply(item$group_diagnostics, `[[`, "", "group"))
            expect_setequal(unlist(item$expected_groups), c("1", "2", "3", "no_grid"))
            expect_equal(groups[["3"]]$reason_code, "no_exact_overlap")
            expect_equal(groups[["no_grid"]]$atoms[[1]]$atomic_id, "99")
            if (method == "legacy") {
                expect_equal(groups[["1"]]$reason_code, "pre_fit_separation")
                expect_equal(groups[["1"]]$counts$post_support$control, 4)
            } else {
                expect_equal(groups[["1"]]$counts$pre_support$control, 4)
                expect_equal(groups[["1"]]$counts$post_support$control, 2)
                expect_equal(groups[["1"]]$control_shortage$n_below_sampling_ceiling, 18)
                expect_equal(groups[["2"]]$reason_code, "shared_support_exhausted")
                expect_equal(groups[["2"]]$counts$eligible$treatment, 2)
                expect_equal(groups[["2"]]$counts$sampled$treatment, 0)
            }
        }
        if (method != "legacy") {
            m <- read_parquet(file.path(out, sidecar$matches_file))
            expect_setequal(m$cell, c(1, 2, 101, 102))
            expect_true(all(m$sampling_weight == 1))
            expect_true(all(m$match_weight == 1))
            expect_setequal(m$atomic_id[m$treatment], unique(t$atomic_id))
        }
    }
})

test_that("sampled boundary separation keeps only common treatment values and drops the term", {
    path <- tempfile("matching_boundary_")
    on.exit(unlink(path, recursive = TRUE))
    t <- tibble(
        cell = 1:8,
        admin = rep(1:2, each = 4),
        x = rep(c(0, 0, 1, 1), 2),
        loss = 0
    )
    c <- tibble(
        cell = 101:108,
        admin = rep(1:2, each = 4),
        x = c(-1, -1, 0, 0, -1, -1, -1, -1),
        loss = 0
    )
    config <- write_support_fixture(path, t, c, NULL, "treatment ~ x")
    config$max_treatment_pixels <- 2
    config$n_replicates <- 1
    config$random_seed <- which(vapply(1:1000, function(seed) {
        set.seed(bitwXor(seed, bitwXor(bitwShiftL(1L, 10L), 1L)))
        sampled <- sample_strata(
            mutate(t, treatment = TRUE, group = admin), 2L, 10L
        )
        all(sampled$x[sampled$treatment] == 0)
    }, logical(1)))[1]
    expect_support_success(config)

    sidecar <- read_json(file.path(path, "output", "matches", "mb_all.json"))
    groups <- setNames(
        sidecar$diagnostics[[1]]$group_diagnostics,
        vapply(sidecar$diagnostics[[1]]$group_diagnostics, `[[`, "", "group")
    )
    expect_equal(groups[["1"]]$reason_code, "matched")
    expect_equal(groups[["1"]]$method, "glm")
    expect_equal(groups[["1"]]$boundary_support$n_controls_retained, 2)
    expect_equal(groups[["1"]]$removed_separating_terms, list("x"))
    expect_equal(groups[["2"]]$reason_code, "shared_support_exhausted")
    expect_equal(groups[["2"]]$counts$sampled$treatment, 2)
})

test_that("a later GLM failure within partial success replays its exact warning on either resume path", {
    # Atom 1 has 60 pixels in stratum 1, so 50 are sampled; one of them lies so
    # far from the controls that, when drawn, its fitted probability is 1 and
    # the propensity model is rejected.  Atom 2 has a single pixel there.
    rare <- 1e4
    t <- tibble(cell = 1:63, admin = c(rep(1, 61), 2, 2), loss = 0,
                x = c(rare, rep(c(0, 1), length.out = 59), 0.5, 0, 1),
                atomic_id = c(rep("1", 60), "2", "1", "1"))
    c <- tibble(cell = 101:183, admin = c(rep(1, 80), rep(2, 3)), loss = 0,
                x = c(seq(-2, 2, length.out = 80), -2, 0, 2))
    f <- "treatment ~ x"
    # Select a seed omitting the rare pixel in the first sample but drawing it in the second.
    seed <- which(vapply(1:100, function(seed) {
        sampled_rare <- function(k) {
            set.seed(bitwXor(seed, bitwXor(bitwShiftL(1L, 10L), k)))
            p <- sample_strata(mutate(t, treatment = TRUE, group = admin), 2L, 10L,
                               MIN_CELL_SAMPLE)
            any(p$x == rare)
        }
        !sampled_rare(1L) && sampled_rare(2L)
    }, logical(1)))[1]
    for (checkpointed in c(FALSE, TRUE)) {
        path <- tempfile("matching_replay_")
        on.exit(unlink(path, recursive = TRUE), add = TRUE)
        config <- write_support_fixture(path, t, c, c("1", "2"), f)
        config$max_treatment_pixels <- 2
        config$random_seed <- seed
        config$match_checkpoints <- checkpointed
        expect_support_success(config)
        out <- file.path(path, "output", "matches")
        paths <- file.path(out, if (checkpointed) c("mb_f1_r1.json", "mb_f1_r2.json") else "mb_all.json")
        load_items <- function() unlist(lapply(paths, function(p) read_json(p)$diagnostics),
                                        recursive = FALSE)
        items <- load_items()
        expect_length(items, 2)
        by_group <- function(item, group) Filter(function(g) g$group == group, item$group_diagnostics)[[1]]
        first <- by_group(items[[1]], "1")
        expect_false(first$glm_failed)
        # Sampling by atom draws the one-pixel atom in every replicate.
        for (item in items) {
            single <- Filter(function(a) a$atomic_id == "2", by_group(item, "1")$atoms)[[1]]
            expect_equal(single$n_sampled, 1)
        }
        expect_equal(first$counts$sampled$treatment, 51)
        failed <- by_group(items[[2]], "1")
        expect_equal(failed$reason_code, "glm_warning")
        expect_true(failed$glm_failed)
        expect_equal(by_group(items[[2]], "2")$status, "matched")
        replay_path <- file.path(out, failed$replay$sample_file)
        expect_equal(unname(tools::md5sum(replay_path)), failed$replay$sample_md5)
        model <- replay_match_propensity(replay_path, failed$replay$group, failed$replay$rng,
                                         scripts_dir = support_scripts)
        expect_equal(model$glm$cause, failed$glm$cause)
        expect_identical(model$glm$diagnostics$warnings, failed$glm$warnings)
        expect_identical(model$provenance$atom_run_id, "kpi-run")
        if (!checkpointed) {
            altered <- model$provenance
            altered$package_versions$stats <- "0.0.0"
            expect_error(verify_match_replay_environment(altered, support_scripts),
                         "Replay package version mismatch: stats")
            altered <- model$provenance
            altered$code_md5[["stats_functions.R"]] <- "00000000000000000000000000000000"
            expect_error(verify_match_replay_environment(altered, support_scripts),
                         "Replay code checksum mismatch: stats_functions.R")
        }
        if (checkpointed) {
            # Keep sample and solved strata, but force completion from checkpoints.
            unlink(paths[[2]])
            expect_support_success(config)
            expect_identical(load_items(), items)
        }
        config$shared_support_controls <- FALSE
        log <- run_support_fixture(config)
        expect_true(any(grepl("Incompatible matching sidecar", log, fixed = TRUE)))
    }
})
