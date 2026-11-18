library(testthat)
library(jsonlite)
source(file.path(Sys.getenv("R_SCRIPTS_DIR", "../scripts"), "matching_diagnostics.R"))

diagnostic_fixture <- function() {
    root <- tempfile("matching_diagnostics_")
    dir.create(root)
    matches <- file.path(root, "matches")
    dir.create(matches)
    config <- list(
        task_id = "task", output_dir = root, matches_dir = matches,
        n_replicates = 2L, shared_support_controls = TRUE,
        diagnostic_expected_ids = 1L, fc_years = 2008:2016
    )
    make_record <- function(rep) list(
        diagnostics_schema_version = 1L, matching_policy = "shared_support_v1",
        task_id = "task", attempt_key = paste("task", 1, rep, "g", sep = "/"),
        site_id = "parent", id_numeric = 1L, sub_site_index = 0L,
        replicate = rep, group = "g", atom_run_id = "job",
        reason_code = if (rep == 1L) "matched" else "glm_warning",
        separation_variables = if (rep == 1L) list() else list("cropland"),
        glm_failed = rep == 2L,
        atoms = list(list(atomic_id = "101", n_eligible = 10L,
                          n_sampled = 5L, n_matched = if (rep == 1L) 5L else 0L)),
        replay = if (rep == 2L) list(sample_file = "f1_r2_sample.rds", group = "g") else NULL
    )
    paths <- vapply(1:2, function(rep) {
        record <- list(id_numeric = 1L, site_id = "parent", replicate = rep,
                       expected_groups = list("g"),
                       expected_attempt_keys = list(make_record(rep)$attempt_key),
                       group_diagnostics = list(make_record(rep)))
        path <- file.path(matches, paste0("mb_f1_r", rep, ".json"))
        write_json(list(
            diagnostics_schema_version = 1L, matching_policy = "shared_support_v1",
            expected_attempt_keys = list(make_record(rep)$attempt_key),
            diagnostics = if (rep == 1L) list(record) else list(),
            failures = if (rep == 2L) list(record) else list()
        ), path, auto_unbox = TRUE, null = "null", digits = NA)
        path
    }, character(1))
    saveRDS(list(data = data.frame(cropland = 0:1), formula = treatment ~ cropland),
            file.path(matches, "f1_r2_sample.rds"))
    fragments <- data.frame(id_numeric = 1L, site_id = "parent", sub_site_index = 0L,
                            start_year = 2014L)
    complete <- data.frame(id_numeric = integer(), group = character())
    declarations <- data.frame(run_id = "job", atomic_id = c("101", "102"), site_id = "parent")
    status <- data.frame(
        run_id = "job", atomic_id = c("101", "102"), site_id = "parent",
        status = c("failed", "not_applicable"), status_reason = c("incomplete", "no grid"),
        n_site_pixels = c(10L, 0L), n_forest_pixels = c(10L, 0L)
    )
    list(root = root, paths = paths, config = config, fragments = fragments,
         complete = complete, declarations = declarations, status = status)
}

export_diagnostic_fixture <- function(fixture) {
    write_matching_diagnostics(fixture$paths, fixture$config, fixture$fragments,
                               fixture$complete, fixture$status, fixture$declarations)
}

test_that("all replicates, failed fits and unsampled declared atoms remain discoverable", {
    fixture <- diagnostic_fixture()
    result <- export_diagnostic_fixture(fixture)
    expect_true(result$diagnostics_complete)
    expect_equal(result$counts$attempts, 2L)
    expect_equal(result$counts$expected_items, 2L)
    expect_equal(result$counts$replay_attempts, 1L)
    expect_equal(result$counts$atoms, 2L)
    expect_equal(result$by_cause$glm_warning$attempts, 1L)
    expect_equal(result$by_replicate[["2"]]$attempts, 1L)
    stream <- gzfile(file.path(fixture$root, result$artifacts$diagnostics), "rt")
    records <- lapply(readLines(stream), fromJSON, simplifyVector = FALSE)
    close(stream)
    expect_equal(length(records), 4L)
    expect_equal(records[[2]]$replicate, 2L)
    expect_false(records[[1]]$stratum_consistent)
    expect_equal(records[[3]]$missing_replicates, 2L)
    expect_equal(records[[4]]$inclusion_reason, "no_grid_pixels")
    replay <- fromJSON(file.path(fixture$root, result$artifacts$replay), simplifyVector = FALSE)
    expect_equal(length(replay$attempts), 1L)
    expect_true(startsWith(replay$attempts[[1]]$uri, "file://"))
    expect_identical(replay$attempts[[1]]$md5,
                     unname(tools::md5sum(file.path(fixture$config$matches_dir, "f1_r2_sample.rds"))))
})

test_that("missing evidence, incompatible runs and incomplete rosters fail explicitly", {
    fixture <- diagnostic_fixture()
    unlink(file.path(fixture$config$matches_dir, "f1_r2_sample.rds"))
    expect_error(export_diagnostic_fixture(fixture), "Missing local replay input")
    fixture <- diagnostic_fixture()
    fixture$paths <- fixture$paths[1]
    expect_error(export_diagnostic_fixture(fixture), "Incomplete matching diagnostic items")
    fixture <- diagnostic_fixture()
    fixture$config$shared_support_controls <- FALSE
    expect_error(export_diagnostic_fixture(fixture), "Incompatible matching policy")
    fixture <- diagnostic_fixture()
    fixture$declarations$run_id <- "wrong-run"
    expect_error(export_diagnostic_fixture(fixture), "Incompatible diagnostic atom run")
    fixture <- diagnostic_fixture()
    sidecar <- fromJSON(fixture$paths[2], simplifyVector = FALSE)
    sidecar$failures[[1]]$expected_groups <- list("g", "missing")
    write_json(sidecar, fixture$paths[2], auto_unbox = TRUE)
    expect_error(export_diagnostic_fixture(fixture), "Incomplete terminal stratum")
    fixture <- diagnostic_fixture()
    sidecar <- fromJSON(fixture$paths[2], simplifyVector = FALSE)
    sidecar$failures[[1]]$group_diagnostics[[1]]$replay$sample_md5 <- paste(rep("0", 32), collapse = "")
    write_json(sidecar, fixture$paths[2], auto_unbox = TRUE)
    expect_error(export_diagnostic_fixture(fixture), "Corrupt local replay input")
})

test_that("identical retry evidence is deduplicated and conflicting attempts are rejected", {
    fixture <- diagnostic_fixture()
    fixture$paths <- c(fixture$paths, fixture$paths[1])
    result <- export_diagnostic_fixture(fixture)
    expect_equal(result$counts$attempts, 2L)
    expect_equal(result$counts$duplicate_attempts_deduplicated, 1L)
    fixture <- diagnostic_fixture()
    duplicate <- file.path(fixture$config$matches_dir, "mb_duplicate.json")
    sidecar <- fromJSON(fixture$paths[1], simplifyVector = FALSE)
    sidecar$diagnostics[[1]]$group_diagnostics[[1]]$reason_code <- "different"
    write_json(sidecar, duplicate, auto_unbox = TRUE)
    fixture$paths <- c(fixture$paths, duplicate)
    expect_error(export_diagnostic_fixture(fixture), "Conflicting diagnostic item")
})

test_that("explicit empty workflow work is complete but missing scheduled work is not", {
    fixture <- diagnostic_fixture()
    fixture$config$require_matching_diagnostics <- TRUE
    expect_error(write_matching_diagnostics(
        character(), fixture$config, fixture$fragments, fixture$complete
    ), "Missing matching diagnostics")
    fixture$config$diagnostic_expected_ids <- integer()
    result <- write_matching_diagnostics(
        character(), fixture$config, fixture$fragments, fixture$complete,
        fixture$status, fixture$declarations
    )
    expect_true(result$diagnostics_complete)
    expect_equal(result$counts$expected_items, 0L)
    expect_equal(result$counts$attempts, 0L)
    expect_equal(result$counts$atoms, 2L)
})
