# End-to-end check of the matching (step 2) and summary (step 3) scripts on a
# small synthetic dataset.  Skipped when the spatial/matching packages used by
# the analysis container are not installed.

for (pkg in c("arrow", "sf", "optmatch", "MatchIt", "tidyverse", "httr")) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
        testthat::skip(paste("package not installed:", pkg))
    }
}
library(arrow, warn.conflicts = FALSE)
library(jsonlite)

scripts_dir <- normalizePath(Sys.getenv("R_SCRIPTS_DIR", "../scripts"))
source(file.path(scripts_dir, "stats_functions.R"))

FC_YEARS <- 2008:2016

forest_cover <- function(n, annual_loss) {
    # Forest cover (percent): 1 point/year loss before the 2014 start, then
    # `annual_loss` points/year, so pre-intervention trends are comparable.
    fc <- sapply(FC_YEARS, function(y) {
        100 - (min(y, 2013) - 2008) - annual_loss * max(0, y - 2013)
    })
    out <- matrix(rep(fc, each = n), nrow = n, dimnames = list(NULL, paste0("fc_", FC_YEARS)))
    as_tibble(out)
}

make_fixture <- function(data_dir, method, n_replicates) {
    out <- file.path(data_dir, "output")
    dir.create(file.path(out, "matches"), recursive = TRUE)
    set.seed(42)
    # Fragments: site A is split across two strata; site C has no controls.
    sites <- tibble(
        site_id = c("A", "A", "B", "C"), site_name = c("A", "A", "B", "C"),
        id_numeric = 1:4, sub_site_index = c(0L, 1L, 0L, 0L),
        start_year = 2014L, end_year = 2016L, area_ha = 3000
    )
    strata <- tibble(id_numeric = 1:4, admin1 = c(1L, 2L, 1L, 3L), ecoregion = c(10, 20, 10, 30))
    n_t <- 30
    treatment <- strata[rep(1:4, each = n_t), ] %>%
        mutate(cell = seq_len(n()), area_ha = 100, x1 = rnorm(n(), 0.3),
               total_biomass_2000 = 100, fc_2000 = 100) %>%
        bind_cols(forest_cover(4 * n_t, 1))
    # One non-forest treatment pixel in site B: not eligible for matching.
    treatment[treatment$id_numeric == 3, ][1, paste0("fc_", FC_YEARS)] <- 0
    n_c <- 400
    controls <- strata[rep(1:2, each = n_c), ] %>%
        mutate(cell = 10000L + seq_len(n()), area_ha = 100, x1 = rnorm(n()),
               total_biomass_2000 = 100, fc_2000 = 100) %>%
        bind_cols(forest_cover(2 * n_c, 3))
    controls$total_biomass_2000[1] <- NA

    write_parquet(sites, file.path(out, "sites_processed.parquet"))
    write_parquet(select(treatment, cell, id_numeric), file.path(out, "treatment_cell_key.parquet"))
    write_parquet(select(treatment, -id_numeric), file.path(out, "treatment_pixels.parquet"))
    write_dataset(controls, file.path(out, "control_pixels"), partitioning = "admin1")
    write_json(list(formula_str = "treatment ~ x1"), file.path(out, "formula.json"), auto_unbox = TRUE)
    write_json(list(width = 1000, transform = c(0.01, 0, 0, 0, -0.01, 10)),
               file.path(out, "grid_metadata.json"), auto_unbox = TRUE)
    config <- list(
        data_dir = data_dir, task_id = "test", fc_years = FC_YEARS,
        max_treatment_pixels = 20, control_multiplier = 5, min_glm_treatment_pixels = 5,
        caliper_width = 1, max_controls_per_treatment = 2, min_control_distance_km = 0,
        exact_match_vars = c("admin1", "ecoregion"), matching_method = method,
        n_replicates = n_replicates, random_seed = 7,
        exact_match_group_mapping = list(
            "1" = list(list("A", 0), list("B", 0)), "2" = list(list("A", 1)),
            "3" = list(list("C", 0))
        )
    )
    path <- file.path(data_dir, "config.json")
    write_json(config, path, auto_unbox = TRUE)
    path
}

run_script <- function(script, config_path, array_index = NULL) {
    env <- c(paste0("R_SCRIPTS_DIR=", scripts_dir), paste0("R_LIBS=", paste(.libPaths(), collapse = ":")))
    if (!is.null(array_index)) env <- c(env, paste0("AWS_BATCH_JOB_ARRAY_INDEX=", array_index))
    log <- suppressWarnings(system2(
        "Rscript", c(file.path(scripts_dir, script), "--config", config_path),
        env = env, stdout = TRUE, stderr = TRUE
    ))
    status <- attr(log, "status")
    if (!is.null(status) && status != 0) stop(paste(c(script, "failed:", tail(log, 30)), collapse = "\n"))
    log
}

read_batches <- function(out) {
    paths <- list.files(file.path(out, "matches"), "^mb_.*\\.parquet$", full.names = TRUE)
    bind_rows(lapply(paths, read_parquet))
}

read_sidecars <- function(out) {
    lapply(list.files(file.path(out, "matches"), "^mb_.*\\.json$", full.names = TRUE),
           fromJSON, simplifyVector = FALSE)
}

test_that("summary uses the same complete strata for estimates and pixel downloads", {
    data_dir <- tempfile("ae_summary_")
    config_path <- make_fixture(data_dir, "nearest", n_replicates = 2)
    out <- file.path(data_dir, "output")
    for (k in 1:2) {
        m <- tibble(
            cell = 1:4, treatment = c(TRUE, FALSE, TRUE, FALSE),
            group = c("stable", "stable", "unstable", "unstable"),
            match_group = c("stable_1", "stable_1", "unstable_1", "unstable_1"),
            match_weight = 1, sampling_weight = 10,
            area_ha = 100, total_biomass_2000 = 100, fc_2000 = 100, x1 = 0, pscore = 0.5,
            n_eligible = 20, n_sampled = 2, n_control_pool = 100, n_control_sampled = 10,
            id_numeric = 1L, replicate = k, n_forest_pixels = 30L
        ) %>% bind_cols(forest_cover(4, 1))
        if (k == 2) m <- filter(m, group == "stable")
        write_parquet(m, file.path(out, "matches", paste0("mb_u1_b", k, ".parquet")))
        write_json(list(batch = paste0("u1_b", k), matches_file = paste0("mb_u1_b", k, ".parquet"),
                        failures = list(), diagnostics = list(), na_exclusions = list()),
                   file.path(out, "matches", paste0("mb_u1_b", k, ".json")), auto_unbox = TRUE)
    }
    run_script("03_summarize_results.R", config_path)
    totals <- read.csv(file.path(out, "results_by_site_total.csv"))
    detail <- read.csv(file.path(out, "results_pixel_year_emissions.csv"))
    covariates <- read.csv(file.path(out, "results_pixel_covariates.csv"))
    summary <- fromJSON(file.path(out, "results_summary.json"))
    expect_equal(totals$n_sample_pixels, 1)
    expect_equal(totals$coverage_fraction, 10 / 60)
    # Fragment 2 of site A has no output, so the site is only partly estimated.
    expect_equal(totals$forest_coverage_fraction, 10 / 30)
    expect_equal(totals$status, "partial")
    expect_equal(sort(unique(detail$cell)), sort(unique(covariates$cell)))
    expect_equal(unique(detail$cell), 1:2)
    expect_true(any(summary$methodology_warnings$code == "inconsistent_strata"))
    # Pixel-level weighted totals agree with the reported annual sample.
    by_year <- read.csv(file.path(out, "results_by_site_year.csv"))
    detail_totals <- detail %>%
        filter(treatment) %>%
        group_by(year) %>%
        summarise(loss = sum(forest_loss_ha * match_weight), .groups = "drop")
    expect_equal(detail_totals$loss, by_year$sample_treatment_defor_ha)
})

test_that("summary reports no estimates when all fragments fail", {
    data_dir <- tempfile("ae_empty_")
    config_path <- make_fixture(data_dir, "optimal", n_replicates = 1)
    run_script("03_summarize_results.R", config_path)
    out <- file.path(data_dir, "output")
    summary <- fromJSON(file.path(out, "results_summary.json"))
    expect_equal(summary$n_sites, 0)
    expect_equal(summary$n_failed_sites, 3)
    expect_equal(nrow(read.csv(file.path(out, "results_by_site_total.csv"))), 0)
    expect_equal(nrow(read.csv(file.path(out, "results_pixel_year_emissions.csv"))), 0)
})

test_that("overlapping features keep independent treatment memberships and results", {
    data_dir <- tempfile("ae_overlap_")
    config_path <- make_fixture(data_dir, "nearest", n_replicates = 1)
    out <- file.path(data_dir, "output")
    sites <- read_parquet(file.path(out, "sites_processed.parquet"))
    sites$display_site_id <- "duplicate display label"
    write_parquet(sites, file.path(out, "sites_processed.parquet"))
    key <- read_parquet(file.path(out, "treatment_cell_key.parquet"))
    original_cell <- key$cell[which(key$id_numeric == 3)[1]]
    shared_cell <- key$cell[which(key$id_numeric == 1)[1]]
    key$cell[key$cell == original_cell] <- shared_cell
    write_parquet(key, file.path(out, "treatment_cell_key.parquet"))
    pixels <- read_parquet(file.path(out, "treatment_pixels.parquet"))
    write_parquet(filter(pixels, cell != original_cell), file.path(out, "treatment_pixels.parquet"))
    write_json(list(formula_str = "treatment ~ factor(admin1)"),
               file.path(out, "formula.json"), auto_unbox = TRUE)
    config <- fromJSON(config_path, simplifyVector = FALSE)
    config$max_treatment_pixels <- 100
    write_json(config, config_path, auto_unbox = TRUE)
    run_script("02_perform_matching.R", config_path)
    batch <- read_batches(out)
    for (id in c(1, 3)) {
        matched <- filter(batch, id_numeric == id, replicate == 1)
        expect_true(shared_cell %in% matched$cell[matched$treatment])
    }
    run_script("03_summarize_results.R", config_path)
    totals <- read.csv(file.path(out, "results_by_site_total.csv"))
    expect_equal(sort(totals$site_id), c("A", "B"))
    identity <- read.csv(file.path(out, "site_identity.csv"))
    expect_equal(sort(identity$site_id), c("A", "B", "C"))
    expect_true(all(identity$display_site_id == "duplicate display label"))
})

for (method in c("optimal", "nearest")) {
    test_that(paste(method, "supports constant factor covariates and a single replicate"), {
        data_dir <- tempfile("ae_constant_")
        config_path <- make_fixture(data_dir, method, n_replicates = 1)
        out <- file.path(data_dir, "output")
        write_json(list(formula_str = "treatment ~ factor(admin1)"),
                   file.path(out, "formula.json"), auto_unbox = TRUE)
        if (method == "optimal") {
            config <- fromJSON(config_path, simplifyVector = FALSE)
            config$max_controls_per_treatment <- 0
            write_json(config, config_path, auto_unbox = TRUE)
        }
        run_script("02_perform_matching.R", config_path)
        run_script("03_summarize_results.R", config_path)
        totals <- read.csv(file.path(out, "results_by_site_total.csv"))
        expect_equal(sort(totals$site_id), c("A", "B"))
        expect_false(any(grepl("_ci_", names(totals))))
        m <- filter(read_batches(out), id_numeric == 1)
        expect_true(all(is.na(m$pscore)))
        diagnostics <- read_sidecars(out)[[1]]$diagnostics
        expect_equal(diagnostics[[1]]$group_diagnostics[[1]]$method, "constant")
    })
}

for (method in c("optimal", "nearest")) {
    test_that(paste("pipeline produces consistent results with", method, "matching"), {
        data_dir <- tempfile("ae_")
        config_path <- make_fixture(data_dir, method, n_replicates = 3)
        # 3 groups x 3 replicates with one group per array element, plus one
        # out-of-range element that must do nothing.
        for (i in 0:9) run_script("02_perform_matching.R", config_path, i)
        run_script("03_summarize_results.R", config_path)
        out <- file.path(data_dir, "output")

        batches <- read_batches(out)
        expect_equal(nrow(distinct(batches, id_numeric, replicate)), 9)
        for (m in split(batches, list(batches$id_numeric, batches$replicate), drop = TRUE)) {
            sets <- m %>%
                group_by(match_group) %>%
                summarise(t = sum(match_weight[treatment]), c = sum(match_weight[!treatment]))
            expect_equal(sets$t, sets$c)
            expect_false(any(is.na(m$total_biomass_2000)))
            expect_true(all(m[[paste0("fc_", 2008)]] > 0))
        }

        totals <- read.csv(file.path(out, "results_by_site_total.csv"))
        by_year <- read.csv(file.path(out, "results_by_site_year.csv"))
        summary <- fromJSON(file.path(out, "results_summary.json"))
        failed <- read.csv(file.path(out, "results_failed_sites.csv"))

        expect_equal(sort(totals$site_id), c("A", "B"))
        expect_equal(failed$site_id, "C")
        expect_equal(failed$status, "failed")
        expect_match(failed$error, "No overlapping")
        status <- read.csv(file.path(out, "results_site_status.csv"))
        expect_equal(setNames(status$status, status$site_id),
                     c(A = "estimated", B = "estimated", C = "failed")[status$site_id])
        # Site B's non-forest pixel is outside the forest denominator.
        expect_equal(totals$forest_coverage_fraction, c(1, 1))
        reps <- read.csv(file.path(out, "results_by_site_replicate_total.csv"))
        expect_equal(nrow(reps), 6)
        rep_means <- tapply(reps$extrapolated_emissions_avoided_mgco2e, reps$site_id, mean)
        expect_equal(as.vector(rep_means[totals$site_id]),
                     totals$extrapolated_emissions_avoided_mgco2e)
        expect_equal(summary$n_failed_sites, 1)
        expect_equal(summary$biomass_reference_year, 2000)
        expect_false("sub_site_index" %in% names(by_year))
        expect_true(all(totals$sampled_fraction <= 1))
        expect_true(all(totals$extrapolated_emissions_avoided_mgco2e > 0))
        expect_true(all(totals$extrapolated_emissions_avoided_mgco2e_ci_lower <=
                        totals$extrapolated_emissions_avoided_mgco2e_ci_upper))
        # Site A sums two fragments; B lost one non-forest pixel.
        expect_equal(totals$n_treatment_pixels[totals$site_id == "A"], 60)
        expect_lt(totals$coverage_fraction[totals$site_id == "B"], 1)
        # The portfolio mean equals the sum of site means.
        expect_equal(summary$portfolio$total$extrapolated_emissions_avoided_mgco2e,
                     sum(totals$extrapolated_emissions_avoided_mgco2e))
        expect_equal(summary$extrapolated_total_emissions_avoided_mgco2e,
                     sum(totals$extrapolated_emissions_avoided_mgco2e))
        yearly <- summary$portfolio$yearly
        expect_equal(nrow(yearly), length(unique(by_year$year)))
        expect_true(all(c("cum_control_defor_ha_ci_upper", "emissions_avoided_mgco2e_ci_lower")
                        %in% names(yearly)))
        expect_true(any(summary$methodology_warnings$code == "replicate_range"))
    })
}

test_that("replicate blocks reproduce single-replicate matches", {
    run_blocked <- function(block_size, n_array) {
        data_dir <- tempfile("ae_block_")
        config_path <- make_fixture(data_dir, "nearest", n_replicates = 3)
        config <- fromJSON(config_path, simplifyVector = FALSE)
        config$replicate_block_size <- block_size
        write_json(config, config_path, auto_unbox = TRUE)
        for (i in seq_len(n_array) - 1L) run_script("02_perform_matching.R", config_path, i)
        out <- file.path(data_dir, "output")
        list(
            n_batches = length(list.files(file.path(out, "matches"), "^mb_.*\\.json$")),
            matches = read_batches(out) %>% arrange(id_numeric, replicate, cell, treatment)
        )
    }
    single <- run_blocked(1L, 9L)
    blocked <- run_blocked(2L, 6L)
    expect_equal(single$n_batches, 9)
    expect_equal(blocked$n_batches, 6)
    cols <- c("id_numeric", "replicate", "cell", "treatment", "match_weight", "sampling_weight")
    expect_equal(blocked$matches[cols], single$matches[cols])
})

test_that("fragments without baseline forest are not applicable", {
    data_dir <- tempfile("ae_noforest_")
    config_path <- make_fixture(data_dir, "nearest", n_replicates = 1)
    out <- file.path(data_dir, "output")
    pixels <- read_parquet(file.path(out, "treatment_pixels.parquet"))
    key <- read_parquet(file.path(out, "treatment_cell_key.parquet"))
    c_cells <- key$cell[key$id_numeric == 4]
    pixels[pixels$cell %in% c_cells, paste0("fc_", FC_YEARS)] <- 0
    write_parquet(pixels, file.path(out, "treatment_pixels.parquet"))
    run_script("02_perform_matching.R", config_path)
    run_script("03_summarize_results.R", config_path)
    failed <- read.csv(file.path(out, "results_failed_sites.csv"))
    summary <- fromJSON(file.path(out, "results_summary.json"))
    expect_equal(failed$site_id, "C")
    expect_equal(failed$status, "not_applicable")
    expect_equal(summary$n_failed_sites, 0)
    expect_equal(summary$n_not_applicable_sites, 1)
    expect_equal(length(summary$failed_sites), 0)
})
