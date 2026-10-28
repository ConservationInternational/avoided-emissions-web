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
            n_eligible = 20, n_sampled = 2, n_control_pool = 100, n_control_sampled = 10
        ) %>% bind_cols(forest_cover(4, 1))
        if (k == 2) m <- filter(m, group == "stable")
        saveRDS(m, file.path(out, "matches", paste0("m_1_rep", k, ".rds")))
    }
    run_script("03_summarize_results.R", config_path)
    totals <- read.csv(file.path(out, "results_by_site_total.csv"))
    detail <- read.csv(file.path(out, "results_pixel_year_emissions.csv"))
    covariates <- read.csv(file.path(out, "results_pixel_covariates.csv"))
    summary <- fromJSON(file.path(out, "results_summary.json"))
    expect_equal(totals$n_sample_pixels, 1)
    expect_equal(totals$coverage_fraction, 10 / 60)
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
        m <- readRDS(file.path(out, "matches", "m_1_rep1.rds"))
        expect_true(all(is.na(m$pscore)))
        expect_equal(attr(m, "group_diagnostics")[[1]]$method, "constant")
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

        for (path in list.files(file.path(out, "matches"), "^m_.*\\.rds$", full.names = TRUE)) {
            m <- readRDS(path)
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
        expect_match(failed$error, "No overlapping")
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
