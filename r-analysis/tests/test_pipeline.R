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

run_script <- function(script, config_path, array_index = NULL, script_dir = scripts_dir) {
    env <- c(paste0("R_SCRIPTS_DIR=", script_dir), paste0("R_LIBS=", paste(.libPaths(), collapse = ":")))
    if (!is.null(array_index)) env <- c(env, paste0("AWS_BATCH_JOB_ARRAY_INDEX=", array_index))
    log <- suppressWarnings(system2(
        "Rscript", c(file.path(script_dir, script), "--config", config_path),
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


write_fixture_matches <- function(out, batches) {
    # Write hand-made matches the way step 2 does: treatment rows carry their
    # cell's eligible and sampled pixel counts, and each sidecar the census of
    # observed outcomes by cell.  The census here equals the expanded sample,
    # as if the matched treatment pixels were every eligible pixel.
    cells_of <- function(m) intersect(c("group", "atomic_id"), names(m))
    batches <- lapply(batches, function(m) {
        if ("cell_n_eligible" %in% names(m)) return(m)
        m %>%
            group_by(across(all_of(cells_of(m)))) %>%
            mutate(
                cell_n_eligible = if_else(treatment, sampling_weight * sum(treatment), NA_real_),
                cell_n_sampled = if_else(treatment, as.numeric(sum(treatment)), NA_real_)
            ) %>%
            ungroup()
    })
    everything <- bind_rows(batches)
    census <- lapply(split(everything, everything$id_numeric), function(d) {
        by <- cells_of(d)
        treated <- d %>%
            filter(treatment) %>%
            group_by(across(all_of(by))) %>%
            filter(replicate == min(replicate)) %>%
            ungroup()
        tryCatch({
            outcomes <- pixel_outcomes(treated, 2014, min(FC_YEARS), max(FC_YEARS))
            rows <- split(seq_len(nrow(treated)), cell_key(treated, by))
            list(
                id_numeric = d$id_numeric[1], years = outcomes$years,
                cells = unname(lapply(rows, function(r) {
                    w <- treated$sampling_weight[r[1]]
                    c(lapply(as.list(treated[r[1], by]), as.character), list(
                        n_pixels = treated$cell_n_eligible[r[1]],
                        area_ha = sum(treated$area_ha[r]) * w,
                        loss_ha = as.list(colSums(outcomes$loss_ha[r, , drop = FALSE]) * w),
                        emissions_mgco2e =
                            as.list(colSums(outcomes$emissions[r, , drop = FALSE]) * w)
                    ))
                }))
            )
        }, invalid_pixel_outcomes = function(e) {
            list(id_numeric = d$id_numeric[1], error = conditionMessage(e))
        })
    })
    for (batch in names(batches)) {
        m <- batches[[batch]]
        write_parquet(m, file.path(out, "matches", paste0("mb_", batch, ".parquet")))
        write_json(
            list(batch = batch, matches_file = paste0("mb_", batch, ".parquet"),
                 failures = list(), diagnostics = list(), na_exclusions = list(),
                 treatment_census = unname(census[as.character(unique(m$id_numeric))])),
            file.path(out, "matches", paste0("mb_", batch, ".json")),
            auto_unbox = TRUE, digits = NA
        )
    }
}

test_that("summary uses the same included strata for estimates and pixel downloads", {
    data_dir <- tempfile("ae_summary_")
    config_path <- make_fixture(data_dir, "nearest", n_replicates = 2)
    out <- file.path(data_dir, "output")
    batches <- list()
    for (k in 1:2) {
        m <- tibble(
            cell = 1:4, treatment = c(TRUE, FALSE, TRUE, FALSE),
            group = c("stable", "stable", "unstable", "unstable"),
            match_group = c("stable_1", "stable_1", "unstable_1", "unstable_1"),
            match_weight = 1, sampling_weight = 10,
            area_ha = 100, total_biomass_2000 = 100, fc_2000 = 100, x1 = 0, pscore = 0.5,
            defor_pre_intervention = 1,
            n_eligible = 20, n_sampled = 2, n_control_pool = 100, n_control_sampled = 10,
            id_numeric = 1L, replicate = k, n_forest_pixels = 30L
        ) %>% bind_cols(forest_cover(4, 1))
        if (k == 2) m <- filter(m, group == "stable")
        batches[[paste0("u1_b", k)]] <- m
    }
    write_fixture_matches(out, batches)
    run_script("03_summarize_results.R", config_path)
    totals <- read.csv(file.path(out, "results_by_site_total.csv"))
    detail <- read.csv(file.path(out, "results_pixel_year_emissions.csv"))
    covariates <- read.csv(file.path(out, "results_pixel_covariates.csv"))
    replicate_balance <- read.csv(
        file.path(out, "results_covariate_balance_replicates.csv")
    )
    summary <- fromJSON(file.path(out, "results_summary.json"))
    match_quality <- fromJSON(file.path(out, "results_match_quality_summary.json"))
    expect_equal(totals$n_sample_pixels, 1)
    expect_equal(totals$coverage_fraction, 10 / 60)
    # Fragment 2 of site A has no output, so the site is only partly estimated.
    expect_equal(totals$forest_coverage_fraction, 10 / 30)
    expect_equal(totals$status, "partial")
    expect_equal(sort(unique(detail$cell)), sort(unique(covariates$cell)))
    expect_equal(unique(detail$cell), 1:2)
    expect_true(all(c(
        "site_id", "treatment", "match_weight", "pscore", "x1"
    ) %in% names(covariates)))
    expect_equal(sort(unique(replicate_balance$replicate)), 1:2)
    expect_true(all(c("matched_sample", "represented_domain") %in%
                    unique(replicate_balance$scope)))
    expect_true(all(c("portfolio", "matching_unit", "fragment", "stratum") %in%
                    unique(replicate_balance$scope_type)))
    expect_equal(unique(replicate_balance$task_id), "test")
    expect_true(all(c("baseline_forest_ha", "baseline_biomass_mg",
                      "defor_pre_intervention") %in%
                    unique(sub("=.*$", "", replicate_balance$covariate))))
    expect_true(all(c(
        "n_treatment", "n_control", "weight_treatment", "weight_control",
        "mean_difference", "sd_method", "weight_method",
        "n_replicates_defined", "worst_abs_smd",
        "share_replicates_abs_smd_gt_010", "status"
    ) %in% names(replicate_balance)))
    expect_true("categorical_cols" %in% names(match_quality))
    expect_true(any(summary$methodology_warnings$code == "inconsistent_strata"))
    # Pixel-level weighted totals agree with the reported annual sample.
    by_year <- read.csv(file.path(out, "results_by_site_year.csv"))
    expect_true(all(c(
        "extrapolated_cumulative_treatment_defor_ha",
        "extrapolated_cumulative_control_defor_ha",
        "extrapolated_cumulative_treatment_defor_ha_ci_lower",
        "extrapolated_cumulative_treatment_defor_ha_ci_upper",
        "extrapolated_cumulative_control_defor_ha_ci_lower",
        "extrapolated_cumulative_control_defor_ha_ci_upper"
    ) %in% names(by_year)))
    detail_totals <- detail %>%
        filter(treatment) %>%
        group_by(year) %>%
        summarise(loss = sum(forest_loss_ha * match_weight), .groups = "drop")
    expect_equal(detail_totals$loss, by_year$sample_treatment_defor_ha)
})

test_that("atom outputs require complete replicate domains and reconcile parent results", {
    data_dir <- tempfile("ae_atoms_")
    config_path <- make_fixture(data_dir, "nearest", n_replicates = 2)
    out <- file.path(data_dir, "output")
    atoms <- list(
        A = list(
            list(atomic_id = 101, geometry = list(type = "Polygon", coordinates = list())),
            list(atomic_id = 102, geometry = list(type = "Polygon", coordinates = list()))
        ),
        B = list(
            list(atomic_id = 201, geometry = list(type = "Polygon", coordinates = list()))
        ),
        C = list(
            list(atomic_id = 301, geometry = list(type = "Polygon", coordinates = list()))
        )
    )
    sites <- read_parquet(file.path(out, "sites_processed.parquet"))
    sites$ae_atoms <- vapply(sites$site_id, function(site_id) {
        toJSON(
            list(run_id = "kpi-job-42", atoms = atoms[[site_id]]),
            auto_unbox = TRUE, null = "null"
        )
    }, character(1))
    write_parquet(sites, file.path(out, "sites_processed.parquet"))

    keys <- bind_rows(lapply(1:3, function(id) {
        tibble(
            cell = seq_len(10) + id * 100,
            id_numeric = id,
            site_id = c("A", "A", "B")[id],
            atomic_id = c("101", "102", "201")[id]
        )
    }))
    write_parquet(keys, file.path(out, "treatment_cell_key.parquet"))
    write_parquet(
        tibble(
            site_id = c("A", "A", "B", "C"),
            atomic_id = c(101, 102, 201, 301),
            n_site_pixels = c(10, 10, 10, 0),
            n_forest_pixels = c(10, 10, 8, 0)
        ),
        file.path(out, "ae_atom_counts.parquet")
    )

    batches <- list()
    for (id in 1:3) {
        for (replicate in if (id == 2) 1 else 1:2) {
            treatment <- tibble(
                cell = c(id * 10, id * 10 + 1),
                treatment = c(TRUE, FALSE),
                atomic_id = c(c(101L, 102L, 201L)[id], NA_integer_),
                group = paste0("group", id),
                match_group = paste0("group", id, "_set"),
                match_weight = 1,
                sampling_weight = c(5, 5, 8)[id],
                area_ha = 100,
                total_biomass_2000 = 100,
                fc_2000 = 100,
                x1 = 0,
                pscore = 0.5,
                n_eligible = 10,
                n_sampled = 1,
                n_control_pool = 100,
                n_control_sampled = 5,
                id_numeric = id,
                replicate = replicate,
                n_forest_pixels = 10
            ) %>% bind_cols(forest_cover(2, 1))
            if (id == 1 && replicate == 1) {
                # Atom 101 is estimated from its included stratum only.
                treatment <- bind_rows(treatment, treatment %>%
                    mutate(cell = cell + 100L, group = "unstable",
                           match_group = "unstable_set"))
            }
            batches[[paste0("atoms", id, "_", replicate)]] <- treatment
        }
    }
    write_fixture_matches(out, batches)

    run_script("03_summarize_results.R", config_path)
    atom_status <- read.csv(file.path(out, "results_atom_status.csv"))
    atom_reps <- read.csv(file.path(out, "results_by_atom_replicate_year.csv"))
    parent_reps <- read.csv(file.path(out, "results_by_site_replicate_year.csv"))
    summary <- fromJSON(file.path(out, "results_summary.json"))
    expect_equal(summary$atom_results_schema_version, 2)
    expect_equal(summary$atom_run_id, "kpi-job-42")
    expect_true(is.integer(summary$n_replicates))
    expect_type(atom_reps$is_intervention_period, "logical")
    expect_equal(
        atom_status$first_year[match(c("101", "201"), atom_status$atomic_id)],
        c(2009, 2009)
    )
    expect_equal(
        atom_status$last_year[match(c("101", "201"), atom_status$atomic_id)],
        c(2016, 2016)
    )
    expect_equal(
        setNames(atom_status$status, atom_status$atomic_id),
        c("101" = "partial", "102" = "failed", "201" = "estimated", "301" = "not_applicable")
    )
    # Coverage counts eligible pixels in included strata, not every forest
    # pixel of an atom that has an estimate: atom 101 loses its unstable
    # stratum, and atom 102 matched in one of two replicates.
    by_atom <- atom_status[match(c("101", "102", "201", "301"), atom_status$atomic_id), ]
    expect_equal(by_atom$n_eligible_pixels, c(10, 5, 8, 0))
    expect_equal(by_atom$n_supported_pixels, c(5, 0, 8, 0))
    expect_equal(by_atom$forest_coverage_fraction, c(0.5, 0, 1, 0))
    expect_equal(by_atom$treatment_arm, c("census", NA, "census", NA))
    expect_match(by_atom$status_reason[1], "50% of the atom's baseline forest")
    expect_match(by_atom$status_reason[2], "at least 80% of replicates")
    site_status <- read.csv(file.path(out, "results_site_status.csv"))
    expect_equal(site_status$forest_coverage_fraction[site_status$site_id == "A"], 5 / 20)
    expect_equal(summary$coverage$n_represented_pixels, 13)
    expect_equal(summary$estimator$min_replicate_share, 0.8)
    expect_false(any(atom_reps$atomic_id == 102))
    expect_true(all(c("run_id", "atomic_id", "site_id", "replicate", "year",
                      "is_intervention_period",
                      "extrapolated_emissions_avoided_mgco2e",
                      "extrapolated_forest_loss_avoided_ha",
                      "extrapolated_control_emissions_mgco2e") %in% names(atom_reps)))
    expect_equal(sort(unique(atom_reps$year)), 2009:2016)
    expect_equal(
        sort(unique(atom_reps$year[atom_reps$is_intervention_period])),
        2014:2016
    )
    parent_from_atoms <- atom_reps %>%
        group_by(site_id, replicate, year) %>%
        summarise(
            across(starts_with("extrapolated_"), sum),
            .groups = "drop"
        ) %>%
        arrange(site_id, replicate, year)
    expect_equal(
        as.data.frame(parent_from_atoms),
        parent_reps %>% arrange(site_id, replicate, year)
    )
})

fixture_pair <- function(id, k, group, weight = 10, control_loss = 1) {
    # One treatment pixel losing 1 point a year and its matched control.
    tibble(
        cell = c(1L, 2L) + 10L * match(group, c("kept", "dropped", "guarded")),
        treatment = c(TRUE, FALSE), group = group, match_group = paste0(group, "_1"),
        match_weight = 1, sampling_weight = weight,
        area_ha = 100, total_biomass_2000 = 100, fc_2000 = 100, x1 = 0, pscore = 0.5,
        n_eligible = 20, n_sampled = 2, n_control_pool = 100, n_control_sampled = 10,
        id_numeric = id, replicate = k, n_forest_pixels = 30L
    ) %>% bind_cols(bind_rows(forest_cover(1, 1), forest_cover(1, control_loss)))
}

test_that("a stratum matched in 80% of replicates is kept and its missing replicate filled", {
    data_dir <- tempfile("ae_share_")
    config_path <- make_fixture(data_dir, "nearest", n_replicates = 5)
    out <- file.path(data_dir, "output")
    batches <- list()
    for (k in 1:5) {
        # "kept" matches in 4 of 5 replicates, "dropped" in 3 of 5; the control
        # loses more forest in later replicates.
        m <- bind_rows(
            if (k <= 4) fixture_pair(1L, k, "kept", control_loss = k),
            if (k <= 3) fixture_pair(1L, k, "dropped", control_loss = 9)
        )
        if (nrow(m)) batches[[paste0("u1_b", k)]] <- m
    }
    write_fixture_matches(out, batches)
    run_script("03_summarize_results.R", config_path)
    totals <- read.csv(file.path(out, "results_by_site_total.csv"))
    reps <- read.csv(file.path(out, "results_by_site_replicate_total.csv")) %>% arrange(replicate)
    detail <- read.csv(file.path(out, "results_pixel_year_emissions.csv"))
    summary <- fromJSON(file.path(out, "results_summary.json"))
    expect_equal(reps$replicate, 1:5)
    # 3 intervention years, 10 eligible pixels of 100 ha, control losing k points a year.
    expect_equal(reps$extrapolated_control_defor_ha, c(30, 60, 90, 120, 75))
    expect_equal(reps$extrapolated_control_defor_ha[5], mean(reps$extrapolated_control_defor_ha[1:4]))
    expect_true(all(reps$extrapolated_treatment_defor_ha == 30))
    expect_equal(totals$extrapolated_control_defor_ha, 75)
    expect_equal(totals$n_represented_pixels, 10)
    expect_equal(sort(unique(detail$cell)), c(11, 12))
    expect_true(any(summary$methodology_warnings$code == "inconsistent_strata"))
    expect_match(summary$methodology_warnings$message[
        summary$methodology_warnings$code == "inconsistent_strata"], "fewer than 80% of replicates")
})

test_that("a low match rate is not expanded over unmatched pixels", {
    data_dir <- tempfile("ae_guard_")
    config_path <- make_fixture(data_dir, "nearest", n_replicates = 1)
    out <- file.path(data_dir, "output")
    # 100 eligible pixels, 10 sampled, 1 matched: a 10% match rate.
    guarded <- fixture_pair(1L, 1L, "guarded", weight = 100, control_loss = 3) %>%
        mutate(cell_n_eligible = if_else(treatment, 100, NA_real_),
               cell_n_sampled = if_else(treatment, 10, NA_real_))
    write_fixture_matches(out, list(u1_b1 = guarded))
    run_script("03_summarize_results.R", config_path)
    totals <- read.csv(file.path(out, "results_by_site_total.csv"))
    detail <- read.csv(file.path(out, "results_pixel_year_emissions.csv"))
    # The matched pixel stands for the 10 sampled-and-matchable pixels in 100
    # (eligible / sampled = 10), not for all 100.
    expect_equal(totals$n_represented_pixels, 10)
    expect_equal(totals$extrapolated_control_defor_ha, 10 * totals$sample_control_defor_ha)
    expect_equal(totals$extrapolated_treatment_defor_ha, 10 * totals$sample_treatment_defor_ha)
    expect_true(all(detail$sampling_weight == 10))
    expect_equal(totals$status, "partial")
})

test_that("summary rejects match outputs of another estimator", {
    data_dir <- tempfile("ae_estimator_")
    config_path <- make_fixture(data_dir, "nearest", n_replicates = 1)
    out <- file.path(data_dir, "output")
    m <- fixture_pair(1L, 1L, "kept")
    write_fixture_matches(out, list(u1_b1 = m))
    write_parquet(m, file.path(out, "matches", "mb_u1_b1.parquet"))
    expect_error(run_script("03_summarize_results.R", config_path), "lacks cell counts")
    write_fixture_matches(out, list(u1_b1 = m))
    sidecar_path <- file.path(out, "matches", "mb_u1_b1.json")
    sidecar <- fromJSON(sidecar_path, simplifyVector = FALSE)
    sidecar$compatibility <- list(task_id = "test", matching_policy = "shared_support_v2")
    write_json(sidecar, sidecar_path, auto_unbox = TRUE, digits = NA)
    expect_error(run_script("03_summarize_results.R", config_path), "another estimator version")
    sidecar$compatibility$estimator_version <- ESTIMATOR_VERSION
    write_json(sidecar, sidecar_path, auto_unbox = TRUE, digits = NA)
    run_script("03_summarize_results.R", config_path)
    expect_equal(fromJSON(file.path(out, "results_summary.json"))$estimator$version, 3)
})

test_that("atom runs sample every atom and take observed loss from the census", {
    data_dir <- tempfile("ae_census_")
    config_path <- make_fixture(data_dir, "nearest", n_replicates = 2)
    out <- file.path(data_dir, "output")
    sites <- read_parquet(file.path(out, "sites_processed.parquet"))
    atoms <- list(A = c("101", "102", "103"), B = "201", C = "301")
    sites$ae_atoms <- vapply(sites$site_id, function(site) {
        toJSON(list(run_id = "census-run", atoms = lapply(atoms[[site]], function(id) {
            list(atomic_id = id, geometry = list(type = "Polygon", coordinates = list()))
        })), auto_unbox = TRUE)
    }, character(1))
    write_parquet(sites, file.path(out, "sites_processed.parquet"))
    # Atom 102 is a single pixel inside fragment 1, which is larger than the cap.
    key <- read_parquet(file.path(out, "treatment_cell_key.parquet")) %>%
        group_by(id_numeric) %>%
        mutate(site_id = c("A", "A", "B", "C")[id_numeric],
               atomic_id = c("101", "103", "201", "301")[id_numeric],
               atomic_id = if_else(id_numeric == 1 & row_number() == 1, "102", atomic_id)) %>%
        ungroup()
    write_parquet(key, file.path(out, "treatment_cell_key.parquet"))
    forest <- read_parquet(file.path(out, "treatment_pixels.parquet")) %>% select(cell, fc_2008)
    key %>%
        left_join(forest, by = "cell") %>%
        group_by(site_id, atomic_id) %>%
        summarise(n_site_pixels = n(), n_forest_pixels = sum(fc_2008 > 0), .groups = "drop") %>%
        write_parquet(file.path(out, "ae_atom_counts.parquet"))
    config <- fromJSON(config_path, simplifyVector = FALSE)
    config$caliper_width <- 0
    write_json(config, config_path, auto_unbox = TRUE)
    run_script("02_perform_matching.R", config_path)
    matches <- read_batches(out)
    drawn <- matches %>% filter(treatment, id_numeric == 1) %>% count(replicate, atomic_id)
    expect_equal(drawn$atomic_id, rep(c("101", "102"), 2))
    weights <- matches %>% filter(treatment) %>%
        group_by(id_numeric, replicate, atomic_id) %>%
        summarise(represented = sum(sampling_weight), eligible = cell_n_eligible[1], .groups = "drop")
    expect_equal(weights$represented, weights$eligible)
    census <- unlist(lapply(read_sidecars(out), `[[`, "treatment_census"), recursive = FALSE)
    by_id <- setNames(census, vapply(census, function(x) as.character(x$id_numeric), ""))
    expect_setequal(names(by_id), as.character(1:4))
    pixels_of <- function(id) vapply(by_id[[id]]$cells, function(cell) cell$n_pixels, numeric(1))
    expect_setequal(pixels_of("1"), c(29, 1))
    expect_equal(pixels_of("3"), 29)
    expect_length(by_id[["4"]]$cells, 0)

    run_script("03_summarize_results.R", config_path)
    atom_status <- read.csv(file.path(out, "results_atom_status.csv"))
    by_atom <- atom_status[match(c(101, 102, 103, 201, 301), atom_status$atomic_id), ]
    expect_equal(by_atom$status, c(rep("estimated", 4), "failed"))
    expect_equal(by_atom$n_supported_pixels, c(29, 1, 30, 29, 0))
    expect_equal(by_atom$n_forest_pixels, c(29, 1, 30, 29, 30))
    expect_equal(by_atom$forest_coverage_fraction, c(1, 1, 1, 1, 0))
    expect_true(all(by_atom$treatment_arm[1:4] == "census"))
    # Every treatment pixel loses one point of 100 ha a year, so the observed
    # loss of a cell is exactly its eligible pixel count in every replicate.
    atom_reps <- read.csv(file.path(out, "results_by_atom_replicate_year.csv"))
    one <- filter(atom_reps, atomic_id == 102)
    expect_equal(nrow(one), 2 * 8)
    expect_true(all(one$extrapolated_treatment_defor_ha == 1))
    site_reps <- read.csv(file.path(out, "results_by_site_replicate_year.csv"))
    expect_true(all(site_reps$extrapolated_treatment_defor_ha[site_reps$site_id == "B"] == 29))
    expect_true(all(site_reps$extrapolated_treatment_defor_ha[site_reps$site_id == "A"] == 60))
    summary <- fromJSON(file.path(out, "results_summary.json"))
    check <- summary$treatment_census_check
    expect_equal(check$sampled_treatment_defor_ha, check$census_treatment_defor_ha)
    expect_equal(check$census_treatment_defor_ha[check$site_id == "B"], 29 * 8)
    # Both arms start fully forested and lose one point a year before 2014.
    stock <- summary$baseline_stock_balance
    expect_equal(stock$treatment_baseline_forest_ha[stock$site_id == "B"], 2900)
    expect_equal(stock$control_baseline_forest_ha, stock$treatment_baseline_forest_ha)
    expect_equal(stock$control_baseline_biomass_mg, stock$treatment_baseline_biomass_mg)
    pre <- summary$pre_intervention_check
    expect_equal(pre$pre_treatment_defor_ha[pre$site_id == "B"], 29 * 5)
    expect_equal(pre$pre_control_defor_ha, pre$pre_treatment_defor_ha)
})

test_that("summary preserves cell IDs across integer and integer64 match files", {
    data_dir <- tempfile("ae_cell_types_")
    config_path <- make_fixture(data_dir, "nearest", n_replicates = 1)
    out <- file.path(data_dir, "output")
    cells <- list(1:2, bit64::as.integer64(c("2316608818", "4798800599")))
    batches <- list()
    for (id in 1:2) {
        m <- tibble(
            cell = cells[[id]], treatment = c(TRUE, FALSE),
            group = paste0("g", id), match_group = paste0("g", id, "_1"),
            match_weight = 1, sampling_weight = 10,
            area_ha = 100, total_biomass_2000 = 100, fc_2000 = 100,
            x1 = 0, pscore = 0.5, n_eligible = 10, n_sampled = 1,
            n_control_pool = 100, n_control_sampled = 10,
            id_numeric = id, replicate = 1L, n_forest_pixels = 30L
        ) %>% bind_cols(forest_cover(2, 1))
        batches[[paste0("cells", id)]] <- m
    }
    write_fixture_matches(out, batches)
    run_script("03_summarize_results.R", config_path)
    expected_cells <- c(1, 2, 2316608818, 4798800599)
    for (name in c("pixel_year_emissions", "pixel_covariates", "propensity_scores", "pixel_locations")) {
        pixels <- read.csv(file.path(out, paste0("results_", name, ".csv")))
        expect_equal(sort(unique(pixels$cell)), expected_cells, info = name)
    }
    totals <- read.csv(file.path(out, "results_by_site_total.csv"))
    expect_equal(totals$site_id, "A")
    expect_equal(totals$n_sample_pixels, 2)
})

test_that("summary reports no estimates when all fragments fail", {
    data_dir <- tempfile("ae_empty_")
    config_path <- make_fixture(data_dir, "optimal", n_replicates = 1)
    config <- fromJSON(config_path, simplifyVector = FALSE)
    config$summary_workers <- 2L
    config$validated_match_files <- TRUE
    write_json(config, config_path, auto_unbox = TRUE)
    run_script("03_summarize_results.R", config_path)
    out <- file.path(data_dir, "output")
    summary <- fromJSON(file.path(out, "results_summary.json"))
    expect_equal(summary$n_sites, 0)
    expect_equal(summary$n_failed_sites, 3)
    expect_equal(nrow(read.csv(file.path(out, "results_by_site_total.csv"))), 0)
    expect_equal(nrow(read.csv(file.path(out, "results_pixel_year_emissions.csv"))), 0)
})

test_that("invalid outcomes exclude the entire site and preserve other sites", {
    data_dir <- tempfile("ae_invalid_outcomes_")
    config_path <- make_fixture(data_dir, "nearest", n_replicates = 2)
    out <- file.path(data_dir, "output")
    batches <- list()
    for (id in 1:3) {
        for (k in 1:2) {
            m <- tibble(
                cell = c(id * 10, id * 10 + 1), treatment = c(TRUE, FALSE),
                group = paste0("g", id), match_group = paste0("g", id, "_1"),
                match_weight = 1, sampling_weight = 10,
                area_ha = 100, total_biomass_2000 = 100, fc_2000 = 100,
                x1 = 0, pscore = 0.5, n_eligible = 10, n_sampled = 1,
                n_control_pool = 100, n_control_sampled = 10,
                id_numeric = id, replicate = k, n_forest_pixels = 30L
            ) %>% bind_cols(forest_cover(2, 1))
            if (id == 2 && k == 2) m$total_biomass_2000[2] <- -9999
            batches[[paste0("u", id, "_b", k)]] <- m
        }
    }
    write_fixture_matches(out, batches)
    log <- run_script("03_summarize_results.R", config_path)
    result_files <- list.files(out, "^results_.*\\.(csv|json)$", full.names = TRUE)
    baseline <- lapply(result_files, function(path) {
        if (grepl("\\.json$", path)) fromJSON(path, simplifyVector = FALSE) else read.csv(path)
    })
    config <- fromJSON(config_path, simplifyVector = FALSE)
    config$summary_workers <- 2L
    write_json(config, config_path, auto_unbox = TRUE)
    run_script("03_summarize_results.R", config_path)
    parallel_results <- lapply(result_files, function(path) {
        if (grepl("\\.json$", path)) fromJSON(path, simplifyVector = FALSE) else read.csv(path)
    })
    expect_equal(parallel_results, baseline)
    status <- read.csv(file.path(out, "results_site_status.csv"))
    failed <- read.csv(file.path(out, "results_failed_sites.csv"))
    totals <- read.csv(file.path(out, "results_by_site_total.csv"))
    detail <- read.csv(file.path(out, "results_pixel_year_emissions.csv"))
    covariates <- read.csv(file.path(out, "results_pixel_covariates.csv"))
    summary <- fromJSON(file.path(out, "results_summary.json"))
    expect_equal(status$status[status$site_id == "A"], "failed")
    expect_equal(totals$site_id, "B")
    expect_equal(unique(detail$site_id), "B")
    expect_equal(unique(covariates$site_id), "B")
    expect_equal(summary$n_failed_sites, 2)
    expect_equal(nrow(filter(failed, site_id == "A")), 2)
    expect_true(all(failed$reason[failed$site_id == "A"] == "invalid_outcome_inputs"))
    expect_match(status$status_reason[status$site_id == "A"], "fragment=2.*replicate=2")
    expect_match(status$status_reason[status$site_id == "A"], "cell=21.*value=-9999")
    expect_true(any(grepl("site_id=A.*mb_u2_b2.parquet.*cell=21.*value=-9999", log)))
    expect_true(any(grepl("cell=21.*value=-9999", summary$failed_sites$error)))
})

test_that("summary succeeds when the first and only matched site has invalid outcomes", {
    data_dir <- tempfile("ae_only_invalid_")
    config_path <- make_fixture(data_dir, "nearest", n_replicates = 1)
    out <- file.path(data_dir, "output")
    m <- tibble(
        cell = 1:2, treatment = c(TRUE, FALSE), group = "g",
        match_group = "g_1", match_weight = 1, sampling_weight = 10,
        area_ha = 100, total_biomass_2000 = c(-1, 100), fc_2000 = 100,
        x1 = 0, pscore = 0.5, n_eligible = 10, n_sampled = 1,
        n_control_pool = 100, n_control_sampled = 10,
        id_numeric = 1L, replicate = 1L, n_forest_pixels = 30L
    ) %>% bind_cols(forest_cover(2, 1))
    write_fixture_matches(out, list(invalid = m))
    run_script("03_summarize_results.R", config_path)
    status <- read.csv(file.path(out, "results_site_status.csv"))
    summary <- fromJSON(file.path(out, "results_summary.json"))
    expect_equal(status$status[status$site_id == "A"], "failed")
    expect_match(status$status_reason[status$site_id == "A"], "cell=1.*value=-1")
    expect_equal(summary$n_sites, 0)
    expect_equal(nrow(read.csv(file.path(out, "results_pixel_year_emissions.csv"))), 0)
    code <- paste(
        "import logging, sys",
        "from main import _collect_results",
        "result = _collect_results(sys.argv[1], 'test', logging.getLogger())",
        "assert result['summary']['n_sites'] == 0",
        "assert result['summary']['failed_sites'][0]['reason'] == 'invalid_outcome_inputs'",
        sep = "; "
    )
    log <- suppressWarnings(system2(
        Sys.getenv("AE_PYTHON", "python"),
        c("-c", shQuote(code), shQuote(out)),
        env = paste0("PYTHONPATH=", file.path(scripts_dir, "../src")), stdout = TRUE, stderr = TRUE
    ))
    expect_true(is.null(attr(log, "status")), info = paste(log, collapse = "\n"))
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
        yearly_reps <- read.csv(file.path(out, "results_by_site_replicate_year.csv"))
        expect_equal(nrow(yearly_reps), 3 * nrow(by_year))
        annual_means <- yearly_reps %>%
            group_by(site_id, year) %>%
            summarise(value = mean(extrapolated_treatment_defor_ha), .groups = "drop")
        expect_equal(
            arrange(annual_means, site_id, year)$value,
            arrange(by_year, site_id, year)$extrapolated_treatment_defor_ha
        )
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
        expect_true(all(c(
            "cum_control_defor_ha_ci_upper",
            "emissions_avoided_mgco2e_ci_lower",
            "cum_emissions_avoided_mgco2e_ci_lower"
        ) %in% names(yearly)))
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

for (method in c("optimal", "nearest")) {
test_that(paste(method, "checkpoints yield, resume strata, and preserve seeded matches"), {
    legacy <- tempfile("ae_legacy_")
    legacy_config <- make_fixture(legacy, method, n_replicates = 2)
    run_script("02_perform_matching.R", legacy_config)
    expected <- read_batches(file.path(legacy, "output")) %>%
        arrange(id_numeric, replicate, cell, treatment)

    data_dir <- tempfile("ae_checkpoint_")
    config_path <- make_fixture(data_dir, method, n_replicates = 2)
    config <- fromJSON(config_path, simplifyVector = FALSE)
    config$match_checkpoints <- TRUE
    config$checkpoint_uri <- tempfile("ae_store_")
    config$match_work <- list(ids = 1:4, replicates = 1:2)
    config$match_soft_seconds <- 0
    write_json(config, config_path, auto_unbox = TRUE)
    run_script("02_perform_matching.R", config_path)
    expect_length(read_sidecars(file.path(data_dir, "output")), 0)
    expect_equal(nrow(fromJSON(file.path(data_dir, "unfinished.json"))), 8)

    config$match_soft_seconds <- 3600
    write_json(config, config_path, auto_unbox = TRUE)
    run_script("02_perform_matching.R", config_path)
    out <- file.path(data_dir, "output")
    actual <- read_batches(out) %>% arrange(id_numeric, replicate, cell, treatment)
    expect_equal(actual, expected)
    expect_length(read_sidecars(out), 8)
    expect_length(fromJSON(file.path(data_dir, "unfinished.json")), 0)
    receipts <- list.files(config$checkpoint_uri, "\\.receipt\\.json$", recursive = TRUE)
    expect_true("f1/mb_f1_r1.json.receipt.json" %in% receipts)
    expect_true("f1/f1_r1_sample.rds.receipt.json" %in% receipts)

    updated_root <- tempfile("ae_updated_")
    dir.create(updated_root)
    updated_scripts <- file.path(updated_root, "scripts")
    dir.create(updated_scripts)
    file.copy(list.files(scripts_dir, full.names = TRUE), updated_scripts)
    dir.create(file.path(updated_root, "src"))
    file.copy(list.files(file.path(scripts_dir, "../src"), "\\.py$", full.names = TRUE),
              file.path(updated_root, "src"))
    cat("\n# Updated deployment\n", file = file.path(updated_scripts, "02_perform_matching.R"),
        append = TRUE)
    log <- run_script("02_perform_matching.R", config_path, script_dir = updated_scripts)
    expect_false(any(grepl("Starting stratum", log)))
    checkpoints <- file.path(out, "matches", c("f1_r1_sample.rds", "f1_r1_g1.rds"))
    checksums <- tools::md5sum(checkpoints)
    unlink(file.path(out, "matches", c("mb_f1_r1.json", "mb_f1_r1.parquet")))
    unlink(file.path(config$checkpoint_uri, "f1",
                     c("mb_f1_r1.json.receipt.json", "mb_f1_r1.parquet.receipt.json")))
    config$match_work <- list(ids = 1L, replicates = 1L)
    write_json(config, config_path, auto_unbox = TRUE)
    log <- run_script("02_perform_matching.R", config_path, script_dir = updated_scripts)
    expect_false(any(grepl("Starting stratum", log)))
    expect_equal(tools::md5sum(checkpoints), checksums)
    resumed <- read_batches(out) %>% arrange(id_numeric, replicate, cell, treatment)
    expect_equal(resumed, expected)
    log <- run_script("02_perform_matching.R", config_path, script_dir = updated_scripts)
    expect_false(any(grepl("Replicate 1:", log)))
    run_script("03_summarize_results.R", config_path)
    expect_equal(fromJSON(file.path(out, "results_summary.json"))$n_sites, 2)
    config$max_treatment_pixels <- config$max_treatment_pixels + 1L
    write_json(config, config_path, auto_unbox = TRUE)
    expect_error(
        run_script("02_perform_matching.R", config_path, script_dir = updated_scripts),
        "Incompatible matching sidecar"
    )
})
}

test_that("expensive fragments defer replicates that cannot fit and report their cost", {
    data_dir <- tempfile("ae_defer_")
    config_path <- make_fixture(data_dir, "nearest", n_replicates = 2)
    config <- fromJSON(config_path, simplifyVector = FALSE)
    config$match_checkpoints <- TRUE
    config$checkpoint_uri <- tempfile("ae_store_")
    config$match_soft_seconds <- 3600
    # A prior tier measured fragment 1 far above this task's budget.
    config$match_work <- list(ids = 1:4, replicates = 1:2, item_seconds = list("1" = 1e6))
    write_json(config, config_path, auto_unbox = TRUE)
    log <- run_script("02_perform_matching.R", config_path)
    expect_true(any(grepl("Deferring 1 replicate\\(s\\) of fragment 1", log)))
    unfinished <- fromJSON(file.path(data_dir, "unfinished.json"))
    expect_equal(unfinished$id, 1L)
    expect_equal(unfinished$replicate, 2L)
    costs <- fromJSON(file.path(data_dir, "item_seconds.json"))
    expect_equal(costs[["1"]], 1e6)
    expect_true(all(c("2", "3") %in% names(costs)))
})

test_that("Nextflow workers preserve whole-site exclusion for a later invalid replicate", {
    data_dir <- tempfile("ae_workers_")
    config_path <- make_fixture(data_dir, "nearest", n_replicates = 2)
    log <- suppressWarnings(system2(
        Sys.getenv("AE_PYTHON", "python"),
        c(shQuote(file.path(scripts_dir, "../tests/test_nextflow.py")), "--fixture-invalid",
          shQuote(config_path)),
        env = paste0("R_SCRIPTS_DIR=", scripts_dir), stdout = TRUE, stderr = TRUE
    ))
    expect_true(is.null(attr(log, "status")), info = paste(tail(log, 40), collapse = "\n"))
})

test_that("Nextflow atom summary shards retain unmapped parents and empty diagnostic scopes", {
    data_dir <- tempfile("ae_atom_workers_")
    config_path <- make_fixture(data_dir, "nearest", n_replicates = 2)
    out <- file.path(data_dir, "output")
    sites <- read_parquet(file.path(out, "sites_processed.parquet"))
    atoms <- list(A = c("101", "102"), B = "201", C = "301")
    sites$ae_atoms <- vapply(sites$site_id, function(site) {
        toJSON(list(run_id = "atom-shards", atoms = lapply(atoms[[site]], function(id) {
            list(atomic_id = id, geometry = list(type = "Polygon", coordinates = list()))
        })), auto_unbox = TRUE)
    }, character(1))
    sites$site_id <- unname(c(A = "Z", B = "A", C = "M")[sites$site_id])
    write_parquet(sites, file.path(out, "sites_processed.parquet"))
    key <- read_parquet(file.path(out, "treatment_cell_key.parquet")) %>%
        mutate(site_id = c("Z", "Z", "A", "M")[id_numeric],
               atomic_id = c("101", "102", "201", "301")[id_numeric])
    write_parquet(key, file.path(out, "treatment_cell_key.parquet"))
    forest <- read_parquet(file.path(out, "treatment_pixels.parquet")) %>%
        select(cell, fc_2008)
    key %>%
        left_join(forest, by = "cell") %>%
        group_by(site_id, atomic_id) %>%
        summarise(n_site_pixels = n(), n_forest_pixels = sum(fc_2008 > 0),
                  .groups = "drop") %>%
        write_parquet(file.path(out, "ae_atom_counts.parquet"))
    config <- fromJSON(config_path, simplifyVector = FALSE)
    config$exact_match_group_mapping <- list(
        "1" = list(list("Z", 0), list("A", 0)), "2" = list(list("Z", 1))
    )
    write_json(config, config_path, auto_unbox = TRUE)
    log <- suppressWarnings(system2(
        Sys.getenv("AE_PYTHON", "python"),
        c(shQuote(file.path(scripts_dir, "../tests/test_nextflow.py")), "--fixture",
          shQuote(config_path)),
        env = paste0("R_SCRIPTS_DIR=", scripts_dir), stdout = TRUE, stderr = TRUE
    ))
    expect_true(is.null(attr(log, "status")), info = paste(tail(log, 50), collapse = "\n"))
    published <- file.path(data_dir, "published", "output")
    connection <- gzfile(file.path(published, "results_matching_diagnostics.jsonl.gz"), "rt")
    records <- readLines(connection)
    close(connection)
    counts <- fromJSON(file.path(published, "results_matching_diagnostics_summary.json"))$counts
    expect_length(records, counts$attempts + counts$atoms)
    expect_equal(sum(vapply(records, function(record) {
        fromJSON(record)$record_type == "atom_inclusion"
    }, logical(1))), 4)

    source(file.path(scripts_dir, "summary_shards.R"), local = TRUE)
    states <- file.path(
        data_dir, c("summary-s000000", "summary-s000001"), "output", "summary_state.rds"
    )
    first <- readRDS(states[1])
    second <- readRDS(states[2])
    merged <- read_summary_states(states)
    expect_true(all(c(
        "id_numeric", "group", "replicate", "sampling_weight"
    ) %in% names(merged$cov_data)))
    expect_equal(
        sort(unique(merged$cov_data$replicate)),
        seq_len(config$n_replicates)
    )
    bad_path <- file.path(data_dir, "invalid_state.rds")
    duplicate <- second
    duplicate$atom_declarations$atomic_id[1] <- first$atom_declarations$atomic_id[1]
    saveRDS(duplicate, bad_path)
    expect_error(read_summary_states(c(states[1], bad_path)), "Overlapping.*atom")
    second$atom_metadata$run_id <- "other-run"
    saveRDS(second, bad_path)
    expect_error(read_summary_states(c(states[1], bad_path)), "Incompatible.*atom_metadata")
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
