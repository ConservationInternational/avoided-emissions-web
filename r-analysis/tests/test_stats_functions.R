source(file.path(Sys.getenv("R_SCRIPTS_DIR", "../scripts"), "stats_functions.R"))

matched_pixels <- function(control_fc = c(100, 90, 80), weight = 1, n_controls = 1) {
    # One treatment pixel with no loss plus `n_controls` identical controls.
    n <- 1 + n_controls
    tibble(
        cell = seq_len(n), treatment = c(TRUE, rep(FALSE, n_controls)),
        area_ha = 100, total_biomass_2025 = 100,
        match_group = "s_1", sampling_weight = weight,
        fc_2014 = c(100, rep(control_fc[1], n_controls)),
        fc_2015 = c(100, rep(control_fc[2], n_controls)),
        fc_2016 = c(100, rep(control_fc[3], n_controls))
    ) %>% set_match_weights()
}

test_that("emissions scale with pixel area (F1)", {
    m <- matched_pixels()
    out <- pixel_outcomes(m, start_year = 2020, fc_year_min = 2014, fc_year_max = 2016)
    # 100 ha at 100 Mg/ha losing 10% cover: 1000 Mg biomass lost
    expect_equal(out$loss_ha[2, 1], 10)
    expect_equal(out$emissions[2, 1], 1000 * 0.5 * 44 / 12)
    m$area_ha <- 50
    out <- pixel_outcomes(m, start_year = 2020, fc_year_min = 2014, fc_year_max = 2016)
    expect_equal(out$emissions[2, 1], 500 * 0.5 * 44 / 12)
})

test_that("zero baseline forest emits nothing", {
    m <- matched_pixels(control_fc = c(0, 0, 0))
    out <- pixel_outcomes(m, start_year = 2020, fc_year_min = 2014, fc_year_max = 2016)
    expect_true(all(out$emissions == 0))
})

test_that("missing outcome inputs fail instead of becoming zero (F7)", {
    m <- matched_pixels()
    m$total_biomass_2025[2] <- NA
    expect_error(pixel_outcomes(m, 2020, 2014, 2016), "Missing")
})

test_that("control weight equals treated weight in every matched set (F2)", {
    d <- tibble(
        treatment = c(TRUE, FALSE, FALSE, TRUE, TRUE, FALSE),
        match_group = c("a", "a", "a", "b", "b", "b")
    ) %>% set_match_weights()
    sums <- d %>%
        group_by(match_group, treatment) %>%
        summarise(w = sum(match_weight), .groups = "drop")
    expect_equal(sums$w, c(1, 1, 2, 2))
})

test_that("identical outcomes give zero avoided emissions for 1:k matching (F2)", {
    for (k in c(1, 2, 5)) {
        m <- matched_pixels(control_fc = c(100, 100, 100), n_controls = k)
        agg <- add_avoided(aggregate_outcomes(m, pixel_outcomes(m, 2020, 2014, 2016)))
        expect_equal(agg$extrapolated_emissions_avoided_mgco2e, c(0, 0))
    }
})

test_that("1:2 matching does not inflate the counterfactual (F2)", {
    m1 <- matched_pixels(n_controls = 1)
    m2 <- matched_pixels(n_controls = 2)
    a1 <- aggregate_outcomes(m1, pixel_outcomes(m1, 2020, 2014, 2016))
    a2 <- aggregate_outcomes(m2, pixel_outcomes(m2, 2020, 2014, 2016))
    expect_equal(a1$extrapolated_control_defor_ha, a2$extrapolated_control_defor_ha)
})

test_that("stratum weights scale sample totals (F9)", {
    m <- matched_pixels(weight = 4)
    agg <- aggregate_outcomes(m, pixel_outcomes(m, 2020, 2014, 2016))
    expect_equal(agg$extrapolated_control_defor_ha, 4 * agg$sample_control_defor_ha)
})

test_that("replicate bounds are computed after summing (F3, F8)", {
    df <- tibble(
        replicate = c(1, 2, 1, 2), site_id = c("A", "A", "B", "B"),
        value = c(0, 100, 100, 0)
    )
    per_site <- summarise_replicates(df, "site_id", "value")
    expect_equal(per_site$value_ci_lower, c(2.5, 2.5))
    portfolio <- df %>%
        group_by(replicate) %>%
        summarise(value = sum(value)) %>%
        summarise_replicates(character(0), "value")
    expect_equal(c(portfolio$value_ci_lower, portfolio$value_ci_upper), c(100, 100))
})

test_that("a single replicate has no bounds", {
    out <- summarise_replicates(tibble(replicate = 1, site_id = "A", value = 3), "site_id", "value")
    expect_equal(names(out), c("site_id", "value"))
})

test_that("summarise_replicates rejects incomplete replicates", {
    df <- tibble(replicate = c(1, 2, 1), site_id = c("A", "A", "B"), value = 1)
    expect_error(summarise_replicates(df, "site_id", "value"), "one row per replicate")
})

test_that("pre-intervention deforestation ends the year before the start (F5)", {
    vals <- tibble(fc_2009 = 100, fc_2014 = 80, fc_2015 = 0)
    expect_equal(defor_pre_intervention(vals, 2015, 2009:2015), -20)
    expect_null(defor_pre_intervention(vals, 2015, 2010:2015))
    expect_equal(baseline_year(2015, 2000), 2009)
    expect_equal(baseline_year(2003, 2000), 2000)
})

test_that("binary covariates with one-sided values are separated (F10)", {
    d <- tibble(treatment = c(TRUE, TRUE, TRUE, FALSE, FALSE, FALSE), pa = c(0, 1, 1, 0, 0, 0))
    expect_true(check_separation(d, treatment ~ pa)$separated)
    d$pa <- c(0, 1, 1, 0, 1, 0)
    expect_false(check_separation(d, treatment ~ pa)$separated)
    d$pa <- 1
    expect_false(check_separation(d, treatment ~ pa)$separated)
})

test_that("factor levels in only one group are separated", {
    d <- tibble(treatment = c(TRUE, TRUE, FALSE, FALSE), eco = c(1, 2, 1, 1))
    sep <- check_separation(d, treatment ~ factor(eco))
    expect_true(sep$separated)
    expect_equal(sep$sep_vars, "eco")
})

test_that("drop_formula_vars removes terms using separated variables", {
    f <- treatment ~ x + factor(eco) + z
    expect_equal(deparse(drop_formula_vars(f, "eco")), "treatment ~ x + z")
    expect_equal(drop_formula_vars(treatment ~ x, "x"), treatment ~ x)
})

test_that("SMD is invariant to weight scale (F14)", {
    d <- tibble(
        treatment = c(TRUE, TRUE, FALSE, FALSE, FALSE, FALSE),
        x = c(1, 3, 1, 2, 3, 4), match_weight = c(1, 1, 0.5, 0.5, 0.5, 0.5)
    )
    a <- balance_table(d, "x")
    b <- balance_table(mutate(d, match_weight = match_weight * 10), "x")
    expect_equal(a$smd, b$smd)
})

test_that("categorical covariates use per-level proportion differences", {
    d <- tibble(treatment = c(TRUE, TRUE, FALSE, FALSE), eco = c(1, 2, 1, 1), match_weight = 1)
    out <- balance_table(d, "eco", categorical = "eco")
    expect_equal(out$covariate, c("eco=1", "eco=2"))
    expect_equal(out$smd, c(-0.5, 0.5))
})

test_that("constant but different groups give an undefined SMD", {
    d <- tibble(treatment = c(TRUE, TRUE, FALSE, FALSE), x = c(1, 1, 2, 2), match_weight = 1)
    expect_true(is.na(balance_table(d, "x")$smd))
})

test_that("weighted histograms and quantiles use match weights", {
    h <- weighted_histogram(c(0, 1, 0, 1), c(TRUE, TRUE, FALSE, FALSE), c(1, 1, 3, 1), n_bins = 2)
    expect_equal(h$control_pct, c(75, 25))
    expect_equal(weighted_quantiles(c(1, 2), c(3, 1), c(0, 0.5, 1)), c(1, 1, 2))
})

test_that("sample_strata caps treatment per stratum and scales controls", {
    set.seed(1)
    vals <- tibble(
        treatment = rep(c(TRUE, FALSE), c(10, 40)),
        group = factor(rep(c("a", "b"), 25))
    )
    s <- sample_strata(vals, max_treatment = 2, control_multiplier = 3)
    counts <- count(s, group, treatment)
    expect_equal(counts$n, c(6, 2, 6, 2))
})
