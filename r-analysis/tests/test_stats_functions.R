source(file.path(Sys.getenv("R_SCRIPTS_DIR", "../scripts"), "stats_functions.R"))

test_that("balance indices preserve empty replicates and exact scope identities", {
    df <- tibble(
        replicate = c(2L, 1L, 2L, 1L),
        site_id = c("001", "1", "1", "001"),
        id_numeric = c(1L, 2L, 2L, 1L), group = c("a.b", NA, NA, "a.b"),
        treatment = c(TRUE, TRUE, FALSE, FALSE),
        match_weight = c(1, 2, 3, 4), x = c(2, NA, 5, 7), eco = c(1L, 2L, 3L, 1L)
    )
    roster <- distinct(df, site_id, id_numeric, group)
    groups <- balance_group_indices(df, names(roster), roster, 1:3)
    expect_equal(nrow(groups), 6L)
    for (i in seq_len(nrow(groups))) {
        key <- groups[i, ]
        expected <- filter(
            df, replicate == key$replicate, site_id == key$site_id,
            id_numeric == key$id_numeric,
            (is.na(group) & is.na(key$group)) | (!is.na(group) & group == key$group)
        )
        indexed <- df[groups$.rows[[i]], ]
        expect_identical(indexed, expected)
        for (weighted_sd in c(FALSE, TRUE)) {
            expect_equal(
                balance_table(indexed, c("x", "eco"), "eco",
                              weighted_sd = weighted_sd, include_counts = TRUE),
                balance_table(expected, c("x", "eco"), "eco",
                              weighted_sd = weighted_sd, include_counts = TRUE)
            )
        }
    }
    sites <- bind_rows(distinct(df, site_id), tibble(site_id = "unmatched"))
    site_groups <- balance_group_indices(df, "site_id", sites, 1:3)
    expect_equal(nrow(site_groups), 9L)
    expect_true(all(lengths(site_groups$.rows[site_groups$site_id == "unmatched"]) == 0L))
})

matched_pixels <- function(control_fc = c(100, 90, 80), weight = 1, n_controls = 1) {
    # One treatment pixel with no loss plus `n_controls` identical controls.
    n <- 1 + n_controls
    tibble(
        cell = seq_len(n), treatment = c(TRUE, rep(FALSE, n_controls)),
        area_ha = 100, total_biomass_2000 = 100, fc_2000 = 100,
        group = "s", match_group = "s_1", sampling_weight = weight,
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
    m$total_biomass_2000[2] <- NA
    expect_error(pixel_outcomes(m, 2020, 2014, 2016), "Missing")
})

test_that("emissions use year-2000 stock, not remaining stock at the site baseline", {
    m <- matched_pixels(control_fc = c(50, 0, 0))
    out <- pixel_outcomes(m, 2020, 2014, 2016)
    expect_equal(out$emissions[2, 1], 100 * 100 * 0.5 * 0.5 * 44 / 12)
    m$fc_2014[2] <- 100
    out <- pixel_outcomes(m, 2020, 2014, 2016)
    expect_equal(out$emissions[2, 1], 100 * 100 * 0.5 * 44 / 12)
})

test_that("invalid outcome inputs cannot produce emissions estimates", {
    m <- matched_pixels()
    m$total_biomass_2000[2] <- -9999
    expect_error(pixel_outcomes(m, 2020, 2014, 2016), "Invalid")
    m$total_biomass_2000[2] <- 100
    m$fc_2000[2] <- NA
    expect_error(pixel_outcomes(m, 2020, 2014, 2016), "Missing")
    m$fc_2000[2] <- 100
    m$fc_2015[2] <- 101
    expect_error(pixel_outcomes(m, 2020, 2014, 2016), "Invalid")
    m$fc_2015[2] <- 100
    m$fc_2014[2] <- 90
    expect_error(pixel_outcomes(m, 2020, 2014, 2016), "must not increase")
    m$total_biomass_2025 <- 0
    m$total_biomass_2000 <- NULL
    expect_error(pixel_outcomes(m, 2020, 2014, 2016), "Missing outcome columns: total_biomass_2000")
})

test_that("outcome errors identify invalid fields, cells and values", {
    m <- matched_pixels()
    m$total_biomass_2000[2] <- -9999
    m$area_ha[1] <- 0
    m$fc_2015[2] <- 101
    err <- tryCatch(pixel_outcomes(m, 2020, 2014, 2016),
                    invalid_pixel_outcomes = identity)
    expect_s3_class(err, "invalid_pixel_outcomes")
    expect_match(conditionMessage(err), "total_biomass_2000.*cell=2.*value=-9999")
    expect_match(conditionMessage(err), "area_ha.*cell=1.*value=0")
    expect_match(conditionMessage(err), "fc_2015.*cell=2.*value=101")

    m <- matched_pixels()
    m$fc_2015[2] <- 95
    m$fc_2016[2] <- 99
    err <- tryCatch(pixel_outcomes(m, 2020, 2014, 2016),
                    invalid_pixel_outcomes = identity)
    expect_match(conditionMessage(err), "fc_2016.*value=99.*previous_fc_2015=95")

    m$fc_2015[2] <- NA
    err <- tryCatch(pixel_outcomes(m, 2020, 2014, 2016),
                    invalid_pixel_outcomes = identity)
    expect_match(conditionMessage(err), "fc_2015.*cell=2.*value=NA")
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

test_that("outcomes are aggregated per stratum and year", {
    a <- matched_pixels(weight = 2)
    b <- mutate(matched_pixels(control_fc = c(100, 100, 50)), group = "r", match_group = "r_1")
    m <- bind_rows(a, b)
    agg <- aggregate_outcomes(m, pixel_outcomes(m, 2020, 2014, 2016))
    expect_equal(agg$group, c("r", "s", "r", "s"))
    expect_equal(agg$year, c(2015, 2015, 2016, 2016))
    expect_equal(agg$extrapolated_control_defor_ha, c(0, 20, 50, 20))
})

test_that("atom controls follow treatment atoms and reconcile to parent totals", {
    m <- tibble(
        cell = 1:3,
        treatment = c(TRUE, TRUE, FALSE),
        atomic_id = c("101", "102", NA_character_),
        id_numeric = 1L,
        site_id = "site",
        replicate = 1L,
        group = "g",
        match_group = "g_set",
        sampling_weight = 5,
        area_ha = 100,
        total_biomass_2000 = 100,
        fc_2000 = 100,
        fc_2014 = 100,
        fc_2015 = c(90, 80, 50),
        fc_2016 = c(70, 60, 20)
    ) %>% set_match_weights()
    outcomes <- pixel_outcomes(m, 2020, 2014, 2016)
    parent <- add_avoided(aggregate_outcomes(m, outcomes))
    atoms <- add_avoided(aggregate_atom_outcomes(m, outcomes))
    atom_totals <- atoms %>%
        group_by(year) %>%
        summarise(across(all_of(names(parent)[-(1:2)]), sum), .groups = "drop")
    expect_equal(atom_totals, parent %>% select(-group))
    expect_equal(
        atoms %>% filter(atomic_id == "101") %>% pull(extrapolated_control_defor_ha),
        c(250, 150)
    )
    expect_equal(
        atoms %>% filter(atomic_id == "102") %>% pull(extrapolated_control_defor_ha),
        c(250, 150)
    )
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
    df <- tibble(replicate = c(1, 1, 1, 2), site_id = c("A", "A", "B", "B"), value = 1)
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
    expect_equal(sep$evidence$eco$only_treatment, list("2"))
})

test_that("shared support is the exact joint intersection of bare numeric terms", {
    d <- tibble(
        treatment = c(TRUE, TRUE, FALSE, FALSE, FALSE, FALSE),
        atomic_id = c("9223372036854775801", "2", rep(NA_character_, 4)),
        cropland = c(0, 0, 0, 1e-15, 0, 1),
        defor_pre_intervention = c(0, 0, 0, 0, -1, -1),
        x = c(1, 1, 2, 2, 2, 2),
        cat = c(1, 1, 2, 2, 2, 2)
    )
    f <- treatment ~ cropland + defor_pre_intervention + I(x^2) + factor(cat) + x:cat
    out <- restrict_shared_support(d, f)
    expect_equal(out$data, d[1:3, ])
    expect_equal(names(out$constraints), c("cropland", "defor_pre_intervention"))
    expect_equal(out$constraints$cropland, list(value = 0, n_controls_removed = 2))
    expect_equal(out$constraints$defor_pre_intervention$n_controls_removed, 2)
    expect_equal(out$n_controls_removed, 3)
    expect_equal(out$n_controls_retained, 1)
    exhausted <- restrict_shared_support(d[-3, ], f)
    expect_equal(exhausted$data, d[1:2, ])
    expect_equal(exhausted$n_controls_retained, 0)
})

test_that("one atom or a constant sample does not determine full treatment support", {
    d <- tibble(
        treatment = c(TRUE, TRUE, TRUE, FALSE, FALSE),
        atomic_id = c("1", "1", "2", NA, NA),
        group = "stratum",
        cropland = c(0, 0, 1, 0, 1e-15)
    )
    expect_equal(restrict_shared_support(d, treatment ~ cropland)$data, d)
    set.seed(2)
    sampled <- sample_strata(d, max_treatment = 1L, control_multiplier = 2L)
    expect_equal(n_distinct(sampled$cropland[sampled$treatment]), 1)
    expect_equal(sum(!sampled$treatment), 2)
})

test_that("fit diagnostics retain all warning classes, model details and GLM errors", {
    d <- as.data.frame(tibble(
        treatment = rep(c(TRUE, FALSE), each = 6),
        x = c(-1, 1, 2, 3, 4, 5, 1, -1, 2, 3, 5, 5)
    ))
    note <- function(x) {
        warning(warningCondition("custom diagnostic", class = "example_glm_warning"))
        log(x)
    }
    fit <- fit_propensity(treatment ~ note(x) + I(2 * note(x)), d)
    expect_null(fit$cause) # Unrelated warnings do not change the estimator.
    expect_true(fit$diagnostics$converged)
    expect_equal(fit$diagnostics$rank, 2L)
    expect_equal(fit$diagnostics$aliased_coefficients, list("I(2 * note(x))"))
    expect_equal(fit$diagnostics$fitted_probabilities$treatment$n, 5L)
    expect_equal(fit$diagnostics$linear_predictors$control$n, 5L)
    expect_equal(vapply(fit$diagnostics$warnings, `[[`, "", "message"),
                 rep(c("custom diagnostic", "NaNs produced"), 2))
    expect_equal(fit$diagnostics$warnings[[1]]$classes[[1]], "example_glm_warning")

    failed <- fit_propensity(treatment ~ I(stop("fixture model error")), d)
    expect_identical(failed$cause, "glm_error")
    expect_match(failed$diagnostics$error$message, "fixture model error")
    expect_null(failed$diagnostics$converged)
})

test_that("multivariate separation retains the real binomial nonconvergence warning", {
    d <- tibble(treatment = rep(c(TRUE, FALSE), each = 100),
                x = c(rep(c(2, -1), 50), rep(c(-2, 1), 50))) %>%
        mutate(z = if_else(treatment, 1 - x, -1 - x))
    f <- treatment ~ x + z
    expect_false(check_separation(d, f)$separated)
    fit <- fit_propensity(f, d)
    expect_identical(fit$cause, "glm_nonconvergence")
    expect_false(fit$diagnostics$converged)
    expect_match(fit$diagnostics$warnings[[1]]$message, "did not converge")
    expect_equal(fit$diagnostics$fitted_probabilities$treatment$n, 100L)
})

test_that("drop_formula_vars removes terms using separated variables", {
    f <- treatment ~ x + factor(eco) + z
    expect_equal(deparse(drop_formula_vars(f, "eco")), "treatment ~ x + z")
    expect_equal(drop_formula_vars(treatment ~ x, "x"), treatment ~ x)
})

test_that("terms that do not vary within a stratum are dropped", {
    d <- tibble(treatment = c(TRUE, FALSE, FALSE), x = 1:3, eco = 5, k = 2)
    expect_equal(deparse(drop_constant_terms(treatment ~ x + factor(eco) + k, d)), "treatment ~ x")
    expect_equal(deparse(drop_constant_terms(treatment ~ factor(eco) + k, d)), "treatment ~ 1")
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

test_that("represented-domain balance uses expansion weights and weighted SD", {
    d <- tibble(
        treatment = c(TRUE, TRUE, FALSE, FALSE),
        x = c(1, 3, 0, 2),
        match_weight = 1,
        represented_weight = c(1, 3, 1, 3)
    )
    out <- balance_table(
        d, "x", weight_col = "represented_weight",
        weighted_sd = TRUE, include_counts = TRUE
    )
    expect_equal(out$mean_treatment, 2.5)
    expect_equal(out$mean_control, 1.5)
    expect_equal(out$sd_treatment, sqrt(0.75))
    expect_equal(out$n_treatment, 2L)
    expect_equal(out$weight_treatment, 4)
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

test_that("a propensity fit is rejected for treatment, not control, probabilities at the bounds", {
    set.seed(1)
    regular <- rep(c(0, 1), 25)
    controls <- seq(-2, 2, length.out = 80)
    arm <- rep(c(TRUE, FALSE), c(50, 80))
    # Controls far from every treatment pixel are fitted at 0; they are not
    # candidate matches, so the fit stands and its warning is kept.
    far <- data.frame(treatment = c(arm, rep(FALSE, 20)),
                      x = c(regular, controls, rep(-1e4, 20)))
    fit <- fit_propensity(treatment ~ x, far)
    expect_null(fit$cause)
    expect_false(fit$separated)
    expect_length(fit$lp, nrow(far))
    expect_equal(fit$diagnostics$fitted_probabilities$control$n_near_zero, 20L)
    expect_equal(fit$diagnostics$fitted_probabilities$treatment$n_near_one, 0L)
    expect_match(fit$diagnostics$warnings[[1]]$message, "fitted probabilities numerically 0 or 1")
    # A treatment pixel beyond all controls is fitted at 1: no control resembles it.
    beyond <- data.frame(treatment = arm, x = c(1e4, regular[-1], controls))
    fit <- fit_propensity(treatment ~ x, beyond)
    expect_identical(fit$cause, "glm_warning")
    expect_true(fit$separated)
    expect_null(fit$lp)
    expect_equal(fit$diagnostics$fitted_probabilities$treatment$n_near_one, 1L)
})

test_that("sampling by atom draws every atom and keeps small atoms whole", {
    expect_equal(sample_quota(c(400, 100), cap = 100, min_cell = 0), c(80, 20))
    expect_equal(sample_quota(c(940, 60, 3, 1), cap = 100, min_cell = 50), c(50, 50, 3, 1))
    expect_equal(sample_quota(c(900, 100), cap = 200, min_cell = 50), c(144, 55))
    expect_equal(sample_quota(c(30, 20), cap = 100, min_cell = 50), c(30, 20))
    set.seed(3)
    vals <- tibble(
        treatment = rep(c(TRUE, FALSE), c(1004, 5000)),
        group = factor("s"),
        atomic_id = c(rep("big", 940), rep("mid", 60), rep("small", 3), "one", rep(NA, 5000))
    )
    for (i in 1:5) {
        s <- sample_strata(vals, max_treatment = 100, control_multiplier = 2, min_cell = 50)
        drawn <- count(filter(s, treatment), atomic_id)
        expect_equal(setNames(drawn$n, drawn$atomic_id), c(big = 50, mid = 50, one = 1, small = 3))
        expect_equal(sum(!s$treatment), 2 * 104)
    }
    # Without a minimum the stratum cap applies and a small atom can be missed.
    legacy <- sample_strata(vals, max_treatment = 100, control_multiplier = 2)
    expect_equal(sum(legacy$treatment), 100)
})

test_that("cell weights expand each atom to its own eligible pixels", {
    m <- tibble(
        cell = 1:6, treatment = rep(c(TRUE, FALSE), each = 3), group = "g",
        atomic_id = c("a", "a", "b", NA, NA, NA),
        match_group = rep(c("g_1", "g_2", "g_3"), 2)
    ) %>% set_match_weights()
    weights <- tibble(group = "g", atomic_id = c("a", "b"), sampling_weight = c(45, 10))
    w <- apply_cell_weights(m, weights)
    # 90 eligible pixels in atom a and 10 in atom b, not 50 each.
    expect_equal(as.vector(tapply(w$sampling_weight[w$treatment], w$atomic_id[w$treatment], sum)),
                 c(90, 10))
    expect_equal(w$sampling_weight[!w$treatment], c(45, 45, 10))
    # A cell without a factor is dropped together with its matched controls.
    only_a <- apply_cell_weights(m, weights[1, ])
    expect_equal(only_a$cell, c(1, 2, 4, 5))
    # Without atoms the stratum is the cell.
    plain <- apply_cell_weights(select(m, -atomic_id), tibble(group = "g", sampling_weight = 7))
    expect_true(all(plain$sampling_weight == 7))
})

test_that("a control shared by two atoms is expanded with each atom's own factor", {
    m <- tibble(
        cell = 1:3, treatment = c(TRUE, TRUE, FALSE),
        atomic_id = c("101", "102", NA_character_), group = "g", match_group = "g_set",
        area_ha = 100, total_biomass_2000 = 100, fc_2000 = 100,
        fc_2014 = 100, fc_2015 = c(100, 100, 90), fc_2016 = c(100, 100, 90)
    ) %>%
        set_match_weights() %>%
        apply_cell_weights(tibble(group = "g", atomic_id = c("101", "102"),
                                  sampling_weight = c(9, 1)))
    atoms <- aggregate_atom_outcomes(m, pixel_outcomes(m, 2020, 2014, 2016)) %>%
        filter(year == 2015)
    # The control lost 10 ha; each atom's one pixel takes it as counterfactual.
    expect_equal(atoms$extrapolated_control_defor_ha[atoms$atomic_id == "101"], 90)
    expect_equal(atoms$extrapolated_control_defor_ha[atoms$atomic_id == "102"], 10)
    expect_equal(atoms$sample_control_defor_ha, c(10, 10))
})

test_that("cells are included by replicate share and expanded by match rate", {
    attempts <- bind_rows(
        tibble(group = "kept", replicate = 1:4, n_eligible = 100, n_sampled = 50, n_matched = 40),
        tibble(group = "dropped", replicate = 1:3, n_eligible = 100, n_sampled = 50, n_matched = 50),
        tibble(group = "guarded", replicate = 1:5, n_eligible = 100, n_sampled = 50, n_matched = 10)
    ) %>% mutate(id_numeric = 1L, atomic_id = NA_character_)
    out <- cell_inclusion(attempts, n_replicates = 5, min_share = 0.8, min_match_rate = 0.5)
    cells <- out$cells[match(c("kept", "dropped", "guarded"), out$cells$group), ]
    expect_equal(cells$included, c(TRUE, FALSE, TRUE))
    expect_equal(cells$n_replicates_matched, c(4, 3, 5))
    expect_equal(cells$census_arm, c(TRUE, FALSE, FALSE))
    # A low match rate supports only the matched share of the cell.
    expect_equal(cells$n_supported, c(100, 0, 20))
    factors <- out$factors %>% distinct(group, sampling_weight)
    expect_equal(factors$sampling_weight[factors$group == "kept"], 100 / 40)
    expect_equal(factors$sampling_weight[factors$group == "guarded"], 100 / 50)
    expect_false("dropped" %in% factors$group)
    attempts$n_eligible[1] <- 99
    expect_error(cell_inclusion(attempts, 5), "differ between replicates")
})

test_that("a missing replicate takes the cell's mean over the others", {
    df <- tibble(cell = c("a", "a", "a", "b", "b"), year = 2015L,
                 replicate = c(1, 2, 4, 1, 2), value = c(1, 2, 6, 10, 20))
    out <- fill_missing_replicates(df, c("cell", "year"), 1:4, "value") %>%
        arrange(cell, replicate)
    expect_equal(nrow(out), 8)
    expect_equal(out$value[out$cell == "a"], c(1, 2, 3, 6))
    expect_equal(out$value[out$cell == "b"], c(10, 20, 15, 15))
    expect_equal(mean(out$value[out$cell == "a"]), mean(df$value[df$cell == "a"]))
    expect_identical(fill_missing_replicates(df[1:2, ], c("cell", "year"), 1:2, "value"), df[1:2, ])
})

test_that("the treatment census sums every eligible treatment pixel by cell", {
    vals <- tibble(
        treatment = c(TRUE, TRUE, TRUE, FALSE), group = factor(c("g", "g", "h", "g")),
        atomic_id = c("1", "2", "1", NA), area_ha = c(100, 50, 10, 100),
        total_biomass_2000 = 100, fc_2000 = 100,
        fc_2014 = 100, fc_2015 = c(90, 80, 100, 0), fc_2016 = c(90, 60, 50, 0)
    )
    census <- treatment_census(vals, 2020, 2014, 2016)
    expect_equal(census$years, 2015:2016)
    cells <- setNames(census$cells, vapply(census$cells, function(x) {
        paste(x$group, x$atomic_id)
    }, ""))
    expect_setequal(names(cells), c("g 1", "g 2", "h 1"))
    expect_equal(unlist(cells[["g 1"]]$loss_ha), c(10, 0))
    expect_equal(unlist(cells[["g 2"]]$loss_ha), c(10, 10))
    expect_equal(unlist(cells[["h 1"]]$loss_ha), c(0, 5))
    expect_equal(cells[["g 2"]]$n_pixels, 1)
    expect_equal(cells[["h 1"]]$area_ha, 10)
    # Whole strata when the input has no atoms; the control never counts.
    plain <- treatment_census(select(vals, -atomic_id), 2020, 2014, 2016)
    g <- Filter(function(x) x$group == "g", plain$cells)[[1]]
    expect_equal(g$n_pixels, 2)
    expect_equal(unlist(g$loss_ha), c(20, 10))
    expect_length(treatment_census(vals[4, ], 2020, 2014, 2016)$cells, 0)
})
