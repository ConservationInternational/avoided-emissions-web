# Pure statistical functions used by the matching (step 2) and summary
# (step 3) scripts.  Kept free of I/O and global configuration so that they
# can be unit-tested (see r-analysis/tests).

library(dplyr, warn.conflicts = FALSE)

CARBON_FRACTION <- 0.5
CO2_PER_CARBON <- 44 / 12
PRE_INTERVENTION_YEARS <- 5


# -- Timing ------------------------------------------------------------------

baseline_year <- function(start_year, fc_year_min) {
    # First forest-cover year of a site's analysis window.  The baseline ends
    # the year before the intervention starts, so intervention-period outcomes
    # never inform matching.
    max(fc_year_min, start_year - PRE_INTERVENTION_YEARS - 1L)
}


defor_pre_intervention <- function(vals, start_year, fc_years) {
    # Percent change in forest cover over the PRE_INTERVENTION_YEARS years
    # ending the year before the intervention.  Returns NULL when the forest
    # cover record does not reach back far enough.
    init <- paste0("fc_", start_year - PRE_INTERVENTION_YEARS - 1L)
    final <- paste0("fc_", start_year - 1L)
    if (!all(c(init, final) %in% paste0("fc_", fc_years))) return(NULL)
    if_else(
        vals[[init]] > 0,
        (vals[[final]] - vals[[init]]) / vals[[init]] * 100,
        NA_real_
    )
}


# -- Sampling and matching ---------------------------------------------------

sample_strata <- function(vals, max_treatment, control_multiplier) {
    # Sample up to max_treatment treatment pixels per exact-match stratum and
    # control_multiplier controls per sampled treatment pixel.
    treated <- vals %>%
        filter(treatment) %>%
        slice_sample(prop = 1) %>%
        group_by(group) %>%
        filter(row_number() <= max_treatment) %>%
        ungroup()
    n_treated <- count(treated, group, name = "n_treated")
    controls <- vals %>%
        filter(!treatment) %>%
        inner_join(n_treated, by = "group") %>%
        slice_sample(prop = 1) %>%
        group_by(group) %>%
        filter(row_number() <= control_multiplier * n_treated) %>%
        ungroup() %>%
        select(-n_treated)
    bind_rows(treated, controls)
}


set_match_weights <- function(d) {
    # Treatment units weigh 1.  Controls in each matched set share weight so
    # that total control weight equals the number of treated units in the set.
    n_t <- ave(as.numeric(d$treatment), d$match_group, FUN = sum)
    n_c <- ave(as.numeric(!d$treatment), d$match_group, FUN = sum)
    d$match_weight <- ifelse(d$treatment, 1, n_t / n_c)
    d
}


factor_vars <- function(f) {
    # Variables wrapped in factor() on the right-hand side of a formula.
    labels <- attr(terms(f), "term.labels")
    sub("^factor\\((.*)\\)$", "\\1", labels[grepl("^factor\\(", labels)])
}


drop_formula_vars <- function(f, vars) {
    # Remove terms that use any of `vars`.  Returns `f` unchanged when every
    # term would be removed.
    labels <- attr(terms(f), "term.labels")
    keep <- labels[!vapply(
        labels, function(l) any(all.vars(str2lang(l)) %in% vars), logical(1)
    )]
    if (length(keep) == 0) return(f)
    reformulate(keep, response = "treatment")
}


check_separation <- function(d, f) {
    # Detect univariate (quasi-)complete separation before fitting a GLM:
    #   - factor terms: levels present in only one group
    #   - numeric terms: one group lies entirely at or beyond the other's range
    #     (this also catches binary variables such as pa = 1 only in treatment)
    # Multivariable separation is caught by fit_propensity().
    categorical <- factor_vars(f)
    problems <- character(0)
    problem_vars <- character(0)
    for (v in setdiff(all.vars(f), "treatment")) {
        t_vals <- d[[v]][d$treatment]
        c_vals <- d[[v]][!d$treatment]
        if (v %in% categorical || is.character(t_vals) || is.factor(t_vals)) {
            only_t <- setdiff(unique(as.character(t_vals)), as.character(c_vals))
            only_c <- setdiff(unique(as.character(c_vals)), as.character(t_vals))
            if (length(only_t) + length(only_c) > 0) {
                problem_vars <- c(problem_vars, v)
                problems <- c(problems, paste0(
                    v, ": ", length(only_t), " level(s) only in treatment, ",
                    length(only_c), " level(s) only in control"
                ))
            }
        } else {
            t_range <- range(t_vals)
            c_range <- range(c_vals)
            constant <- t_range[1] == t_range[2] && c_range[1] == c_range[2] &&
                t_range[1] == c_range[1]
            if (!constant && (c_range[2] <= t_range[1] || t_range[2] <= c_range[1])) {
                problem_vars <- c(problem_vars, v)
                problems <- c(problems, paste0(
                    v, ": treatment [", signif(t_range[1], 4), ", ",
                    signif(t_range[2], 4), "] vs control [",
                    signif(c_range[1], 4), ", ", signif(c_range[2], 4), "]"
                ))
            }
        }
    }
    list(
        separated = length(problems) > 0,
        details = problems,
        sep_vars = unique(problem_vars)
    )
}


fit_propensity <- function(f, d) {
    # Logistic propensity model.  Non-convergence or fitted probabilities of
    # 0/1 indicate (multivariable) separation, so `separated` is TRUE.
    warned <- FALSE
    model <- withCallingHandlers(
        glm(f, data = d, family = binomial()),
        warning = function(w) {
            if (grepl("converge|fitted probabilities", conditionMessage(w))) {
                warned <<- TRUE
                invokeRestart("muffleWarning")
            }
        }
    )
    list(model = model, separated = warned || !model$converged)
}


# -- Emissions ---------------------------------------------------------------

pixel_outcomes <- function(m, start_year, fc_year_min, fc_year_max) {
    # Annual forest loss (ha) and emissions (MgCO2e) for each matched pixel.
    #
    # total_biomass_2025 is whole-cell biomass density (Mg per ha of cell) at
    # the baseline forest cover, so a pixel's biomass in year t is
    #   biomass_t = density * area_ha * fc_t / fc_baseline
    # and annual emissions are the biomass lost, converted to CO2e.
    years <- seq(baseline_year(start_year, fc_year_min), fc_year_max)
    fc <- unname(as.matrix(m[, paste0("fc_", years), drop = FALSE]))
    if (anyNA(fc) || anyNA(m$total_biomass_2025) || anyNA(m$area_ha)) {
        stop("Missing forest cover, biomass, or area for matched pixels")
    }
    cover_loss <- fc[, -ncol(fc), drop = FALSE] - fc[, -1, drop = FALSE]
    baseline <- fc[, 1]
    biomass_per_pct <- ifelse(
        baseline > 0, m$total_biomass_2025 * m$area_ha / baseline, 0
    )
    list(
        years = years[-1],
        loss_ha = cover_loss / 100 * m$area_ha,
        emissions = cover_loss * biomass_per_pct * CARBON_FRACTION * CO2_PER_CARBON
    )
}


aggregate_outcomes <- function(m, outcomes) {
    # Sum weighted pixel outcomes to one row per year.  "sample_" columns use
    # matching weights only; "extrapolated_" columns also apply the stratum
    # weight that scales matched pixels to all eligible site pixels.
    w_sample <- m$match_weight
    w_extrap <- m$match_weight * m$sampling_weight
    t <- m$treatment
    wsum <- function(x, w, rows) colSums(x[rows, , drop = FALSE] * w[rows])
    tibble(
        year = outcomes$years,
        sample_treatment_defor_ha = wsum(outcomes$loss_ha, w_sample, t),
        sample_control_defor_ha = wsum(outcomes$loss_ha, w_sample, !t),
        sample_treatment_emissions_mgco2e = wsum(outcomes$emissions, w_sample, t),
        sample_control_emissions_mgco2e = wsum(outcomes$emissions, w_sample, !t),
        extrapolated_treatment_defor_ha = wsum(outcomes$loss_ha, w_extrap, t),
        extrapolated_control_defor_ha = wsum(outcomes$loss_ha, w_extrap, !t),
        extrapolated_treatment_emissions_mgco2e =
            wsum(outcomes$emissions, w_extrap, t),
        extrapolated_control_emissions_mgco2e =
            wsum(outcomes$emissions, w_extrap, !t)
    )
}


add_avoided <- function(df) {
    # Avoided = counterfactual (control) minus observed (treatment).
    df %>% mutate(
        sample_forest_loss_avoided_ha =
            sample_control_defor_ha - sample_treatment_defor_ha,
        sample_emissions_avoided_mgco2e =
            sample_control_emissions_mgco2e - sample_treatment_emissions_mgco2e,
        extrapolated_forest_loss_avoided_ha =
            extrapolated_control_defor_ha - extrapolated_treatment_defor_ha,
        extrapolated_emissions_avoided_mgco2e =
            extrapolated_control_emissions_mgco2e -
            extrapolated_treatment_emissions_mgco2e
    )
}


summarise_replicates <- function(df, by, metrics) {
    # Mean of each metric across replicates and, with more than one replicate,
    # the 2.5% / 97.5% quantiles.  `df` must hold exactly one row per
    # replicate for each `by` group: aggregate within each replicate first,
    # because quantiles of sums are not sums of quantiles.
    n_rep <- n_distinct(df$replicate)
    per_group <- count(df, across(all_of(by)))
    if (any(per_group$n != n_rep)) {
        stop("summarise_replicates: each group needs one row per replicate")
    }
    fns <- list(mean = mean)
    if (n_rep > 1) {
        fns$ci_lower <- function(x) unname(quantile(x, 0.025))
        fns$ci_upper <- function(x) unname(quantile(x, 0.975))
    }
    df %>%
        group_by(across(all_of(by))) %>%
        summarise(
            across(all_of(metrics), fns, .names = "{.col}__{.fn}"),
            .groups = "drop"
        ) %>%
        rename_with(~ sub("__mean$", "", .x)) %>%
        rename_with(~ sub("__", "_", .x))
}


# -- Balance diagnostics -----------------------------------------------------

balance_row <- function(covariate, x, treatment, w, binary = FALSE) {
    # Weighted means and the standardized mean difference.  The SMD uses the
    # treated sample's SD (ATT convention), which does not depend on the scale
    # of the weights.  Binary indicators (factor levels) report the raw
    # difference in proportions.  An undefined SMD is NA.
    m_t <- weighted.mean(x[treatment], w[treatment])
    m_c <- weighted.mean(x[!treatment], w[!treatment])
    sd_t <- if (sum(treatment) > 1) sd(x[treatment]) else NA_real_
    smd <- if (binary) {
        m_t - m_c
    } else if (isTRUE(sd_t > 0)) {
        (m_t - m_c) / sd_t
    } else if (isTRUE(all.equal(m_t, m_c))) {
        0
    } else {
        NA_real_
    }
    tibble(
        covariate = covariate, mean_treatment = m_t, mean_control = m_c,
        sd_treatment = sd_t, smd = smd
    )
}


balance_table <- function(df, covariates, categorical = character(0)) {
    # One row per numeric covariate and per level of each categorical one.
    rows <- list()
    for (cov in covariates) {
        ok <- !is.na(df[[cov]])
        x <- df[[cov]][ok]
        t <- df$treatment[ok]
        w <- df$match_weight[ok]
        if (sum(t) == 0 || sum(!t) == 0) next
        if (cov %in% categorical) {
            for (lvl in sort(unique(x))) {
                rows[[length(rows) + 1]] <- balance_row(
                    paste0(cov, "=", lvl), as.numeric(x == lvl), t, w,
                    binary = TRUE
                )
            }
        } else {
            rows[[length(rows) + 1]] <- balance_row(cov, x, t, w)
        }
    }
    bind_rows(rows)
}


weighted_histogram <- function(x, treatment, w, n_bins = 40L) {
    # Weighted percentage of treatment and control in common bins.
    ok <- !is.na(x)
    x <- x[ok]
    treatment <- treatment[ok]
    w <- w[ok]
    if (length(x) < 2 || diff(range(x)) == 0) return(NULL)
    breaks <- seq(min(x), max(x), length.out = n_bins + 1)
    bin <- factor(
        findInterval(x, breaks, rightmost.closed = TRUE, all.inside = TRUE),
        levels = seq_len(n_bins)
    )
    pct <- function(rows) {
        total <- sum(w[rows])
        if (total == 0) return(rep(0, n_bins))
        as.numeric(tapply(w[rows], bin[rows], sum, default = 0)) / total * 100
    }
    list(
        bin_edges = breaks,
        treatment_pct = pct(treatment),
        control_pct = pct(!treatment)
    )
}


weighted_quantiles <- function(x, w, probs) {
    # Inverse of the weighted empirical CDF.
    o <- order(x)
    x <- x[o]
    cw <- cumsum(w[o]) / sum(w)
    vapply(probs, function(p) x[which(cw >= p - 1e-12)[1]], numeric(1))
}
