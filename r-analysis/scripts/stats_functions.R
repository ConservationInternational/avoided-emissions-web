# Pure statistical functions used by the matching (step 2) and summary
# (step 3) scripts.  Kept free of I/O and global configuration so that they
# can be unit-tested (see r-analysis/tests).

library(dplyr, warn.conflicts = FALSE)

CARBON_FRACTION <- 0.5
CO2_PER_CARBON <- 44 / 12
PRE_INTERVENTION_YEARS <- 5
MATCH_DIAGNOSTICS_SCHEMA_VERSION <- 1L


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

restrict_shared_support <- function(d, f) {
    # Operate on one entire eligible stratum. Only bare numeric terms qualify.
    labels <- attr(terms(f), "term.labels")
    direct <- lapply(labels, str2lang)
    vars <- vapply(direct[vapply(direct, is.symbol, logical(1))], as.character, character(1))
    t <- d$treatment
    retained <- rep(TRUE, sum(!t))
    constraints <- list()
    if (any(t)) for (v in vars) {
        x <- d[[v]]
        if (!is.numeric(x) || is.factor(x) || !is.null(dim(x))) next
        tx <- x[t]
        value <- tx[[1]]
        if (!all(is.finite(tx)) || !all(tx == value)) next
        cx <- x[!t]
        keep <- is.finite(cx) & cx == value
        constraints[[v]] <- list(value = value, n_controls_removed = sum(!keep))
        retained <- retained & keep
    }
    rows <- rep(TRUE, nrow(d))
    rows[!t] <- retained
    list(
        data = d[rows, , drop = FALSE], retained_rows = rows, constraints = constraints,
        n_treatment = sum(t), n_controls_before = sum(!t),
        n_controls_retained = sum(retained), n_controls_removed = sum(!retained)
    )
}

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


drop_constant_terms <- function(f, d) {
    # Remove terms whose variables do not vary within `d` (e.g. a covariate
    # that is constant within an exact-match stratum).  They carry no
    # information and a one-level factor breaks glm() and Mahalanobis
    # distances.  Returns `treatment ~ 1` when no term varies.
    labels <- attr(terms(f), "term.labels")
    varies <- vapply(labels, function(l) {
        all(vapply(all.vars(str2lang(l)), function(v) n_distinct(d[[v]]) > 1, logical(1)))
    }, logical(1))
    reformulate(if (any(varies)) labels[varies] else "1", response = "treatment")
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
    evidence <- list()
    for (v in setdiff(all.vars(f), "treatment")) {
        t_vals <- d[[v]][d$treatment]
        c_vals <- d[[v]][!d$treatment]
        if (v %in% categorical || is.character(t_vals) || is.factor(t_vals)) {
            only_t <- setdiff(unique(as.character(t_vals)), as.character(c_vals))
            only_c <- setdiff(unique(as.character(c_vals)), as.character(t_vals))
            if (length(only_t) + length(only_c) > 0) {
                evidence[[v]] <- list(type = "exclusive_levels",
                                      only_treatment = as.list(only_t),
                                      only_control = as.list(only_c))
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
                evidence[[v]] <- list(
                    type = if (t_range[1] == c_range[2] || c_range[1] == t_range[2]) {
                        "range_boundary"
                    } else "range_gap",
                    treatment_min = t_range[1], treatment_max = t_range[2],
                    control_min = c_range[1], control_max = c_range[2]
                )
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
        sep_vars = unique(problem_vars),
        evidence = evidence
    )
}


condition_diagnostic <- function(cnd) {
    list(message = conditionMessage(cnd), classes = as.list(class(cnd)))
}


capture_match_conditions <- function(expr) {
    warnings <- list()
    error <- NULL
    value <- tryCatch(withCallingHandlers(
        expr,
        warning = function(w) {
            warnings[[length(warnings) + 1L]] <<- condition_diagnostic(w)
            invokeRestart("muffleWarning")
        }
    ), error = function(e) {
        error <<- condition_diagnostic(e)
        NULL
    })
    list(value = value, warnings = warnings, error = error)
}


arm_summary <- function(x, categorical = FALSE) {
    numeric <- is.numeric(x) && !categorical && !is.factor(x)
    finite <- if (is.numeric(x)) is.finite(x) else !is.na(x)
    y <- x[finite]
    n <- length(y)
    out <- list(n = length(x), n_finite = n, n_missing = sum(is.na(x)),
                n_nonfinite = sum(!finite & !is.na(x)), n_unique = length(unique(y)))
    if (numeric) {
        out <- c(out, list(
            min = if (n) min(y) else NA_real_,
            max = if (n) max(y) else NA_real_,
            mean = if (n) mean(y) else NA_real_,
            sd = if (n > 1L) sd(y) else NA_real_,
            quartiles = as.list(if (n) unname(quantile(y, c(.25, .5, .75))) else rep(NA_real_, 3)),
            n_zero = sum(y == 0)
        ))
    }
    if (categorical || is.factor(x) || is.character(x) || is.logical(x)) {
        counts <- table(x)
        out$level_counts <- lapply(seq_along(counts), function(i) {
            list(level = names(counts)[[i]], n = as.integer(counts[[i]]))
        })
    }
    out
}


covariate_diagnostics <- function(d, f) {
    vars <- setdiff(all.vars(f), "treatment")
    factors <- factor_vars(f)
    setNames(lapply(vars, function(v) {
        x <- d[[v]][d$treatment]
        y <- d[[v]][!d$treatment]
        categorical <- v %in% factors
        ts <- arm_summary(x, categorical)
        cs <- arm_summary(y, categorical)
        valid <- function(a) if (is.numeric(a)) a[is.finite(a)] else a[!is.na(a)]
        x <- valid(x)
        y <- valid(y)
        shared <- intersect(x, y)
        list(treatment = ts, control = cs, n_shared_values = length(shared),
             n_treatment_at_shared_values = sum(x %in% shared),
             n_control_at_shared_values = sum(y %in% shared))
    }), vars)
}


fit_propensity <- function(f, d) {
    # Preserve the existing rejection criterion, not "any GLM warning".
    lp <- NULL
    model <- NULL
    warned <- FALSE
    phase <- "fit"
    captured <- capture_match_conditions(withCallingHandlers({
        model <- glm(f, data = d, family = binomial())
        if (!warned && isTRUE(model$converged)) {
            phase <- "predict"
            lp <- predict(model, type = "link")
        }
    }, warning = function(w) {
        if (phase == "fit" && grepl("converge|fitted probabilities", conditionMessage(w))) {
            warned <<- TRUE
        }
    }))
    cause <- if (!is.null(captured$error)) {
        "glm_error"
    } else if (!isTRUE(model$converged)) {
        "glm_nonconvergence"
    } else if (warned) {
        "glm_warning"
    } else NULL
    by_arm <- function(x, probabilities = FALSE) {
        if (is.null(x)) return(NULL)
        # Align the model frame to sampled treatment, including fits that
        # subsequently fail on prediction.
        treated <- d$treatment[match(rownames(model$model), rownames(d))]
        arm <- function(y) {
            out <- arm_summary(y)
            if (probabilities) {
                eps <- 10 * .Machine$double.eps  # glm.fit's warning boundary
                y <- y[is.finite(y)]
                out$n_one <- sum(y == 1)
                out$n_near_zero <- sum(y < eps)
                out$n_near_one <- sum(y > 1 - eps)
            }
            out
        }
        list(treatment = arm(x[treated]), control = arm(x[!treated]))
    }
    coefs <- if (!is.null(model)) coef(model) else NULL
    list(
        model = model, lp = lp, separated = warned || (!is.null(model) && !model$converged),
        cause = cause, diagnostics = list(
            warnings = captured$warnings, error = captured$error,
            error_phase = if (!is.null(captured$error)) phase else NULL,
            converged = model$converged, iterations = model$iter, rank = model$rank,
            df_residual = model$df.residual, coefficients = as.list(coefs),
            aliased_coefficients = as.list(names(coefs)[is.na(coefs)]),
            fitted_probabilities = by_arm(model$fitted.values, probabilities = TRUE),
            linear_predictors = by_arm(model$linear.predictors)
        )
    )
}


verify_match_replay_environment <- function(provenance, scripts_dir) {
    if (!identical(provenance$r_version, R.version.string)) {
        stop("Replay R version mismatch: recorded ", provenance$r_version,
             "; running ", R.version.string)
    }
    for (pkg in names(provenance$package_versions)) {
        actual <- tryCatch(as.character(utils::packageVersion(pkg)), error = function(e) "<missing>")
        if (!identical(provenance$package_versions[[pkg]], actual)) {
            stop("Replay package version mismatch: ", pkg, " recorded ",
                 provenance$package_versions[[pkg]], "; installed ", actual)
        }
    }
    files <- c("02_perform_matching.R", "stats_functions.R", "utils.R")
    for (file in files) {
        actual <- unname(tools::md5sum(file.path(scripts_dir, file)))
        if (is.na(actual) || !identical(provenance$code_md5[[file]], actual)) {
            stop("Replay code checksum mismatch: ", file)
        }
    }
}


replay_match_propensity <- function(sample_file, group, rng = NULL, scripts_dir = NULL) {
    sample <- readRDS(sample_file)
    if (!identical(sample$compatibility$diagnostics_schema_version,
                   MATCH_DIAGNOSTICS_SCHEMA_VERSION)) stop("Incompatible matching replay sample")
    # Supply the original script directory to require exact R/package/code
    # identity instead of replaying in the caller's current environment.
    if (!is.null(scripts_dir)) verify_match_replay_environment(sample$provenance, scripts_dir)
    d <- as.data.frame(sample$data[as.character(sample$data$group) == group, , drop = FALSE])
    if (nrow(d) == 0L) stop("Replay group not found in sample: ", group)
    d$group <- as.character(d$group)
    rownames(d) <- NULL
    old_rng <- get0(".Random.seed", envir = .GlobalEnv, inherits = FALSE)
    old_kind <- RNGkind()
    old_options <- options(sample$provenance$model_options)
    old_locale <- Sys.getlocale("LC_MESSAGES")
    on.exit({
        options(old_options)
        Sys.setlocale("LC_MESSAGES", old_locale)
        do.call(RNGkind, as.list(old_kind))
        if (!is.null(old_rng)) {
            assign(".Random.seed", old_rng, envir = .GlobalEnv)
        } else if (exists(".Random.seed", envir = .GlobalEnv, inherits = FALSE)) {
            rm(".Random.seed", envir = .GlobalEnv)
        }
    })
    Sys.setlocale("LC_MESSAGES", sample$provenance$locale_messages)
    do.call(RNGkind, as.list(sample$provenance$rng_kind))
    if (is.null(rng)) rng <- sample$rng
    if (!is.null(rng)) assign(".Random.seed", as.integer(unlist(rng)), envir = .GlobalEnv)
    mf <- drop_constant_terms(as.formula(sample$formula), d)
    separation <- check_separation(d, mf)
    fit <- if (!separation$separated) fit_propensity(mf, d) else NULL
    list(formula = mf, separation = separation, glm = fit, provenance = sample$provenance)
}


# -- Emissions ---------------------------------------------------------------

outcome_input_error <- function(message, m, fields, masks, references = NULL) {
    details <- vapply(seq_along(fields), function(i) {
        rows <- which(masks[[i]])
        examples <- head(rows, 3L)
        describe <- vapply(examples, function(row) {
            paste0(
                "cell=", if ("cell" %in% names(m)) m$cell[row] else row,
                ", treatment=", if ("treatment" %in% names(m)) m$treatment[row] else "unknown",
                ", value=", as.character(m[[fields[i]]][row]),
                if (!is.null(references)) paste0(
                    ", previous_", references[i], "=", as.character(m[[references[i]]][row])
                ) else ""
            )
        }, character(1))
        paste0(fields[i], " (", length(rows), " invalid; ", paste(describe, collapse = "; "), ")")
    }, character(1))
    stop(errorCondition(paste(message, paste(details, collapse = "; ")),
                        class = "invalid_pixel_outcomes"))
}

pixel_outcomes <- function(m, start_year, fc_year_min, fc_year_max) {
    # Annual forest loss (ha) and emissions (MgCO2e) for each matched pixel.
    #
    # total_biomass_2000 is whole-cell biomass density (Mg per ha of cell) in
    # 2000, so a pixel's biomass in year t is
    #   biomass_t = density_2000 * area_ha * fc_t / fc_2000
    # and annual emissions are the biomass lost, converted to CO2e.
    years <- seq(baseline_year(start_year, fc_year_min), fc_year_max)
    required <- unique(c("total_biomass_2000", "fc_2000", "area_ha", paste0("fc_", years)))
    missing <- setdiff(required, names(m))
    if (length(missing) > 0) {
        stop(errorCondition(paste("Missing outcome columns:", paste(missing, collapse = ", ")),
                            class = "invalid_pixel_outcomes"))
    }
    fc <- unname(as.matrix(m[, paste0("fc_", years), drop = FALSE]))
    if (anyNA(fc) || anyNA(m$total_biomass_2000) || anyNA(m$fc_2000) || anyNA(m$area_ha)) {
        fields <- required[vapply(m[required], anyNA, logical(1))]
        outcome_input_error("Missing forest cover, biomass, or area for matched pixels:",
                            m, fields, lapply(m[fields], is.na))
    }
    if (any(!is.finite(fc)) || any(!is.finite(m$total_biomass_2000)) ||
        any(!is.finite(m$fc_2000)) || any(!is.finite(m$area_ha)) ||
        any(m$total_biomass_2000 < 0) || any(m$area_ha <= 0) ||
        any(fc < 0 | fc > 100) || any(m$fc_2000 < 0 | m$fc_2000 > 100)) {
        masks <- lapply(required, function(field) {
            value <- m[[field]]
            !is.finite(value) | if (field == "area_ha") {
                value <= 0
            } else if (field == "total_biomass_2000") {
                value < 0
            } else {
                value < 0 | value > 100
            }
        })
        bad <- vapply(masks, any, logical(1))
        outcome_input_error("Invalid forest cover, biomass, or area for matched pixels:",
                            m, required[bad], masks[bad])
    }
    cover_loss <- fc[, -ncol(fc), drop = FALSE] - fc[, -1, drop = FALSE]
    if (any(cover_loss < -1e-5) || any(fc[, 1] > m$fc_2000 + 1e-5)) {
        previous <- cbind(m$fc_2000, fc[, -ncol(fc), drop = FALSE])
        masks <- lapply(seq_along(years), function(i) fc[, i] > previous[, i] + 1e-5)
        bad <- vapply(masks, any, logical(1))
        fields <- paste0("fc_", years)[bad]
        outcome_input_error("Forest cover must not increase in the loss-only Hansen series:",
                            m, fields, masks[bad], c("fc_2000", paste0("fc_", head(years, -1)))[bad])
    }
    biomass_per_pct <- ifelse(
        m$fc_2000 > 0, m$total_biomass_2000 * m$area_ha / m$fc_2000, 0
    )
    list(
        years = years[-1],
        loss_ha = cover_loss / 100 * m$area_ha,
        emissions = cover_loss * biomass_per_pct * CARBON_FRACTION * CO2_PER_CARBON
    )
}


aggregate_outcomes <- function(m, outcomes) {
    # Sum weighted pixel outcomes to one row per exact-match stratum (`group`)
    # and year.  "sample_" columns use matching weights only; "extrapolated_"
    # columns also apply the stratum weight that scales matched pixels to all
    # eligible pixels in the stratum.
    groups <- sort(unique(m$group))
    idx <- match(m$group, groups)
    w_sample <- m$match_weight
    w_extrap <- m$match_weight * m$sampling_weight
    t <- m$treatment
    wsum <- function(x, w, rows) {
        out <- matrix(0, length(groups), ncol(x))
        sums <- rowsum(x[rows, , drop = FALSE] * w[rows], idx[rows])
        out[as.integer(rownames(sums)), ] <- sums
        as.vector(out)  # groups vary fastest, matching the columns below
    }
    tibble(
        group = rep(groups, times = length(outcomes$years)),
        year = rep(outcomes$years, each = length(groups)),
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


aggregate_atom_outcomes <- function(m, outcomes) {
    # Attribute treated pixels to their source atom.  A matched control set
    # can contain several treatment atoms, so allocate each control's matching
    # weight in proportion to the treatment-unit weights in that set.
    if (!"atomic_id" %in% names(m) || anyNA(m$atomic_id[m$treatment])) {
        stop("Atom aggregation requires atomic_id for every matched treatment pixel.")
    }
    indexed <- m %>% mutate(.row = row_number())
    treated <- indexed %>% filter(treatment)
    if (nrow(treated) == 0) return(tibble())
    atom_shares <- treated %>%
        group_by(match_group, atomic_id) %>%
        summarise(atom_share_weight = sum(match_weight), .groups = "drop") %>%
        group_by(match_group) %>%
        mutate(atom_share = atom_share_weight / sum(atom_share_weight)) %>%
        ungroup() %>%
        select(match_group, atomic_id, atom_share)
    controls <- indexed %>%
        filter(!treatment) %>%
        select(-atomic_id) %>%
        inner_join(atom_shares, by = "match_group")
    if (nrow(controls) == 0 && any(!m$treatment)) {
        stop("Matched controls could not be attributed to treatment atoms.")
    }
    expanded <- bind_rows(
        treated %>% mutate(atom_share = 1),
        controls
    ) %>%
        mutate(
            sample_weight = match_weight * atom_share,
            extrapolation_weight = sample_weight * sampling_weight
        )

    bind_rows(lapply(seq_along(outcomes$years), function(i) {
        loss <- outcomes$loss_ha[expanded$.row, i]
        emissions <- outcomes$emissions[expanded$.row, i]
        expanded %>%
            mutate(
                year = outcomes$years[i],
                .loss = loss,
                .emissions = emissions
            ) %>%
            group_by(atomic_id, group, year) %>%
            summarise(
                sample_treatment_defor_ha =
                    sum(if_else(treatment, .loss * sample_weight, 0)),
                sample_control_defor_ha =
                    sum(if_else(!treatment, .loss * sample_weight, 0)),
                sample_treatment_emissions_mgco2e =
                    sum(if_else(treatment, .emissions * sample_weight, 0)),
                sample_control_emissions_mgco2e =
                    sum(if_else(!treatment, .emissions * sample_weight, 0)),
                extrapolated_treatment_defor_ha =
                    sum(if_else(treatment, .loss * extrapolation_weight, 0)),
                extrapolated_control_defor_ha =
                    sum(if_else(!treatment, .loss * extrapolation_weight, 0)),
                extrapolated_treatment_emissions_mgco2e =
                    sum(if_else(treatment, .emissions * extrapolation_weight, 0)),
                extrapolated_control_emissions_mgco2e =
                    sum(if_else(!treatment, .emissions * extrapolation_weight, 0)),
                .groups = "drop"
            )
    }))
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
    per_replicate <- count(df, across(all_of(c(by, "replicate"))))
    if (any(per_group$n != n_rep) || any(per_replicate$n != 1L)) {
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
