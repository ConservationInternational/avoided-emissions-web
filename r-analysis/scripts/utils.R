# Shared utility functions for avoided emissions analysis scripts.
#
# Loaded by each analysis step via source(). Provides configuration parsing,
# exact-match group filtering, and Rollbar error tracking.

library(tidyverse)
library(jsonlite)
library(httr)


# -- Rollbar error tracking --------------------------------------------------

.rollbar_env <- new.env(parent = emptyenv())
.rollbar_env$token <- ""
.rollbar_env$environment <- "development"
.rollbar_env$enabled <- FALSE

rollbar_init <- function(token = Sys.getenv("ROLLBAR_SCRIPT_TOKEN", ""),
                         environment = Sys.getenv("ROLLBAR_ENVIRONMENT",
                                                  Sys.getenv("ENVIRONMENT", "development"))) {
    # Initialize Rollbar error tracking. Call once at the start of each script.
    # If ROLLBAR_SCRIPT_TOKEN is not set, Rollbar calls are silently skipped.
    .rollbar_env$token <- token
    .rollbar_env$environment <- environment
    .rollbar_env$enabled <- nchar(token) > 0

    if (.rollbar_env$enabled) {
        message("Rollbar initialized (environment=", environment, ")")
    } else {
        message("ROLLBAR_SCRIPT_TOKEN not set \u2014 error tracking disabled")
    }
}

rollbar_report_error <- function(error_msg, trace_calls = sys.calls(),
                                  extra = list()) {
    # Report an error to Rollbar using the Items API.
    if (!.rollbar_env$enabled) return(invisible(NULL))

    # Build a simplified stack trace from R's call stack
    frames <- lapply(rev(trace_calls), function(cl) {
        list(
            filename = tryCatch(getSrcFilename(cl, full.names = TRUE),
                                error = function(e) "<unknown>"),
            lineno = tryCatch(getSrcLocation(cl, "line"),
                              error = function(e) 0),
            method = paste(deparse(cl, width.cutoff = 120), collapse = " ")
        )
    })

    body <- list(
        access_token = .rollbar_env$token,
        data = list(
            environment = .rollbar_env$environment,
            body = list(
                trace = list(
                    frames = frames,
                    exception = list(
                        class = "RError",
                        message = as.character(error_msg)
                    )
                )
            ),
            level = "error",
            language = "r",
            framework = "Rscript",
            platform = R.version.string,
            custom = extra,
            server = list(
                host = Sys.info()[["nodename"]]
            )
        )
    )

    tryCatch({
        resp <- POST(
            url = "https://api.rollbar.com/api/1/item/",
            body = toJSON(body, auto_unbox = TRUE, null = "null"),
            content_type_json(),
            timeout(10)
        )
        if (status_code(resp) == 200) {
            message("Rollbar: error reported successfully")
        } else {
            message("Rollbar: failed to report (HTTP ", status_code(resp), ")")
        }
    }, error = function(e) {
        message("Rollbar: could not send error report: ", e$message)
    })

    invisible(NULL)
}

rollbar_report_message <- function(msg, level = "info", extra = list()) {
    # Report an informational message to Rollbar.
    if (!.rollbar_env$enabled) return(invisible(NULL))

    body <- list(
        access_token = .rollbar_env$token,
        data = list(
            environment = .rollbar_env$environment,
            body = list(
                message = list(
                    body = msg
                )
            ),
            level = level,
            language = "r",
            framework = "Rscript",
            platform = R.version.string,
            custom = extra,
            server = list(
                host = Sys.info()[["nodename"]]
            )
        )
    )

    tryCatch({
        resp <- POST(
            url = "https://api.rollbar.com/api/1/item/",
            body = toJSON(body, auto_unbox = TRUE, null = "null"),
            content_type_json(),
            timeout(10)
        )
    }, error = function(e) {
        message("Rollbar: could not send message: ", e$message)
    })

    invisible(NULL)
}

with_rollbar <- function(expr, step_name = "R analysis") {
    # Wrapper that evaluates an expression and reports any error to Rollbar
    # before re-raising it. Use: with_rollbar({ ... }, step_name = "Step 1")
    tryCatch(
        expr,
        error = function(e) {
            rollbar_report_error(
                error_msg = conditionMessage(e),
                trace_calls = sys.calls(),
                extra = list(step = step_name)
            )
            stop(e)
        }
    )
}


# -- Configuration -----------------------------------------------------------

parse_config <- function(args = commandArgs(trailingOnly = TRUE)) {
    # Parse --config and --site-id from command-line arguments
    config_path <- NULL
    site_id <- NULL
    data_dir <- NULL

    i <- 1
    while (i <= length(args)) {
        if (args[i] == "--config" && i < length(args)) {
            config_path <- args[i + 1]
            i <- i + 2
        } else if (args[i] == "--site-id" && i < length(args)) {
            site_id <- args[i + 1]
            i <- i + 2
        } else if (args[i] == "--data-dir" && i < length(args)) {
            data_dir <- args[i + 1]
            i <- i + 2
        } else {
            i <- i + 1
        }
    }

    if (is.null(config_path)) {
        stop("--config argument is required")
    }

    config <- fromJSON(config_path)
    config$config_path <- config_path

    # Override data_dir if provided on command line
    if (!is.null(data_dir)) {
        config$data_dir <- data_dir
    }
    if (!is.null(site_id)) {
        config$site_id <- site_id
    }

    # Validate required parameters (no silent defaults)
    required_params <- c(
        "max_treatment_pixels", "control_multiplier",
        "min_glm_treatment_pixels", "caliper_width", "max_controls_per_treatment"
    )
    missing <- required_params[!required_params %in% names(config)]
    if (length(missing) > 0) {
        stop("Missing required config parameters: ", paste(missing, collapse = ", "))
    }

    # Set up directory paths
    config$input_dir <- file.path(config$data_dir, "input")
    config$output_dir <- file.path(config$data_dir, "output")
    config$matches_dir <- file.path(config$output_dir, "matches")

    dir.create(config$output_dir, showWarnings = FALSE, recursive = TRUE)
    dir.create(config$matches_dir, showWarnings = FALSE, recursive = TRUE)

    return(config)
}


# -- Matching helpers --------------------------------------------------------

filter_groups <- function(vals, exact_match_vars) {
    # Assign group interaction from the exact-match variables and keep
    # only groups present in both treatment and control sets.
    vals$group <- interaction(vals[, exact_match_vars, drop = FALSE])

    # Drop control-only groups (no treatment pixels)
    all_groups <- unique(vals$group)
    treatment_groups <- unique(filter(vals, treatment)$group)
    control_only <- setdiff(all_groups, treatment_groups)
    if (length(control_only) > 0) {
        n_ctrl_lost <- sum(!vals$treatment & vals$group %in% control_only)
        message("    filter_groups: dropping ", length(control_only),
                " control-only group(s) (", n_ctrl_lost,
                " control pixels removed)")
    }
    vals <- filter(vals, group %in% treatment_groups)

    # Drop treatment-only groups (no control pixels)
    control_groups <- unique(filter(vals, !treatment)$group)
    shared_groups <- treatment_groups[treatment_groups %in% control_groups]
    treatment_only <- setdiff(as.character(treatment_groups),
                              as.character(shared_groups))
    if (length(treatment_only) > 0) {
        n_treat_lost <- sum(vals$treatment &
                            vals$group %in% treatment_only)
        message("    filter_groups: dropping ", length(treatment_only),
                " treatment-only group(s) (", n_treat_lost,
                " treatment pixels lost): ",
                paste(head(treatment_only, 10), collapse = ", "))
    }
    vals <- filter(vals, group %in% shared_groups)
    vals$group <- droplevels(vals$group)

    message("    filter_groups: ", length(shared_groups),
            " shared group(s) remaining, ",
            sum(vals$treatment), " treatment / ",
            sum(!vals$treatment), " control pixels")

    return(vals)
}
