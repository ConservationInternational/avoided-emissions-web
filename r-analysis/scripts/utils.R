# Shared utility functions for avoided emissions analysis scripts.
#
# Loaded by each analysis step via source(). Provides configuration parsing,
# covariate loading, filtering, matching helpers, area calculation, and
# Rollbar error tracking.

library(sf)
library(terra)
library(tidyverse)
library(jsonlite)
library(units)
library(foreach)
library(httr)


# -- Rollbar error tracking --------------------------------------------------

.rollbar_env <- new.env(parent = emptyenv())
.rollbar_env$token <- ""
.rollbar_env$environment <- "development"
.rollbar_env$enabled <- FALSE

rollbar_init <- function(token = Sys.getenv("ROLLBAR_ACCESS_TOKEN", ""),
                         environment = Sys.getenv("ROLLBAR_ENVIRONMENT",
                                                  Sys.getenv("ENVIRONMENT", "development"))) {
    # Initialize Rollbar error tracking. Call once at the start of each script.
    # If ROLLBAR_ACCESS_TOKEN is not set, Rollbar calls are silently skipped.
    .rollbar_env$token <- token
    .rollbar_env$environment <- environment
    .rollbar_env$enabled <- nchar(token) > 0

    if (.rollbar_env$enabled) {
        message("Rollbar initialized (environment=", environment, ")")
    } else {
        message("ROLLBAR_ACCESS_TOKEN not set \u2014 error tracking disabled")
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

    # Override data_dir if provided on command line
    if (!is.null(data_dir)) {
        config$data_dir <- data_dir
    }
    if (!is.null(site_id)) {
        config$site_id <- site_id
    }

    # Apply defaults for optional parameters
    if (is.null(config$max_treatment_pixels)) config$max_treatment_pixels <- 1000
    if (is.null(config$control_multiplier)) config$control_multiplier <- 50
    if (is.null(config$min_site_area_ha)) config$min_site_area_ha <- 100
    if (is.null(config$min_glm_treatment_pixels)) config$min_glm_treatment_pixels <- 15

    # Set up directory paths
    config$input_dir <- file.path(config$data_dir, "input")
    config$output_dir <- file.path(config$data_dir, "output")
    config$matches_dir <- file.path(config$output_dir, "matches")

    dir.create(config$output_dir, showWarnings = FALSE, recursive = TRUE)
    dir.create(config$matches_dir, showWarnings = FALSE, recursive = TRUE)

    return(config)
}


# -- Site loading ------------------------------------------------------------

load_sites <- function(sites_path) {
    # Load sites from GeoJSON or GeoPackage. Expects columns:
    # site_id, site_name, start_date, end_date (optional)
    ext <- tools::file_ext(sites_path)

    if (ext == "geojson" || ext == "json") {
        sites <- st_read(sites_path, quiet = TRUE)
    } else if (ext == "gpkg") {
        sites <- st_read(sites_path, quiet = TRUE)
    } else {
        stop(paste("Unsupported file format:", ext))
    }

    # Validate required columns
    required_cols <- c("site_id", "site_name", "start_date")
    missing <- setdiff(required_cols, names(sites))
    if (length(missing) > 0) {
        stop(paste("Missing required columns:", paste(missing, collapse = ", ")))
    }

    # Ensure CRS is EPSG:4326
    sites <- st_transform(sites, "EPSG:4326")

    # Parse dates
    sites$start_date <- as.Date(sites$start_date)
    if ("end_date" %in% names(sites)) {
        sites$end_date <- as.Date(sites$end_date)
    } else {
        sites$end_date <- as.Date(NA)
    }

    sites$start_year <- as.integer(format(sites$start_date, "%Y"))
    sites$end_year <- ifelse(
        is.na(sites$end_date), 2099L,
        as.integer(format(sites$end_date, "%Y"))
    )

    # Compute area in hectares
    sites_cea <- st_transform(sites, "+proj=cea")
    sites$area_ha <- as.numeric(st_area(sites_cea)) / 10000

    # Assign numeric IDs for rasterization
    sites$id_numeric <- seq_len(nrow(sites))

    return(sites)
}


# -- Covariate loading -------------------------------------------------------

build_covariate_vrt <- function(gcs_bucket, gcs_prefix, covariate_names,
                                 local_dir = NULL) {
    # Open covariate COGs from a public GCS bucket via GDAL /vsicurl/.
    # No download is required — GDAL reads Cloud-Optimised GeoTIFFs
    # through HTTP range requests, fetching only the tiles needed for
    # the analysis extent.
    #
    # Returns a terra SpatRaster stack with one layer per covariate.

    # Tune GDAL HTTP settings for better COG performance
    terra::gdalCache(1024)  # 1 GB GDAL block cache
    Sys.setenv(
        GDAL_HTTP_MULTIPLEX   = "YES",
        GDAL_HTTP_MERGE_CONSECUTIVE_RANGES = "YES",
        GDAL_HTTP_MAX_RETRY   = "5",
        GDAL_HTTP_RETRY_DELAY = "2",
        VSI_CACHE             = "TRUE",
        VSI_CACHE_SIZE        = "50000000"  # 50 MB per file
    )

    vsicurl_uris <- vapply(covariate_names, function(name) {
        paste0("/vsicurl/https://storage.googleapis.com/",
               gcs_bucket, "/", gcs_prefix, "/", name, ".tif")
    }, character(1), USE.NAMES = FALSE)

    rast_list <- lapply(seq_along(vsicurl_uris), function(i) {
        message(paste("  Opening:", covariate_names[i]))
        terra::rast(vsicurl_uris[i])
    })
    d <- do.call(c, rast_list)

    return(d)
}


# -- Pixel area calculation --------------------------------------------------

calc_pixel_area_ha <- function(y, yres, xres) {
    # Calculate area of a raster cell in hectares on the WGS84 ellipsoid.
    # Based on the slice area formula for an ellipsoid.
    a <- 6378137       # semi-major axis (m)
    b <- 6356752.3142  # semi-minor axis (m)
    e <- sqrt(1 - (b / a)^2)

    ymin_rad <- (y - yres / 2) * pi / 180
    ymax_rad <- (y + yres / 2) * pi / 180

    slice_area <- function(f) {
        zp <- 1 + e * sin(f)
        zm <- 1 - e * sin(f)
        pi * b^2 * ((2 * atanh(e * sin(f))) / (2 * e) + sin(f) / (zp * zm))
    }

    area_m2 <- (slice_area(ymax_rad) - slice_area(ymin_rad)) * (xres / 360)
    return(area_m2 / 10000)
}


# -- Matching helpers --------------------------------------------------------

filter_groups <- function(vals) {
    # Assign group interaction and keep only groups present in both
    # treatment and control sets
    vals$group <- interaction(vals$region, vals$ecoregion, vals$pa)
    vals <- filter(vals, group %in% unique(filter(vals, treatment)$group))

    treatment_groups <- unique(filter(vals, treatment)$group)
    control_groups <- unique(filter(vals, !treatment)$group)
    shared_groups <- treatment_groups[treatment_groups %in% control_groups]
    vals <- filter(vals, group %in% shared_groups)
    vals$group <- droplevels(vals$group)

    return(vals)
}


foreach_rbind <- function(d1, d2) {
    # Robust rbind for use with foreach .combine, handles NULL inputs
    if (is.null(d1) & is.null(d2)) return(NULL)
    if (!is.null(d1) & is.null(d2)) return(d1)
    if (is.null(d1) & !is.null(d2)) return(d2)
    return(bind_rows(d1, d2))
}


# -- Formula building -------------------------------------------------------

build_matching_formula <- function(covariates) {
    # Build a formula object: treatment ~ cov1 + cov2 + ...
    rhs <- paste(covariates, collapse = " + ")
    as.formula(paste("treatment ~", rhs))
}
