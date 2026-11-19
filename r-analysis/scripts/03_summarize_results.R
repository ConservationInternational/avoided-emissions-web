# Step 3: Summarize matching results into avoided emissions estimates.
#
# Reads each match file once, computes annual forest loss and emissions for
# the matched pixels (see pixel_outcomes() in stats_functions.R), and
# aggregates within each replicate.  Means and replicate ranges (2.5% / 97.5%
# quantiles across replicates) are then computed separately at every reported
# scope -- site-year, site total, and portfolio -- because quantiles of sums
# are not sums of quantiles.
#
# Estimates are built from cells: an atom within one exact-match stratum of a
# site fragment, or the whole stratum for inputs without atoms.  A cell is
# included when it matched in at least MIN_REPLICATE_SHARE of the replicates;
# a replicate in which it did not match takes the cell's mean over the others.
# Matched pixels are scaled to the cell's eligible pixels, and the observed
# (treatment) loss of an included cell is the census over all its eligible
# pixels (see cell_inclusion() in stats_functions.R).  Fragments without an
# included cell are reported as failed, or as not applicable when they have no
# exact-match region or no forest at baseline.
#
# Output (in {output_dir}):
#   results_by_site_year.csv, results_by_site_total.csv,
#   results_by_site_replicate_year.csv, results_by_site_replicate_total.csv,
#   results_by_atom_replicate_year.csv, results_atom_status.csv (atom inputs),
#   results_site_status.csv,
#   results_summary.json,
#   results_pixel_year_emissions.csv (replicate 1), results_pixel_covariates.csv,
#   results_covariate_balance.csv, results_propensity_scores.csv,
#   results_pixel_locations.csv, results_match_quality_summary.json,
#   results_sampling_by_site.csv, results_failed_sites.csv,
#   na_exclusion_summary.csv

library(arrow, warn.conflicts = FALSE)
library(jsonlite)

scripts_dir <- Sys.getenv("R_SCRIPTS_DIR", "/app/scripts")
source(file.path(scripts_dir, "utils.R"))
source(file.path(scripts_dir, "stats_functions.R"))
source(file.path(scripts_dir, "matching_diagnostics.R"))
source(file.path(scripts_dir, "summary_shards.R"))
rollbar_init()

with_rollbar({

config <- parse_config()
message("Step 3: Summarizing results")
step3_timer <- proc.time()

RANDOM_SEED <- if (is.null(config$random_seed)) NULL else as.integer(config$random_seed)
N_REPLICATES <- if (is.null(config$n_replicates)) 1L else as.integer(config$n_replicates)
FC_YEARS <- as.integer(config$fc_years)
FC_YEAR_MIN <- min(FC_YEARS)
FC_YEAR_MAX <- max(FC_YEARS)
GROUP_MAPPING <- fromJSON(config$config_path, simplifyVector = FALSE)$exact_match_group_mapping
BOUNDS_LABEL <- "Range across matching replicates"
LOW_COVERAGE <- 0.8
out_path <- function(name) file.path(config$output_dir, name)
summary_mode <- if (is.null(config$summary_mode)) "full" else config$summary_mode
if (!summary_mode %in% c("full", "shard", "reduce")) stop("Invalid summary mode")

formula_json <- fromJSON(out_path("formula.json"))
f <- as.formula(formula_json$formula_str)
CATEGORICAL <- factor_vars(f)
COVARIATES <- c(setdiff(all.vars(f), "treatment"), "defor_pre_intervention")

if (summary_mode == "reduce") {
    list2env(read_summary_states(unlist(config$summary_states)), envir = environment())
    if (!setequal(sites$site_id, unlist(config$summary_site_ids))) {
        stop("Incomplete summary site coverage")
    }
    sites <- arrange(sites, site_id)
    site_counts <- arrange(site_counts, site_id)
    results_by_year <- arrange(results_by_year, site_id, year)
    results_total <- arrange(results_total, site_id)
    census_agreement <- arrange(census_agreement, site_id)
    stock_balance <- arrange(stock_balance, site_id)
    write_csv(results_by_year, out_path("results_by_site_year.csv"))
    write_csv(results_total, out_path("results_by_site_total.csv"))
} else {
fragments <- read_parquet(
    out_path("sites_processed.parquet"),
    col_select = any_of(c("site_id", "display_site_id", "site_name", "id_numeric", "sub_site_index",
                         "start_year", "end_year", "area_ha", "ae_atoms"))
) %>%
    as_tibble() %>%
    mutate(site_id = as.character(site_id))
if (!is.null(config$summary_site_ids)) {
    fragments <- filter(fragments, site_id %in% unlist(config$summary_site_ids))
}
if (!"display_site_id" %in% names(fragments)) fragments$display_site_id <- fragments$site_id
atom_metadata <- NULL
atom_declarations <- tibble(
    run_id = character(), atomic_id = character(), site_id = character()
)
if ("ae_atoms" %in% names(fragments) && any(!is.na(fragments$ae_atoms))) {
    if (any(is.na(fragments$ae_atoms))) {
        stop("Atom-enabled sites_processed.parquet must declare ae_atoms for every parent.")
    }
    parent_metadata <- fragments %>%
        distinct(site_id, ae_atoms) %>%
        arrange(site_id)
    parsed_metadata <- lapply(parent_metadata$ae_atoms, function(value) {
        jsonlite::parse_json(value, simplifyVector = FALSE, bigint_as_char = TRUE)
    })
    run_ids <- vapply(parsed_metadata, function(value) value$run_id, character(1))
    if (length(unique(run_ids)) != 1L) {
        stop("Atom-enabled input rows must use one consistent ae_atoms run_id.")
    }
    atom_declarations <- bind_rows(lapply(seq_along(parsed_metadata), function(i) {
        atoms <- parsed_metadata[[i]]$atoms
        if (length(atoms) == 0L) stop("Atom-enabled parent has no declared atoms.")
        tibble(
            run_id = run_ids[i],
            atomic_id = vapply(atoms, function(atom) as.character(atom$atomic_id), character(1)),
            site_id = parent_metadata$site_id[i]
        )
    }))
    if (anyDuplicated(atom_declarations$atomic_id)) {
        stop("Atom IDs must be globally unique.")
    }
    atom_metadata <- list(run_id = unname(run_ids[1]))
}
atom_enabled <- !is.null(atom_metadata)
site_pixels <- read_parquet(out_path("treatment_cell_key.parquet"),
                            col_select = "id_numeric") %>%
    count(id_numeric, name = "n_site_pixels")
fragments <- fragments %>%
    left_join(site_pixels, by = "id_numeric") %>%
    mutate(n_site_pixels = coalesce(n_site_pixels, 0L))
sites <- fragments %>%
    group_by(site_id) %>%
    summarise(
        display_site_id = first(display_site_id),
        site_name = first(site_name), start_year = first(start_year),
        end_year = first(end_year), area_ha = sum(area_ha),
        n_site_pixels = sum(n_site_pixels), .groups = "drop"
    )
write_csv(select(sites, site_id, display_site_id, site_name), out_path("site_identity.csv"))


# -- Batch outputs -----------------------------------------------------------

# Each match batch writes mb_{batch}.parquet (matched pixels) and
# mb_{batch}.json (diagnostics, failure markers, missing-data exclusions).
read_sidecar <- function(path) {
    sidecar <- fromJSON(path, simplifyVector = FALSE)
    # Sampling, weights and the census must all come from this estimator.
    if (!is.null(sidecar$compatibility) &&
        !identical(as.integer(sidecar$compatibility$estimator_version), ESTIMATOR_VERSION)) {
        stop("Match outputs were written by another estimator version; rerun matching: ",
             basename(path))
    }
    compact_match_sidecar(sidecar)
}
sidecar_paths <- list.files(config$matches_dir, "^mb_.*\\.json$", full.names = TRUE)
sidecars <- lapply(
    sidecar_paths, read_sidecar
)

# Observed treatment outcomes of every eligible pixel, by cell.  Each worker
# that processed a fragment wrote the same census; the first copy is used.
census_records <- unlist(lapply(sidecars, function(x) x$treatment_census), recursive = FALSE)
sidecars <- lapply(sidecars, function(x) {
    x$treatment_census <- NULL
    x
})
census_records <- census_records[!duplicated(
    vapply(census_records, function(x) as.character(x$id_numeric), character(1))
)]
census_records <- Filter(function(x) x$id_numeric %in% fragments$id_numeric, census_records)
census_cell_ids <- function(x) {
    tibble(
        id_numeric = x$id_numeric,
        group = vapply(x$cells, function(cell) cell$group, character(1)),
        atomic_id = vapply(x$cells, function(cell) {
            if (is.null(cell$atomic_id)) NA_character_ else as.character(cell$atomic_id)
        }, character(1))
    )
}
census_cells <- bind_rows(
    tibble(id_numeric = integer(), group = character(), atomic_id = character(),
           n_eligible = numeric(), area_ha = numeric()),
    lapply(Filter(function(x) length(x$cells) > 0, census_records), function(x) {
        census_cell_ids(x) %>% mutate(
            n_eligible = vapply(x$cells, function(cell) as.numeric(cell$n_pixels), numeric(1)),
            area_ha = vapply(x$cells, function(cell) as.numeric(cell$area_ha), numeric(1))
        )
    })
)
census_years <- bind_rows(
    tibble(id_numeric = integer(), group = character(), atomic_id = character(),
           year = integer(), census_defor_ha = numeric(), census_emissions_mgco2e = numeric()),
    lapply(Filter(function(x) length(x$cells) > 0, census_records), function(x) {
        years <- as.integer(unlist(x$years))
        ids <- census_cell_ids(x)
        tibble(
            id_numeric = x$id_numeric,
            group = rep(ids$group, each = length(years)),
            atomic_id = rep(ids$atomic_id, each = length(years)),
            year = rep(years, times = nrow(ids)),
            census_defor_ha = as.numeric(unlist(lapply(x$cells, `[[`, "loss_ha"))),
            census_emissions_mgco2e = as.numeric(unlist(lapply(x$cells, `[[`, "emissions_mgco2e")))
        )
    })
)
census_errors <- Filter(function(x) !is.null(x$error), census_records)
rm(census_records)
batch_files <- unlist(lapply(sidecars, function(x) x$matches_file))
batch_paths <- file.path(config$matches_dir, batch_files)
if (isTRUE(config$validated_match_files) && any(!file.exists(batch_paths))) {
    stop("Missing validated match file")
}
batch_paths <- batch_paths[file.exists(batch_paths)]

read_timer <- proc.time()
matched_reps <- if (length(batch_paths) > 0 && isTRUE(config$validated_match_files)) {
    parts <- regmatches(
        basename(batch_paths),
        regexec("^mb_f([0-9]+)_r([0-9]+)\\.parquet$", basename(batch_paths))
    )
    if (any(lengths(parts) != 3L)) stop("Invalid validated match filename")
    ids <- as.numeric(vapply(parts, `[[`, character(1), 2L))
    replicates <- as.numeric(vapply(parts, `[[`, character(1), 3L))
    if (any(!is.finite(ids) | ids >= 2^53) || any(!is.finite(replicates))) {
        stop("Invalid validated match identifier")
    }
    tibble(
        id_numeric = ids,
        replicate = replicates
    )
} else if (length(batch_paths) > 0) {
    open_dataset(batch_paths) %>%
        distinct(id_numeric, replicate) %>%
        collect()
} else {
    tibble(id_numeric = integer(), replicate = integer())
}
# Fragments with matches in any replicate; inclusion is decided by cell below.
matched_ids <- unique(matched_reps$id_numeric)
frag_row <- function(id) fragments[match(id, fragments$id_numeric), ]
diagnostics_by_rep <- unlist(lapply(sidecars, function(x) x$diagnostics), recursive = FALSE)
message("[TIMING] Match index: ", round((proc.time() - read_timer)["elapsed"], 1), "s")


# -- Read each batch once ----------------------------------------------------

count_rows <- list()
cov_rows <- list()
diagnostics <- list()
detail_path <- out_path("results_pixel_year_emissions.csv")
if (file.exists(detail_path)) file.remove(detail_path)
# Replicate-1 pixel detail is staged by fragment, then written with its final
# weights once inclusion is known across replicates.
detail_parts <- out_path("pixel_detail_parts")
if (dir.exists(detail_parts)) {
    if (summary_mode == "shard") {
        stop("Summary detail partition directory already exists: ", detail_parts)
    }
    unlink(detail_parts, recursive = TRUE)
}
if (!dir.create(detail_parts)) stop("Cannot create summary detail partition directory")
CELL_KEYS <- c("id_numeric", "group", "atomic_id")

summarize_batch <- function(path) {
    batch <- read_parquet(path) %>%
        filter(id_numeric %in% matched_ids) %>%
        as_tibble() %>%
        mutate(cell = as.numeric(cell))
    if (!all(c("cell_n_eligible", "cell_n_sampled") %in% names(batch))) {
        stop("Match file lacks cell counts; rerun matching with the current estimator: ",
             basename(path))
    }
    lapply(split(batch, list(batch$id_numeric, batch$replicate), drop = TRUE), function(m) {
        id <- m$id_numeric[1]
        k <- m$replicate[1]
        frag <- frag_row(id)
        # Delay errors until the ordered merge can skip duplicate or invalid sites.
        result <- tryCatch({
            outcomes <- pixel_outcomes(m, frag$start_year, FC_YEAR_MIN, FC_YEAR_MAX)
            ids <- tibble(id_numeric = id, site_id = frag$site_id, replicate = k)
            atom_ids <- if (atom_enabled) as.character(m$atomic_id) else rep(NA_character_, nrow(m))
            detail <- NULL
            covariates <- NULL
            if (k == 1L) {
                n_years <- length(outcomes$years)
                detail <- tibble(
                    cell = rep(m$cell, times = n_years), id_numeric = id,
                    site_id = frag$site_id, group = rep(m$group, times = n_years),
                    year = rep(outcomes$years, each = nrow(m)),
                    treatment = rep(m$treatment, times = n_years),
                    match_group = rep(m$match_group, times = n_years),
                    match_weight = rep(m$match_weight, times = n_years),
                    sampling_weight = rep(m$sampling_weight, times = n_years),
                    forest_loss_ha = as.vector(outcomes$loss_ha),
                    emissions_mgco2e = as.vector(outcomes$emissions),
                    atomic_id = rep(atom_ids, times = n_years)
                )
                # Forest and biomass each pixel held at the site's baseline: what
                # it could lose.  Outcomes are absolute, so these stocks should
                # be alike between a treatment pixel and its controls.
                baseline_fc <- m[[paste0("fc_", baseline_year(frag$start_year, FC_YEAR_MIN))]]
                covariates <- m %>%
                    select(cell, group, treatment, match_group, match_weight, pscore,
                           any_of(COVARIATES)) %>%
                    mutate(id_numeric = id, site_id = frag$site_id, .before = 1) %>%
                    mutate(
                        atomic_id = atom_ids,
                        baseline_forest_ha = baseline_fc / 100 * m$area_ha,
                        baseline_biomass_mg = if_else(
                            m$fc_2000 > 0,
                            m$total_biomass_2000 * m$area_ha * baseline_fc / m$fc_2000, 0
                        )
                    )
            }
            # Matched-sample sums per cell; expansion is applied after all
            # replicates are read.
            sums <- if (atom_enabled) {
                aggregate_atom_outcomes(m, outcomes) %>%
                    mutate(atomic_id = as.character(atomic_id))
            } else {
                aggregate_outcomes(m, outcomes) %>% mutate(atomic_id = NA_character_)
            }
            treated <- m$treatment
            list(
                cell_rows = bind_cols(ids, select(sums, group, atomic_id, year,
                                                  starts_with("sample_"))),
                cells = bind_cols(ids, tibble(
                    group = as.character(m$group[treated]), atomic_id = atom_ids[treated],
                    n_eligible = as.numeric(m$cell_n_eligible[treated]),
                    n_sampled = as.numeric(m$cell_n_sampled[treated])
                ) %>%
                    group_by(group, atomic_id) %>%
                    summarise(n_distinct_counts = n_distinct(n_eligible, n_sampled),
                              n_eligible = n_eligible[1], n_sampled = n_sampled[1],
                              n_matched = n(), .groups = "drop")),
                counts = bind_cols(ids, tibble(
                    n_eligible = m$n_eligible[1], n_sampled = m$n_sampled[1],
                    n_forest_pixels = m$n_forest_pixels[1],
                    n_control_pool = m$n_control_pool[1], n_control_sampled = m$n_control_sampled[1]
                )),
                detail = detail, covariates = covariates
            )
        }, error = function(e) e)
        list(id = id, replicate = k, fragment = frag, path = path, result = result)
    })
}

process_batches <- function(paths, invalid_sites = list()) {
    workers <- if (is.null(config$summary_workers)) 1L else as.integer(config$summary_workers)
    if (is.na(workers) || workers < 1L) stop("Invalid summary worker count")
    cluster <- NULL
    if (workers > 1L && length(paths) > 0L) {
        cluster <- parallel::makePSOCKcluster(workers)
        on.exit(parallel::stopCluster(cluster), add = TRUE)
        parallel::clusterExport(
            cluster, c("summarize_batch", "matched_ids", "fragments", "frag_row",
                       "FC_YEAR_MIN", "FC_YEAR_MAX", "COVARIATES", "scripts_dir",
                       "atom_enabled"),
            envir = environment()
        )
        parallel::clusterEvalQ(cluster, {
            library(arrow, warn.conflicts = FALSE)
            arrow::set_cpu_count(1L)
            library(tidyverse)
            source(file.path(scripts_dir, "stats_functions.R"))
            NULL
        })
    }
    seen <- new.env(hash = TRUE, parent = emptyenv())
    cell_rows <- list()
    cell_stats <- list()
    count_rows <- list()
    cov_rows <- list()
    for (start in if (length(paths)) seq.int(1L, length(paths), by = workers * 2L) else integer()) {
        chunk <- paths[start:min(length(paths), start + workers * 2L - 1L)]
        results <- if (is.null(cluster)) lapply(chunk, summarize_batch) else
            parallel::parLapply(cluster, chunk, summarize_batch)
        for (batch in results) for (entry in batch) {
            id <- entry$id
            k <- entry$replicate
            frag <- entry$fragment
            key <- paste(id, k)
            if (exists(key, envir = seen, inherits = FALSE)) next
            assign(key, TRUE, envir = seen)
            if (frag$site_id %in% names(invalid_sites)) next
            result <- entry$result
            if (inherits(result, "invalid_pixel_outcomes")) {
                detail <- paste0(
                    "Invalid outcome inputs: site_id=", frag$site_id,
                    ", site_name=", frag$site_name, ", fragment=", id,
                    ", sub_site_index=", frag$sub_site_index, ", replicate=", k,
                    ", match_file=", basename(entry$path), ": ", conditionMessage(result)
                )
                invalid_sites[[frag$site_id]] <- detail
                message("  ", detail, "; excluding entire site from estimates")
                next
            }
            if (inherits(result, "error")) stop(result)
            i <- length(cell_rows) + 1L
            cell_rows[[i]] <- result$cell_rows
            cell_stats[[i]] <- result$cells
            count_rows[[i]] <- result$counts
            if (!is.null(result$detail)) {
                write_parquet(result$detail, file.path(
                    detail_parts, sprintf("part%06d.parquet", length(cov_rows) + 1L)
                ))
                cov_rows[[length(cov_rows) + 1L]] <- result$covariates
            }
        }
    }
    list(cell_rows = cell_rows, cell_stats = cell_stats, count_rows = count_rows,
         cov_rows = cov_rows, invalid_sites = invalid_sites)
}
aggregate_timer <- proc.time()
# A fragment whose census found invalid outcome inputs invalidates its site.
census_invalid <- list()
for (record in census_errors) {
    frag <- frag_row(record$id_numeric)
    if (is.null(census_invalid[[frag$site_id]])) {
        census_invalid[[frag$site_id]] <- paste0(
            "Invalid outcome inputs: site_id=", frag$site_id, ", site_name=", frag$site_name,
            ", fragment=", frag$id_numeric, ", sub_site_index=", frag$sub_site_index,
            ", treatment census: ", record$error
        )
        message("  ", census_invalid[[frag$site_id]], "; excluding entire site from estimates")
    }
}
processed <- process_batches(batch_paths, census_invalid)
message("[TIMING] Match aggregation: ", round((proc.time() - aggregate_timer)["elapsed"], 1),
        "s across ", length(batch_paths), " files")
count_rows <- processed$count_rows
cov_rows <- processed$cov_rows
invalid_sites <- processed$invalid_sites
diagnostics <- unlist(lapply(
    Filter(function(d) d$replicate == 1L && d$id_numeric %in% matched_ids &&
               !d$id_numeric %in% fragments$id_numeric[fragments$site_id %in% names(invalid_sites)],
           diagnostics_by_rep),
    function(d) d$group_diagnostics
), recursive = FALSE)

# -- Cell inclusion and expansion --------------------------------------------

valid_ids <- fragments$id_numeric[!fragments$site_id %in% names(invalid_sites)]
cell_stats <- bind_rows(
    tibble(id_numeric = integer(), site_id = character(), replicate = integer(),
           group = character(), atomic_id = character(), n_distinct_counts = integer(),
           n_eligible = numeric(), n_sampled = numeric(), n_matched = integer()),
    processed$cell_stats
) %>% filter(id_numeric %in% valid_ids)
if (any(cell_stats$n_distinct_counts != 1L) || anyNA(cell_stats$n_eligible) ||
    anyNA(cell_stats$n_sampled)) {
    stop("Match files carry inconsistent cell counts; rerun matching")
}
census_cells <- filter(census_cells, id_numeric %in% valid_ids)
census_years <- filter(census_years, id_numeric %in% valid_ids)
# The census is the roster of eligible cells; every matched cell must be in
# it with the same number of eligible pixels.
census_check <- cell_stats %>%
    distinct(across(all_of(CELL_KEYS)), n_eligible) %>%
    left_join(rename(census_cells, n_census = n_eligible), by = CELL_KEYS)
if (any(is.na(census_check$n_census) | census_check$n_census != census_check$n_eligible)) {
    stop("Matched cells disagree with the treatment census; rerun matching")
}
inclusion <- cell_inclusion(cell_stats, N_REPLICATES)
# One row per eligible cell, matched or not.
cells <- census_cells %>%
    left_join(select(inclusion$cells, -n_eligible), by = CELL_KEYS) %>%
    mutate(
        across(c(n_replicates_matched, match_rate, mean_matched, mean_sampled, n_supported),
               ~ coalesce(as.numeric(.x), 0)),
        across(c(included, census_arm), ~ coalesce(.x, FALSE)),
        supported_area_ha = if_else(n_eligible > 0, area_ha * n_supported / n_eligible, 0)
    ) %>%
    left_join(select(fragments, id_numeric, site_id), by = "id_numeric")
complete_strata <- cells %>%
    filter(included) %>%
    distinct(id_numeric, group)
n_inconsistent_strata <- nrow(distinct(cell_stats, id_numeric, group)) - nrow(complete_strata)
included_ids <- unique(complete_strata$id_numeric)

sample_cols <- c(
    "sample_treatment_defor_ha", "sample_control_defor_ha",
    "sample_treatment_emissions_mgco2e", "sample_control_emissions_mgco2e"
)
extrapolated_cols <- sub("^sample_", "extrapolated_", sample_cols)
# Matched-sample sums are scaled to each cell's eligible pixels.  Where the
# cell supports it, the observed arm is replaced by the census, which has no
# sampling error; the scaled sample is kept to check the expansion.
cell_year_rep <- bind_rows(
    tibble(id_numeric = integer(), site_id = character(), replicate = integer(),
           group = character(), atomic_id = character(), year = integer()),
    as_tibble(setNames(rep(list(numeric()), 4), sample_cols)),
    processed$cell_rows
) %>%
    inner_join(inclusion$factors, by = c(CELL_KEYS, "replicate")) %>%
    left_join(census_years, by = c(CELL_KEYS, "year"))
rm(processed)
if (anyNA(cell_year_rep$census_defor_ha[cell_year_rep$census_arm])) {
    stop("Missing treatment census for an included cell; rerun matching")
}
cell_year_rep <- cell_year_rep %>%
    mutate(
        sampled_treatment_defor_ha = sample_treatment_defor_ha * sampling_weight,
        extrapolated_treatment_defor_ha =
            if_else(census_arm, census_defor_ha, sampled_treatment_defor_ha),
        extrapolated_control_defor_ha = sample_control_defor_ha * sampling_weight,
        extrapolated_treatment_emissions_mgco2e = if_else(
            census_arm, census_emissions_mgco2e,
            sample_treatment_emissions_mgco2e * sampling_weight
        ),
        extrapolated_control_emissions_mgco2e =
            sample_control_emissions_mgco2e * sampling_weight
    ) %>%
    select(all_of(CELL_KEYS), site_id, replicate, year, all_of(sample_cols),
           all_of(extrapolated_cols), sampled_treatment_defor_ha) %>%
    fill_missing_replicates(
        c(CELL_KEYS, "site_id", "year"), seq_len(N_REPLICATES),
        c(sample_cols, extrapolated_cols, "sampled_treatment_defor_ha")
    )
# Agreement of the expanded treatment sample with the census, by site.
census_agreement <- cell_year_rep %>%
    semi_join(filter(cells, census_arm), by = CELL_KEYS) %>%
    group_by(site_id, replicate) %>%
    summarise(census = sum(extrapolated_treatment_defor_ha),
              sampled = sum(sampled_treatment_defor_ha), .groups = "drop") %>%
    group_by(site_id) %>%
    summarise(
        census_treatment_defor_ha = census[1],
        sampled_treatment_defor_ha = mean(sampled),
        sampled_treatment_defor_ha_se = if (n() > 1) sd(sampled) / sqrt(n()) else NA_real_,
        .groups = "drop"
    )

# Final weights for the replicate-1 pixel detail and covariates.
factors_r1 <- filter(inclusion$factors, replicate == 1L)
final_pixels <- function(pixels) {
    # Keep pixels of included cells, with their final weights.
    if (nrow(pixels) == 0) return(pixels)
    weights <- factors_r1[factors_r1$id_numeric == pixels$id_numeric[1], ]
    if (!atom_enabled) weights$atomic_id <- NULL
    apply_cell_weights(if (atom_enabled) pixels else select(pixels, -atomic_id), weights) %>%
        select(-any_of("atomic_id"))
}
for (path in list.files(detail_parts, "\\.parquet$", full.names = TRUE)) {
    detail <- final_pixels(read_parquet(path))
    if (nrow(detail)) write_csv(detail, detail_path, append = file.exists(detail_path))
}
unlink(detail_parts, recursive = TRUE)
if (!file.exists(detail_path)) {
    write_csv(tibble(cell = integer(), site_id = character(), year = integer()), detail_path)
}


# -- Work reconciliation -----------------------------------------------------

# Fragment-level markers come from the batch sidecars; worker-level markers
# (failed_array_*.json) are written when a whole match worker crashed.
fragment_markers <- unlist(lapply(sidecars, function(x) x$failures), recursive = FALSE)
worker_failures <- lapply(
    list.files(config$matches_dir, "^failed_array_.*\\.json$", full.names = TRUE),
    fromJSON, simplifyVector = FALSE
)
markers <- if (length(fragment_markers) > 0) {
    tibble(
        id_numeric = vapply(fragment_markers, function(x) as.integer(x$id_numeric), integer(1)),
        reason = vapply(fragment_markers, function(x) {
            if (is.null(x$reason)) "error" else x$reason
        }, character(1)),
        error = vapply(fragment_markers, function(x) x$error, character(1)),
        n_forest_pixels = vapply(fragment_markers, function(x) {
            if (is.null(x$n_forest_pixels)) NA_real_ else as.numeric(x$n_forest_pixels)
        }, numeric(1))
    ) %>% distinct(id_numeric, .keep_all = TRUE)
} else {
    tibble(id_numeric = integer(), reason = character(), error = character(),
           n_forest_pixels = numeric())
}

mapped_keys <- if (is.null(GROUP_MAPPING)) {
    NULL
} else {
    vapply(unlist(GROUP_MAPPING, recursive = FALSE),
           function(m) paste(m[[1]], m[[2]], sep = "\r"), character(1))
}
# Fragments without any pixel (e.g. slivers smaller than a pixel) have nothing
# to estimate and are neither failures nor not applicable.
unused_fragments <- fragments %>%
    filter(!id_numeric %in% included_ids, n_site_pixels > 0) %>%
    left_join(markers, by = "id_numeric")
unmapped <- if (is.null(mapped_keys)) {
    rep(FALSE, nrow(unused_fragments))
} else {
    !paste(unused_fragments$site_id, unused_fragments$sub_site_index, sep = "\r") %in% mapped_keys
}
worker_error <- if (length(worker_failures) > 0) {
    paste("No match output; worker failure:", worker_failures[[1]]$error)
} else {
    "No match output for one or more replicates"
}
unused_fragments <- unused_fragments %>%
    mutate(
        outcome_error = vapply(site_id, function(id) {
            if (is.null(invalid_sites[[id]])) NA_character_ else invalid_sites[[id]]
        }, character(1)),
        error = case_when(
            !is.na(outcome_error) ~ outcome_error,
            unmapped ~ "Site has no exact-match region assignment and was not matched",
            !is.na(error) ~ error,
            id_numeric %in% matched_ids ~ paste0(
                "No exact-match stratum was matched in at least ",
                MIN_REPLICATE_SHARE * 100, "% of replicates"
            ),
            TRUE ~ worker_error
        ),
        reason = if_else(!is.na(outcome_error), "invalid_outcome_inputs", reason),
        status = if_else(is.na(outcome_error) &
                             (unmapped | coalesce(reason, "") == "no_eligible_treatment"),
                         "not_applicable", "failed")
    )
failed_fragments <- filter(unused_fragments, status == "failed")
write_csv(
    select(unused_fragments, id_numeric, site_id, site_name, sub_site_index, status, reason, error),
    out_path("results_failed_sites.csv")
)
n_failed_sites <- n_distinct(failed_fragments$site_id)
message("  ", length(included_ids), " fragment(s) included; ", nrow(failed_fragments),
        " fragment(s) in ", n_failed_sites, " site(s) failed; ",
        nrow(unused_fragments) - nrow(failed_fragments), " fragment(s) not applicable; ",
        n_inconsistent_strata, " stratum/strata excluded for matching in too few replicates")


# -- Estimates ---------------------------------------------------------------

metric_cols <- c(
    "sample_treatment_defor_ha", "sample_control_defor_ha",
    "sample_treatment_emissions_mgco2e", "sample_control_emissions_mgco2e",
    "extrapolated_treatment_defor_ha", "extrapolated_control_defor_ha",
    "extrapolated_treatment_emissions_mgco2e", "extrapolated_control_emissions_mgco2e",
    "sample_forest_loss_avoided_ha", "sample_emissions_avoided_mgco2e",
    "extrapolated_forest_loss_avoided_ha", "extrapolated_emissions_avoided_mgco2e"
)

atom_status <- NULL
atom_year_rep <- NULL
if (atom_enabled) {
    atom_counts <- if (file.exists(out_path("ae_atom_counts.parquet"))) {
        read_parquet(out_path("ae_atom_counts.parquet")) %>%
            group_by(site_id, atomic_id) %>%
            summarise(
                n_site_pixels = sum(n_site_pixels),
                n_forest_pixels = sum(n_forest_pixels),
                .groups = "drop"
            ) %>%
            mutate(atomic_id = as.character(atomic_id))
    } else {
        tibble(
            site_id = character(), atomic_id = character(),
            n_site_pixels = numeric(), n_forest_pixels = numeric()
        )
    }
    atom_counts <- atom_declarations %>%
        select(run_id, atomic_id, site_id) %>%
        left_join(atom_counts, by = c("site_id", "atomic_id")) %>%
        mutate(
            n_site_pixels = coalesce(n_site_pixels, 0),
            n_forest_pixels = coalesce(n_forest_pixels, 0)
        )
    # Every included cell has every replicate and year, so atoms do too.
    atom_year_rep <- cell_year_rep %>%
        group_by(site_id, atomic_id, replicate, year) %>%
        summarise(across(all_of(metric_cols[1:8]), sum), .groups = "drop") %>%
        add_avoided()
    atom_metrics <- atom_year_rep %>%
        select(starts_with("extrapolated_"))
    if (nrow(atom_metrics) > 0 &&
        any(!vapply(atom_metrics, function(value) all(is.finite(value)), logical(1)))) {
        stop("Atom annual results contain non-finite additive metrics.")
    }
    atom_replicates <- atom_year_rep %>%
        inner_join(select(sites, site_id, start_year, end_year), by = "site_id") %>%
        transmute(
            run_id = atom_metadata$run_id,
            atomic_id, site_id, replicate, year,
            is_intervention_period = year >= start_year & year <= end_year,
            across(starts_with("extrapolated_"))
        ) %>%
        arrange(run_id, atomic_id, replicate, year)
    write_csv(atom_replicates, out_path("results_by_atom_replicate_year.csv"))

    # Coverage comes from the roster of eligible cells, not from the atoms
    # that happen to have estimates: an atom is covered only as far as its
    # eligible pixels lie in included cells.
    atom_cells <- cells %>%
        group_by(site_id, atomic_id) %>%
        summarise(
            n_eligible_pixels = sum(n_eligible),
            n_supported_pixels = sum(n_supported),
            eligible_area_ha = sum(area_ha),
            supported_area_ha = sum(supported_area_ha),
            n_sample_pixels = sum(mean_matched[included]),
            min_replicates_matched = if (any(included)) {
                min(n_replicates_matched[included])
            } else 0,
            sample_arm = any(included & !census_arm),
            .groups = "drop"
        )
    atom_status <- atom_counts %>%
        left_join(atom_cells, by = c("site_id", "atomic_id")) %>%
        mutate(
            across(c(n_eligible_pixels, n_supported_pixels, eligible_area_ha,
                     supported_area_ha, n_sample_pixels, min_replicates_matched),
                   ~ coalesce(.x, 0)),
            sample_arm = coalesce(sample_arm, FALSE),
            outcome_error = vapply(site_id, function(id) {
                if (is.null(invalid_sites[[id]])) NA_character_ else invalid_sites[[id]]
            }, character(1))
        ) %>%
        left_join(select(sites, site_id, start_year, end_year), by = "site_id") %>%
        left_join(
            atom_year_rep %>%
                group_by(site_id, atomic_id) %>%
                summarise(
                    first_year = min(year), last_year = max(year),
                    .groups = "drop"
                ),
            by = c("site_id", "atomic_id")
        ) %>%
        mutate(
            forest_coverage_fraction = if_else(
                n_forest_pixels > 0, pmin(1, n_supported_pixels / n_forest_pixels), 0
            ),
            status = case_when(
                n_site_pixels == 0 ~ "not_applicable",
                n_forest_pixels == 0 ~ "not_applicable",
                n_supported_pixels == 0 ~ "failed",
                forest_coverage_fraction < LOW_COVERAGE | sample_arm ~ "partial",
                TRUE ~ "estimated"
            ),
            treatment_arm = case_when(
                !status %in% c("estimated", "partial") ~ NA_character_,
                sample_arm ~ "sample",
                TRUE ~ "census"
            ),
            status_reason = case_when(
                n_site_pixels == 0 ~ "No atom pixels fall on the analysis grid.",
                n_forest_pixels == 0 ~ "No forest pixels at the parent baseline year.",
                status == "failed" & !is.na(outcome_error) ~ outcome_error,
                status == "failed" ~ paste0(
                    "No exact-match stratum of the atom matched in at least ",
                    MIN_REPLICATE_SHARE * 100, "% of replicates."
                ),
                status == "partial" ~ paste0(
                    "Estimates represent ", round(forest_coverage_fraction * 100, 1),
                    "% of the atom's baseline forest pixels",
                    if_else(sample_arm, paste0(
                        "; a match rate below ", MIN_MATCH_RATE * 100,
                        "% leaves unmatched pixels unrepresented"
                    ), ""),
                    "."
                ),
                TRUE ~ NA_character_
            )
        ) %>%
        select(
            run_id, atomic_id, site_id, status, status_reason,
            n_site_pixels, n_forest_pixels, n_eligible_pixels, n_supported_pixels,
            forest_coverage_fraction, eligible_area_ha, supported_area_ha,
            n_sample_pixels, min_replicates_matched, treatment_arm,
            first_year, last_year, start_year, end_year
        )
    write_csv(atom_status, out_path("results_atom_status.csv"))
}

# Cells are summed to sites within each replicate.
site_year_rep <- cell_year_rep %>%
    group_by(replicate, site_id, year) %>%
    summarise(across(all_of(metric_cols[1:8]), sum), .groups = "drop") %>%
    add_avoided()
write_csv(
    site_year_rep %>%
        select(site_id, replicate, year, starts_with("extrapolated_")) %>%
        arrange(site_id, replicate, year),
    out_path("results_by_site_replicate_year.csv")
)

# Sampling counts are averaged over the replicates in which a fragment
# matched; represented pixels are the supported pixels of included cells.
site_counts <- bind_rows(
    tibble(id_numeric = integer(), site_id = character(), replicate = integer(),
           n_eligible = numeric(), n_sampled = numeric(), n_forest_pixels = numeric(),
           n_control_pool = numeric(), n_control_sampled = numeric()),
    count_rows
) %>%
    filter(id_numeric %in% included_ids) %>%
    group_by(id_numeric, site_id) %>%
    summarise(across(c(n_eligible, n_sampled, n_forest_pixels, n_control_pool,
                       n_control_sampled), mean), .groups = "drop") %>%
    left_join(
        cells %>%
            filter(included) %>%
            group_by(id_numeric) %>%
            summarise(n_matched = sum(mean_matched), n_represented = sum(n_supported),
                      sample_arm = any(!census_arm), .groups = "drop"),
        by = "id_numeric"
    ) %>%
    group_by(site_id) %>%
    summarise(
        n_sample_pixels = as.integer(round(sum(n_matched))),
        n_eligible_pixels = sum(n_eligible),
        n_represented_pixels = sum(n_represented),
        n_forest_pixels = sum(n_forest_pixels),
        sampled_fraction = sum(n_sampled) / sum(n_eligible),
        match_rate = sum(n_matched) / sum(n_sampled),
        n_control_pool = sum(n_control_pool),
        n_control_sampled = sum(n_control_sampled),
        sample_arm = any(sample_arm),
        .groups = "drop"
    ) %>%
    left_join(select(sites, site_id, n_site_pixels), by = "site_id") %>%
    # Forest pixels of fragments that were matched but not used.
    left_join(
        failed_fragments %>%
            group_by(site_id) %>%
            summarise(n_failed_fragments = n(),
                      n_failed_forest = sum(n_forest_pixels, na.rm = TRUE), .groups = "drop"),
        by = "site_id"
    ) %>%
    mutate(
        n_failed_fragments = coalesce(n_failed_fragments, 0L),
        n_forest_pixels = n_forest_pixels + coalesce(n_failed_forest, 0),
        # Share of the site's pixels, and of its forested pixels, that the
        # estimates represent.
        coverage_fraction = n_represented_pixels / n_site_pixels,
        forest_coverage_fraction = n_represented_pixels / n_forest_pixels,
        status = if_else(
            forest_coverage_fraction < LOW_COVERAGE | n_failed_fragments > 0 | sample_arm,
            "partial", "estimated"
        )
    ) %>%
    select(-n_failed_forest, -n_failed_fragments, -sample_arm)

if (atom_enabled) {
    prior_counts <- site_counts %>%
        select(site_id, n_control_pool, n_control_sampled, match_rate)
    atom_site_counts <- atom_status %>%
        group_by(site_id) %>%
        summarise(
            n_atom_forest_pixels = sum(n_forest_pixels),
            n_eligible_pixels = sum(n_eligible_pixels),
            n_represented_pixels = sum(n_supported_pixels),
            n_sample_pixels = sum(n_sample_pixels),
            sample_arm = any(coalesce(treatment_arm, "") == "sample"),
            .groups = "drop"
        )
    site_counts <- sites %>%
        select(site_id, n_site_pixels) %>%
        left_join(atom_site_counts, by = "site_id") %>%
        left_join(prior_counts, by = "site_id") %>%
        mutate(
            n_sample_pixels = coalesce(n_sample_pixels, 0),
            n_eligible_pixels = coalesce(n_eligible_pixels, 0),
            n_represented_pixels = coalesce(n_represented_pixels, 0),
            n_forest_pixels = coalesce(n_atom_forest_pixels, 0),
            n_control_pool = coalesce(n_control_pool, 0),
            n_control_sampled = coalesce(n_control_sampled, 0),
            sampled_fraction = if_else(
                n_eligible_pixels > 0, n_sample_pixels / n_eligible_pixels, 0
            ),
            match_rate = coalesce(match_rate, 0),
            coverage_fraction = if_else(
                n_site_pixels > 0, n_represented_pixels / n_site_pixels, 0
            ),
            forest_coverage_fraction = if_else(
                n_forest_pixels > 0, pmin(1, n_represented_pixels / n_forest_pixels), 0
            ),
            status = case_when(
                n_site_pixels == 0 | n_forest_pixels == 0 ~ "not_applicable",
                n_represented_pixels == 0 ~ "failed",
                forest_coverage_fraction < LOW_COVERAGE | coalesce(sample_arm, FALSE) ~ "partial",
                TRUE ~ "estimated"
            )
        ) %>%
        select(
            site_id, n_sample_pixels, n_eligible_pixels, n_represented_pixels,
            n_forest_pixels, sampled_fraction, match_rate, n_control_pool,
            n_control_sampled, n_site_pixels, coverage_fraction,
            forest_coverage_fraction, status
        )
}

# Status of every site with pixels: estimated / partial from site_counts,
# otherwise failed if any fragment failed, else not applicable.
site_status <- sites %>%
    filter(n_site_pixels > 0) %>%
    select(site_id, display_site_id, site_name, n_site_pixels) %>%
    left_join(select(site_counts, site_id, status, n_forest_pixels,
                     coverage_fraction, forest_coverage_fraction), by = "site_id") %>%
    left_join(
        unused_fragments %>%
            group_by(site_id) %>%
            summarise(
                any_failed = any(status == "failed"),
                status_reason = paste(unique(error), collapse = "; "),
                .groups = "drop"
            ),
        by = "site_id"
    ) %>%
    mutate(
        status = coalesce(status, if_else(coalesce(any_failed, FALSE), "failed", "not_applicable")),
        status_reason = case_when(
            status == "estimated" ~ NA_character_,
            status == "partial" & is.na(status_reason) ~ paste0(
                "Estimates represent ", round(forest_coverage_fraction * 100, 1),
                "% of the site's baseline forest pixels"
            ),
            TRUE ~ status_reason
        )
    ) %>%
    select(-any_failed)
if (atom_enabled) n_failed_sites <- sum(site_status$status == "failed")
write_csv(site_status, out_path("results_site_status.csv"))

results_by_year <- (if (nrow(site_year_rep) > 0) {
    summarise_replicates(site_year_rep, c("site_id", "year"), metric_cols)
} else {
    tibble(site_id = character(), year = integer())
}) %>%
    left_join(
        (if (nrow(site_year_rep) > 0) {
            cumulative_site_year_rep <- site_year_rep %>%
                arrange(replicate, site_id, year) %>%
                group_by(replicate, site_id) %>%
                mutate(
                    extrapolated_cumulative_treatment_defor_ha =
                        cumsum(extrapolated_treatment_defor_ha),
                    extrapolated_cumulative_control_defor_ha =
                        cumsum(extrapolated_control_defor_ha)
                ) %>%
                ungroup()
            summarise_replicates(
                cumulative_site_year_rep,
                c("site_id", "year"),
                c(
                    "extrapolated_cumulative_treatment_defor_ha",
                    "extrapolated_cumulative_control_defor_ha"
                )
            )
        } else {
            tibble(site_id = character(), year = integer())
        }),
        by = c("site_id", "year")
    ) %>%
    inner_join(select(sites, site_id, site_name, start_year, end_year), by = "site_id") %>%
    left_join(select(site_counts, site_id, n_sample_pixels, sampled_fraction), by = "site_id") %>%
    mutate(
        is_pre_intervention = year < start_year,
        is_post_intervention = year > end_year
    ) %>%
    select(-start_year, -end_year)
write_csv(results_by_year, out_path("results_by_site_year.csv"))

intervention_rep <- site_year_rep %>%
    inner_join(select(sites, site_id, start_year, end_year), by = "site_id") %>%
    filter(year >= start_year, year <= end_year)
site_total_rep <- intervention_rep %>%
    group_by(replicate, site_id) %>%
    summarise(across(all_of(metric_cols), sum), .groups = "drop")
# Per-replicate totals let consumers aggregate sites with correct ranges.
write_csv(
    site_total_rep %>%
        select(site_id, replicate, starts_with("extrapolated_")) %>%
        arrange(site_id, replicate),
    out_path("results_by_site_replicate_total.csv")
)
results_total <- (if (nrow(site_total_rep) > 0) {
    summarise_replicates(site_total_rep, "site_id", metric_cols)
} else {
    tibble(site_id = character())
}) %>%
    inner_join(
        intervention_rep %>%
            filter(replicate == 1L) %>%
            group_by(site_id) %>%
            summarise(first_year = min(year), last_year = max(year),
                      n_years = n_distinct(year), .groups = "drop"),
        by = "site_id"
    ) %>%
    inner_join(select(sites, site_id, site_name, area_ha), by = "site_id") %>%
    inner_join(site_counts, by = "site_id") %>%
    rename(n_treatment_pixels = n_site_pixels)
write_csv(results_total, out_path("results_by_site_total.csv"))
message("  Per-site results: ", nrow(results_by_year), " site-years, ",
        nrow(results_total), " sites")
}
phase_timer <- proc.time()[["elapsed"]]
log_summary_phase <- function(name) {
    now <- proc.time()[["elapsed"]]
    message("[TIMING] ", name, ": ", round(now - phase_timer, 1), "s")
    phase_timer <<- now
}
message("  Finalizing: portfolio statistics")

# Portfolio series are summed within each replicate, then summarised.  The
# names match the fields plotted by the web app.
portfolio_cols <- c(
    treatment_defor_ha = "extrapolated_treatment_defor_ha",
    control_defor_ha = "extrapolated_control_defor_ha",
    treatment_emissions_mgco2e = "extrapolated_treatment_emissions_mgco2e",
    control_emissions_mgco2e = "extrapolated_control_emissions_mgco2e",
    forest_loss_avoided_ha = "extrapolated_forest_loss_avoided_ha",
    emissions_avoided_mgco2e = "extrapolated_emissions_avoided_mgco2e"
)
portfolio_yearly <- list()
portfolio_total <- list()
if (nrow(site_year_rep) > 0) {
    yearly_rep <- site_year_rep %>%
        group_by(replicate, year) %>%
        summarise(across(all_of(unname(portfolio_cols)), sum), n_sites = n(), .groups = "drop") %>%
        rename(all_of(portfolio_cols)) %>%
        arrange(replicate, year) %>%
        group_by(replicate) %>%
        mutate(
            cum_treatment_defor_ha = cumsum(treatment_defor_ha),
            cum_control_defor_ha = cumsum(control_defor_ha)
        ) %>%
        ungroup()
    yearly_metrics <- c(names(portfolio_cols), "cum_treatment_defor_ha", "cum_control_defor_ha")
    portfolio_yearly <- summarise_replicates(
        select(yearly_rep, -n_sites), "year", yearly_metrics
    ) %>%
        inner_join(distinct(yearly_rep, year, n_sites), by = "year")
}
if (nrow(site_total_rep) > 0) {
    portfolio_total <- site_total_rep %>%
        group_by(replicate) %>%
        summarise(across(all_of(metric_cols), sum), .groups = "drop") %>%
        summarise_replicates(character(0), metric_cols) %>%
        as.list()
}


# -- Match quality -----------------------------------------------------------

log_summary_phase("Portfolio statistics")
message("  Finalizing: pixel covariate exports")
if (summary_mode != "reduce") {
    cov_data <- bind_rows(
        tibble(id_numeric = integer(), site_id = character(), cell = numeric(),
               group = character(), treatment = logical(), match_group = character(),
               match_weight = numeric(), pscore = numeric(), sampling_weight = numeric(),
               baseline_forest_ha = numeric(), baseline_biomass_mg = numeric()),
        lapply(cov_rows, final_pixels)
    )
    # Baseline stock of the treatment sample and of its matched controls,
    # weighted as in the estimates (replicate 1).  A ratio far from 1 means
    # the two arms could not lose the same amount, whatever their loss rates.
    stock_balance <- cov_data %>%
        mutate(weight = match_weight * sampling_weight) %>%
        group_by(site_id) %>%
        summarise(
            treatment_baseline_forest_ha = sum(weight[treatment] * baseline_forest_ha[treatment]),
            control_baseline_forest_ha = sum(weight[!treatment] * baseline_forest_ha[!treatment]),
            treatment_baseline_biomass_mg =
                sum(weight[treatment] * baseline_biomass_mg[treatment]),
            control_baseline_biomass_mg =
                sum(weight[!treatment] * baseline_biomass_mg[!treatment]),
            .groups = "drop"
        )
    cov_data <- select(cov_data, -id_numeric, -group, -sampling_weight)
}
covariates <- intersect(COVARIATES, names(cov_data))
if (summary_mode != "reduce") {
    write_csv(cov_data, out_path("results_pixel_covariates.csv"))
    write_csv(
        select(cov_data, cell, site_id, treatment, match_group, match_weight, pscore),
        out_path("results_propensity_scores.csv")
    )
}

log_summary_phase("Pixel covariate exports")
message("  Finalizing: covariate balance")
site_balance <- if (summary_mode == "reduce") {
    read_csv(out_path("results_covariate_balance.csv"), show_col_types = FALSE) %>%
        filter(site_id != "__all__")
} else {
    cov_data %>%
        group_by(site_id) %>%
        group_modify(~ balance_table(.x, covariates, CATEGORICAL)) %>%
        ungroup()
}
balance <- bind_rows(
    balance_table(cov_data, covariates, CATEGORICAL) %>% mutate(site_id = "__all__"),
    site_balance
)
if (nrow(balance) == 0) {
    balance <- tibble(site_id = character(), covariate = character(), smd = numeric())
}
write_csv(balance, out_path("results_covariate_balance.csv"))

log_summary_phase("Covariate balance")
message("  Finalizing: match-quality distributions")
numeric_covariates <- setdiff(covariates, CATEGORICAL)
scope_quality <- function(df) {
    ps <- df[!is.na(df$pscore), ]
    qq <- NULL
    if (sum(ps$treatment) >= 2 && sum(!ps$treatment) >= 2) {
        probs <- seq(0, 1, length.out = 500)
        qq <- list(
            quantiles = probs,
            treatment_values = weighted_quantiles(
                ps$pscore[ps$treatment], ps$match_weight[ps$treatment], probs
            ),
            control_values = weighted_quantiles(
                ps$pscore[!ps$treatment], ps$match_weight[!ps$treatment], probs
            )
        )
    }
    hists <- list()
    for (cov in numeric_covariates) {
        h <- weighted_histogram(df[[cov]], df$treatment, df$match_weight)
        if (!is.null(h)) hists[[cov]] <- h
    }
    list(histograms = hists, qq = qq)
}
mq <- list(summary_stats = list(), histograms = list(), qq_quantiles = list(),
           covariate_cols = covariates, categorical_cols = CATEGORICAL)
if (summary_mode == "reduce") {
    prior_quality <- fromJSON(out_path("results_match_quality_summary.json"),
                              simplifyVector = FALSE)
    for (field in c("summary_stats", "histograms", "qq_quantiles")) {
        mq[[field]] <- prior_quality[[field]]
    }
}
if (nrow(cov_data) > 0) {
    scopes <- if (summary_mode == "reduce") "__all__" else c("__all__", unique(cov_data$site_id))
    for (scope in scopes) {
        df <- if (scope == "__all__") cov_data else cov_data[cov_data$site_id == scope, ]
        counts <- if (scope == "__all__") {
            site_counts
        } else {
            site_counts[site_counts$site_id == scope, ]
        }
        mq$summary_stats[[scope]] <- list(
            n_treatment = sum(df$treatment), n_control = sum(!df$treatment),
            n_sites = n_distinct(df$site_id),
            n_treatment_total = sum(counts$n_site_pixels),
            n_control_sampled = round(sum(counts$n_control_sampled)),
            n_control_pool = round(sum(counts$n_control_pool))
        )
        q <- scope_quality(df)
        mq$histograms[[scope]] <- q$histograms
        if (!is.null(q$qq)) mq$qq_quantiles[[scope]] <- q$qq
        if (scope == tail(scopes, 1) || match(scope, scopes) %% 25L == 0L) {
            message("  Match-quality distributions: ", match(scope, scopes),
                    "/", length(scopes), " scopes")
        }
    }
}
write_json(mq, out_path("results_match_quality_summary.json"), auto_unbox = TRUE)
log_summary_phase("Match-quality distributions")
message("  Finalizing: pixel locations")

if (summary_mode != "reduce") {
gm <- fromJSON(out_path("grid_metadata.json"))
tf <- gm$transform
cov_data %>%
    transmute(
        cell, site_id, treatment, match_group,
        lon = tf[3] + (cell %% gm$width + 0.5) * tf[1],
        lat = tf[6] + (cell %/% gm$width + 0.5) * tf[5]
    ) %>%
    write_csv(out_path("results_pixel_locations.csv"))
}


# -- Diagnostics and summary -------------------------------------------------

log_summary_phase("Pixel locations")
message("  Finalizing: summary metadata")
sampling_by_site <- site_counts %>%
    transmute(site_id, sampled_fraction, sampled_percent = sampled_fraction * 100,
              was_subsampled = sampled_fraction < 1)
write_csv(sampling_by_site, out_path("results_sampling_by_site.csv"))

as_records <- function(df) unname(lapply(seq_len(nrow(df)), function(i) as.list(df[i, ])))

if (summary_mode != "reduce") {
    na_records <- unlist(lapply(sidecars, function(x) x$na_exclusions), recursive = FALSE)
}
na_summary <- NULL
if (length(na_records) > 0) {
    na_df <- bind_rows(lapply(na_records, function(x) {
        x$variables_with_na <- paste(
            names(x$variables_with_na), unlist(x$variables_with_na), sep = "=", collapse = "; "
        )
        as_tibble(x)
    })) %>%
        distinct(id_numeric, .keep_all = TRUE)
    write_csv(na_df, out_path("na_exclusion_summary.csv"))
    substantial <- filter(na_df, substantial_exclusion)
    na_summary <- list(
        n_sites_with_exclusions = sum(na_df$n_dropped > 0),
        n_sites_with_substantial_exclusions = nrow(substantial),
        avg_pct_excluded_overall = round(mean(na_df$pct_excluded_overall), 2),
        avg_pct_treatment_excluded = round(mean(na_df$pct_treatment_excluded), 2),
        avg_pct_control_excluded = round(mean(na_df$pct_control_excluded), 2),
        max_pct_excluded_overall = round(max(na_df$pct_excluded_overall), 2),
        sites_with_substantial_exclusions = as_records(select(
            substantial, site_id, site_name, pct_excluded_overall,
            pct_treatment_excluded, pct_control_excluded
        ))
    )
}

site_names <- function(ids) {
    filter(sites, site_id %in% ids) %>% transmute(site_id, site_name) %>% as_records()
}
warnings <- list()
if (N_REPLICATES > 1L) {
    warnings[[length(warnings) + 1]] <- list(
        code = "replicate_range",
        message = paste0(
            "Bounds are the ", BOUNDS_LABEL, " (2.5% to 97.5% of ", N_REPLICATES,
            " replicates). They reflect variability from pixel sampling and ",
            "matching only, not a confidence interval for the avoided emissions.",
            if (N_REPLICATES < 50L) " Fewer than 50 replicates were used, so the bounds are imprecise." else ""
        )
    )
}
no_baseline <- filter(sites, start_year - PRE_INTERVENTION_YEARS - 1L < FC_YEAR_MIN)
if (nrow(no_baseline) > 0) {
    warnings[[length(warnings) + 1]] <- list(
        code = "no_baseline_deforestation",
        message = paste0(
            nrow(no_baseline), " site(s) start before ", FC_YEAR_MIN + PRE_INTERVENTION_YEARS + 1L,
            ", so pre-intervention deforestation could not be used as a matching covariate."
        ),
        affected_sites = site_names(no_baseline$site_id)
    )
}
low_coverage <- filter(site_counts, coverage_fraction < LOW_COVERAGE | match_rate < LOW_COVERAGE)
if (nrow(low_coverage) > 0) {
    warnings[[length(warnings) + 1]] <- list(
        code = "low_coverage",
        message = paste0(
            nrow(low_coverage), " site(s) have less than ", LOW_COVERAGE * 100,
            "% of their pixels represented by the estimates (forested, complete data, ",
            "in an exact-match stratum with matches) or less than ", LOW_COVERAGE * 100,
            "% of sampled pixels matched. Totals cover represented pixels only. Unmatched ",
            "pixels are assumed to resemble matched ones only where at least ",
            MIN_MATCH_RATE * 100, "% of a stratum's sampled pixels matched."
        ),
        affected_sites = site_names(low_coverage$site_id)
    )
}

if (n_inconsistent_strata > 0) {
    warnings[[length(warnings) + 1]] <- list(
        code = "inconsistent_strata",
        message = paste0(
            n_inconsistent_strata, " exact-match stratum/strata were matched in fewer than ",
            MIN_REPLICATE_SHARE * 100, "% of replicates and were excluded from the estimates, ",
            "so their pixels are not represented."
        )
    )
}

failed_summary <- as_records(select(failed_fragments, id_numeric, site_id, site_name,
                                     sub_site_index, reason, error))
subsampled <- sampling_by_site %>%
    filter(was_subsampled) %>%
    inner_join(select(sites, site_id, site_name), by = "site_id") %>%
    select(site_id, site_name, sampled_fraction, sampled_percent)

total_value <- function(name) if (length(portfolio_total) > 0) portfolio_total[[name]] else 0
summary_data <- list(
    task_id = config$task_id,
    resolution_m = config$resolution_m,
    n_sites = nrow(results_total),
    n_failed_sites = n_failed_sites,
    n_not_applicable_sites = sum(site_status$status == "not_applicable"),
    n_partial_sites = sum(site_status$status == "partial"),
    random_seed = RANDOM_SEED,
    n_replicates = N_REPLICATES,
    biomass_reference_year = 2000L,
    bounds_label = BOUNDS_LABEL,
    r_analysis_git_sha = Sys.getenv("CODE_VERSION", unset = NA_character_),
    extrapolated_total_emissions_avoided_mgco2e = total_value("extrapolated_emissions_avoided_mgco2e"),
    extrapolated_total_forest_loss_avoided_ha = total_value("extrapolated_forest_loss_avoided_ha"),
    sample_total_emissions_avoided_mgco2e = total_value("sample_emissions_avoided_mgco2e"),
    sample_total_forest_loss_avoided_ha = total_value("sample_forest_loss_avoided_ha"),
    total_area_ha = sum(results_total$area_ha),
    year_range = if (nrow(results_by_year) > 0) {
        list(min = min(results_by_year$year), max = max(results_by_year$year))
    } else {
        list(min = NA, max = NA)
    },
    failed_sites = failed_summary,
    worker_failures = lapply(worker_failures, function(x) x[c("array_index", "error")]),
    subsampled_sites = as_records(subsampled),
    na_exclusion_summary = na_summary,
    methodology_warnings = warnings,
    group_diagnostics = c(diagnostics, unlist(lapply(
        Filter(function(x) identical(as.integer(x$replicate), 1L), fragment_markers),
        function(x) x$group_diagnostics
    ), recursive = FALSE)),
    portfolio = list(
        yearly = if (length(portfolio_yearly) > 0) as_records(portfolio_yearly) else list(),
        total = portfolio_total
    ),
    # The rules that decide which forest the totals represent, and how much.
    estimator = list(
        version = ESTIMATOR_VERSION, min_cell_sample = MIN_CELL_SAMPLE,
        min_replicate_share = MIN_REPLICATE_SHARE, min_match_rate = MIN_MATCH_RATE
    ),
    coverage = list(
        n_site_pixels = sum(site_status$n_site_pixels),
        n_forest_pixels = sum(site_status$n_forest_pixels, na.rm = TRUE),
        n_eligible_pixels = sum(site_counts$n_eligible_pixels),
        n_represented_pixels = sum(site_counts$n_represented_pixels)
    ),
    treatment_census_check = as_records(census_agreement),
    # Checks on the comparison itself: baseline stock of the two arms, and
    # their loss in the years before the intervention, which matching cannot
    # have been caused by it.
    baseline_stock_balance = as_records(stock_balance),
    pre_intervention_check = as_records(
        site_year_rep %>%
            inner_join(select(sites, site_id, start_year), by = "site_id") %>%
            filter(year < start_year) %>%
            group_by(site_id, replicate) %>%
            summarise(treatment = sum(extrapolated_treatment_defor_ha),
                      control = sum(extrapolated_control_defor_ha), .groups = "drop") %>%
            group_by(site_id) %>%
            summarise(pre_treatment_defor_ha = mean(treatment),
                      pre_control_defor_ha = mean(control), .groups = "drop") %>%
            arrange(site_id)
    )
)
if (atom_enabled) {
    summary_data$atom_results_schema_version <- ATOM_RESULTS_SCHEMA_VERSION
    summary_data$atom_run_id <- atom_metadata$run_id
}
log_summary_phase("Summary metadata")
matching_diagnostics <- if (summary_mode == "full") write_matching_diagnostics(
    sidecar_paths, config, fragments, complete_strata, atom_status,
    atom_declarations, invalid_sites,
    if (atom_enabled) atom_year_rep else NULL
) else NULL
if (summary_mode == "shard") {
    saveRDS(mget(summary_state_fields, envir = environment()),
            out_path("summary_state.rds"), compress = FALSE)
    saveRDS(list(
        fragments = fragments, complete_strata = complete_strata,
        atom_status = atom_status, atom_declarations = atom_declarations,
        invalid_sites = invalid_sites,
        atom_domains = if (atom_enabled) atom_year_rep else NULL
    ), out_path("diagnostic_state.rds"), compress = FALSE)
}
if (!is.null(matching_diagnostics)) summary_data$matching_diagnostics <- matching_diagnostics
message("  Finalizing: summary JSON")
phase_timer <- proc.time()[["elapsed"]]
write_json(summary_data, out_path("results_summary.json"), auto_unbox = TRUE,
           pretty = TRUE, null = "null", na = "null", digits = NA)
log_summary_phase("Summary JSON")

message("[TIMING] Step 3 total: ", round((proc.time() - step3_timer)["elapsed"], 1), "s")
message("Step 3 complete. Results written to: ", config$output_dir)

}, step_name = "03_summarize_results")
