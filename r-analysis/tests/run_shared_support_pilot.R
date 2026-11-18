# Run manually in the analysis image; outputs are retained for downstream import/replay.
library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
library(jsonlite)

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 1L) stop("Usage: Rscript run_shared_support_pilot.R OUTPUT_DIRECTORY")
root <- normalizePath(args[1], mustWork = FALSE)
if (dir.exists(root)) stop("Pilot output directory already exists: ", root)
dir.create(root, recursive = TRUE)
scripts <- normalizePath(Sys.getenv("R_SCRIPTS_DIR", "../scripts"))
source(file.path(scripts, "stats_functions.R"))
started <- proc.time()

make_pilot <- function(name, enabled) {
    data <- file.path(root, name)
    out <- file.path(data, "output")
    dir.create(file.path(out, "matches"), recursive = TRUE)
    sites <- tibble(
        site_id = c("shared", "glm_failure", "no_controls", "mixed"),
        site_name = site_id, id_numeric = 1:4, sub_site_index = 0L,
        start_year = 2014L, end_year = 2016L, area_ha = c(200, 1000, 200, 200)
    )
    declarations <- list(c("101", "102", "103"), "201", "301", c("401", "402"))
    sites$ae_atoms <- vapply(declarations, function(ids) {
        toJSON(list(run_id = "shared-support-pilot", atoms = lapply(ids, function(id) {
            list(atomic_id = id, geometry = list(type = "Polygon", coordinates = list()))
        })), auto_unbox = TRUE)
    }, character(1))
    treatment <- bind_rows(lapply(1:4, function(id) {
        n <- if (id == 2L) 100L else 20L
        tibble(
            id_numeric = id, site_id = sites$site_id[id], admin1 = id,
            ecoregion = 1L, pa = 0L, treatment = TRUE,
            atomic_id = if (id == 1L) rep(c("101", "102"), each = 10L) else
                if (id == 4L) rep(c("401", "402"), each = 10L) else declarations[[id]],
            cropland_2019 = if (id == 4L) rep(c(0, 1), each = 10L) else 0,
            x = if (id == 2L) rep(c(2, -1), n / 2L) else 0,
            z = if (id == 2L) 1 - x else 0
        )
    }))
    controls <- bind_rows(lapply(c(1L, 2L, 4L), function(id) {
        tibble(
            admin1 = id, ecoregion = 1L, pa = 0L, treatment = FALSE,
            cropland_2019 = if (id == 1L) c(rep(0, 40L), rep(1, 60L)) else
                if (id == 4L) rep(c(0, 1), each = 50L) else rep(0, 100L),
            x = if (id == 2L) rep(c(-2, 1), 50L) else rep(0, 100L),
            z = if (id == 2L) -1 - x else 0
        )
    }))
    add_outcomes <- function(pixels, offset, loss) {
        pixels <- pixels %>% mutate(
            cell = offset + seq_len(n()), area_ha = 10,
            total_biomass_2000 = 100, fc_2000 = 100
        )
        for (year in 2008:2016) pixels[[paste0("fc_", year)]] <- 100 - pmax(0, year - 2013) * loss
        pixels
    }
    treatment <- add_outcomes(treatment, 0, 1)
    controls <- add_outcomes(controls, 10000, 2)
    write_parquet(sites, file.path(out, "sites_processed.parquet"))
    write_parquet(select(treatment, cell, id_numeric, site_id, atomic_id),
                  file.path(out, "treatment_cell_key.parquet"))
    write_parquet(select(treatment, -id_numeric, -site_id, -atomic_id, -treatment),
                  file.path(out, "treatment_pixels.parquet"))
    write_dataset(select(controls, -treatment), file.path(out, "control_pixels"), partitioning = "admin1")
    atom_counts <- treatment %>% count(site_id, atomic_id, name = "n_site_pixels") %>%
        mutate(n_forest_pixels = n_site_pixels)
    atom_counts <- bind_rows(atom_counts, tibble(
        site_id = "shared", atomic_id = "103", n_site_pixels = 0L, n_forest_pixels = 0L
    ))
    write_parquet(atom_counts, file.path(out, "ae_atom_counts.parquet"))
    write_json(list(formula_str = "treatment ~ cropland_2019 + x + z"),
               file.path(out, "formula.json"), auto_unbox = TRUE)
    write_json(list(width = 1000, transform = c(0.01, 0, 0, 0, -0.01, 10)),
               file.path(out, "grid_metadata.json"), auto_unbox = TRUE)
    config <- list(
        data_dir = data, task_id = paste0("pilot-", name), fc_years = 2008:2016,
        shared_support_controls = enabled, max_treatment_pixels = 100,
        control_multiplier = 2, min_glm_treatment_pixels = 15, caliper_width = 0,
        max_controls_per_treatment = 1, exact_match_vars = c("admin1", "ecoregion", "pa"),
        matching_method = "nearest", n_replicates = 100L, random_seed = 1806781614L,
        separation_fallback_mahalanobis = FALSE, min_control_distance_km = 10,
        diagnostic_expected_ids = 1:4
    )
    path <- file.path(data, "config.json")
    write_json(config, path, auto_unbox = TRUE, digits = NA)
    for (script in c("02_perform_matching.R", "03_summarize_results.R")) {
        log <- file.path(data, paste0(script, ".log"))
        status <- system2("Rscript", c(shQuote(file.path(scripts, script)), "--config", shQuote(path)),
                          stdout = log, stderr = log)
        if (status != 0L) stop("Pilot failed; inspect ", log)
    }
    out
}

baseline <- make_pilot("legacy", FALSE)
pilot <- make_pilot("shared_support", TRUE)
legacy <- read.csv(file.path(baseline, "results_atom_status.csv"), colClasses = c(atomic_id = "character"))
recovered <- read.csv(file.path(pilot, "results_atom_status.csv"), colClasses = c(atomic_id = "character"))
stopifnot(
    all(legacy$status[legacy$atomic_id %in% c("101", "102")] == "failed"),
    all(recovered$status[recovered$atomic_id %in% c("101", "102", "401", "402")] == "estimated"),
    recovered$status[recovered$atomic_id == "103"] == "not_applicable",
    recovered$status[recovered$atomic_id == "201"] == "failed",
    recovered$status[recovered$atomic_id == "301"] == "failed"
)
annual <- read.csv(file.path(pilot, "results_by_atom_replicate_year.csv"))
parent <- read.csv(file.path(pilot, "results_by_site_replicate_year.csv"))
summed <- annual %>% group_by(site_id, replicate, year) %>%
    summarise(across(starts_with("extrapolated_"), sum), .groups = "drop") %>%
    arrange(site_id, replicate, year)
stopifnot(isTRUE(all.equal(
    as.data.frame(summed), parent %>% arrange(site_id, replicate, year), check.attributes = FALSE
)))
diagnostics <- fromJSON(file.path(pilot, "results_matching_diagnostics_summary.json"))
stopifnot(diagnostics$diagnostics_complete, diagnostics$counts$expected_items == 400L)
replay <- fromJSON(file.path(pilot, "results_matching_replay_manifest.json"), simplifyVector = FALSE)
entry <- Filter(function(value) value$replicate == 2L, replay$attempts)[[1]]
sample_path <- sub("^file://", "", entry$uri)
stopifnot(unname(tools::md5sum(sample_path)) == entry$sample_md5)
replayed <- replay_match_propensity(sample_path, entry$group, entry$rng, scripts_dir = scripts)
stopifnot(replayed$glm$cause == "glm_nonconvergence",
          length(replayed$glm$diagnostics$warnings) > 0L)
balance <- read.csv(file.path(pilot, "results_covariate_balance.csv"))
stopifnot(all(is.finite(balance$smd)), all(abs(balance$smd) <= 0.1))
matches <- read_parquet(file.path(pilot, "matches", "mb_all.parquet"))
stopifnot(all(matches$cropland_2019[matches$site_id == "shared"] == 0))
unchanged <- annual %>% filter(site_id == "mixed") %>% arrange(atomic_id, replicate, year)
legacy_annual <- read.csv(file.path(baseline, "results_by_atom_replicate_year.csv")) %>%
    filter(site_id == "mixed") %>% arrange(atomic_id, replicate, year)
stopifnot(isTRUE(all.equal(unchanged, legacy_annual, check.attributes = FALSE)))
write_json(list(
    n_replicates = 100L, policy_disabled = baseline, policy_enabled = pilot,
    recovered_atoms = c("101", "102"), unchanged_atoms = c("401", "402"),
    diagnostics_complete = TRUE, parent_atom_totals_agree = TRUE,
    n_failed_glm_replays = length(replay$attempts),
    replayed_replicate = entry$replicate, replayed_cause = replayed$glm$cause,
    max_absolute_smd = max(abs(balance$smd)),
    elapsed_seconds = unname((proc.time() - started)["elapsed"]),
    diagnostic_output_bytes = diagnostics$output_bytes,
    warnings = "Synthetic pilot; distance exclusion is extraction-stage and covered by extraction tests."
), file.path(root, "pilot_report.json"), auto_unbox = TRUE, pretty = TRUE)
cat("Pilot outputs:", root, "\n")
