library(jsonlite)
scripts_dir <- Sys.getenv("R_SCRIPTS_DIR", "/app/scripts")
source(file.path(scripts_dir, "utils.R"))
source(file.path(scripts_dir, "stats_functions.R"))
source(file.path(scripts_dir, "matching_diagnostics.R"))
rollbar_init()

with_rollbar({
    config <- parse_config()
    state <- readRDS(config$diagnostic_state)
    write_matching_diagnostics(
        list.files(config$matches_dir, "^mb_.*\\.json$", full.names = TRUE),
        config, state$fragments, state$complete_strata, state$atom_status,
        state$atom_declarations, state$invalid_sites, state$atom_domains
    )
}, step_name = "04_export_diagnostics")
