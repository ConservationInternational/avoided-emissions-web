summary_state_fields <- c(
    "sites", "site_counts", "site_year_rep", "site_total_rep", "results_total",
    "results_by_year", "unused_fragments", "failed_fragments", "site_status",
    "n_failed_sites", "n_inconsistent_strata", "worker_failures", "diagnostics",
    "fragment_markers", "atom_enabled", "atom_metadata", "atom_declarations",
    "metric_cols", "cov_data", "na_records", "census_agreement", "stock_balance"
)

read_summary_states <- function(paths) {
    states <- lapply(paths, readRDS)
    if (!length(states) || any(vapply(states, function(state) {
        !setequal(names(state), summary_state_fields)
    }, logical(1)))) stop("Missing or incompatible summary state")
    result <- states[[1]]
    for (field in c(
        "sites", "site_counts", "site_year_rep", "site_total_rep", "results_total",
        "results_by_year", "unused_fragments", "failed_fragments", "site_status",
        "atom_declarations", "cov_data", "census_agreement", "stock_balance"
    )) {
        result[[field]] <- dplyr::bind_rows(lapply(states, `[[`, field))
    }
    if (anyDuplicated(result$sites$site_id) ||
        anyDuplicated(result$atom_declarations$atomic_id)) {
        stop("Overlapping summary site or atom domains")
    }
    for (field in c("atom_enabled", "atom_metadata", "metric_cols")) {
        if (any(!vapply(states, function(state) identical(state[[field]], result[[field]]),
                        logical(1)))) stop("Incompatible summary state: ", field)
    }
    for (field in c("n_failed_sites", "n_inconsistent_strata")) {
        result[[field]] <- sum(vapply(states, `[[`, numeric(1), field))
    }
    for (field in c("worker_failures", "diagnostics", "fragment_markers", "na_records")) {
        result[[field]] <- unlist(lapply(states, `[[`, field), recursive = FALSE)
    }
    result
}
