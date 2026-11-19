# Durable diagnostic exports are separate from the compact legacy summary.
diagnostic_records <- function(sidecar) {
    unlist(lapply(c(sidecar$diagnostics, sidecar$failures), function(item) {
        lapply(item$group_diagnostics, function(record) {
            for (field in c("id_numeric", "site_id", "replicate", "sub_site_index")) {
                if (is.null(record[[field]])) record[[field]] <- item[[field]]
            }
            record
        })
    }), recursive = FALSE)
}

compact_match_sidecar <- function(sidecar) {
    compact <- function(items) lapply(items, function(item) {
        item$group_diagnostics <- if (identical(as.integer(item$replicate), 1L)) {
            lapply(item$group_diagnostics, function(record) {
                record[intersect(names(record), c(
                    "group", "n_treatment", "n_control", "n_matched",
                    "method", "separation_details", "error"
                ))]
            })
        } else NULL
        item
    })
    sidecar$diagnostics <- compact(sidecar$diagnostics)
    sidecar$failures <- compact(sidecar$failures)
    sidecar
}

encode_diagnostic_records <- function(entries) {
    json <- function(value) as.character(jsonlite::toJSON(
        value, auto_unbox = TRUE, null = "null", na = "null", digits = NA
    ))
    list(
        records = vapply(entries, function(entry) json(entry$record), character(1)),
        replays = vapply(
            Filter(function(entry) !is.null(entry$replay), entries),
            function(entry) json(entry$replay), character(1)
        )
    )
}

write_matching_diagnostics <- function(paths, config, fragments, complete_strata,
                                       atom_status = NULL, atom_declarations = NULL,
                                       invalid_sites = list(), atom_domains = NULL) {
    if (!length(paths)) {
        if (!isTRUE(config$require_matching_diagnostics)) return(NULL)
        if (is.null(config$diagnostic_expected_ids) || length(config$diagnostic_expected_ids)) {
            stop("Missing matching diagnostics")
        }
    }
    started <- proc.time()[["elapsed"]]
    message("  Matching diagnostic export: starting (", length(paths), " sidecars)")
    read_sidecar <- function(path) jsonlite::fromJSON(path, simplifyVector = FALSE)
    schema_version <- function(sidecar) {
        version <- sidecar$diagnostics_schema_version
        if (is.null(version)) return(0L)
        if (length(version) != 1L || is.na(version) || !as.integer(version) %in% 0:1) {
            stop("Missing or incompatible matching diagnostic schema")
        }
        as.integer(version)
    }
    first_sidecar <- if (length(paths)) read_sidecar(paths[[1]]) else NULL
    if (length(paths) && schema_version(first_sidecar) == 0L &&
        !isTRUE(config$require_matching_diagnostics)) {
        for (path in paths[-1]) {
            if (schema_version(read_sidecar(path)) != 0L) {
                stop("Missing or incompatible matching diagnostic schema")
            }
        }
        return(NULL)
    }
    workers <- if (is.null(config$summary_workers)) 1L else as.integer(config$summary_workers)
    if (length(workers) != 1L || is.na(workers) || workers < 1L) {
        stop("Invalid summary worker count")
    }
    cluster <- NULL
    on.exit(if (!is.null(cluster)) parallel::stopCluster(cluster), add = TRUE)
    batch_size <- 64L
    pending <- vector("list", workers * batch_size)
    n_pending <- 0L

    policy <- if (isTRUE(config$shared_support_controls)) "shared_support_v2" else "legacy"
    output <- function(name) file.path(config$output_dir, name)
    artifacts <- list(
        diagnostics = "results_matching_diagnostics.jsonl.gz",
        summary = "results_matching_diagnostics_summary.json",
        replay = "results_matching_replay_manifest.json"
    )
    stream <- gzfile(output(artifacts$diagnostics), "wt")
    manifest <- file(output(artifacts$replay), "wt")
    streams_closed <- FALSE
    on.exit({
        if (!streams_closed) {
            close(stream)
            close(manifest)
        }
    }, add = TRUE)
    writeLines('{"diagnostics_schema_version":1,"attempts":[', manifest)
    first_replay <- TRUE
    flush <- function() {
        if (!n_pending) return(invisible(NULL))
        if (workers > 1L && is.null(cluster)) {
            cluster <<- parallel::makePSOCKcluster(workers)
        }
        batches <- split(pending[seq_len(n_pending)],
                         ceiling(seq_len(n_pending) / batch_size))
        encoded <- if (is.null(cluster)) {
            lapply(batches, encode_diagnostic_records)
        } else {
            parallel::parLapply(cluster, batches, encode_diagnostic_records)
        }
        for (batch in encoded) {
            writeLines(batch$records, stream)
            if (length(batch$replays)) {
                if (!first_replay) writeLines(",", manifest)
                writeLines(paste(batch$replays, collapse = ",\n"), manifest)
                first_replay <<- FALSE
            }
        }
        pending <<- vector("list", workers * batch_size)
        n_pending <<- 0L
        invisible(NULL)
    }
    emit <- function(value, replay = NULL) {
        n_pending <<- n_pending + 1L
        pending[[n_pending]] <<- list(record = value, replay = replay)
        if (n_pending == length(pending)) flush()
    }
    seen <- new.env(hash = TRUE, parent = emptyenv())
    items <- new.env(hash = TRUE, parent = emptyenv())
    scopes <- lapply(c("fragments", "strata", "atoms"), function(x) {
        new.env(hash = TRUE, parent = emptyenv())
    })
    names(scopes) <- c("fragments", "strata", "atoms")
    causes <- new.env(hash = TRUE, parent = emptyenv())
    variables <- new.env(hash = TRUE, parent = emptyenv())
    replicates <- new.env(hash = TRUE, parent = emptyenv())
    atom_evidence <- new.env(hash = TRUE, parent = emptyenv())
    complete_keys <- new.env(hash = TRUE, parent = emptyenv())
    declared_atoms <- new.env(hash = TRUE, parent = emptyenv())
    n_attempts <- 0L
    n_replays <- 0L
    n_duplicates <- 0L
    set_add <- function(env, values) {
        for (value in unique(as.character(values))) assign(value, TRUE, envir = env)
    }
    set_add(complete_keys, paste(complete_strata$id_numeric, complete_strata$group, sep = "\r"))
    if (!is.null(atom_declarations)) set_add(declared_atoms, atom_declarations$atomic_id)
    count_add <- function(env, key, record, atom_ids) {
        if (!exists(key, env, inherits = FALSE)) {
            value <- new.env(parent = emptyenv())
            value$attempts <- 0L
            for (scope in c("fragments", "strata", "atoms")) {
                value[[scope]] <- new.env(hash = TRUE, parent = emptyenv())
            }
            assign(key, value, env)
        }
        value <- get(key, env, inherits = FALSE)
        value$attempts <- value$attempts + 1L
        set_add(value$fragments, record$id_numeric)
        set_add(value$strata, paste(record$id_numeric, record$group, sep = "\r"))
        set_add(value$atoms, atom_ids)
    }
    atom_run <- if (!is.null(atom_declarations) && nrow(atom_declarations)) {
        unique(as.character(atom_declarations$run_id))
    } else NULL
    if (length(atom_run) > 1L) stop("Inconsistent diagnostic atom run")

    for (path_index in seq_along(paths)) {
        path <- paths[[path_index]]
        sidecar <- if (path_index == 1L) first_sidecar else read_sidecar(path)
        if (schema_version(sidecar) != 1L) {
            stop("Missing or incompatible matching diagnostic schema")
        }
        if (!identical(sidecar$matching_policy, policy)) {
            stop("Incompatible matching policy in diagnostic sidecar: ", basename(path))
        }
        item_records <- c(sidecar$diagnostics, sidecar$failures)
        roster <- unlist(lapply(item_records, `[[`, "expected_attempt_keys"), use.names = FALSE)
        if (is.null(sidecar$expected_attempt_keys) ||
            !identical(unlist(sidecar$expected_attempt_keys, use.names = FALSE), roster)) {
            stop("Incomplete sidecar attempt roster: ", basename(path))
        }
        for (item in item_records) {
            item_key <- paste(item$id_numeric, item$replicate, sep = "\r")
            if (is.null(item$id_numeric) || is.null(item$replicate) ||
                !item$id_numeric %in% fragments$id_numeric ||
                !item$replicate %in% seq_len(config$n_replicates)) {
                stop("Invalid diagnostic fragment/replicate identity: ", basename(path))
            }
            groups <- vapply(item$group_diagnostics, function(record) {
                as.character(record$group)
            }, character(1))
            if (!length(groups) || anyDuplicated(groups)) {
                stop("Missing or duplicate terminal stratum diagnostics: ", basename(path))
            }
            keys <- vapply(item$group_diagnostics, function(record) record$attempt_key, character(1))
            if (is.null(item$expected_groups) || is.null(item$expected_attempt_keys) ||
                anyDuplicated(keys) ||
                !setequal(groups, unlist(item$expected_groups, use.names = FALSE)) ||
                !setequal(keys, unlist(item$expected_attempt_keys, use.names = FALSE))) {
                stop("Incomplete terminal stratum diagnostic roster: ", basename(path))
            }
            if (exists(item_key, items, inherits = FALSE)) {
                previous <- get(item_key, items, inherits = FALSE)
                previous_sidecar <- read_sidecar(previous)
                previous_items <- c(previous_sidecar$diagnostics, previous_sidecar$failures)
                previous_item <- Filter(function(value) {
                    identical(paste(value$id_numeric, value$replicate, sep = "\r"), item_key)
                }, previous_items)
                if (length(previous_item) != 1L || !identical(previous_item[[1]], item)) {
                    stop("Conflicting diagnostic item: ", item_key)
                }
            } else assign(item_key, path, items)
        }
        records <- diagnostic_records(sidecar)
        for (index in seq_along(records)) {
            record <- records[[index]]
            required <- c("attempt_key", "id_numeric", "site_id", "replicate", "group")
            if (any(vapply(required, function(field) {
                is.null(record[[field]]) || length(record[[field]]) != 1L
            }, logical(1)))) stop("Incomplete diagnostic attempt identity: ", basename(path))
            key <- record$attempt_key
            if (exists(key, seen, inherits = FALSE)) {
                previous <- get(key, seen, inherits = FALSE)
                original <- diagnostic_records(read_sidecar(previous$path))[[previous$index]]
                if (!identical(original, record)) stop("Conflicting diagnostic attempt: ", key)
                n_duplicates <- n_duplicates + 1L
                next
            }
            assign(key, list(path = path, index = index), seen)
            if (!identical(as.character(record$task_id), as.character(config$task_id)) ||
                !identical(record$matching_policy, policy) ||
                !identical(as.integer(record$diagnostics_schema_version), 1L)) {
                stop("Incompatible diagnostic attempt provenance: ", key)
            }
            if (!is.null(atom_run) &&
                !identical(as.character(record$atom_run_id), atom_run)) {
                stop("Incompatible diagnostic atom run: ", key)
            }
            stratum_key <- paste(record$id_numeric, record$group, sep = "\r")
            record$record_type <- "attempt"
            record$stratum_consistent <- exists(stratum_key, complete_keys, inherits = FALSE)
            record$site_valid <- !record$site_id %in% names(invalid_sites)
            atom_ids <- vapply(record$atoms, function(atom) as.character(atom$atomic_id), character(1))
            set_add(scopes$fragments, record$id_numeric)
            set_add(scopes$strata, stratum_key)
            set_add(scopes$atoms, atom_ids)
            reason <- record$reason_code
            if (is.null(reason)) stop("Missing structured diagnostic cause: ", key)
            count_add(causes, reason, record, atom_ids)
            count_add(replicates, as.character(record$replicate), record, atom_ids)
            for (variable in unique(unlist(record$separation_variables, use.names = FALSE))) {
                count_add(variables, variable, record, atom_ids)
            }
            for (atom in record$atoms) {
                atom_id <- as.character(atom$atomic_id)
                if (!is.null(atom_declarations) &&
                    !exists(atom_id, declared_atoms, inherits = FALSE)) {
                    stop("Undeclared diagnostic atom: ", atom_id)
                }
                if (!exists(atom_id, atom_evidence, inherits = FALSE)) {
                    assign(atom_id, list(
                        sampled_replicates = integer(), matched_replicates = integer(),
                        eligible_replicates = integer(), cause_codes = character(),
                        missing_strata = list()
                    ), atom_evidence)
                }
                evidence <- get(atom_id, atom_evidence, inherits = FALSE)
                for (stage in c("eligible", "sampled", "matched")) {
                    value <- atom[[paste0("n_", stage)]]
                    if (!is.null(value) && value > 0) {
                        field <- paste0(stage, "_replicates")
                        evidence[[field]] <- union(evidence[[field]], as.integer(record$replicate))
                    }
                }
                if (is.null(atom$n_matched) || atom$n_matched == 0 ||
                    !record$stratum_consistent || !record$site_valid) {
                    final_reason <- if (!record$site_valid) "invalid_outcomes" else
                        if (!record$stratum_consistent && !is.null(atom$n_matched) &&
                            atom$n_matched > 0) "inconsistent_stratum" else reason
                    evidence$cause_codes <- union(evidence$cause_codes, final_reason)
                    evidence$missing_strata[[length(evidence$missing_strata) + 1L]] <- list(
                        id_numeric = record$id_numeric, group = record$group,
                        replicate = record$replicate, attempt_key = key,
                        sampled = atom$n_sampled, matched = atom$n_matched,
                        stratum_consistent = record$stratum_consistent
                    )
                }
                assign(atom_id, evidence, atom_evidence)
            }
            entry <- NULL
            if (isTRUE(record$glm_failed)) {
                replay <- record$replay
                if (is.null(replay) || is.null(replay$sample_file)) {
                    stop("Missing failed-GLM replay reference: ", key)
                }
                expected_sample <- paste0("f", record$id_numeric, "_r", record$replicate, "_sample.rds")
                if (!identical(replay$sample_file, expected_sample)) {
                    stop("Invalid failed-GLM sample identity: ", key)
                }
                if (is.null(replay$uri)) {
                    sample <- file.path(config$matches_dir, replay$sample_file)
                    if (!file.exists(sample)) stop("Missing local replay input: ", sample)
                    replay$uri <- paste0("file://", normalizePath(sample))
                    replay$md5 <- unname(tools::md5sum(sample))
                    if (!is.null(replay$sample_md5) &&
                        !identical(replay$sample_md5, replay$md5)) {
                        stop("Corrupt local replay input: ", sample)
                    }
                }
                if (is.null(replay$sha256) && is.null(replay$md5)) {
                    stop("Missing replay input checksum: ", key)
                }
                entry <- utils::modifyList(list(
                    attempt_key = key, task_id = config$task_id,
                    id_numeric = record$id_numeric, site_id = record$site_id,
                    replicate = record$replicate, group = record$group,
                    atom_run_id = record$atom_run_id
                ), replay, keep.null = TRUE)
                n_replays <- n_replays + 1L
                record$replay <- replay
            }
            emit(record, entry)
            n_attempts <- n_attempts + 1L
        }
        if (path_index %% 1000L == 0L || path_index == length(paths)) {
            message("  Matching diagnostics: ", path_index, "/", length(paths),
                    " sidecars validated; ", n_attempts, " attempts; ",
                    round(proc.time()[["elapsed"]] - started, 1), "s elapsed")
        }
    }
    expected_ids <- config$diagnostic_expected_ids
    if (is.null(expected_ids)) {
        expected_ids <- fragments$id_numeric
        if (!is.null(config$site_id)) {
            expected_ids <- fragments$id_numeric[fragments$site_id == config$site_id]
        }
        mapping <- if (!is.null(config$config_path)) {
            jsonlite::fromJSON(config$config_path, simplifyVector = FALSE)$exact_match_group_mapping
        } else config$exact_match_group_mapping
        if (!is.null(mapping)) {
            pairs <- unlist(mapping, recursive = FALSE)
            mapped <- vapply(pairs, function(pair) paste(pair[[1]], pair[[2]], sep = "\r"), character(1))
            expected_ids <- fragments$id_numeric[
                fragments$id_numeric %in% expected_ids &
                paste(fragments$site_id, fragments$sub_site_index, sep = "\r") %in% mapped
            ]
        }
    }
    expected <- as.vector(outer(unique(expected_ids), seq_len(config$n_replicates), paste, sep = "\r"))
    missing <- setdiff(expected, ls(items, all.names = TRUE))
    unexpected <- setdiff(ls(items, all.names = TRUE), expected)
    if (length(missing) || length(unexpected)) {
        stop("Incomplete matching diagnostic items: ", length(missing),
             " missing, ", length(unexpected), " unexpected")
    }
    if (!is.null(atom_status)) for (i in seq_len(nrow(atom_status))) {
        status <- as.list(atom_status[i, ])
        atom_id <- as.character(status$atomic_id)
        evidence <- if (exists(atom_id, atom_evidence, inherits = FALSE)) {
            get(atom_id, atom_evidence, inherits = FALSE)
        } else list(eligible_replicates = integer(), sampled_replicates = integer(),
                    matched_replicates = integer(), cause_codes = character(), missing_strata = list())
        status$record_type <- "atom_inclusion"
        status$diagnostics_schema_version <- 1L
        status$atom_run_id <- atom_run
        status$atomic_id <- atom_id
        status$task_id <- config$task_id
        status$evidence <- evidence
        status$missing_replicates <- setdiff(seq_len(config$n_replicates), evidence$matched_replicates)
        if (status$n_site_pixels == 0) {
            status$inclusion_reason <- "no_grid_pixels"
        } else if (status$n_forest_pixels == 0) {
            status$inclusion_reason <- "no_forest_pixels"
        } else if (status$site_id %in% names(invalid_sites)) {
            status$inclusion_reason <- "invalid_outcomes"
            status$outcome_error <- invalid_sites[[status$site_id]]
        } else if (status$status %in% c("estimated", "partial")) {
            status$inclusion_reason <- "represented"
        } else if (any(evidence$cause_codes %in% c("shared_support_exhausted", "no_exact_overlap"))) {
            status$inclusion_reason <- "unsupported_controls"
        } else if (!length(evidence$eligible_replicates)) {
            status$inclusion_reason <- "no_eligible_treatment"
        } else if (!length(evidence$sampled_replicates)) {
            status$inclusion_reason <- "never_sampled"
        } else if (!length(evidence$matched_replicates)) {
            status$inclusion_reason <- "sampled_unmatched"
        } else status$inclusion_reason <- "incomplete_domain"
        if (!is.null(atom_domains)) {
            domain <- atom_domains[as.character(atom_domains$atomic_id) == atom_id, ]
            site <- fragments[match(status$site_id, fragments$site_id), ]
            years <- seq(baseline_year(site$start_year, min(config$fc_years)) + 1L,
                         max(config$fc_years))
            status$missing_years <- lapply(seq_len(config$n_replicates), function(rep) {
                list(replicate = rep, years = setdiff(years, domain$year[domain$replicate == rep]))
            })
        }
        emit(status)
    }
    flush()
    writeLines("]}", manifest)
    close(manifest)
    close(stream)
    streams_closed <- TRUE
    counts <- function(env) {
        setNames(lapply(sort(ls(env, all.names = TRUE)), function(key) {
            value <- get(key, env, inherits = FALSE)
            list(attempts = value$attempts, fragments = length(value$fragments),
                 strata = length(value$strata), atoms = length(value$atoms))
        }), sort(ls(env, all.names = TRUE)))
    }
    summary <- list(
        diagnostics_schema_version = 1L, matching_policy = policy,
        atom_run_id = atom_run, diagnostics_complete = TRUE,
        counts = list(
            attempts = n_attempts, fragments = length(ls(scopes$fragments)),
            strata = length(ls(scopes$strata)),
            atoms = if (is.null(atom_status)) 0L else nrow(atom_status),
            atoms_in_attempts = length(ls(scopes$atoms)),
            expected_items = length(expected), observed_items = length(ls(items)),
            missing_items = length(missing), unexpected_items = length(unexpected),
            duplicate_attempts_deduplicated = n_duplicates, replay_attempts = n_replays
        ),
        by_cause = counts(causes), by_covariate = counts(variables),
        by_replicate = counts(replicates),
        atom_status_counts = if (is.null(atom_status)) list() else as.list(table(atom_status$status)),
        covariate_attribution_overlaps = TRUE, artifacts = artifacts,
        output_bytes = list(
            diagnostics = unname(file.info(output(artifacts$diagnostics))$size),
            replay = unname(file.info(output(artifacts$replay))$size)
        )
    )
    jsonlite::write_json(summary, output(artifacts$summary), auto_unbox = TRUE,
                         pretty = TRUE, null = "null", na = "null", digits = NA)
    message("[TIMING] Matching diagnostic export: ",
            round(proc.time()[["elapsed"]] - started, 1), "s; ",
            n_attempts, " attempts, ", n_replays, " replay references")
    summary
}
