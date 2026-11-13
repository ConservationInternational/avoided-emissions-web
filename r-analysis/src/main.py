"""Result serialization for the Nextflow-managed avoided-emissions workflow."""

import csv
import json
import math
import os

from te_schemas.analysis import AnalysisRecord, AnalysisResults, AnalysisTimeStep

REQUIRES_GEE = False


def run(params, logger):
    """Reject obsolete direct execution rather than bypassing Nextflow."""
    message = "Avoided-emissions analyses must be submitted through Nextflow."
    logger.error(message)
    raise RuntimeError(message)


YEAR_METRICS = [
    "extrapolated_forest_loss_avoided_ha",
    "extrapolated_emissions_avoided_mgco2e",
    "extrapolated_treatment_defor_ha",
    "extrapolated_control_defor_ha",
    "extrapolated_treatment_emissions_mgco2e",
    "extrapolated_control_emissions_mgco2e",
]
TOTAL_METRICS = [
    "extrapolated_forest_loss_avoided_ha",
    "extrapolated_emissions_avoided_mgco2e",
]


def _number(row, key):
    """Return a required finite number from a result CSV row."""
    try:
        value = float(row[key])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"Invalid result value {key}={row.get(key)!r}") from exc
    if not math.isfinite(value):
        raise ValueError(f"Invalid result value {key}={row[key]!r}")
    return value


def _metric_values(row, metrics, n_replicates):
    """Required metrics plus their replicate bounds when n_replicates > 1."""
    values = {key: _number(row, key) for key in metrics}
    if n_replicates > 1:
        for key in metrics:
            for suffix in ("_ci_lower", "_ci_upper"):
                values[key + suffix] = _number(row, key + suffix)
    return values


def _flag(row, key):
    value = row[key]
    if value not in ("TRUE", "FALSE"):
        raise ValueError(f"Invalid result flag {key}={value!r}")
    return value == "TRUE"


def _collect_results(output_dir, task_id, log):
    """Read complete summary files and build an AnalysisResults dictionary."""
    with open(os.path.join(output_dir, "results_summary.json")) as f:
        summary = json.load(f)
    n_replicates = int(summary["n_replicates"])

    summary_dict = {
        "task_id": task_id,
        "n_sites": summary["n_sites"],
        "n_failed_sites": summary["n_failed_sites"],
        "n_replicates": n_replicates,
        "biomass_reference_year": summary["biomass_reference_year"],
        "bounds_label": summary["bounds_label"],
        "r_analysis_git_sha": summary.get("r_analysis_git_sha"),
        "total_emissions_avoided_mgco2e": summary[
            "extrapolated_total_emissions_avoided_mgco2e"
        ],
        "total_forest_loss_avoided_ha": summary[
            "extrapolated_total_forest_loss_avoided_ha"
        ],
        "sample_total_emissions_avoided_mgco2e": summary[
            "sample_total_emissions_avoided_mgco2e"
        ],
        "sample_total_forest_loss_avoided_ha": summary[
            "sample_total_forest_loss_avoided_ha"
        ],
        "total_area_ha": summary["total_area_ha"],
        "year_range_min": summary["year_range"]["min"],
        "year_range_max": summary["year_range"]["max"],
        "failed_sites": summary["failed_sites"],
        "worker_failures": summary["worker_failures"],
        "subsampled_sites": summary["subsampled_sites"],
        "methodology_warnings": summary["methodology_warnings"],
        "portfolio": summary["portfolio"],
    }

    time_series = []
    with open(os.path.join(output_dir, "results_by_site_year.csv"), newline="") as f:
        for row in csv.DictReader(f):
            time_series.append(
                AnalysisTimeStep(
                    entity_id=row["site_id"],
                    year=int(row["year"]),
                    values=_metric_values(row, YEAR_METRICS, n_replicates),
                    entity_name=row["site_name"] or None,
                    metadata={
                        "n_sample_pixels": int(_number(row, "n_sample_pixels")),
                        "sampled_fraction": _number(row, "sampled_fraction"),
                        "is_pre_intervention": _flag(row, "is_pre_intervention"),
                        "is_post_intervention": _flag(row, "is_post_intervention"),
                    },
                )
            )

    records = []
    with open(os.path.join(output_dir, "results_by_site_total.csv"), newline="") as f:
        for row in csv.DictReader(f):
            values = _metric_values(row, TOTAL_METRICS, n_replicates)
            values["area_ha"] = _number(row, "area_ha")
            records.append(
                AnalysisRecord(
                    entity_id=row["site_id"],
                    values=values,
                    entity_name=row["site_name"] or None,
                    period_start=int(row["first_year"]),
                    period_end=int(row["last_year"]),
                    metadata={
                        "n_sample_pixels": int(_number(row, "n_sample_pixels")),
                        "n_eligible_pixels": int(_number(row, "n_eligible_pixels")),
                        "n_treatment_pixels": int(_number(row, "n_treatment_pixels")),
                        "sampled_fraction": _number(row, "sampled_fraction"),
                        "coverage_fraction": _number(row, "coverage_fraction"),
                        "match_rate": _number(row, "match_rate"),
                        "n_years": int(row["n_years"]),
                    },
                )
            )

    analysis = AnalysisResults(
        name="Avoided emissions",
        analysis_type="avoided_emissions",
        summary=summary_dict,
        records=records or None,
        time_series=time_series or None,
    )

    log.info(
        "Collected results: %d sites (%d failed), %.1f MgCO2e avoided",
        summary_dict["n_sites"],
        summary_dict["n_failed_sites"],
        summary_dict["total_emissions_avoided_mgco2e"],
    )
    return analysis.dump()


__all__ = ["REQUIRES_GEE", "run"]
