"""Match quality assessment helpers and plot builders."""

import io
import logging

import dash_bootstrap_components as dbc
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import dcc, html
from layouts.common import _make_ag_grid, option_filter
from layouts.documentation import help_link
from services import download_results_csv
from services.task_result_cache import QUALITY_WARNINGS_KEY, cached_task_result
from task_timing import task_timing, timed_task_step

from callbacks._helpers import (
    _attach_totals_to_geojson,
    _normalize_metadata_list,
    _openlayers_map_component,
)
from callbacks._plot_colors import CONTROL_COLOR, PROJECT_COLOR

logger = logging.getLogger(__name__)

# Thresholds for automated quality checks (from propensity score matching
# best-practice literature).
_SMD_CRITICAL = 0.25  # |SMD| above this → critical imbalance
_SMD_WARN = 0.1  # |SMD| above this → imperfect balance
_SMD_POOR_FRAC = 0.20  # fraction of covariates with |SMD| > 0.1 to trigger warning
_PCT_MATCHED_CRITICAL = 5  # % matched below this → critical
_PCT_MATCHED_WARN = 25  # % matched below this → warning


def _build_group_diagnostics_card(group_diags):
    """Build a card showing per-group matching outcomes and separation causes.

    Parameters
    ----------
    group_diags : list[dict]
        Each dict has: group, n_treatment, n_control, n_matched,
        separation_detected, separation_details.
    """
    if not group_diags:
        return html.Div()

    # Deduplicate groups (replicates may produce identical diagnostics)
    seen = {}
    for gd in group_diags:
        g = gd.get("group", "?")
        if g not in seen:
            seen[g] = gd

    groups = sorted(seen.values(), key=lambda g: g.get("n_treatment", 0), reverse=True)
    failed = [
        g for g in groups if g.get("n_matched", 0) == 0 and g.get("n_treatment", 0) > 0
    ]

    if not failed:
        return html.Div()

    total_failed_t = sum(g.get("n_treatment", 0) for g in failed)
    total_t = sum(
        g.get("n_treatment", 0) for g in groups if g.get("n_treatment", 0) > 0
    )

    rows = []
    for g in failed:
        group_name = g.get("group", "?")
        n_t = g.get("n_treatment", 0)
        n_c = g.get("n_control", 0)
        ratio = f"{n_c / n_t:.1f}x" if n_t > 0 else "N/A"

        # Determine failure reason
        sep_details = g.get("separation_details") or []
        if sep_details:
            reason = "; ".join(sep_details)
        elif n_t > 0 and n_c < n_t:
            reason = f"Insufficient controls (ratio {ratio})"
        else:
            reason = "No feasible matches within caliper"

        rows.append(
            html.Tr(
                [
                    html.Td(group_name, style={"fontFamily": "monospace"}),
                    html.Td(f"{n_t:,}"),
                    html.Td(f"{n_c:,}"),
                    html.Td(ratio),
                    html.Td(
                        reason,
                        className="small",
                        style={"maxWidth": "400px"},
                    ),
                ]
            )
        )

    pct = total_failed_t / total_t * 100 if total_t > 0 else 0
    return dbc.Card(
        [
            dbc.CardHeader(
                [
                    html.I(
                        className="bi bi-diagram-3 text-warning me-2",
                    ),
                    html.Strong("Exact-Match Group Failures"),
                    html.Span(
                        f" \u2014 {len(failed)} of {len(groups)} group(s) "
                        f"produced zero matches "
                        f"({total_failed_t:,} treatment pixels, {pct:.0f}%)",
                        className="text-muted ms-1",
                    ),
                ]
            ),
            dbc.CardBody(
                [
                    html.P(
                        "Treatment and control pixels are grouped by "
                        "exact-match variables (e.g. ecoregion, admin "
                        "boundary). Matching only occurs within each group. "
                        "The groups below had zero successful matches:",
                        className="text-muted mb-2 small",
                    ),
                    html.Table(
                        [
                            html.Thead(
                                html.Tr(
                                    [
                                        html.Th("Group"),
                                        html.Th("Treatment"),
                                        html.Th("Control"),
                                        html.Th("Ratio"),
                                        html.Th("Failure Reason"),
                                    ]
                                )
                            ),
                            html.Tbody(rows),
                        ],
                        className="table table-sm table-hover mb-0",
                    ),
                    html.Details(
                        [
                            html.Summary(
                                "What causes group failures?",
                                style={
                                    "cursor": "pointer",
                                    "fontSize": "0.9em",
                                    "fontWeight": "600",
                                    "marginTop": "0.75rem",
                                },
                            ),
                            html.Div(
                                [
                                    html.P(
                                        "Common causes of group matching failures:",
                                        className="mb-1 mt-2",
                                    ),
                                    html.Ul(
                                        [
                                            html.Li(
                                                [
                                                    html.Strong("Covariate separation"),
                                                    (
                                                        " \u2014 A covariate value "
                                                        "appears exclusively in "
                                                        "treatment or control pixels "
                                                        "(e.g. all treatment pixels "
                                                        "are in a protected area but "
                                                        "no control pixels are). This "
                                                        "makes the propensity score "
                                                        "model predict treatment "
                                                        "perfectly, so no control can "
                                                        "match within the caliper."
                                                    ),
                                                ]
                                            ),
                                            html.Li(
                                                [
                                                    html.Strong(
                                                        "Insufficient controls"
                                                    ),
                                                    (
                                                        " \u2014 Too few control "
                                                        "pixels in the group relative "
                                                        "to treatment pixels. The "
                                                        "matching algorithm cannot "
                                                        "find enough similar controls."
                                                    ),
                                                ]
                                            ),
                                            html.Li(
                                                [
                                                    html.Strong("Caliper rejection"),
                                                    (
                                                        " \u2014 Even when controls "
                                                        "exist, none are similar "
                                                        "enough (within the caliper "
                                                        "threshold) to be matched."
                                                    ),
                                                ]
                                            ),
                                        ],
                                        className="mb-2",
                                    ),
                                    html.P(
                                        "Consider adjusting the covariates, "
                                        "exact-match variables, or enabling "
                                        "the Mahalanobis fallback for "
                                        "separation.",
                                        className="mb-0 text-muted small",
                                    ),
                                ],
                                style={"fontSize": "0.88em"},
                            ),
                        ],
                    ),
                ],
                className="p-2",
            ),
        ],
        className="mb-3 border-warning",
    )


def _assess_match_quality(balance_df=None, totals=None):
    """Run automated quality checks and return a list of warning dicts.

    Each warning has:

    * ``level`` ΓÇô ``"danger"`` (critical) or ``"warning"`` (caution).
    * ``scope`` ΓÇô ``"aggregate"`` or a *site_id* string.
    * ``message`` ΓÇô human-readable description.
    """
    warnings = []

    # -- Matched-pixel percentage per site ----------------------------------
    if totals:
        for t in totals:
            n_px = t.n_sample_pixels
            # Matched pixels as a share of the eligible pixels that were sampled.
            sampled = t.n_eligible_pixels * t.sampled_fraction
            pct = n_px / sampled * 100 if sampled > 0 else None

            if pct is not None and pct < _PCT_MATCHED_CRITICAL:
                warnings.append(
                    {
                        "level": "danger",
                        "scope": str(t.site_id),
                        "message": (
                            f"Only ~{pct:.0f}% of treatment pixels matched "
                            f"({n_px:,} pixels), which is very low for "
                            f"reliable results. Check the exact-match group "
                            f"diagnostics below for specific failure causes."
                        ),
                    }
                )
            elif pct is not None and pct < _PCT_MATCHED_WARN:
                warnings.append(
                    {
                        "level": "warning",
                        "scope": str(t.site_id),
                        "message": (
                            f"Only ~{pct:.0f}% of treatment pixels matched "
                            f"({n_px:,} pixels), which may limit result "
                            f"reliability. Check the exact-match group "
                            f"diagnostics for details."
                        ),
                    }
                )

    # -- Covariate balance (SMD) from balance statistics --------------------
    if balance_df is not None and not balance_df.empty:
        _check_balance_warnings(
            balance_df, warnings, scope="aggregate", site_filter="__all__"
        )
        per_site = balance_df[balance_df["site_id"] != "__all__"]
        for sid in per_site["site_id"].unique():
            _check_balance_warnings(
                balance_df, warnings, scope=str(sid), site_filter=str(sid)
            )

    return warnings


def _check_balance_warnings(balance_df, warnings, scope, site_filter):
    """Append SMD-based warnings for *site_filter* to *warnings*."""
    rows = balance_df[balance_df["site_id"].astype(str) == str(site_filter)]
    undefined = rows.loc[rows["smd"].isna(), "covariate"].tolist()
    if undefined:
        warnings.append(
            {
                "level": "warning",
                "scope": scope,
                "message": (
                    "Balance could not be assessed for "
                    + ", ".join(f"\u2018{c}\u2019" for c in undefined)
                    + " (the covariate is constant within treatment but differs "
                    "from the matched controls)."
                ),
            }
        )
    smds = rows["smd"].dropna()
    if smds.empty:
        return
    max_smd = smds.abs().max()
    n_poor = int((smds.abs() > _SMD_WARN).sum())
    n_total = len(smds)
    pct_poor = n_poor / n_total if n_total > 0 else 0

    n_critical = int((smds.abs() >= _SMD_CRITICAL).sum())

    if n_critical > 0:
        worst_idx = smds.abs().idxmax()
        worst_cov = rows.loc[worst_idx, "covariate"]
        # List all covariates exceeding the critical threshold
        critical_mask = smds.abs() >= _SMD_CRITICAL
        critical_covs = sorted(
            rows.loc[smds[critical_mask].index, "covariate"].tolist(),
            key=lambda c: abs(
                smds[rows["covariate"] == c].values[0]
                if len(rows[rows["covariate"] == c]) > 0
                else 0
            ),
            reverse=True,
        )
        if len(critical_covs) == 1:
            cov_detail = f"\u2018{worst_cov}\u2019 has |SMD|\u2009=\u2009{max_smd:.2f}"
        else:
            cov_detail = (
                f"{len(critical_covs)} covariates have "
                f"|SMD|\u2009\u2265\u2009{_SMD_CRITICAL} "
                f"(worst: \u2018{worst_cov}\u2019 at {max_smd:.2f})"
            )
        warnings.append(
            {
                "level": "danger",
                "scope": scope,
                "message": f"Covariate balance is poor: {cov_detail}.",
            }
        )

    # Also warn about the overall fraction of imbalanced covariates
    # (this fires independently of the critical check above)
    if n_poor > 0 and pct_poor > _SMD_POOR_FRAC:
        warnings.append(
            {
                "level": "warning",
                "scope": scope,
                "message": (
                    f"{n_poor} of {n_total} covariates "
                    f"({pct_poor:.0%}) have |SMD|\u2009>\u2009{_SMD_WARN}, "
                    f"suggesting imperfect matching."
                ),
            }
        )


def _build_quality_warning_banner(warnings, scope_filter=None):
    """Build a dismissable warning alert from quality assessment warnings.

    Parameters
    ----------
    warnings : list[dict]
        Output of :func:`_assess_match_quality`.
    scope_filter : str | None
        ``None`` to show aggregate + per-site summary.  A site-id string
        to show only that site's warnings.
    """
    if not warnings:
        return html.Div()

    if scope_filter:
        # Show only warnings for the given site
        filtered = [w for w in warnings if w["scope"] == scope_filter]
        if not filtered:
            return html.Div()
        items = [html.Li(w["message"]) for w in filtered]
        has_danger = any(w["level"] == "danger" for w in filtered)
    else:
        # Aggregate view: show aggregate warnings and summarise per-site
        agg = [w for w in warnings if w["scope"] == "aggregate"]
        site_warnings = [w for w in warnings if w["scope"] != "aggregate"]

        items = [html.Li(w["message"]) for w in agg]

        danger_sites = sorted(
            {w["scope"] for w in site_warnings if w["level"] == "danger"}
        )
        caution_sites = sorted(
            {w["scope"] for w in site_warnings if w["level"] == "warning"}
            - set(danger_sites)
        )

        if danger_sites:
            items.append(
                html.Li(
                    f"Critical quality issues detected for {len(danger_sites)} site(s)."
                )
            )
        if caution_sites:
            items.append(
                html.Li(
                    f"Quality concerns detected for "
                    f"{len(caution_sites)} additional site(s)."
                )
            )

        has_danger = any(w["level"] == "danger" for w in warnings)

    if not items:
        return html.Div()

    color = "danger" if has_danger else "warning"

    return dbc.Alert(
        [
            html.Div(
                [
                    html.I(
                        className="bi bi-exclamation-triangle-fill me-2",
                        style={"fontSize": "1.2em"},
                    ),
                    html.Strong("Match Quality Warning"),
                ],
                className="d-flex align-items-center mb-2",
            ),
            html.Ul(items, className="mb-2"),
            html.P(
                "These matching results may not be reliable. Review "
                "the matching results carefully, and seek expert "
                "advice if needed.",
                className="mb-0 fst-italic",
            ),
            help_link("results", "covariate-balance", "Understanding these warnings"),
            html.Details(
                [
                    html.Summary(
                        "What do these warnings mean?",
                        style={
                            "cursor": "pointer",
                            "fontSize": "0.9em",
                            "fontWeight": "600",
                            "marginBottom": "0.5rem",
                        },
                    ),
                    html.Div(
                        [
                            html.P(
                                [
                                    html.Strong("Critical quality issue"),
                                    (
                                        " (red) — Results are likely unreliable. "
                                        "Triggers include:"
                                    ),
                                ],
                                className="mb-1",
                            ),
                            html.Ul(
                                [
                                    html.Li(
                                        f"Fewer than {_PCT_MATCHED_CRITICAL}% "
                                        f"of treatment pixels matched — "
                                        f"too few for statistical confidence. "
                                        f"This is often caused by covariate "
                                        f"separation (see group diagnostics "
                                        f"below)."
                                    ),
                                    html.Li(
                                        f"One or more covariates with "
                                        f"|SMD| \u2265 {_SMD_CRITICAL} — "
                                        f"severe imbalance between treatment "
                                        f"and control groups, meaning matching "
                                        f"failed to find comparable areas."
                                    ),
                                ],
                                className="mb-2",
                            ),
                            html.P(
                                [
                                    html.Strong("Quality concern"),
                                    (
                                        " (yellow) — Results may be limited but "
                                        "are not necessarily invalid. "
                                        "Triggers include:"
                                    ),
                                ],
                                className="mb-1",
                            ),
                            html.Ul(
                                [
                                    html.Li(
                                        f"Fewer than {_PCT_MATCHED_WARN}% "
                                        f"of treatment pixels matched — "
                                        f"low sample size that may limit "
                                        f"reliability."
                                    ),
                                    html.Li(
                                        f"More than {_SMD_POOR_FRAC:.0%} of "
                                        f"covariates with |SMD| > {_SMD_WARN} "
                                        f"— overall matching quality is "
                                        f"imperfect."
                                    ),
                                ],
                                className="mb-2",
                            ),
                            html.P(
                                [
                                    html.Strong("SMD"),
                                    (
                                        " (Standardized Mean Difference) measures "
                                        "how similar the treatment and control "
                                        "groups are for each covariate. Values "
                                        "closer to 0 indicate better balance."
                                    ),
                                ],
                                className="mb-0 text-muted",
                                style={"fontSize": "0.85em"},
                            ),
                        ],
                        style={
                            "fontSize": "0.88em",
                            "marginTop": "0.5rem",
                        },
                    ),
                ],
                className="mt-2",
            ),
        ],
        color=color,
        className="mb-3",
    )


@timed_task_step("task_quality_warnings")
def _compute_quality_warnings(task_id, task, totals):
    """Read imported warnings, or compute and cache them for legacy tasks."""
    meta = task.extra_metadata or {}
    if QUALITY_WARNINGS_KEY in meta:
        return meta[QUALITY_WARNINGS_KEY]
    return cached_task_result(
        task,
        QUALITY_WARNINGS_KEY,
        lambda: collect_quality_warnings(task_id, task.results_s3_uri, meta, totals),
    )


def collect_quality_warnings(task_id, results_s3_uri, meta, totals):
    """Compute quality warnings using the same rules for import and legacy tasks."""
    balance_df = None
    with task_timing("task_balance_s3"):
        balance_csv = download_results_csv(
            task_id, "balance", results_s3_uri=results_s3_uri
        )
    if balance_csv:
        with task_timing("task_balance_parse"):
            balance_df = pd.read_csv(io.StringIO(balance_csv))

    warnings = _assess_match_quality(balance_df=balance_df, totals=totals)

    # Methodology warnings from the R summary step (stored on import).
    for mw in meta.get("methodology_warnings", []):
        message = mw["message"]
        affected = [
            s.get("site_name") or s["site_id"] for s in mw.get("affected_sites", [])
        ]
        if affected:
            shown = ", ".join(affected[:3])
            if len(affected) > 3:
                shown += f", and {len(affected) - 3} more"
            message = f"{message} Affected sites: {shown}."
        warnings.append({"level": "warning", "scope": "aggregate", "message": message})

    return warnings


SITE_QUALITY_GRID_ID = "results-site-quality-grid"
SITE_QUALITY_BLOCK_SIZE = 50


def _site_quality_rows(warnings, totals, start, end):
    """Return one block of site warnings and the total number of affected sites."""
    site_map = {}
    for warning in warnings:
        if warning["scope"] != "aggregate":
            site_map.setdefault(warning["scope"], []).append(warning)
    names = {str(t.site_id): t.site_name for t in totals}
    rows = [
        {
            "site_id": sid,
            "site": names.get(sid) or sid,
            "severity": (
                "Critical" if any(w["level"] == "danger" for w in ws) else "Warning"
            ),
            "issues": "; ".join(w["message"] for w in ws),
        }
        for sid, ws in sorted(site_map.items())[start:end]
    ]
    return {"rowData": rows, "rowCount": len(site_map)}


def _build_site_quality_table(warnings):
    if not any(w["scope"] != "aggregate" for w in warnings):
        return html.Div()

    columns = [
        {
            "field": "severity",
            "width": 105,
            "cellClassRules": {
                "text-danger": "value === 'Critical'",
                "text-warning-emphasis": "value === 'Warning'",
            },
        },
        {"field": "site", "flex": 1, "minWidth": 130, "tooltipField": "site"},
        {
            "field": "issues",
            "headerName": "Issue(s)",
            "flex": 2,
            "minWidth": 180,
            "tooltipField": "issues",
        },
    ]
    for column in columns:
        column.update(sortable=False, filter=False)
    return dbc.Card(
        [
            dbc.CardHeader(
                [
                    html.I(
                        className="bi bi-exclamation-triangle-fill text-warning me-2"
                    ),
                    "Sites with Potential Quality Issues",
                ]
            ),
            dbc.CardBody(
                _make_ag_grid(
                    SITE_QUALITY_GRID_ID,
                    columns,
                    row_model="infinite",
                    height="320px",
                    grid_options_extra={
                        "rowHeight": 64,
                        "cacheBlockSize": SITE_QUALITY_BLOCK_SIZE,
                        "maxBlocksInCache": 4,
                        "enableBrowserTooltips": True,
                        "alwaysShowHorizontalScroll": False,
                        "getRowId": {"function": "params.data.site_id"},
                    },
                ),
            ),
        ],
        className="mb-3",
    )


SITE_STATUS_GRID_ID = "task-site-status-grid"
SITE_STATUS_BLOCK_SIZE = 100
_SITE_STATUS_QUALITY_RANK = {"Critical": 0, "Caution": 1, "No results": 2, "OK": 3}
_SITE_STATUS_TEXT_FIELDS = {"quality", "site", "issues", "excluded_fragments"}
_SITE_STATUS_NUMERIC_FIELDS = {
    "area_ha",
    "matched_pixels",
    "pct_matched",
    "n_excluded_fragments",
    "sampled_pct",
}


def _metadata_site_id(entry):
    site_id = entry.get("site_id")
    if site_id is None or site_id == "":
        site_id = entry.get("id_numeric", "?")
    return str(site_id)


def _excluded_fragment_text(fragment):
    text = str(fragment.get("error") or "Unknown error")
    separation = fragment.get("separation_warnings")
    if isinstance(separation, dict):
        details = [
            f"Group {group}: {detail}"
            for group, group_details in separation.items()
            if isinstance(group_details, list)
            for detail in group_details
        ]
        if details:
            text += f" ({'; '.join(details)})"
    return text


def build_site_status_rows(meta, totals, site_index, warnings, *, has_results):
    """Merge per-site quality, excluded fragments, and subsampling into one row each."""
    rows = {}

    def row_for(site_id):
        return rows.setdefault(
            site_id,
            {
                "site_id": site_id,
                "display_site_id": None,
                "site_name": None,
                "area_ha": None,
                "matched_pixels": None,
                "pct_matched": None,
                "issues": "",
                "_levels": set(),
                "_fragments": [],
                "sampled_pct": None,
                "_has_totals": False,
            },
        )

    def fill(row, display_site_id=None, site_name=None):
        row["display_site_id"] = row["display_site_id"] or display_site_id
        row["site_name"] = row["site_name"] or site_name

    for site in site_index:
        row = row_for(str(site["site_id"]))
        fill(row, site.get("display_site_id"), site.get("site_name"))
        row["area_ha"] = site.get("area_ha")

    for total in totals:
        row = row_for(str(total.site_id))
        fill(row, getattr(total, "display_site_id", None), total.site_name)
        row["_has_totals"] = True
        if total.area_ha is not None:
            row["area_ha"] = total.area_ha
        row["matched_pixels"] = total.n_sample_pixels
        sampled = (total.n_eligible_pixels or 0) * (total.sampled_fraction or 0)
        if total.n_sample_pixels is not None and sampled > 0:
            row["pct_matched"] = round(total.n_sample_pixels / sampled * 100, 1)

    site_messages = {}
    for warning in warnings or []:
        if warning["scope"] == "aggregate":
            continue
        row = row_for(str(warning["scope"]))
        row["_levels"].add(warning["level"])
        site_messages.setdefault(row["site_id"], []).append(warning["message"])
    for site_id, messages in site_messages.items():
        rows[site_id]["issues"] = "; ".join(messages)

    for fragment in _normalize_metadata_list(meta.get("failed_sites", [])):
        row = row_for(_metadata_site_id(fragment))
        fill(row, fragment.get("display_site_id"), fragment.get("site_name"))
        row["_fragments"].append(_excluded_fragment_text(fragment))

    for subsampled in _normalize_metadata_list(meta.get("subsampled_sites", [])):
        row = row_for(_metadata_site_id(subsampled))
        fill(row, subsampled.get("display_site_id"), subsampled.get("site_name"))
        percent = subsampled.get("sampled_percent")
        if percent is None and subsampled.get("sampled_fraction") is not None:
            percent = float(subsampled["sampled_fraction"]) * 100
        if percent is not None:
            row["sampled_pct"] = round(float(percent), 1)

    result = []
    for row in rows.values():
        levels = row.pop("_levels")
        fragments = row.pop("_fragments")
        has_totals = row.pop("_has_totals")
        display_id = str(row.pop("display_site_id") or row["site_id"])
        name = row.pop("site_name")
        row["site"] = (
            f"{name} ({display_id})" if name and name != display_id else display_id
        )
        if "danger" in levels:
            row["quality"] = "Critical"
        elif levels:
            row["quality"] = "Caution"
        elif has_results and not has_totals:
            row["quality"] = "No results"
        elif has_results:
            row["quality"] = "OK"
        else:
            row["quality"] = None
        row["n_excluded_fragments"] = len(fragments)
        row["excluded_fragments"] = (
            f"{len(fragments)}: {'; '.join(fragments)}" if fragments else ""
        )
        result.append(row)
    return result


def _site_status_sort_key(field, row):
    if field == "quality":
        return _SITE_STATUS_QUALITY_RANK.get(row["quality"], 4)
    if field == "excluded_fragments":
        return row["n_excluded_fragments"]
    value = row.get(field)
    return value.lower() if isinstance(value, str) else value


def _matches_text_filter(value, spec):
    if "conditions" in spec:
        results = [_matches_text_filter(value, c) for c in spec["conditions"]]
        return all(results) if spec.get("operator") == "AND" else any(results)
    op = spec.get("type", "contains")
    value = str(value or "").lower()
    if op.startswith("is:"):
        return value == op[3:].lower()
    if op == "blank":
        return not value
    if op == "notBlank":
        return bool(value)
    needle = str(spec.get("filter") or "").lower()
    if not needle:
        return True
    return {
        "contains": needle in value,
        "notContains": needle not in value,
        "equals": value == needle,
        "notEqual": value != needle,
        "startsWith": value.startswith(needle),
        "endsWith": value.endswith(needle),
    }.get(op, True)


def _matches_number_filter(value, spec):
    if "conditions" in spec:
        results = [_matches_number_filter(value, c) for c in spec["conditions"]]
        return all(results) if spec.get("operator") == "AND" else any(results)
    op = spec.get("type", "equals")
    if op == "blank":
        return value is None
    if op == "notBlank":
        return value is not None
    target = spec.get("filter")
    if target is None:
        return True
    if value is None:
        return False
    if op == "inRange":
        upper = spec.get("filterTo")
        return target <= value <= upper if upper is not None else value >= target
    return {
        "equals": value == target,
        "notEqual": value != target,
        "lessThan": value < target,
        "lessThanOrEqual": value <= target,
        "greaterThan": value > target,
        "greaterThanOrEqual": value >= target,
    }.get(op, True)


def site_status_block(rows, request):
    """Filter, sort, and slice merged site rows for an infinite-grid request."""
    for field, spec in (request.get("filterModel") or {}).items():
        if not isinstance(spec, dict):
            continue
        if field in _SITE_STATUS_TEXT_FIELDS:
            rows = [r for r in rows if _matches_text_filter(r.get(field), spec)]
        elif field in _SITE_STATUS_NUMERIC_FIELDS:
            rows = [r for r in rows if _matches_number_filter(r.get(field), spec)]

    rows = sorted(
        rows,
        key=lambda r: (
            _SITE_STATUS_QUALITY_RANK.get(r["quality"], 4),
            -r["n_excluded_fragments"],
            r["site"].lower(),
        ),
    )
    for item in reversed(request.get("sortModel") or []):
        field = item.get("colId")
        if field not in _SITE_STATUS_TEXT_FIELDS | _SITE_STATUS_NUMERIC_FIELDS:
            continue
        descending = item.get("sort") == "desc"
        present = [r for r in rows if _site_status_sort_key(field, r) is not None]
        missing = [r for r in rows if _site_status_sort_key(field, r) is None]
        present.sort(key=lambda r: _site_status_sort_key(field, r), reverse=descending)
        rows = present + missing

    start, end = request["startRow"], request["endRow"]
    return {"rowData": rows[start:end], "rowCount": len(rows)}


def _build_site_status_card(task, totals, warnings):
    """Overview card listing every site with quality, exclusions, and sampling."""
    meta = task.extra_metadata or {}
    failed = _normalize_metadata_list(meta.get("failed_sites", []))
    subsampled = _normalize_metadata_list(meta.get("subsampled_sites", []))
    if not totals and not failed and not subsampled:
        return None

    site_levels = {}
    for warning in warnings or []:
        if warning["scope"] != "aggregate":
            site_levels.setdefault(warning["scope"], set()).add(warning["level"])
    n_critical = sum("danger" in levels for levels in site_levels.values())
    summary = []
    if n_critical:
        summary.append(f"{n_critical} site(s) with critical issues")
    if len(site_levels) > n_critical:
        summary.append(f"{len(site_levels) - n_critical} site(s) with quality concerns")
    if failed:
        n_failed_sites = len({_metadata_site_id(f) for f in failed})
        summary.append(
            f"{len(failed)} site fragment(s) not included across "
            f"{n_failed_sites} site(s)"
        )
    if subsampled:
        summary.append(f"{len(subsampled)} site(s) subsampled")
    aggregate_warnings = [w for w in warnings or [] if w["scope"] == "aggregate"]
    aggregate_note = (
        dbc.Alert(
            [
                html.Strong("Across all sites: "),
                html.Ul(
                    [html.Li(w["message"]) for w in aggregate_warnings],
                    className="mb-0",
                ),
            ],
            color=(
                "danger"
                if any(w["level"] == "danger" for w in aggregate_warnings)
                else "warning"
            ),
            className="small py-2 mb-2",
        )
        if aggregate_warnings
        else None
    )

    def numeric(field, header, fmt, width, suffix=""):
        return {
            "headerName": header,
            "field": field,
            "minWidth": width,
            "flex": 0.6,
            "type": "numericColumn",
            "filter": "agNumberColumnFilter",
            "valueFormatter": {
                "function": (
                    "params.data == null ? '' : "
                    f"params.value != null ? d3.format('{fmt}')(params.value)"
                    f"{' + ' + repr(suffix) if suffix else ''} : '\u2014'"
                )
            },
        }

    wrapped = {"whiteSpace": "normal", "lineHeight": "1.35", "overflow": "hidden"}
    text_filter = {
        "filter": "agTextColumnFilter",
        "filterParams": {"maxNumConditions": 1},
    }
    columns = [
        {
            "headerName": "Quality",
            "field": "quality",
            "width": 80,
            "cellRenderer": "SeverityIcon",
            "headerTooltip": "Match quality: Critical, Caution, OK, or No results",
            **option_filter(
                {q: q for q in _SITE_STATUS_QUALITY_RANK}, server_side=True
            ),
        },
        {
            "headerName": "Site",
            "field": "site",
            "flex": 1.2,
            "minWidth": 180,
            "tooltipField": "site",
            "cellStyle": wrapped,
            **text_filter,
        },
        numeric("area_ha", "Area (ha)", ",.0f", 95),
        numeric("matched_pixels", "Matched Px", ",", 95),
        numeric("pct_matched", "% Matched", ".1f", 90, "%"),
        {
            "headerName": "Quality issues",
            "field": "issues",
            "flex": 2.2,
            "minWidth": 260,
            "tooltipField": "issues",
            "cellStyle": wrapped,
            **text_filter,
        },
        {
            "headerName": "Fragments not included",
            "field": "excluded_fragments",
            "flex": 1.5,
            "minWidth": 200,
            "tooltipField": "excluded_fragments",
            "cellStyle": wrapped,
            **text_filter,
        },
        {
            "headerName": "Subsampled",
            "field": "sampled_pct",
            "minWidth": 115,
            "flex": 0.6,
            "type": "numericColumn",
            "filter": "agNumberColumnFilter",
            "headerTooltip": "Percent of the site's eligible pixels sampled for matching",
            "valueFormatter": {
                "function": (
                    "params.data == null ? '' : params.value != null"
                    " ? 'Yes (' + d3.format('.1f')(params.value) + '%)' : 'No'"
                )
            },
        },
    ]
    row_styles = [
        {
            "condition": "params.data && params.data.quality === 'Critical'",
            "style": {"backgroundColor": "#fce4e4"},
        },
        {
            "condition": "params.data && params.data.quality === 'Caution'",
            "style": {"backgroundColor": "#fff8e1"},
        },
        {
            "condition": "params.data && params.data.quality === 'No results'",
            "style": {"backgroundColor": "#f1f3f5"},
        },
    ]
    return dbc.Card(
        [
            dbc.CardHeader(
                [
                    html.I(className="bi bi-table text-primary me-2"),
                    html.Strong("Site Matching Status"),
                    html.Span(
                        f" — {'; '.join(summary)}" if summary else "",
                        className="text-muted ms-1",
                    ),
                ]
            ),
            dbc.CardBody(
                [
                    html.P(
                        [
                            (
                                "All sites, with match quality issues, site fragments "
                                "that could not be included, and subsampling. Large "
                                "sites were subsampled for matching; their results are "
                                "scaled up from the sampled fraction. "
                            ),
                            help_link(
                                "results",
                                "covariate-balance",
                                "Understanding these warnings",
                            ),
                        ],
                        className="small text-muted mb-2",
                    ),
                    aggregate_note,
                    _make_ag_grid(
                        SITE_STATUS_GRID_ID,
                        columns,
                        row_model="infinite",
                        height="560px",
                        style_conditions=row_styles,
                        grid_options_extra={
                            "rowHeight": 64,
                            "cacheBlockSize": SITE_STATUS_BLOCK_SIZE,
                            "maxBlocksInCache": 10,
                            "enableBrowserTooltips": True,
                            "alwaysShowHorizontalScroll": False,
                            "getRowId": {"function": "params.data.site_id"},
                        },
                    ),
                ],
                className="p-2",
            ),
        ],
        className="mb-3",
    )


def _weighted_mean(values, weights):
    weight_total = weights.sum()
    if weight_total <= 0:
        return np.nan
    return float(np.average(values, weights=weights))


def _weighted_quantiles(values, weights, probabilities):
    order = np.argsort(values)
    sorted_values = values[order]
    sorted_weights = weights[order]
    weight_total = sorted_weights.sum()
    if weight_total <= 0:
        return []
    cumulative = np.cumsum(sorted_weights) / weight_total
    indexes = np.searchsorted(cumulative, probabilities - 1e-12, side="left")
    return sorted_values[np.minimum(indexes, len(sorted_values) - 1)].tolist()


def _calculate_match_quality_summary(pixels, covariate_cols, categorical_cols):
    """Recompute match-quality statistics from selected matched pixel rows."""
    required = {"treatment", "match_weight", "pscore"}
    if missing := required - set(pixels.columns):
        raise ValueError(
            "Matched pixel data is missing required columns: "
            + ", ".join(sorted(missing))
        )

    pixels = pixels.copy()
    if pd.api.types.is_bool_dtype(pixels["treatment"]):
        treatment = pixels["treatment"]
    else:
        treatment = (
            pixels["treatment"]
            .astype(str)
            .str.lower()
            .map({"true": True, "false": False, "1": True, "0": False})
        )
    if treatment.isna().any():
        raise ValueError("Matched pixel data contains invalid treatment values.")
    pixels["treatment"] = treatment.astype(bool)
    pixels["match_weight"] = pd.to_numeric(pixels["match_weight"], errors="raise")
    if (
        not np.isfinite(pixels["match_weight"]).all()
        or pixels["match_weight"].lt(0).any()
    ):
        raise ValueError("Matched pixel data contains invalid match weights.")

    balance_rows = []
    for covariate in covariate_cols:
        if covariate not in pixels:
            raise ValueError(f"Matched pixel data is missing {covariate!r}.")
        values = pixels[covariate]
        weights = pixels["match_weight"]
        valid = values.notna() & weights.notna()
        x = values.loc[valid]
        t = pixels.loc[valid, "treatment"]
        w = weights.loc[valid]
        if not t.any() or t.all():
            continue

        levels = sorted(x.unique()) if covariate in categorical_cols else [None]
        for level in levels:
            if level is not None:
                x_values = x.eq(level).astype(float)
                name = f"{covariate}={level}"
                binary = True
            else:
                x_values = pd.to_numeric(x, errors="coerce")
                numeric_valid = x_values.notna()
                x_values = x_values.loc[numeric_valid]
                t = t.loc[numeric_valid]
                w = w.loc[numeric_valid]
                if not t.any() or t.all():
                    continue
                name = covariate
                binary = False

            treatment_values = x_values.loc[t]
            control_values = x_values.loc[~t]
            treatment_weights = w.loc[t]
            control_weights = w.loc[~t]
            mean_treatment = _weighted_mean(treatment_values, treatment_weights)
            mean_control = _weighted_mean(control_values, control_weights)
            sd_treatment = (
                float(treatment_values.std(ddof=1))
                if len(treatment_values) > 1
                else np.nan
            )
            if binary:
                smd = mean_treatment - mean_control
            elif sd_treatment > 0:
                smd = (mean_treatment - mean_control) / sd_treatment
            elif np.isclose(mean_treatment, mean_control, equal_nan=False):
                smd = 0.0
            else:
                smd = np.nan
            balance_rows.append(
                {
                    "covariate": name,
                    "mean_treatment": mean_treatment,
                    "mean_control": mean_control,
                    "sd_treatment": sd_treatment,
                    "smd": smd,
                }
            )

    histograms = {}
    numeric_covariates = [
        column for column in covariate_cols if column not in categorical_cols
    ]
    for covariate in numeric_covariates:
        values = pd.to_numeric(pixels[covariate], errors="coerce")
        valid = values.notna()
        x = values.loc[valid].to_numpy(dtype=float)
        t = pixels.loc[valid, "treatment"].to_numpy(dtype=bool)
        w = pixels.loc[valid, "match_weight"].to_numpy(dtype=float)
        if len(x) < 2 or x.min() == x.max():
            continue
        treatment_counts, edges = np.histogram(
            x[t], bins=40, range=(x.min(), x.max()), weights=w[t]
        )
        control_counts, _ = np.histogram(x[~t], bins=edges, weights=w[~t])
        treatment_total = treatment_counts.sum()
        control_total = control_counts.sum()
        histograms[covariate] = {
            "bin_edges": edges.tolist(),
            "treatment_pct": (
                treatment_counts / treatment_total * 100
                if treatment_total > 0
                else np.zeros(40)
            ).tolist(),
            "control_pct": (
                control_counts / control_total * 100
                if control_total > 0
                else np.zeros(40)
            ).tolist(),
        }

    propensity = pd.to_numeric(pixels["pscore"], errors="coerce")
    valid = propensity.notna()
    t = pixels.loc[valid, "treatment"]
    w = pixels.loc[valid, "match_weight"]
    qq = None
    if t.sum() >= 2 and (~t).sum() >= 2:
        probabilities = np.linspace(0, 1, 500)
        treatment_mask = t.to_numpy(dtype=bool)
        weights = w.to_numpy(dtype=float)
        values = propensity.loc[valid].to_numpy(dtype=float)
        qq = {
            "quantiles": probabilities.tolist(),
            "treatment_values": _weighted_quantiles(
                values[treatment_mask], weights[treatment_mask], probabilities
            ),
            "control_values": _weighted_quantiles(
                values[~treatment_mask], weights[~treatment_mask], probabilities
            ),
        }

    stats = {
        "n_treatment": int(pixels["treatment"].sum()),
        "n_control": int((~pixels["treatment"]).sum()),
        "n_sites": int(pixels["site_id"].nunique()) if "site_id" in pixels else 1,
    }
    return {
        "balance_rows": balance_rows,
        "histograms": histograms,
        "qq_quantiles": qq,
        "summary_stats": stats,
    }


def _build_selected_match_quality(
    pixels, summary, selected_site_ids, selected_totals, selected_site_id=None
):
    """Build selected-site/group diagnostics from the matched pixel records."""
    covariate_cols = summary.get("covariate_cols", [])
    if "categorical_cols" not in summary:
        return dbc.Alert(
            "This task does not include the metadata needed to recompute "
            "selection-specific match quality. Rerun it with the current "
            "processing version.",
            color="warning",
        )
    categorical_cols = summary.get("categorical_cols") or []
    if isinstance(covariate_cols, str):
        covariate_cols = [covariate_cols]
    if isinstance(categorical_cols, str):
        categorical_cols = [categorical_cols]

    metrics = _calculate_match_quality_summary(pixels, covariate_cols, categorical_cols)
    scope_key = str(selected_site_id) if selected_site_id else "__all__"
    stored_site_stats = summary.get("summary_stats", {})
    selected_stats = [
        stored_site_stats.get(site_id, {}) for site_id in selected_site_ids
    ]
    stats = metrics["summary_stats"]
    for field in ("n_treatment_total", "n_control_sampled", "n_control_pool"):
        stats[field] = sum(site.get(field, 0) or 0 for site in selected_stats)

    balance_df = pd.DataFrame(metrics["balance_rows"])
    if not balance_df.empty:
        balance_df["site_id"] = scope_key
    warnings = _assess_match_quality(balance_df, selected_totals)
    store_data = {
        "summary_stats": {scope_key: stats},
        "histograms": {scope_key: metrics["histograms"]},
        "qq_quantiles": (
            {scope_key: metrics["qq_quantiles"]} if metrics["qq_quantiles"] else {}
        ),
        "covariate_cols": covariate_cols,
        "site_areas": {
            str(total.site_id): total.area_ha or 0 for total in selected_totals
        },
        "quality_warnings": warnings,
        "balance_rows": (balance_df.to_dict("records") if not balance_df.empty else []),
    }
    missing_sites = set(selected_site_ids) - set(pixels["site_id"].astype(str))
    children = []
    children.append(
        html.P(
            f"Based on matched pixels from {stats['n_sites']:,} selected site(s).",
            className="text-muted",
        )
    )
    if missing_sites:
        children.append(
            dbc.Alert(
                f"Match data is available for {len(selected_site_ids) - len(missing_sites)} "
                f"of {len(selected_site_ids)} selected sites; sites without matches "
                "are not included in these plots.",
                color="warning",
            )
        )
    children.append(
        _build_quality_warning_banner(
            warnings,
            scope_filter=str(selected_site_id) if selected_site_id else None,
        )
    )
    children.extend(_build_plots_from_summary(store_data, selected_site_id))
    return html.Div(children)


def _build_love_plot(balance_df, cov_df, site_filter=None):
    """Build a Love plot (balance dot plot) from the balance statistics CSV.

    Shows a horizontal dot plot with one row per covariate.  The x-axis is
    the Standardized Mean Difference (SMD) and dashed vertical lines mark
    the ┬▒0.1 threshold that is conventionally considered acceptable.

    Parameters
    ----------
    balance_df : pd.DataFrame
        Balance statistics with columns ``site_id``, ``covariate``, ``smd``.
    cov_df : pd.DataFrame
        Full covariate data (used only as a fallback if balance_df is
        missing aggregate rows).
    site_filter : str | None
        ``None`` for aggregate view; otherwise the site id to display.
    """
    if site_filter:
        agg = balance_df[balance_df["site_id"].astype(str) == str(site_filter)].copy()
        if agg.empty:
            return html.P(
                "No balance statistics available for this site.",
                className="text-muted",
            )
    else:
        # Use aggregate balance (site_id == "__all__")
        agg = balance_df[balance_df["site_id"] == "__all__"].copy()
        if agg.empty:
            return html.P(
                "No aggregate balance statistics available.",
                className="text-muted",
            )

    # Drop rows with missing SMD
    agg = agg.dropna(subset=["smd"])
    if agg.empty:
        return html.P(
            "All covariates have insufficient data for SMD calculation.",
            className="text-muted",
        )

    # Sort by absolute SMD for visual clarity
    agg = agg.sort_values("smd", key=lambda s: s.abs(), ascending=True)

    # Colour-code by whether SMD is within the ┬▒0.1 threshold
    colors = [PROJECT_COLOR if abs(v) <= 0.1 else CONTROL_COLOR for v in agg["smd"]]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=agg["smd"],
            y=agg["covariate"],
            mode="markers",
            marker={"size": 10, "color": colors},
            hovertemplate="%{y}: SMD = %{x:.3f}<extra></extra>",
        )
    )
    # Reference lines at ┬▒0.1
    fig.add_vline(x=0.1, line_dash="dash", line_color="gray", opacity=0.6)
    fig.add_vline(x=-0.1, line_dash="dash", line_color="gray", opacity=0.6)
    fig.add_vline(x=0, line_color="black", opacity=0.3)

    fig.update_layout(
        title="Standardized Mean Differences After Matching",
        xaxis_title="Standardized Mean Difference (SMD)",
        yaxis_title="",
        showlegend=False,
        height=max(300, 40 * len(agg) + 100),
        margin={"l": 200, "r": 40, "t": 50, "b": 50},
        xaxis={"zeroline": True},
    )

    return dcc.Graph(figure=fig)


# ---------------------------------------------------------------------------
# Summary-based plot builders (no raw pixel data needed)
# ---------------------------------------------------------------------------


def _build_plots_from_summary(store_data, site_filter=None):
    """Build stat boxes, Love plot, QQ plot, and histograms from the
    summary data stored in ``store_data``.

    This is the memory-safe path: only small pre-aggregated data is
    used rather than the full pixel-level CSVs.
    """
    site_areas = store_data.get("site_areas", {})

    components = []

    # --- Summary stat boxes ------------------------------------------------
    scope_key = str(site_filter) if site_filter else "__all__"
    stats = store_data.get("summary_stats", {}).get(scope_key, {})
    n_treatment = stats.get("n_treatment", 0)
    n_control = stats.get("n_control", 0)
    n_sites = stats.get("n_sites", 0) if not site_filter else 1

    if site_filter:
        total_area = site_areas.get(str(site_filter), 0)
    else:
        total_area = sum(site_areas.values())

    # Treatment pixels card with optional total-in-site sub-number
    n_treatment_total = stats.get("n_treatment_total")
    treatment_body = [
        html.H6("Treatment Pixels", className="text-muted mb-1"),
        html.H4(f"{n_treatment:,}"),
    ]
    if n_treatment_total and n_treatment_total > n_treatment:
        treatment_body.append(
            html.Small(
                f"of {n_treatment_total:,} in site",
                className="text-muted",
            )
        )

    # Control pixels card with sampled / pool sub-numbers
    n_control_sampled = stats.get("n_control_sampled")
    n_control_pool = stats.get("n_control_pool")
    control_body = [
        html.H6("Control Pixels", className="text-muted mb-1"),
        html.H4(f"{n_control:,}"),
    ]
    if n_control_sampled and n_control_sampled > n_control:
        pct = n_control_sampled / n_control_pool * 100 if n_control_pool else 0
        if n_control_pool and pct < 100:
            control_body.append(
                html.Small(
                    f"of {n_control_sampled:,} sampled "
                    f"({pct:.1f}% of {n_control_pool:,})",
                    className="text-muted",
                )
            )
        else:
            control_body.append(
                html.Small(
                    f"of {n_control_sampled:,} sampled",
                    className="text-muted",
                )
            )
    elif n_control_pool and n_control_pool > n_control:
        control_body.append(
            html.Small(
                f"of {n_control_pool:,} candidates",
                className="text-muted",
            )
        )

    stat_cols = [
        dbc.Col(
            dbc.Card(
                dbc.CardBody(treatment_body),
                className="text-center",
            ),
            md=3,
        ),
        dbc.Col(
            dbc.Card(
                dbc.CardBody(control_body),
                className="text-center",
            ),
            md=3,
        ),
        dbc.Col(
            dbc.Card(
                dbc.CardBody(
                    [
                        html.H6(
                            "Site Area (ha)" if site_filter else "Total Area (ha)",
                            className="text-muted mb-1",
                        ),
                        html.H4(f"{total_area:,.1f}"),
                    ]
                ),
                className="text-center",
            ),
            md=3,
        ),
    ]
    if not site_filter:
        stat_cols.append(
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.H6("Sites", className="text-muted mb-1"),
                            html.H4(f"{n_sites:,}"),
                        ]
                    ),
                    className="text-center",
                ),
                md=3,
            ),
        )
    components.append(dbc.Row(stat_cols, className="mb-4"))

    # --- Love plot (from balance CSV — already small) ----------------------
    balance_rows = store_data.get("balance_rows")
    if balance_rows:
        balance_df = pd.DataFrame(balance_rows)
        if not balance_df.empty:
            components.append(
                html.H6("Covariate Balance (Love Plot)", className="mt-3 mb-2")
            )
            components.append(
                html.P(
                    "Standardized mean differences (SMD) for each covariate "
                    "after matching.  Values within the dashed lines "
                    "(|SMD| < 0.1) indicate good balance between treatment "
                    "and control groups.",
                    className="text-muted mb-2",
                )
            )
            components.append(_build_love_plot(balance_df, pd.DataFrame(), site_filter))

    # --- QQ plot from pre-computed quantiles --------------------------------
    qq_data = store_data.get("qq_quantiles", {}).get(scope_key)
    if qq_data:
        components.append(html.H6("Propensity Score QQ Plot", className="mt-3 mb-2"))
        components.append(
            html.P(
                "Empirical quantile-quantile plot comparing the propensity "
                "score distributions of matched treatment and control "
                "pixels. Points close to the 45\u00b0 line indicate similar "
                "distributions.",
                className="text-muted mb-2",
            )
        )
        components.append(_build_qq_from_summary(qq_data))

    # --- Histograms from pre-computed bins ---------------------------------
    hist_data = store_data.get("histograms", {}).get(scope_key, {})
    if hist_data:
        components.append(html.H6("Covariate Distributions", className="mt-3 mb-2"))
        components.extend(_build_histograms_from_summary(hist_data))

    return components


def _build_qq_from_summary(qq_data):
    """Render a QQ plot from pre-computed quantile pairs.

    ``qq_data`` is a dict with keys ``treatment_values`` and
    ``control_values`` (lists of floats of equal length).
    """
    t_q = qq_data["treatment_values"]
    c_q = qq_data["control_values"]

    fig = go.Figure()

    q_min = min(min(t_q), min(c_q))
    q_max = max(max(t_q), max(c_q))
    fig.add_trace(
        go.Scatter(
            x=[q_min, q_max],
            y=[q_min, q_max],
            mode="lines",
            line={"color": "gray", "dash": "dash"},
            name="45\u00b0 line",
            hoverinfo="skip",
        )
    )
    fig.add_trace(
        go.Scattergl(
            x=c_q,
            y=t_q,
            mode="markers",
            marker={"size": 4, "color": "#1f77b4", "opacity": 0.6},
            name="Matched Pixels",
            hovertemplate=(
                "Control quantile: %{x:.3f}<br>"
                "Treatment quantile: %{y:.3f}<extra></extra>"
            ),
        )
    )
    fig.update_layout(
        title="Propensity Score QQ Plot (Treatment vs Control)",
        xaxis_title="Control Quantiles",
        yaxis_title="Treatment Quantiles",
        height=450,
        margin={"t": 50, "b": 50, "l": 60, "r": 30},
        legend={"yanchor": "top", "y": 0.99, "xanchor": "left", "x": 0.01},
        xaxis={"scaleanchor": "y", "scaleratio": 1},
    )

    return dcc.Graph(figure=fig)


def _build_histograms_from_summary(hist_data):
    """Build histogram figures from pre-computed bin counts.

    ``hist_data`` is ``{covariate_name: {bin_edges, treatment_pct,
    control_pct}}``.

    Returns a list of ``dcc.Graph`` components.
    """
    plots = []
    for col, data in hist_data.items():
        edges = data["bin_edges"]
        t_pct = data["treatment_pct"]
        c_pct = data["control_pct"]

        if not edges or len(edges) < 2:
            continue

        # Compute bin midpoints for the bar chart x-axis
        mids = [(edges[i] + edges[i + 1]) / 2 for i in range(len(edges) - 1)]
        bin_width = edges[1] - edges[0] if len(edges) > 1 else 1

        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=mids,
                y=t_pct,
                name="Treatment",
                opacity=0.6,
                marker_color=PROJECT_COLOR,
                width=bin_width,
            )
        )
        fig.add_trace(
            go.Bar(
                x=mids,
                y=c_pct,
                name="Control",
                opacity=0.6,
                marker_color=CONTROL_COLOR,
                width=bin_width,
            )
        )
        fig.update_layout(
            title=f"Covariate: {col}",
            xaxis_title=col,
            yaxis_title="Frequency (%)",
            barmode="overlay",
            legend={"yanchor": "top", "y": 0.99, "xanchor": "right", "x": 0.99},
            height=350,
            margin={"t": 40, "b": 40, "l": 50, "r": 20},
        )
        plots.append(dcc.Graph(figure=fig))

    if not plots:
        return [html.P("No numeric covariates to display.", className="text-muted")]

    return plots


def _build_map(
    sites_geojson,
    totals,
    covariates=None,
    task_id=None,
    sites=None,
    resolution_m=None,
    share_token=None,
    use_tiles=True,
):
    """Build an OpenLayers map for task sites and summary values."""
    enriched_geojson = _attach_totals_to_geojson(sites_geojson, totals)
    if not enriched_geojson:
        return html.P("No site geometries available.", className="text-muted")

    # Build tile URL when task_id is available so the rendering layer fetches
    # MVT tiles directly (no Dash round-trip on pan/zoom).  The enriched_geojson
    # (centroid points + emissions) is still passed as data-geojson; the JS uses
    # it to build _emissionsBySiteId for colouring circles on task-scoped tiles.
    tile_url = (
        f"/api/task-sites-tiles/{task_id}/{{z}}/{{x}}/{{y}}"
        if task_id and use_tiles
        else None
    )
    map_component = _openlayers_map_component(
        "task-sites-map",
        enriched_geojson,
        height="500px",
        enable_cog_layers=True,
        cog_filter_covariates=covariates,
        task_id=task_id,
        resolution_m=resolution_m,
        tile_url=tile_url,
        share_token=share_token,
    )

    return map_component
