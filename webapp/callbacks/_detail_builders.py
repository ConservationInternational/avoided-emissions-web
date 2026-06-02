"""Helper functions for building the task detail page content."""

import logging

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html

from callbacks._helpers import (
    _fmt_dt,
    _normalize_metadata_list,
)
from layouts import (
    RESULTS_TOTAL_COLUMNS,
    RESULTS_YEARLY_COLUMNS,
    _make_ag_grid,
)
from services import (
    download_results_csv,
    list_task_s3_files,
)
from callbacks._match_quality import (
    _build_group_diagnostics_card,
    _build_site_quality_table,
)

logger = logging.getLogger(__name__)


def _build_overview(task, sites, totals, quality_warnings=None):
    """Build the overview cards for a task detail page."""
    cards = []

    # Task info card
    config = task.config or {}

    def _detail_row(label, value):
        return html.Div(
            [
                html.Span(label, className="text-muted", style={"minWidth": "200px"}),
                html.Span(str(value), style={"fontWeight": "500"}),
            ],
            style={"display": "flex", "gap": "0.5rem", "marginBottom": "0.25rem"},
        )

    caliper_val = config.get("caliper_width")
    caliper_display = (
        "Disabled" if caliper_val == 0 else (caliper_val if caliper_val else "ΓÇö")
    )
    max_ctrl = config.get("max_controls_per_treatment")
    max_ctrl_display = (
        "No limit" if max_ctrl == 0 else (max_ctrl if max_ctrl else "ΓÇö")
    )
    mem_mib = config.get("match_memory_mib")
    mem_display = f"{mem_mib / 1024:.1f} GB" if mem_mib else "ΓÇö"

    # Derive API execution ID and batch job names from extract_job_id.
    api_exec_id = None
    batch_job_names = []
    job_id_str = task.extract_job_id or ""
    if job_id_str.startswith("api:"):
        api_exec_id = job_id_str[4:]
        pipeline = config.get("pipeline") or []
        if pipeline:
            for step in pipeline:
                if isinstance(step, dict) and step.get("name"):
                    batch_job_names.append(f"te-{step['name']}-{api_exec_id[:8]}")
        else:
            batch_job_names.append(f"te-{api_exec_id[:8]}")

    cards.append(
        dbc.Card(
            [
                dbc.CardHeader("Task Information"),
                dbc.CardBody(
                    [
                        _detail_row("Description", task.description or "None"),
                        _detail_row("Sites", task.n_sites or 0),
                        html.Div(
                            [
                                html.Span(
                                    "Created",
                                    className="text-muted",
                                    style={"minWidth": "200px"},
                                ),
                                html.Span(
                                    _fmt_dt(task.created_at),
                                    className="utc-datetime",
                                    style={"fontWeight": "500"},
                                    **{"data-utc": _fmt_dt(task.created_at)},
                                ),
                            ],
                            style={
                                "display": "flex",
                                "gap": "0.5rem",
                                "marginBottom": "0.25rem",
                            },
                        ),
                        _detail_row("Status", task.status),
                        html.Hr(className="my-2"),
                        html.H6(
                            "Matching Settings",
                            className="mb-2",
                            style={"fontWeight": "600"},
                        ),
                        _detail_row(
                            "Covariates",
                            ", ".join(task.covariates or []) or "ΓÇö",
                        ),
                        _detail_row(
                            "Exact match variables",
                            ", ".join(config.get("exact_match_vars", [])) or "ΓÇö",
                        ),
                        _detail_row(
                            "Resolution",
                            (
                                "250 m"
                                if config.get("resolution_m") == 250
                                else "1 km"
                                if config.get("resolution_m") in (1000, None)
                                else f"{config.get('resolution_m')} m"
                            ),
                        ),
                        _detail_row(
                            "Max treatment pixels",
                            config.get("max_treatment_pixels", "ΓÇö"),
                        ),
                        _detail_row(
                            "Control multiplier",
                            config.get("control_multiplier", "ΓÇö"),
                        ),
                        _detail_row(
                            "Min site area (ha)",
                            config.get("min_site_area_ha", "ΓÇö"),
                        ),
                        _detail_row(
                            "Min GLM treatment pixels",
                            config.get("min_glm_treatment_pixels", "ΓÇö"),
                        ),
                        _detail_row("Caliper width (SD)", caliper_display),
                        _detail_row("Max controls per treatment", max_ctrl_display),
                        _detail_row(
                            "Min control distance (km)",
                            config.get("min_control_distance_km", "ΓÇö"),
                        ),
                        _detail_row(
                            "Separation fallback",
                            "Mahalanobis"
                            if config.get("separation_fallback_mahalanobis")
                            else "Disabled",
                        ),
                        _detail_row(
                            "Group by exact-match regions",
                            "Enabled"
                            if config.get("group_by_exact_matches")
                            else "Disabled",
                        ),
                        _detail_row(
                            "Matching method",
                            "Nearest neighbour (MatchIt)"
                            if config.get("matching_method") == "nearest"
                            else "Optimal (optmatch)",
                        ),
                        _detail_row(
                            "Replicates",
                            config.get("n_replicates", 1),
                        ),
                        _detail_row(
                            "Random seed",
                            config.get("random_seed", "Not set"),
                        ),
                        _detail_row("Matching memory", mem_display),
                        _detail_row(
                            "Batch job queue",
                            config.get("matching_job_queue", "ΓÇö"),
                        ),
                        html.Hr(className="my-2"),
                        html.H6(
                            "Execution Details",
                            className="mb-2",
                            style={"fontWeight": "600"},
                        ),
                        _detail_row(
                            "API execution ID",
                            api_exec_id or "ΓÇö",
                        ),
                        _detail_row(
                            "Batch job names",
                            ", ".join(batch_job_names) if batch_job_names else "ΓÇö",
                        ),
                        _detail_row(
                            "S3 output path",
                            task.results_s3_uri or "ΓÇö",
                        ),
                        _detail_row(
                            "Internal task ID",
                            str(task.id),
                        ),
                        # Webapp git SHA with link to GitHub commit
                        html.Div(
                            [
                                html.Span(
                                    "Webapp version",
                                    className="text-muted",
                                    style={"minWidth": "200px"},
                                ),
                                (
                                    html.A(
                                        config.get("code_git_sha", "ΓÇö")[:7],
                                        href=(
                                            "https://github.com/ConservationInternational"
                                            "/avoided-emissions-web/commit/"
                                            f"{config.get('code_git_sha', '')}"
                                        ),
                                        target="_blank",
                                        rel="noopener noreferrer",
                                        style={
                                            "fontWeight": "500",
                                            "fontFamily": "monospace",
                                        },
                                    )
                                    if config.get("code_git_sha")
                                    else html.Span("ΓÇö", style={"fontWeight": "500"})
                                ),
                            ],
                            style={
                                "display": "flex",
                                "gap": "0.5rem",
                                "marginBottom": "0.25rem",
                            },
                        ),
                        # R analysis git SHA with link to GitHub commit
                        html.Div(
                            [
                                html.Span(
                                    "R analysis version",
                                    className="text-muted",
                                    style={"minWidth": "200px"},
                                ),
                                (
                                    html.A(
                                        (task.extra_metadata or {}).get(
                                            "r_analysis_git_sha", "ΓÇö"
                                        )[:7],
                                        href=(
                                            "https://github.com/ConservationInternational"
                                            "/avoided-emissions-web/commit/"
                                            f"{(task.extra_metadata or {}).get('r_analysis_git_sha', '')}"
                                        ),
                                        target="_blank",
                                        rel="noopener noreferrer",
                                        style={
                                            "fontWeight": "500",
                                            "fontFamily": "monospace",
                                        },
                                    )
                                    if (task.extra_metadata or {}).get(
                                        "r_analysis_git_sha"
                                    )
                                    else html.Span("ΓÇö", style={"fontWeight": "500"})
                                ),
                            ],
                            style={
                                "display": "flex",
                                "gap": "0.5rem",
                                "marginBottom": "0.25rem",
                            },
                        ),
                    ]
                ),
            ],
            className="mb-3",
        )
    )

    if task.error_message:
        cards.append(dbc.Alert(f"Error: {task.error_message}", color="danger"))

    # Summary stats if results exist
    if totals:
        total_emissions = sum(
            t.extrapolated_emissions_avoided_mgco2e or 0 for t in totals
        )
        total_forest = sum(t.extrapolated_forest_loss_avoided_ha or 0 for t in totals)
        total_area = sum(t.area_ha or 0 for t in totals)

        cards.append(
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardBody(
                                    [
                                        html.H4(
                                            f"{total_emissions:,.0f}",
                                            className="text-success",
                                        ),
                                        html.P(
                                            "Total Avoided Emissions (MgCOΓéée)",
                                            className="text-muted mb-0",
                                        ),
                                    ]
                                ),
                            ],
                            color="success",
                            outline=True,
                        )
                    ),
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardBody(
                                    [
                                        html.H4(
                                            f"{total_forest:,.0f}",
                                            className="text-info",
                                        ),
                                        html.P(
                                            "Forest Loss Avoided (ha)",
                                            className="text-muted mb-0",
                                        ),
                                    ]
                                ),
                            ],
                            color="info",
                            outline=True,
                        )
                    ),
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardBody(
                                    [
                                        html.H4(f"{total_area:,.0f}"),
                                        html.P(
                                            "Total Site Area (ha)",
                                            className="text-muted mb-0",
                                        ),
                                    ]
                                ),
                            ],
                            color="secondary",
                            outline=True,
                        )
                    ),
                ],
                className="mb-3",
            )
        )

    # --- Matching statistics boxes -------------------------------------------
    if task.status == "succeeded":
        import json

        mq_raw = download_results_csv(
            task.id, "match_quality_summary", results_s3_uri=task.results_s3_uri
        )
        if mq_raw:
            try:
                mq_summary = json.loads(mq_raw)
            except (json.JSONDecodeError, ValueError):
                mq_summary = None
            if mq_summary:
                stats = mq_summary.get("summary_stats", {})
                if not isinstance(stats, dict):
                    stats = {}
                agg = stats.get("__all__", {})
                n_treat = agg.get("n_treatment", 0)
                n_ctrl = agg.get("n_control", 0)
                n_treat_total = agg.get("n_treatment_total")
                n_ctrl_sampled = agg.get("n_control_sampled")
                n_ctrl_pool = agg.get("n_control_pool")

                # Treatment card body
                treat_body = [
                    html.H4(f"{n_treat:,}", className="text-primary"),
                    html.P("Treatment Pixels Matched", className="text-muted mb-0"),
                ]
                if n_treat_total and n_treat_total > n_treat:
                    pct_t = n_treat / n_treat_total * 100
                    treat_body.append(
                        html.Small(
                            f"of {n_treat_total:,} in site ({pct_t:.1f}% sampled)",
                            className="text-muted",
                        )
                    )

                # Control card body
                ctrl_body = [
                    html.H4(f"{n_ctrl:,}", className="text-primary"),
                    html.P("Control Pixels Matched", className="text-muted mb-0"),
                ]
                if n_ctrl_sampled and n_ctrl_sampled > n_ctrl:
                    sub = f"of {n_ctrl_sampled:,} sampled"
                    if n_ctrl_pool and n_ctrl_pool > n_ctrl_sampled:
                        pct_c = n_ctrl_sampled / n_ctrl_pool * 100
                        sub += f" ({pct_c:.1f}% of {n_ctrl_pool:,} candidates)"
                    ctrl_body.append(html.Small(sub, className="text-muted"))
                elif n_ctrl_pool and n_ctrl_pool > n_ctrl:
                    ctrl_body.append(
                        html.Small(
                            f"of {n_ctrl_pool:,} candidates",
                            className="text-muted",
                        )
                    )

                cards.append(
                    dbc.Row(
                        [
                            dbc.Col(
                                dbc.Card(
                                    dbc.CardBody(treat_body),
                                    color="primary",
                                    outline=True,
                                )
                            ),
                            dbc.Col(
                                dbc.Card(
                                    dbc.CardBody(ctrl_body),
                                    color="primary",
                                    outline=True,
                                )
                            ),
                        ],
                        className="mb-3",
                    )
                )

    # --- Quality issues (compact table instead of long bullet list) --------
    if quality_warnings:
        per_site = [w for w in quality_warnings if w["scope"] != "aggregate"]
        if per_site:
            # Build lookups from totals
            name_lookup = {}
            totals_lookup = {}  # sid ΓåÆ {n_matched, area_ha, ...}
            if totals:
                for t in totals:
                    name_lookup[str(t.site_id)] = t.site_name or str(t.site_id)
                    totals_lookup[str(t.site_id)] = {
                        "n_matched": t.n_sample_pixels or 0,
                        "n_treatment": t.n_treatment_pixels,
                        "area_ha": t.area_ha,
                        "sampled_fraction": t.sampled_fraction,
                    }

            # Group by site, preserving insertion order
            site_map = {}
            for w in per_site:
                site_map.setdefault(w["scope"], []).append(w)

            # Build AG Grid rows
            grid_rows = []
            for sid, ws in site_map.items():
                sname = name_lookup.get(sid, sid)
                if sname and str(sname) != str(sid):
                    site_label = f"{sname} ({sid})"
                else:
                    site_label = str(sid)
                has_danger = any(w["level"] == "danger" for w in ws)
                issues_text = "; ".join(w["message"] for w in ws)
                tinfo = totals_lookup.get(sid, {})
                n_matched = tinfo.get("n_matched", 0) or 0
                area_ha = tinfo.get("area_ha")
                sampled_frac = tinfo.get("sampled_fraction") or 1.0

                # Estimate % of treatment pixels matched
                n_treatment = tinfo.get("n_treatment")
                pct_matched = None
                if n_treatment and n_treatment > 0:
                    eligible = n_treatment * sampled_frac
                    if eligible > 0:
                        pct_matched = min(n_matched / eligible * 100, 100)
                elif area_ha and area_ha > 0:
                    # Fallback for old results without n_treatment_pixels
                    approx_pixels = area_ha / 86.0
                    eligible = approx_pixels * sampled_frac
                    if eligible > 0:
                        pct_matched = min(n_matched / eligible * 100, 100)

                grid_rows.append(
                    {
                        "severity": "Critical" if has_danger else "Caution",
                        "site": site_label,
                        "area_ha": round(area_ha, 1) if area_ha else None,
                        "matched_pixels": n_matched,
                        "pct_matched": round(pct_matched, 1) if pct_matched else None,
                        "issues": issues_text,
                    }
                )

            # Sort: critical first, then alphabetical
            grid_rows.sort(
                key=lambda r: (
                    0 if r["severity"] == "Critical" else 1,
                    r["site"].lower(),
                )
            )

            n_danger = sum(1 for r in grid_rows if r["severity"] == "Critical")
            n_caution = len(grid_rows) - n_danger
            summary_parts = []
            if n_danger:
                summary_parts.append(f"{n_danger} site(s) with critical issues")
            if n_caution:
                summary_parts.append(f"{n_caution} site(s) with quality concerns")
            subtitle = ", ".join(summary_parts)

            quality_cols = [
                {
                    "headerName": "",
                    "field": "severity",
                    "width": 50,
                    "maxWidth": 50,
                    "cellRenderer": "SeverityIcon",
                    "sortable": False,
                    "filter": False,
                },
                {
                    "headerName": "Site",
                    "field": "site",
                    "flex": 1.2,
                    "minWidth": 180,
                    "filter": True,
                },
                {
                    "headerName": "Area (ha)",
                    "field": "area_ha",
                    "flex": 0.6,
                    "minWidth": 95,
                    "type": "numericColumn",
                    "valueFormatter": {
                        "function": (
                            "params.value != null"
                            " ? d3.format(',.0f')(params.value)"
                            " : '\u2014'"
                        )
                    },
                },
                {
                    "headerName": "Matched Px",
                    "field": "matched_pixels",
                    "flex": 0.6,
                    "minWidth": 95,
                    "type": "numericColumn",
                    "valueFormatter": {
                        "function": (
                            "params.value != null"
                            " ? d3.format(',')(params.value)"
                            " : '\u2014'"
                        )
                    },
                },
                {
                    "headerName": "% Matched",
                    "field": "pct_matched",
                    "flex": 0.55,
                    "minWidth": 90,
                    "type": "numericColumn",
                    "valueFormatter": {
                        "function": (
                            "params.value != null"
                            " ? d3.format('.1f')(params.value) + '%'"
                            " : '\u2014'"
                        )
                    },
                },
                {
                    "headerName": "Issues",
                    "field": "issues",
                    "flex": 2.5,
                    "minWidth": 300,
                    "wrapText": True,
                    "autoHeight": True,
                    "cellStyle": {"whiteSpace": "normal", "lineHeight": "1.4"},
                },
            ]

            severity_styles = [
                {
                    "condition": "params.data.severity === 'Critical'",
                    "style": {"backgroundColor": "#fce4e4"},
                },
                {
                    "condition": "params.data.severity === 'Caution'",
                    "style": {"backgroundColor": "#fff8e1"},
                },
            ]

            # Compute table height: header + rows capped at 10 visible
            row_height = 60
            visible_rows = min(len(grid_rows), 10)
            table_h = 42 + visible_rows * row_height

            cards.append(
                dbc.Card(
                    [
                        dbc.CardHeader(
                            [
                                html.I(
                                    className=(
                                        "bi bi-exclamation-triangle-fill "
                                        "text-danger me-2"
                                    )
                                ),
                                html.Strong("Match Quality Issues"),
                                html.Span(
                                    f" ΓÇö {subtitle}",
                                    className="text-muted ms-1",
                                ),
                            ]
                        ),
                        dbc.CardBody(
                            _make_ag_grid(
                                "quality-issues-grid",
                                quality_cols,
                                row_data=grid_rows,
                                height=f"{table_h}px",
                                style_conditions=severity_styles,
                                grid_options_extra={
                                    "domLayout": (
                                        "autoHeight"
                                        if len(grid_rows) <= 10
                                        else "normal"
                                    ),
                                },
                            ),
                            className="p-2",
                        ),
                    ],
                    className="mb-3 border-danger",
                )
            )

    # --- Failed sites alert --------------------------------------------------
    meta = task.extra_metadata or {}
    failed_sites = _normalize_metadata_list(meta.get("failed_sites", []))
    if failed_sites:
        failed_items = []
        for fs in failed_sites:
            site_id = fs.get("site_id") or fs.get("id_numeric", "?")
            site_name = fs.get("site_name", "")
            if site_name and site_name != site_id:
                label = f"{site_name} ({site_id})"
            else:
                label = str(site_id)
            error = fs.get("error", "Unknown error")
            item_children = [html.Strong(f"{label}: "), error]
            # Show separation diagnostics when available
            sep_warnings = fs.get("separation_warnings")
            if sep_warnings and isinstance(sep_warnings, dict):
                sep_details = []
                for group_name, details in sep_warnings.items():
                    if isinstance(details, list):
                        for d in details:
                            sep_details.append(
                                html.Li(
                                    f"Group {group_name}: {d}",
                                    style={"fontSize": "0.85em"},
                                )
                            )
                if sep_details:
                    item_children.append(html.Ul(sep_details, className="mb-0 mt-1"))
            failed_items.append(html.Li(item_children))
        cards.append(
            dbc.Alert(
                [
                    html.H5(
                        f"{len(failed_sites)} site(s) failed matching",
                        className="alert-heading",
                    ),
                    html.P(
                        "The following sites could not be matched and are "
                        "excluded from the results:",
                        className="mb-2",
                    ),
                    html.Ul(failed_items, className="mb-0"),
                ],
                color="warning",
                className="mb-3",
            )
        )

    # --- Group-level matching diagnostics ------------------------------------
    group_diags = _normalize_metadata_list(meta.get("group_diagnostics", []))
    if group_diags:
        cards.append(_build_group_diagnostics_card(group_diags))

    # --- Subsampled sites info -----------------------------------------------
    subsampled_sites = _normalize_metadata_list(meta.get("subsampled_sites", []))
    if subsampled_sites:
        site_name_map = {s.site_id: s.site_name for s in (sites or []) if s.site_name}
        sub_items = []
        for ss in subsampled_sites:
            site_id = ss.get("site_id")
            if site_id is None or site_id == "":
                site_id = ss.get("id_numeric", "?")
            site_name = ss.get("site_name") or site_name_map.get(str(site_id), "")
            if site_name and str(site_name) != str(site_id):
                label = f"{site_name} ({site_id})"
            else:
                label = str(site_id)
            pct = ss.get("sampled_percent", 100)
            frac = ss.get("sampled_fraction", 1.0)
            sub_items.append(
                html.Li(f"{label}: {pct:.1f}% sampled (fraction {frac:.4f})")
            )
        cards.append(
            dbc.Alert(
                [
                    html.H5(
                        f"{len(subsampled_sites)} site(s) were subsampled",
                        className="alert-heading",
                    ),
                    html.P(
                        "Large sites were subsampled for matching. Their "
                        "results are scaled up from the sampled fraction:",
                        className="mb-2",
                    ),
                    html.Ul(sub_items, className="mb-0"),
                ],
                color="info",
                className="mb-3",
            )
        )

    return html.Div(cards)


def _build_results_content(
    results,
    totals,
    sites=None,
    quality_warnings=None,
    agg_yearly=None,
    is_large=False,
    n_sites=0,
):
    """Build the results section with sites table, AG Grid tables, and downloads.

    For tasks with more than ``LARGE_TASK_THRESHOLD`` sites (``is_large=True``)
    the function skips the per-site-year grid (which would contain tens of
    thousands of rows) and instead shows an aggregated yearly summary built
    from the SQL-pre-aggregated ``agg_yearly`` list.  The per-site totals
    grid is always shown because it has one row per site and AG Grid handles
    that with virtualisation.
    """
    content = []

    # Show per-site quality issues table (detail, not a duplicate banner)
    if quality_warnings:
        content.append(_build_site_quality_table(quality_warnings, totals))

    if is_large:
        content.append(
            dbc.Alert(
                [
                    html.I(className="bi bi-info-circle me-2"),
                    html.Strong(f"Large task: {n_sites:,} sites. "),
                    "Per-site yearly rows are not shown to keep the page responsive. "
                    "Use the Raw Results tab to download site-level CSVs.",
                ],
                color="info",
                className="mb-3",
            )
        )
    elif sites:
        # Sites table — only shown for small tasks where rows are loaded
        site_rows = [
            {
                "site_id": s.site_id,
                "site_name": s.site_name or "-",
                "start_date": str(s.start_date)[:10] if s.start_date else "-",
                "end_date": str(s.end_date)[:10] if s.end_date else "Ongoing",
                "area_ha": s.area_ha,
            }
            for s in sites
        ]

        site_cols = [
            {"headerName": "Site ID", "field": "site_id", "flex": 1, "minWidth": 120},
            {"headerName": "Name", "field": "site_name", "flex": 1.5, "minWidth": 150},
            {"headerName": "Start", "field": "start_date", "flex": 1, "minWidth": 110},
            {"headerName": "End", "field": "end_date", "flex": 1, "minWidth": 110},
            {
                "headerName": "Area (ha)",
                "field": "area_ha",
                "flex": 1,
                "minWidth": 100,
                "type": "numericColumn",
                "valueFormatter": {"function": "d3.format(',.0f')(params.value)"},
            },
        ]

        content.append(
            dbc.Card(
                [
                    dbc.CardHeader("Sites"),
                    dbc.CardBody(
                        _make_ag_grid(
                            "results-sites-table",
                            site_cols,
                            row_data=site_rows,
                            height="300px",
                        ),
                    ),
                ],
                className="mb-3",
            )
        )

    if not totals:
        content.append(html.P("Results not yet available.", className="text-muted"))
        return html.Div(content)

    # --- Totals by site (always shown — one row per site) -------------------
    totals_rows = [
        {
            "site_id": t.site_id,
            "site_name": t.site_name or "-",
            "emissions_avoided_mgco2e": t.extrapolated_emissions_avoided_mgco2e or 0,
            "forest_loss_avoided_ha": t.extrapolated_forest_loss_avoided_ha or 0,
            "area_ha": t.area_ha or 0,
            "period": (f"{t.first_year}-{t.last_year}" if t.first_year else "-"),
            "sampled_percent": ((t.sampled_fraction or 1.0) * 100),
        }
        for t in totals
    ]

    content.extend(
        [
            html.H5("Totals by Site"),
            _make_ag_grid(
                "results-totals-table",
                RESULTS_TOTAL_COLUMNS,
                row_data=totals_rows,
                height="350px",
                grid_options_extra={
                    "rowSelection": {
                        "mode": "singleRow",
                        "enableClickSelection": True,
                    },
                    "getRowId": {"function": "params.data.site_id"},
                },
            ),
        ]
    )

    # --- Yearly results table -----------------------------------------------
    # Large tasks: use the SQL aggregate (one row per year, summed across sites)
    # Small tasks: use the per-site-year ORM rows (site_id column included)
    if is_large and agg_yearly:
        agg_yearly_rows = [
            {
                "year": r["year"],
                "treatment_defor_ha": r.get("treatment_defor_ha") or 0,
                "control_defor_ha": r.get("control_defor_ha") or 0,
                "emissions_avoided_mgco2e": r.get("emissions_avoided_mgco2e") or 0,
                "forest_loss_avoided_ha": r.get("forest_loss_avoided_ha") or 0,
                "n_matched_pixels": None,
            }
            for r in agg_yearly
        ]
        # Aggregate columns drop the site_id column and add an "All sites" label
        agg_year_cols = [
            {"headerName": "Year", "field": "year", "flex": 1, "minWidth": 90},
            {
                "headerName": "Treatment Defor. (ha)",
                "field": "treatment_defor_ha",
                "flex": 1.2,
                "minWidth": 140,
                "type": "numericColumn",
                "valueFormatter": {"function": "d3.format(',.1f')(params.value)"},
            },
            {
                "headerName": "Control Defor. (ha)",
                "field": "control_defor_ha",
                "flex": 1.2,
                "minWidth": 140,
                "type": "numericColumn",
                "valueFormatter": {"function": "d3.format(',.1f')(params.value)"},
            },
            {
                "headerName": "Emissions Avoided (MgCO₂e)",
                "field": "emissions_avoided_mgco2e",
                "flex": 1.5,
                "minWidth": 180,
                "type": "numericColumn",
                "valueFormatter": {"function": "d3.format(',.1f')(params.value)"},
            },
            {
                "headerName": "Forest Loss Avoided (ha)",
                "field": "forest_loss_avoided_ha",
                "flex": 1.3,
                "minWidth": 160,
                "type": "numericColumn",
                "valueFormatter": {"function": "d3.format(',.1f')(params.value)"},
            },
        ]
        content.extend(
            [
                html.H5(
                    f"Yearly Results — All {n_sites:,} Sites Combined",
                    className="mt-4",
                ),
                html.P(
                    "Values are summed across all sites for each year.",
                    className="text-muted small mb-2",
                ),
                _make_ag_grid(
                    "results-yearly-table",
                    agg_year_cols,
                    row_data=agg_yearly_rows,
                    height="400px",
                ),
            ]
        )
    elif results:
        yearly_rows = [
            {
                "site_id": r.site_id,
                "year": r.year,
                "treatment_defor_ha": r.extrapolated_treatment_defor_ha or 0,
                "control_defor_ha": r.extrapolated_control_defor_ha or 0,
                "emissions_avoided_mgco2e": r.extrapolated_emissions_avoided_mgco2e
                or 0,
                "forest_loss_avoided_ha": r.extrapolated_forest_loss_avoided_ha or 0,
                "n_matched_pixels": r.n_sample_pixels or 0,
            }
            for r in results
        ]
        content.extend(
            [
                html.H5("Results by Year", className="mt-4"),
                _make_ag_grid(
                    "results-yearly-table",
                    RESULTS_YEARLY_COLUMNS,
                    row_data=yearly_rows,
                    height="400px",
                ),
            ]
        )

    content.extend(
        [
            dbc.ButtonGroup(
                [
                    dbc.Button(
                        "Download CSV (by year)",
                        id="download-by-year",
                        color="secondary",
                        size="sm",
                    ),
                    dbc.Button(
                        "Download CSV (totals)",
                        id="download-totals",
                        color="secondary",
                        size="sm",
                    ),
                ],
                className="mt-3",
            ),
            dcc.Download(id="download-results"),
        ]
    )

    return html.Div(content)


_FILE_DESCRIPTIONS = {
    "formula.json": "Propensity score model formula used for matching",
    "grid_metadata.json": "Raster grid dimensions and affine transform",
    "results_by_site_total.csv": "Avoided emissions totals per site",
    "results_by_site_year.csv": "Avoided emissions per site per year",
    "results_covariate_balance.csv": "Covariate balance SMD statistics (Love plot)",
    "results_failed_sites.csv": "Sites that failed matching with reasons",
    "results_match_quality_summary.json": "Match quality diagnostics per site",
    "results_pixel_covariates.csv": "Covariate values for each matched pixel",
    "results_pixel_locations.csv": "Lon/lat coordinates for matched pixels (map)",
    "results_pixel_year_emissions.csv": "Per-pixel per-year forest change and emissions",
    "results_propensity_scores.csv": "Propensity scores for matched pixels (QQ plot)",
    "results_sampling_by_site.csv": "Subsampling fraction per site",
    "results_summary.json": "Global summary of avoided emissions and site counts",
    "site_id_key.csv": "Mapping between numeric and string site IDs",
    "sites_processed.parquet": "Processed site geometries and metadata",
    "treatment_cell_key.parquet": "Raster cells belonging to each treatment site",
    "treatments_and_controls.parquet": "Extracted covariates for all treatment/control pixels",
}


def _build_raw_results(task):
    """Build a table of downloadable S3 files for the Raw Results tab."""
    if task.status != "succeeded":
        return html.P(
            "Raw results are only available after the task completes.",
            className="text-muted",
        )
    try:
        files = list_task_s3_files(task.id, task.results_s3_uri)
    except Exception:
        logger.exception("Failed to list S3 files for task %s", task.id)
        return html.P(
            "Unable to retrieve file listing from S3.",
            className="text-danger",
        )
    if not files:
        return html.P("No output files found on S3.", className="text-muted")

    def _fmt_size(n):
        for unit in ("B", "KB", "MB", "GB"):
            if abs(n) < 1024:
                return f"{n:.1f} {unit}"
            n /= 1024
        return f"{n:.1f} TB"

    rows = []
    for f in sorted(files, key=lambda x: x["filename"]):
        desc = _FILE_DESCRIPTIONS.get(f["filename"], "")
        # Match files inside subdirectories (e.g. matches/m_1.rds)
        if not desc and "/" in f["filename"]:
            prefix = f["filename"].split("/")[0]
            if prefix == "matches":
                desc = "Serialized R match object for one site"
        rows.append(
            html.Tr(
                [
                    html.Td(f["filename"]),
                    html.Td(desc, className="text-muted"),
                    html.Td(_fmt_size(f["size_bytes"])),
                    html.Td(
                        html.A(
                            "Download",
                            href=f["download_url"],
                            target="_blank",
                            rel="noopener noreferrer",
                            className="btn btn-sm btn-outline-primary",
                        )
                    ),
                ]
            )
        )
    table = dbc.Table(
        [
            html.Thead(
                html.Tr(
                    [
                        html.Th("File"),
                        html.Th("Description"),
                        html.Th("Size"),
                        html.Th(""),
                    ]
                )
            ),
            html.Tbody(rows),
        ],
        bordered=True,
        hover=True,
        size="sm",
        className="mt-2",
    )
    return html.Div(
        [
            html.P(
                f"{len(files)} files available. Download links expire after 1 hour.",
                className="text-muted mb-2",
            ),
            table,
        ]
    )


def _add_ci_band(fig, x, y_lower, y_upper, color, name):
    """Add a shaded confidence-interval band to a Plotly figure."""
    fig.add_trace(
        go.Scatter(
            x=x,
            y=y_upper,
            mode="lines",
            line=dict(width=0),
            showlegend=False,
            hoverinfo="skip",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=x,
            y=y_lower,
            mode="lines",
            line=dict(width=0),
            fill="tonexty",
            fillcolor=color,
            name=name,
            showlegend=True,
            hoverinfo="skip",
        )
    )


def _build_plots(
    results,
    totals,
    sites=None,
    task=None,
    quality_warnings=None,
    agg_yearly=None,
    is_large=False,
):
    """Build interactive plots for task results.

    Includes aggregate deforestation comparison (project sites vs matched
    controls), existing avoided-emissions/forest-loss bar charts, and a
    site-level drill-down section with intervention date markers.

    For tasks with ``is_large=True`` the ``results`` argument is ``None``.
    In this case the SQL-pre-aggregated ``agg_yearly`` list (one dict per
    year, values already summed across all sites) is used for aggregate plots
    so the Dash process never holds tens of thousands of per-site-year rows
    in memory.

    Parameters
    ----------
    task : AnalysisTask, optional
        The parent task object, used to read ``extra_metadata`` for
        failed-site and subsampled-site annotations.
    quality_warnings : list[dict], optional
        Output of :func:`_assess_match_quality`.
    agg_yearly : list[dict], optional
        Pre-aggregated yearly data (from ``_get_task_results_aggregated``).
    is_large : bool
        When True, ``results`` is None; use ``agg_yearly`` instead.
    """
    # Normalise: for large tasks agg_yearly holds pre-summed year rows
    if is_large or results is None:
        if not agg_yearly:
            return html.P("No results to plot.", className="text-muted")
        df = pd.DataFrame(
            [
                {
                    "year": r["year"],
                    "emissions_avoided_mgco2e": r.get("emissions_avoided_mgco2e") or 0,
                    "forest_loss_avoided_ha": r.get("forest_loss_avoided_ha") or 0,
                    "treatment_defor_ha": r.get("treatment_defor_ha") or 0,
                    "control_defor_ha": r.get("control_defor_ha") or 0,
                    "treatment_emissions_mgco2e": r.get("treatment_emissions_mgco2e")
                    or 0,
                    "control_emissions_mgco2e": r.get("control_emissions_mgco2e") or 0,
                    "treatment_defor_ha_ci_lower": r.get("treatment_defor_ha_ci_lower"),
                    "treatment_defor_ha_ci_upper": r.get("treatment_defor_ha_ci_upper"),
                    "control_defor_ha_ci_lower": r.get("control_defor_ha_ci_lower"),
                    "control_defor_ha_ci_upper": r.get("control_defor_ha_ci_upper"),
                    "forest_loss_avoided_ha_ci_lower": r.get(
                        "forest_loss_avoided_ha_ci_lower"
                    ),
                    "forest_loss_avoided_ha_ci_upper": r.get(
                        "forest_loss_avoided_ha_ci_upper"
                    ),
                    "emissions_avoided_mgco2e_ci_lower": r.get(
                        "emissions_avoided_mgco2e_ci_lower"
                    ),
                    "emissions_avoided_mgco2e_ci_upper": r.get(
                        "emissions_avoided_mgco2e_ci_upper"
                    ),
                    # No is_pre_intervention for aggregate path
                    "is_pre_intervention": False,
                }
                for r in agg_yearly
            ]
        )
        # n_sites for the plot title
        n_sites_display = agg_yearly[0].get("n_sites", 0) if agg_yearly else 0
    else:
        if not results:
            return html.P("No results to plot.", className="text-muted")
        df = pd.DataFrame(
            [
                {
                    "site_id": r.site_id,
                    "year": r.year,
                    "emissions_avoided_mgco2e": r.extrapolated_emissions_avoided_mgco2e
                    or 0,
                    "forest_loss_avoided_ha": r.extrapolated_forest_loss_avoided_ha
                    or 0,
                    "treatment_defor_ha": r.extrapolated_treatment_defor_ha or 0,
                    "control_defor_ha": r.extrapolated_control_defor_ha or 0,
                    "treatment_emissions_mgco2e": r.extrapolated_treatment_emissions_mgco2e
                    or 0,
                    "control_emissions_mgco2e": r.extrapolated_control_emissions_mgco2e
                    or 0,
                    "is_pre_intervention": bool(r.is_pre_intervention),
                    "is_post_intervention": bool(
                        getattr(r, "is_post_intervention", False)
                    ),
                    "treatment_defor_ha_ci_lower": getattr(
                        r, "extrapolated_treatment_defor_ha_ci_lower", None
                    ),
                    "treatment_defor_ha_ci_upper": getattr(
                        r, "extrapolated_treatment_defor_ha_ci_upper", None
                    ),
                    "control_defor_ha_ci_lower": getattr(
                        r, "extrapolated_control_defor_ha_ci_lower", None
                    ),
                    "control_defor_ha_ci_upper": getattr(
                        r, "extrapolated_control_defor_ha_ci_upper", None
                    ),
                    "forest_loss_avoided_ha_ci_lower": getattr(
                        r, "extrapolated_forest_loss_avoided_ha_ci_lower", None
                    ),
                    "forest_loss_avoided_ha_ci_upper": getattr(
                        r, "extrapolated_forest_loss_avoided_ha_ci_upper", None
                    ),
                    "emissions_avoided_mgco2e_ci_lower": getattr(
                        r, "extrapolated_emissions_avoided_mgco2e_ci_lower", None
                    ),
                    "emissions_avoided_mgco2e_ci_upper": getattr(
                        r, "extrapolated_emissions_avoided_mgco2e_ci_upper", None
                    ),
                }
                for r in results
            ]
        )
        n_sites_display = None  # will be computed below from df

    # Extract diagnostic metadata from the task (common to both paths)
    meta = (task.extra_metadata or {}) if task else {}
    failed_site_ids = {
        fs.get("site_id") or fs.get("id_numeric")
        for fs in _normalize_metadata_list(meta.get("failed_sites", []))
    }
    subsampled_map = {
        ss.get("site_id") or ss.get("id_numeric"): ss
        for ss in _normalize_metadata_list(meta.get("subsampled_sites", []))
    }

    has_ci = df["treatment_defor_ha_ci_lower"].notna().any()

    plots = []

    # --- Aggregate deforestation comparison (treatment vs control) ----------
    has_defor_data = (
        df["treatment_defor_ha"].sum() > 0 or df["control_defor_ha"].sum() > 0
    )
    if has_defor_data:
        agg_cols = {
            "treatment_defor_ha": ("treatment_defor_ha", "sum"),
            "control_defor_ha": ("control_defor_ha", "sum"),
        }
        if has_ci:
            agg_cols.update(
                {
                    "treatment_defor_ha_ci_lower": (
                        "treatment_defor_ha_ci_lower",
                        "sum",
                    ),
                    "treatment_defor_ha_ci_upper": (
                        "treatment_defor_ha_ci_upper",
                        "sum",
                    ),
                    "control_defor_ha_ci_lower": ("control_defor_ha_ci_lower", "sum"),
                    "control_defor_ha_ci_upper": ("control_defor_ha_ci_upper", "sum"),
                }
            )
        # For large tasks df is already grouped by year; for small tasks we
        # need to group (aggregate across sites).
        if "site_id" in df.columns:
            agg_df = (
                df.groupby("year").agg(**agg_cols).reset_index().sort_values("year")
            )
        else:
            agg_df = df[
                ["year", "treatment_defor_ha", "control_defor_ha"]
                + (
                    [
                        "treatment_defor_ha_ci_lower",
                        "treatment_defor_ha_ci_upper",
                        "control_defor_ha_ci_lower",
                        "control_defor_ha_ci_upper",
                    ]
                    if has_ci
                    else []
                )
            ].sort_values("year")
        fig_defor = go.Figure()
        fig_defor.add_trace(
            go.Scatter(
                x=agg_df["year"],
                y=agg_df["treatment_defor_ha"],
                mode="lines+markers",
                name="Project Sites",
                line=dict(color="#2ca02c", width=2),
                marker=dict(size=6),
            )
        )
        fig_defor.add_trace(
            go.Scatter(
                x=agg_df["year"],
                y=agg_df["control_defor_ha"],
                mode="lines+markers",
                name="Matched Controls",
                line=dict(color="#d62728", width=2),
                marker=dict(size=6),
            )
        )
        if has_ci:
            _add_ci_band(
                fig_defor,
                agg_df["year"],
                agg_df["treatment_defor_ha_ci_lower"],
                agg_df["treatment_defor_ha_ci_upper"],
                color="rgba(44,160,44,0.15)",
                name="Project Sites 95% CI",
            )
            _add_ci_band(
                fig_defor,
                agg_df["year"],
                agg_df["control_defor_ha_ci_lower"],
                agg_df["control_defor_ha_ci_upper"],
                color="rgba(214,39,40,0.15)",
                name="Matched Controls 95% CI",
            )
        if n_sites_display is None:
            n_sites_display = (
                len(df["site_id"].unique()) if "site_id" in df.columns else 0
            )
        title_suffix = f" ({n_sites_display} sites"
        if failed_site_ids:
            title_suffix += f", {len(failed_site_ids)} failed"
        title_suffix += ")"
        fig_defor.update_layout(
            title=(
                "Annual Deforestation: Project Sites vs Matched Controls" + title_suffix
            ),
            xaxis_title="Year",
            yaxis_title="Deforestation (ha)",
            legend=dict(yanchor="top", y=0.99, xanchor="left", x=1.02),
            hovermode="x unified",
        )
        # Only shade pre/post-intervention when all sites share the
        # same start/end year — mixed dates make a single band misleading.
        # For large tasks `sites` is empty so these bands are omitted.
        unique_start_years = set()
        unique_end_years = set()
        if sites:
            for s in sites:
                if s.start_date:
                    unique_start_years.add(s.start_date.year)
                if s.end_date:
                    unique_end_years.add(s.end_date.year)

        if len(unique_start_years) == 1 and "is_pre_intervention" in df.columns:
            pre_years = df.loc[df["is_pre_intervention"], "year"]
            if not pre_years.empty:
                fig_defor.add_vrect(
                    x0=pre_years.min() - 0.5,
                    x1=pre_years.max() + 0.5,
                    fillcolor="gray",
                    opacity=0.12,
                    line_width=0,
                    annotation_text="Pre-intervention",
                    annotation_position="top left",
                    annotation_font_color="gray",
                )
        if len(unique_end_years) == 1:
            (post_start,) = unique_end_years
            fig_defor.add_vrect(
                x0=post_start + 0.5,
                x1=agg_df["year"].max() + 0.5,
                fillcolor="gray",
                opacity=0.12,
                line_width=0,
                annotation_text="Post-intervention",
                annotation_position="top right",
                annotation_font_color="gray",
            )
        plots.append(dcc.Graph(figure=fig_defor))

        # --- Cumulative deforestation plot ---
        agg_df = agg_df.sort_values("year")
        agg_df["cum_treatment_defor_ha"] = agg_df["treatment_defor_ha"].cumsum()
        agg_df["cum_control_defor_ha"] = agg_df["control_defor_ha"].cumsum()
        if has_ci:
            agg_df["cum_treatment_defor_ha_ci_lower"] = agg_df[
                "treatment_defor_ha_ci_lower"
            ].cumsum()
            agg_df["cum_treatment_defor_ha_ci_upper"] = agg_df[
                "treatment_defor_ha_ci_upper"
            ].cumsum()
            agg_df["cum_control_defor_ha_ci_lower"] = agg_df[
                "control_defor_ha_ci_lower"
            ].cumsum()
            agg_df["cum_control_defor_ha_ci_upper"] = agg_df[
                "control_defor_ha_ci_upper"
            ].cumsum()

        fig_cum = go.Figure()
        fig_cum.add_trace(
            go.Scatter(
                x=agg_df["year"],
                y=agg_df["cum_treatment_defor_ha"],
                mode="lines+markers",
                name="Project Sites",
                line=dict(color="#2ca02c", width=2),
                marker=dict(size=6),
            )
        )
        fig_cum.add_trace(
            go.Scatter(
                x=agg_df["year"],
                y=agg_df["cum_control_defor_ha"],
                mode="lines+markers",
                name="Matched Controls",
                line=dict(color="#d62728", width=2),
                marker=dict(size=6),
            )
        )
        if has_ci:
            _add_ci_band(
                fig_cum,
                agg_df["year"],
                agg_df["cum_treatment_defor_ha_ci_lower"],
                agg_df["cum_treatment_defor_ha_ci_upper"],
                color="rgba(44,160,44,0.15)",
                name="Project Sites 95% CI",
            )
            _add_ci_band(
                fig_cum,
                agg_df["year"],
                agg_df["cum_control_defor_ha_ci_lower"],
                agg_df["cum_control_defor_ha_ci_upper"],
                color="rgba(214,39,40,0.15)",
                name="Matched Controls 95% CI",
            )
        fig_cum.update_layout(
            title=(
                "Cumulative Deforestation: Project Sites vs Matched Controls"
                + title_suffix
            ),
            xaxis_title="Year",
            yaxis_title="Cumulative Deforestation (ha)",
            legend=dict(yanchor="top", y=0.99, xanchor="left", x=1.02),
            hovermode="x unified",
        )
        if len(unique_start_years) == 1 and "is_pre_intervention" in df.columns:
            pre_years = df.loc[df["is_pre_intervention"], "year"]
            if not pre_years.empty:
                fig_cum.add_vrect(
                    x0=pre_years.min() - 0.5,
                    x1=pre_years.max() + 0.5,
                    fillcolor="gray",
                    opacity=0.12,
                    line_width=0,
                    annotation_text="Pre-intervention",
                    annotation_position="top left",
                    annotation_font_color="gray",
                )
        if len(unique_end_years) == 1:
            (post_start,) = unique_end_years
            fig_cum.add_vrect(
                x0=post_start + 0.5,
                x1=agg_df["year"].max() + 0.5,
                fillcolor="gray",
                opacity=0.12,
                line_width=0,
                annotation_text="Post-intervention",
                annotation_position="top right",
                annotation_font_color="gray",
            )
        plots.append(dcc.Graph(figure=fig_cum))

    # --- Emissions / forest-loss bar charts ---------------------------------
    # For small tasks: stacked by site (shows per-site contribution).
    # For large tasks: single aggregate bar (no `color="site_id"` with 1000
    # legend entries which is unreadable and extremely slow to render).
    if "site_id" in df.columns:
        # Small-task path: stacked bars coloured by site
        fig_emissions = px.bar(
            df,
            x="year",
            y="emissions_avoided_mgco2e",
            color="site_id",
            title="Avoided Emissions by Year",
            labels={
                "emissions_avoided_mgco2e": "Emissions Avoided (MgCO₂e)",
                "year": "Year",
                "site_id": "Site",
            },
        )
        fig_emissions.update_layout(barmode="stack")
        if has_ci:
            agg_em = df.groupby("year", as_index=False).agg(
                total=("emissions_avoided_mgco2e", "sum"),
                ci_lower=("emissions_avoided_mgco2e_ci_lower", "sum"),
                ci_upper=("emissions_avoided_mgco2e_ci_upper", "sum"),
            )
            fig_emissions.add_trace(
                go.Scatter(
                    x=agg_em["year"],
                    y=agg_em["total"],
                    mode="markers",
                    marker=dict(size=0, opacity=0),
                    error_y=dict(
                        type="data",
                        symmetric=False,
                        array=agg_em["ci_upper"] - agg_em["total"],
                        arrayminus=agg_em["total"] - agg_em["ci_lower"],
                        color="rgba(0,0,0,0.4)",
                        thickness=1.5,
                        width=4,
                    ),
                    name="95% CI",
                    showlegend=True,
                )
            )
        plots.append(dcc.Graph(figure=fig_emissions))

        fig_forest = px.bar(
            df,
            x="year",
            y="forest_loss_avoided_ha",
            color="site_id",
            title="Forest Loss Avoided by Year",
            labels={
                "forest_loss_avoided_ha": "Forest Loss Avoided (ha)",
                "year": "Year",
                "site_id": "Site",
            },
        )
        fig_forest.update_layout(barmode="stack")
        if has_ci:
            agg_fl = df.groupby("year", as_index=False).agg(
                total=("forest_loss_avoided_ha", "sum"),
                ci_lower=("forest_loss_avoided_ha_ci_lower", "sum"),
                ci_upper=("forest_loss_avoided_ha_ci_upper", "sum"),
            )
            fig_forest.add_trace(
                go.Scatter(
                    x=agg_fl["year"],
                    y=agg_fl["total"],
                    mode="markers",
                    marker=dict(size=0, opacity=0),
                    error_y=dict(
                        type="data",
                        symmetric=False,
                        array=agg_fl["ci_upper"] - agg_fl["total"],
                        arrayminus=agg_fl["total"] - agg_fl["ci_lower"],
                        color="rgba(0,0,0,0.4)",
                        thickness=1.5,
                        width=4,
                    ),
                    name="95% CI",
                    showlegend=True,
                )
            )
        plots.append(dcc.Graph(figure=fig_forest))
    else:
        # Large-task path: single aggregate bar per year (df already summed)
        fig_emissions = px.bar(
            df.sort_values("year"),
            x="year",
            y="emissions_avoided_mgco2e",
            title=f"Avoided Emissions by Year (all {n_sites_display:,} sites)",
            labels={
                "emissions_avoided_mgco2e": "Emissions Avoided (MgCO₂e)",
                "year": "Year",
            },
        )
        if has_ci:
            fig_emissions.add_trace(
                go.Scatter(
                    x=df["year"],
                    y=df["emissions_avoided_mgco2e"],
                    mode="markers",
                    marker=dict(size=0, opacity=0),
                    error_y=dict(
                        type="data",
                        symmetric=False,
                        array=(
                            df["emissions_avoided_mgco2e_ci_upper"]
                            - df["emissions_avoided_mgco2e"]
                        ),
                        arrayminus=(
                            df["emissions_avoided_mgco2e"]
                            - df["emissions_avoided_mgco2e_ci_lower"]
                        ),
                        color="rgba(0,0,0,0.4)",
                        thickness=1.5,
                        width=4,
                    ),
                    name="95% CI",
                    showlegend=True,
                )
            )
        plots.append(dcc.Graph(figure=fig_emissions))

        fig_forest = px.bar(
            df.sort_values("year"),
            x="year",
            y="forest_loss_avoided_ha",
            title=f"Forest Loss Avoided by Year (all {n_sites_display:,} sites)",
            labels={
                "forest_loss_avoided_ha": "Forest Loss Avoided (ha)",
                "year": "Year",
            },
        )
        if has_ci:
            fig_forest.add_trace(
                go.Scatter(
                    x=df["year"],
                    y=df["forest_loss_avoided_ha"],
                    mode="markers",
                    marker=dict(size=0, opacity=0),
                    error_y=dict(
                        type="data",
                        symmetric=False,
                        array=(
                            df["forest_loss_avoided_ha_ci_upper"]
                            - df["forest_loss_avoided_ha"]
                        ),
                        arrayminus=(
                            df["forest_loss_avoided_ha"]
                            - df["forest_loss_avoided_ha_ci_lower"]
                        ),
                        color="rgba(0,0,0,0.4)",
                        thickness=1.5,
                        width=4,
                    ),
                    name="95% CI",
                    showlegend=True,
                )
            )
        plots.append(dcc.Graph(figure=fig_forest))

    # --- Per-site totals bar chart ------------------------------------------
    # Only shown for small tasks (large tasks would render 1000 bars which is
    # unreadable; the sortable AG Grid in the Results tab covers that need).
    if totals and not is_large:
        df_totals = pd.DataFrame(
            [
                {
                    "site_id": t.site_id,
                    "site_name": t.site_name or t.site_id,
                    "emissions_avoided_mgco2e": t.extrapolated_emissions_avoided_mgco2e
                    or 0,
                    "forest_loss_avoided_ha": t.extrapolated_forest_loss_avoided_ha
                    or 0,
                }
                for t in totals
            ]
        )

        fig_site_totals = px.bar(
            df_totals,
            x="site_name",
            y="emissions_avoided_mgco2e",
            title="Total Avoided Emissions by Site",
            labels={
                "emissions_avoided_mgco2e": "Emissions Avoided (MgCO₂e)",
                "site_name": "Site",
            },
        )
        plots.append(dcc.Graph(figure=fig_site_totals))

    # --- Site-level drill-down section -------------------------------------
    if has_defor_data:
        # Build site options for the dropdown.
        # For large tasks we read IDs/names from `totals` (always loaded).
        # For small tasks we read from `df["site_id"]` as before.
        totals_name_map = {}
        if totals:
            for t in totals:
                if t.site_name:
                    totals_name_map[str(t.site_id)] = t.site_name

        if is_large or "site_id" not in df.columns:
            # Populate dropdown from totals; no results pre-loaded in store
            site_ids = sorted(str(t.site_id) for t in totals) if totals else []
            site_info_map = {
                str(t.site_id): {
                    "site_name": t.site_name or str(t.site_id),
                    "start_date": None,
                    "end_date": None,
                }
                for t in (totals or [])
            }
            # Store just the metadata; drill-down callback will fetch data
            # from DB on demand.
            store_data = {
                "large_task": True,
                "sites": site_info_map,
                "subsampled": {sid: sub for sid, sub in subsampled_map.items()},
            }
        else:
            site_ids = sorted(df["site_id"].unique())
            site_info_map = {}
            if sites:
                for s in sites:
                    sid = str(s.site_id)
                    name = s.site_name if s.site_name and s.site_name != sid else None
                    site_info_map[sid] = {
                        "site_name": name or totals_name_map.get(sid) or sid,
                        "start_date": (
                            str(s.start_date)[:10] if s.start_date else None
                        ),
                        "end_date": str(s.end_date)[:10] if s.end_date else None,
                    }
            store_data = {
                "large_task": False,
                "results": df.to_dict("records"),
                "sites": site_info_map,
                "subsampled": {sid: sub for sid, sub in subsampled_map.items()},
            }

        # Annotate dropdown labels for subsampled sites
        site_options = []
        for sid in site_ids:
            sname = (
                site_info_map.get(str(sid), {}).get("site_name")
                or totals_name_map.get(str(sid))
                or str(sid)
            )
            if sname and str(sname) != str(sid):
                label = f"{sname} ({sid})"
            else:
                label = str(sid)
            if sid in subsampled_map:
                pct = subsampled_map[sid].get("sampled_percent", 100)
                label += f" (subsampled {pct:.0f}%)"
            site_options.append({"label": label, "value": sid})
        site_options_by_name = sorted(site_options, key=lambda o: o["label"].lower())

        plots.append(html.Hr(className="my-4"))
        plots.append(html.H5("Site-Level Deforestation Detail", className="mt-3"))
        if is_large:
            plots.append(
                html.P(
                    "Select a site to load its individual deforestation trajectory. "
                    "Data is fetched on demand from the database.",
                    className="text-muted",
                )
            )
        else:
            plots.append(
                html.P(
                    "Select a site to view its deforestation trajectory "
                    "compared to matched controls, with intervention dates marked.",
                    className="text-muted",
                )
            )
        plots.append(
            dcc.Store(
                id="site-defor-sort-options",
                data={
                    "by_site": site_options,
                    "by_name": site_options_by_name,
                },
            )
        )
        plots.append(
            html.Div(
                [
                    html.Label("Sort sites:", className="fw-bold me-2"),
                    dbc.RadioItems(
                        id="site-defor-sort-order",
                        options=[
                            {"label": "By site ID", "value": "by_site"},
                            {"label": "Alphabetical", "value": "by_name"},
                        ],
                        value="by_name",
                        inline=True,
                        className="d-inline-flex",
                    ),
                ],
                className="mb-2 d-flex align-items-center",
            )
        )
        plots.append(
            dbc.Select(
                id="site-defor-selector",
                options=site_options_by_name,
                placeholder="Select a site...",
                className="mb-3",
                style={"maxWidth": "400px"},
            )
        )
        plots.append(dcc.Store(id="site-defor-store", data=store_data))
        plots.append(html.Div(id="site-defor-plot-container"))

    return html.Div(plots)


# ---------------------------------------------------------------------------
# Match quality assessment helpers
# ---------------------------------------------------------------------------

# Thresholds for automated quality checks (from propensity score matching
# best-practice literature).
_SMD_CRITICAL = 0.25  # |SMD| above this ΓåÆ critical imbalance
_SMD_WARN = 0.1  # |SMD| above this ΓåÆ imperfect balance
_SMD_POOR_FRAC = 0.20  # fraction of covariates with |SMD| > 0.1 to trigger warning
_PCT_MATCHED_CRITICAL = 5  # % matched below this ΓåÆ critical
_PCT_MATCHED_WARN = 25  # % matched below this ΓåÆ warning
