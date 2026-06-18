"""Match quality assessment helpers and plot builders."""

import json
import logging

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from dash import dcc, html

from callbacks._helpers import (
    _attach_totals_to_geojson,
    _openlayers_map_component,
)
from services import download_results_csv

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
                                                    " \u2014 A covariate value "
                                                    "appears exclusively in "
                                                    "treatment or control pixels "
                                                    "(e.g. all treatment pixels "
                                                    "are in a protected area but "
                                                    "no control pixels are). This "
                                                    "makes the propensity score "
                                                    "model predict treatment "
                                                    "perfectly, so no control can "
                                                    "match within the caliper.",
                                                ]
                                            ),
                                            html.Li(
                                                [
                                                    html.Strong(
                                                        "Insufficient controls"
                                                    ),
                                                    " \u2014 Too few control "
                                                    "pixels in the group relative "
                                                    "to treatment pixels. The "
                                                    "matching algorithm cannot "
                                                    "find enough similar controls.",
                                                ]
                                            ),
                                            html.Li(
                                                [
                                                    html.Strong("Caliper rejection"),
                                                    " \u2014 Even when controls "
                                                    "exist, none are similar "
                                                    "enough (within the caliper "
                                                    "threshold) to be matched.",
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
            n_px = t.n_sample_pixels or 0
            n_treatment = t.n_treatment_pixels
            area_ha = t.area_ha
            sampled_frac = t.sampled_fraction or 1.0

            if n_treatment and n_treatment > 0:
                eligible = n_treatment * sampled_frac
                pct = (n_px / eligible * 100) if eligible > 0 else 0
                pct = min(pct, 100)
            elif area_ha and area_ha > 0:
                # Fallback for old results without n_treatment_pixels
                approx_pixels = area_ha / 86.0
                eligible = approx_pixels * sampled_frac
                pct = (n_px / eligible * 100) if eligible > 0 else 0
                pct = min(pct, 100)
            else:
                pct = None

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
                                    " (red) ΓÇö Results are likely unreliable. "
                                    "Triggers include:",
                                ],
                                className="mb-1",
                            ),
                            html.Ul(
                                [
                                    html.Li(
                                        f"Fewer than {_PCT_MATCHED_CRITICAL}% "
                                        f"of treatment pixels matched ΓÇö "
                                        f"too few for statistical confidence. "
                                        f"This is often caused by covariate "
                                        f"separation (see group diagnostics "
                                        f"below)."
                                    ),
                                    html.Li(
                                        f"One or more covariates with "
                                        f"|SMD| \u2265 {_SMD_CRITICAL} ΓÇö "
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
                                    " (yellow) ΓÇö Results may be limited but "
                                    "are not necessarily invalid. "
                                    "Triggers include:",
                                ],
                                className="mb-1",
                            ),
                            html.Ul(
                                [
                                    html.Li(
                                        f"Fewer than {_PCT_MATCHED_WARN}% "
                                        f"of treatment pixels matched ΓÇö "
                                        f"low sample size that may limit "
                                        f"reliability."
                                    ),
                                    html.Li(
                                        f"More than {_SMD_POOR_FRAC:.0%} of "
                                        f"covariates with |SMD| > {_SMD_WARN} "
                                        f"ΓÇö overall matching quality is "
                                        f"imperfect."
                                    ),
                                ],
                                className="mb-2",
                            ),
                            html.P(
                                [
                                    html.Strong("SMD"),
                                    " (Standardized Mean Difference) measures "
                                    "how similar the treatment and control "
                                    "groups are for each covariate. Values "
                                    "closer to 0 indicate better balance.",
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


def _compute_quality_warnings(task_id, task, totals):
    """Load balance data from S3 and run quality checks.

    Returns
    -------
    list[dict]
        Warning dicts from :func:`_assess_match_quality`.
    """
    import io
    import json

    balance_df = None
    if task.status == "succeeded":
        balance_csv = download_results_csv(
            task_id, "balance", results_s3_uri=task.results_s3_uri
        )
        if balance_csv:
            balance_df = pd.read_csv(io.StringIO(balance_csv))
            if balance_df.empty:
                balance_df = None

    warnings = _assess_match_quality(balance_df=balance_df, totals=totals)

    # Load methodology warnings from results_summary.json (if available)
    if task.status == "succeeded":
        summary_raw = download_results_csv(
            task_id, "summary", results_s3_uri=task.results_s3_uri
        )
        if summary_raw:
            try:
                summary = json.loads(summary_raw)
                methodology_warnings = summary.get("methodology_warnings", [])
                for mw in methodology_warnings:
                    code = mw.get("code", "")
                    message = mw.get("message", "")
                    if code == "ci_low_replicates":
                        warnings.append(
                            {
                                "level": "warning",
                                "scope": "aggregate",
                                "message": message,
                            }
                        )
                    elif code == "pre_2005_sites":
                        affected = mw.get("affected_sites", [])
                        site_names = [
                            s.get("site_name") or s.get("site_id") for s in affected
                        ]
                        if len(site_names) <= 3:
                            sites_str = ", ".join(site_names)
                        else:
                            sites_str = (
                                f"{', '.join(site_names[:3])}, "
                                f"and {len(site_names) - 3} more"
                            )
                        warnings.append(
                            {
                                "level": "warning",
                                "scope": "aggregate",
                                "message": (f"{message} Affected sites: {sites_str}."),
                            }
                        )
            except (json.JSONDecodeError, ValueError, TypeError):
                pass  # Summary not available or malformed

    return warnings


def _build_site_quality_table(warnings, totals=None):
    """Build a table listing sites with quality concerns.

    Returns an empty ``html.Div`` when there are no per-site warnings.
    """
    site_warnings = [w for w in warnings if w["scope"] != "aggregate"]
    if not site_warnings:
        return html.Div()

    # Group warnings by site
    site_map = {}
    for w in site_warnings:
        site_map.setdefault(w["scope"], []).append(w)

    # Build name lookup from totals
    name_lookup = {}
    if totals:
        for t in totals:
            name_lookup[str(t.site_id)] = t.site_name or str(t.site_id)

    rows = []
    for sid, ws in sorted(site_map.items()):
        has_danger = any(w["level"] == "danger" for w in ws)
        icon_cls = (
            "bi bi-exclamation-triangle-fill text-danger"
            if has_danger
            else "bi bi-exclamation-triangle text-warning"
        )
        issues = "; ".join(w["message"] for w in ws)
        rows.append(
            html.Tr(
                [
                    html.Td(
                        html.I(className=icon_cls),
                        style={"width": "30px", "textAlign": "center"},
                    ),
                    html.Td(name_lookup.get(sid, sid)),
                    html.Td(issues, className="text-muted small"),
                ]
            )
        )

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
                html.Table(
                    [
                        html.Thead(
                            html.Tr(
                                [
                                    html.Th("", style={"width": "30px"}),
                                    html.Th("Site"),
                                    html.Th("Issue(s)"),
                                ]
                            )
                        ),
                        html.Tbody(rows),
                    ],
                    className="table table-sm table-hover mb-0",
                ),
            ),
        ],
        className="mb-3",
    )


def _build_match_quality(task_id, task, sites=None, totals=None):
    """Build the match quality assessment section.

    Loads a pre-computed summary JSON (small, ~250 KB) produced by the
    R summarize step instead of the full pixel-level CSVs that could be
    hundreds of MB and cause the webapp to OOM.  For tasks that were
    completed before the R script started writing the summary, a
    background Celery task is kicked off to generate it from the raw
    CSVs using chunked reads.

    Produces:

    * **Love plot** ΓÇö standardized mean differences (uses balance CSV,
      which is always small).
    * **Propensity score QQ plot** ΓÇö from pre-computed quantiles.
    * **Covariate histograms** ΓÇö from pre-computed bin counts.

    Also provides download buttons for the underlying CSVs.
    """
    import io
    import json

    # Always render the callback target IDs so Dash doesn't error when
    # the callbacks reference them, even if the tab has no data yet.
    placeholder_ids = html.Div(
        [
            dbc.Select(
                id="match-quality-site-selector",
                options=[],
                style={"display": "none"},
            ),
            dcc.Store(id="match-quality-data-store", data={}),
            dcc.Store(id="match-quality-sort-options", data={}),
            dbc.RadioItems(
                id="match-quality-sort-order",
                style={"display": "none"},
            ),
            html.Div(id="match-quality-plots-container"),
            dbc.Button(
                id="download-match-covariates",
                style={"display": "none"},
            ),
            dcc.Download(id="download-match-quality"),
        ],
        style={"display": "none"},
    )

    if task.status != "succeeded":
        return html.Div(
            [
                html.P("Results not yet available.", className="text-muted"),
                placeholder_ids,
            ]
        )

    # Build a lookup of site_id -> area_ha from totals.
    site_areas = {}
    for t in totals or []:
        sid = t.site_id if hasattr(t, "site_id") else t.get("site_id")
        area = t.area_ha if hasattr(t, "area_ha") else t.get("area_ha")
        if sid is not None:
            site_areas[str(sid)] = area or 0

    # Fetch balance statistics (Love plot data) ΓÇö always small.
    balance_df = None
    balance_csv = download_results_csv(
        task_id, "balance", results_s3_uri=task.results_s3_uri
    )
    if balance_csv:
        balance_df = pd.read_csv(io.StringIO(balance_csv))
        if balance_df.empty:
            balance_df = None

    # Run quality checks (uses balance_df + totals ΓÇö both small).
    quality_warnings = _assess_match_quality(balance_df=balance_df, totals=totals)

    # ---- Try loading the pre-computed summary JSON -----------------------
    summary = None
    summary_raw = download_results_csv(
        task_id,
        "match_quality_summary",
        results_s3_uri=task.results_s3_uri,
    )
    if summary_raw:
        try:
            summary = json.loads(summary_raw)
        except (json.JSONDecodeError, ValueError):
            summary = None

    if not summary or not summary.get("histograms"):
        # Summary not available ΓÇö try to generate it in the background
        # via a Celery task (routed to the merge queue which has more
        # memory), then show a placeholder for now.
        try:
            from celery_app import celery_app

            celery_app.send_task(
                "tasks.generate_match_quality_summary",
                args=[str(task_id)],
                kwargs={"results_s3_uri": task.results_s3_uri},
            )
            logger.info("Dispatched backfill summary task for %s", task_id)
        except Exception:
            pass  # best effort

        return html.Div(
            [
                html.P(
                    "Match quality plots are being generated. "
                    "Please refresh the page in a few moments.",
                    className="text-muted",
                ),
                placeholder_ids,
            ]
        )

    covariate_cols = summary.get("covariate_cols", [])

    if not covariate_cols and not summary.get("qq_quantiles"):
        return html.Div(
            [
                html.P(
                    "No covariate data available for quality assessment.",
                    className="text-muted",
                ),
                placeholder_ids,
            ]
        )

    content = []

    content.append(
        html.P(
            "Assessment of match quality between treatment and control "
            "pixels. The Love plot shows covariate balance (standardized "
            "mean differences), the QQ plot compares propensity score "
            "distributions, and the histograms show per-covariate overlap. "
            "Use the site filter to view diagnostics for individual sites.",
            className="text-muted mb-3",
        )
    )

    content.append(html.H5("Match Quality Diagnostics", className="mt-4 mb-2"))

    # Per-site selector ΓÇö derive from summary stats keys
    site_ids = sorted(k for k in summary.get("summary_stats", {}) if k != "__all__")
    site_name_map = {}
    for t in totals or []:
        sid = t.site_id if hasattr(t, "site_id") else t.get("site_id")
        sname = t.site_name if hasattr(t, "site_name") else t.get("site_name")
        if sid is not None and sname and str(sname) != str(sid):
            site_name_map[str(sid)] = sname
    per_site_options = []
    for sid in site_ids:
        sname = site_name_map.get(str(sid))
        label = f"{sname} ({sid})" if sname else str(sid)
        per_site_options.append({"label": label, "value": sid})
    per_site_by_name = sorted(per_site_options, key=lambda o: o["label"].lower())
    aggregate_entry = [{"label": "All sites (aggregate)", "value": "__all__"}]
    site_options_by_site = aggregate_entry + per_site_options
    site_options_by_name = aggregate_entry + per_site_by_name

    content.append(
        dcc.Store(
            id="match-quality-sort-options",
            data={
                "by_site": site_options_by_site,
                "by_name": site_options_by_name,
            },
        )
    )
    content.append(
        html.Div(
            [
                html.Label("Filter by site:", className="fw-bold me-2"),
                dbc.RadioItems(
                    id="match-quality-sort-order",
                    options=[
                        {"label": "By site ID", "value": "by_site"},
                        {"label": "Alphabetical", "value": "by_name"},
                    ],
                    value="by_name",
                    inline=True,
                    className="d-inline-flex me-3",
                ),
                dbc.Select(
                    id="match-quality-site-selector",
                    options=site_options_by_name,
                    value="__all__",
                    style={
                        "maxWidth": "350px",
                        "display": "inline-block",
                    },
                ),
            ],
            className="mb-3 d-flex align-items-center",
        )
    )

    # Store only the small summary data (not raw pixel rows)
    store_data = {
        "has_summary": True,
        "summary_stats": summary.get("summary_stats", {}),
        "histograms": summary.get("histograms", {}),
        "qq_quantiles": summary.get("qq_quantiles", {}),
        "covariate_cols": covariate_cols,
        "site_areas": site_areas,
        "quality_warnings": quality_warnings,
    }
    if balance_df is not None:
        store_data["balance_rows"] = balance_df.to_dict("records")
    content.append(dcc.Store(id="match-quality-data-store", data=store_data))

    # Render initial plots for all sites (aggregate)
    content.append(
        html.Div(
            _build_plots_from_summary(store_data, site_filter=None),
            id="match-quality-plots-container",
        )
    )

    # Download button
    content.append(html.Hr(className="my-3"))
    content.append(
        dbc.Button(
            "Download Match Covariates CSV",
            id="download-match-covariates",
            color="secondary",
            size="sm",
        )
    )
    content.append(dcc.Download(id="download-match-quality"))

    return html.Div(content)


def _build_all_match_quality_plots(
    df,
    covariate_cols,
    balance_df,
    pscore_df,
    site_filter=None,
    site_areas=None,
):
    """Build summary stats, Love plot, QQ plot, and covariate histograms.

    Parameters
    ----------
    df : pd.DataFrame
        Matched-pixel covariate data (already filtered to the target site
        when *site_filter* is not ``None``).
    covariate_cols : list[str]
        Covariate column names.
    balance_df : pd.DataFrame | None
        Balance statistics with ``site_id``, ``covariate``, ``smd``.
    pscore_df : pd.DataFrame | None
        Propensity scores with ``treatment``, ``pscore``, ``site_id``.
    site_filter : str | None
        ``None`` (or ``"__all__"``) for aggregate; otherwise the site id
        string to show per-site diagnostics.
    site_areas : dict | None
        Mapping of ``str(site_id)`` to area in hectares.
    """
    if site_areas is None:
        site_areas = {}

    components = []

    # --- Summary stat boxes ------------------------------------------------
    n_treatment = int(df["treatment"].sum())
    n_control = int((~df["treatment"]).sum())
    n_sites = df["site_id"].nunique()

    # Compute total area for the selected site(s)
    if site_filter:
        total_area = site_areas.get(str(site_filter), 0)
    else:
        total_area = sum(site_areas.values())

    # Treatment pixels card body
    treatment_body = [
        html.H6("Treatment Pixels", className="text-muted mb-1"),
        html.H4(f"{n_treatment:,}"),
    ]

    # Control pixels card body
    control_body = [
        html.H6("Control Pixels", className="text-muted mb-1"),
        html.H4(f"{n_control:,}"),
    ]

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

    # --- Love plot ---------------------------------------------------------
    if balance_df is not None:
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
        components.append(_build_love_plot(balance_df, df, site_filter))

    # --- Propensity score QQ plot ------------------------------------------
    if pscore_df is not None:
        components.append(html.H6("Propensity Score QQ Plot", className="mt-3 mb-2"))
        components.append(
            html.P(
                "Empirical quantile-quantile plot comparing the propensity "
                "score distributions of matched treatment and control "
                "pixels. Points close to the 45┬░ line indicate similar "
                "distributions.",
                className="text-muted mb-2",
            )
        )
        components.append(_build_pscore_qq_plot(pscore_df, df, site_filter))

    # --- Covariate histograms ----------------------------------------------
    components.append(html.H6("Covariate Distributions", className="mt-3 mb-2"))
    components.extend(_build_match_quality_plots(df, covariate_cols))

    return components


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
    colors = ["#2ca02c" if abs(v) <= 0.1 else "#d62728" for v in agg["smd"]]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=agg["smd"],
            y=agg["covariate"],
            mode="markers",
            marker=dict(size=10, color=colors),
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
        margin=dict(l=200, r=40, t=50, b=50),
        xaxis=dict(zeroline=True),
    )

    return dcc.Graph(figure=fig)


def _build_pscore_qq_plot(pscore_df, cov_df, site_filter=None):
    """Build an empirical QQ plot comparing treatment vs control propensity
    score distributions.

    For each group, scores are sorted and quantile-aligned.  If the two
    groups have different sizes, the smaller set is linearly interpolated
    to match the larger set's quantile positions.

    Parameters
    ----------
    pscore_df : pd.DataFrame
        Propensity scores with columns ``treatment``, ``pscore``,
        ``site_id``.
    cov_df : pd.DataFrame
        Unused; kept for API consistency with other helpers.
    site_filter : str | None
        ``None`` for aggregate view; otherwise the site id to display.
    """
    import numpy as np

    if site_filter:
        pscore_df = pscore_df[pscore_df["site_id"].astype(str) == str(site_filter)]

    treatment_scores = np.sort(
        pscore_df.loc[pscore_df["treatment"], "pscore"].dropna().values
    )
    control_scores = np.sort(
        pscore_df.loc[~pscore_df["treatment"], "pscore"].dropna().values
    )

    if len(treatment_scores) < 2 or len(control_scores) < 2:
        return html.P(
            "Insufficient propensity scores for a QQ plot.",
            className="text-muted",
        )

    # Align quantiles via linear interpolation to the larger sample
    n_points = max(len(treatment_scores), len(control_scores))
    quantiles = np.linspace(0, 1, n_points)
    t_quantiles = np.quantile(treatment_scores, quantiles)
    c_quantiles = np.quantile(control_scores, quantiles)

    fig = go.Figure()
    # 45┬░ reference line
    q_min = min(t_quantiles.min(), c_quantiles.min())
    q_max = max(t_quantiles.max(), c_quantiles.max())
    fig.add_trace(
        go.Scatter(
            x=[q_min, q_max],
            y=[q_min, q_max],
            mode="lines",
            line=dict(color="gray", dash="dash"),
            name="45┬░ line",
            hoverinfo="skip",
        )
    )
    fig.add_trace(
        go.Scattergl(
            x=c_quantiles,
            y=t_quantiles,
            mode="markers",
            marker=dict(size=4, color="#1f77b4", opacity=0.6),
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
        margin=dict(t=50, b=50, l=60, r=30),
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
        xaxis=dict(scaleanchor="y", scaleratio=1),
    )

    return dcc.Graph(figure=fig)


# ---------------------------------------------------------------------------
# Summary-based plot builders (no raw pixel data needed)
# ---------------------------------------------------------------------------


def _build_plots_from_summary(store_data, site_filter=None):
    """Build stat boxes, Love plot, QQ plot, and histograms from the
    pre-computed summary stored in ``store_data``.

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

    # --- Love plot (from balance CSV ΓÇö already small) ----------------------
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
                "pixels. Points close to the 45┬░ line indicate similar "
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
            line=dict(color="gray", dash="dash"),
            name="45┬░ line",
            hoverinfo="skip",
        )
    )
    fig.add_trace(
        go.Scattergl(
            x=c_q,
            y=t_q,
            mode="markers",
            marker=dict(size=4, color="#1f77b4", opacity=0.6),
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
        margin=dict(t=50, b=50, l=60, r=30),
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
        xaxis=dict(scaleanchor="y", scaleratio=1),
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
                marker_color="#2ca02c",
                width=bin_width,
            )
        )
        fig.add_trace(
            go.Bar(
                x=mids,
                y=c_pct,
                name="Control",
                opacity=0.6,
                marker_color="#d62728",
                width=bin_width,
            )
        )
        fig.update_layout(
            title=f"Covariate: {col}",
            xaxis_title=col,
            yaxis_title="Frequency (%)",
            barmode="overlay",
            legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99),
            height=350,
            margin=dict(t=40, b=40, l=50, r=20),
        )
        plots.append(dcc.Graph(figure=fig))

    if not plots:
        return [html.P("No numeric covariates to display.", className="text-muted")]

    return plots


def _build_match_quality_plots(df, covariate_cols):
    """Build overlaid histogram figures for each covariate.

    Returns a list of ``dcc.Graph`` components comparing treatment vs
    control distributions.  Both traces share identical bin edges so
    their bar widths are directly comparable.
    """

    plots = []
    n_bins = 40

    treatment_df = df[df["treatment"]]
    control_df = df[~df["treatment"]]

    for col in covariate_cols:
        # Skip columns with no variance
        col_vals = df[col].dropna()
        if col_vals.empty or col_vals.nunique() < 2:
            continue

        # Compute shared bin edges from the combined data
        col_min = float(col_vals.min())
        col_max = float(col_vals.max())
        bin_size = (col_max - col_min) / n_bins if col_max > col_min else 1
        xbins = dict(start=col_min, end=col_max + bin_size, size=bin_size)

        fig = go.Figure()
        fig.add_trace(
            go.Histogram(
                x=treatment_df[col].dropna(),
                name="Treatment",
                histnorm="percent",
                opacity=0.6,
                marker_color="#2ca02c",
                xbins=xbins,
            )
        )
        fig.add_trace(
            go.Histogram(
                x=control_df[col].dropna(),
                name="Control",
                histnorm="percent",
                opacity=0.6,
                marker_color="#d62728",
                xbins=xbins,
            )
        )
        fig.update_layout(
            title=f"Covariate: {col}",
            xaxis_title=col,
            yaxis_title="Frequency (%)",
            barmode="overlay",
            legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99),
            height=350,
            margin=dict(t=40, b=40, l=50, r=20),
        )
        plots.append(dcc.Graph(figure=fig))

    if not plots:
        return [html.P("No numeric covariates to display.", className="text-muted")]

    return plots


def _build_map(
    sites_geojson, totals, covariates=None, task_id=None, sites=None, resolution_m=None
):
    """Build an OpenLayers map for task sites and summary values."""
    enriched_geojson = _attach_totals_to_geojson(sites_geojson, totals)
    if not enriched_geojson:
        return html.P("No site geometries available.", className="text-muted")

    # Build site dropdown options for zoom-to-site control
    site_name_map = {}
    if totals:
        for t in totals:
            if t.site_name and str(t.site_name) != str(t.site_id):
                site_name_map[str(t.site_id)] = t.site_name
    if sites:
        for s in sites:
            sid = str(s.site_id)
            if s.site_name and s.site_name != sid and sid not in site_name_map:
                site_name_map[sid] = s.site_name

    # Extract site IDs from GeoJSON features
    fc = json.loads(enriched_geojson)
    site_ids = sorted(
        {str(f["properties"].get("site_id", "")) for f in fc.get("features", [])} - {""}
    )

    site_options = []
    for sid in site_ids:
        sname = site_name_map.get(sid)
        label = f"{sname} ({sid})" if sname else sid
        site_options.append({"label": label, "value": sid})
    site_options_by_name = sorted(site_options, key=lambda o: o["label"].lower())

    # Build tile URL when task_id is available so the rendering layer fetches
    # MVT tiles directly (no Dash round-trip on pan/zoom).  The enriched_geojson
    # (centroid points + emissions) is still passed as data-geojson; the JS uses
    # it to build _emissionsBySiteId for colouring circles on task-scoped tiles.
    tile_url = f"/api/task-sites-tiles/{task_id}/{{z}}/{{x}}/{{y}}" if task_id else None
    map_component = _openlayers_map_component(
        "task-sites-map",
        enriched_geojson,
        height="500px",
        enable_cog_layers=True,
        cog_filter_covariates=covariates,
        task_id=task_id,
        resolution_m=resolution_m,
        tile_url=tile_url,
    )

    controls = html.Div(
        [
            dcc.Store(
                id="map-site-sort-options",
                data={
                    "by_site": [{"label": "All sites", "value": ""}] + site_options,
                    "by_name": [{"label": "All sites", "value": ""}]
                    + site_options_by_name,
                },
            ),
            html.Label("Zoom to site:", className="fw-bold me-2"),
            dbc.RadioItems(
                id="map-site-sort-order",
                options=[
                    {"label": "By site ID", "value": "by_site"},
                    {"label": "Alphabetical", "value": "by_name"},
                ],
                value="by_name",
                inline=True,
                className="d-inline-flex me-3",
            ),
            dbc.Select(
                id="map-site-selector",
                options=[{"label": "All sites", "value": ""}] + site_options_by_name,
                value="",
                style={"maxWidth": "350px", "display": "inline-block"},
            ),
        ],
        className="mb-3 d-flex align-items-center",
    )

    return html.Div([controls, map_component])
