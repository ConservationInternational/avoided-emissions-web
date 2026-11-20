"""Dash callback registration for the avoided emissions web application."""

import json
import logging
import random
import uuid as _uuid
from datetime import UTC, datetime

import dash_bootstrap_components as dbc
import flask_login
import pandas as pd
import plotly.graph_objects as go
from auth import get_current_user
from botocore.exceptions import BotoCoreError, ClientError
from config import report_exception
from dash import (
    ALL,
    ClientsideFunction,
    Input,
    Output,
    State,
    callback_context,
    dcc,
    html,
    no_update,
)
from dash.exceptions import PreventUpdate
from layouts import (
    EXACT_MATCH_OPTIONS,
    TASK_LIST_COLUMNS,
    _make_ag_grid,
)
from layouts.common import TEXT_FILTER, server_date_filter, server_text_filter
from layouts.documentation import variable_cards, variable_links
from plot_selection import parse_plot_selection, parse_task_tab, plot_selection_query
from services import (
    ANALYSIS_DEFAULTS,
    AppUsersError,
    archive_user_site_set,
    can_resume_task,
    cancel_task,
    cancel_user_site_upload,
    create_share_link,
    create_user_site_upload,
    delete_matching_preset,
    delete_user_data,
    delete_user_site_set,
    delete_user_site_upload,
    discard_staged_site_upload,
    download_results_csv,
    force_reexport,
    force_remerge,
    get_covariate_inventory,
    get_matching_presets,
    get_ready_covariate_names,
    get_ready_exact_match_names,
    get_task_detail,
    get_task_list,
    get_task_site_index,
    get_task_site_results,
    get_user_site_set_detail,
    get_user_site_set_preview_rows,
    list_app_users,
    list_share_links,
    list_user_site_sets,
    list_user_site_uploads,
    queue_analysis_task,
    rename_user_site_set,
    resume_task,
    revoke_share_link,
    save_matching_preset,
    set_app_user_access,
    start_gee_export,
    update_task_info,
)
from services.analysis_task import (
    get_filtered_match_quality_pixels,
    get_filtered_portfolio_total,
    get_filtered_portfolio_yearly,
    get_task_execution_logs,
    get_task_header,
    get_task_top_site_yearly,
    matching_replay_links,
)
from services.site_set import filterable_source_attributes
from services.task_result_cache import cached_task_result
from session_store import SessionExpired, SessionUnavailable
from task_timing import timed_task_step
from trendsearth_client import TrendsEarthAPIError

from callbacks._detail_builders import (
    BOUNDS_NAME,
    _add_ci_band,
    _add_range_error_bars,
    _build_overview,
    _build_plots,
    _build_raw_results,
    _build_results_content,
    _build_selection_summary,
    _build_yearly_results,
    _filter_site_ids,
    _site_filter_values,
)
from callbacks._helpers import (
    _authorize_task_access,
    _check_task_access,
    _fmt_dt,
    _openlayers_map_component,
    _record_covariate_action_failure,
    _render_share_links_list,
)
from callbacks._match_quality import (
    SITE_QUALITY_BLOCK_SIZE,
    SITE_QUALITY_GRID_ID,
    SITE_STATUS_BLOCK_SIZE,
    SITE_STATUS_GRID_ID,
    _build_map,
    _build_selected_match_quality,
    _compute_quality_warnings,
    _site_quality_rows,
    build_site_status_rows,
    site_status_block,
)
from callbacks._plot_colors import (
    CONTROL_BAND_COLOR,
    CONTROL_COLOR,
    PROJECT_BAND_COLOR,
    PROJECT_COLOR,
)
from gee_export import gee_config

logger = logging.getLogger(__name__)


def _filtered_portfolio(task, site_ids, kind):
    loader = (
        get_filtered_portfolio_yearly
        if kind == "yearly"
        else get_filtered_portfolio_total
    )
    return cached_task_result(
        task,
        f"portfolio_{kind}",
        lambda: loader(
            str(task.id),
            site_ids,
            int((task.extra_metadata or {}).get("n_replicates", 1)),
            results_s3_uri=task.results_s3_uri,
        ),
        site_ids=site_ids,
    )


def _site_catalog_rows(site_sets, include_archived=False):
    """Build the reusable catalog without depending on retained import history."""
    return [
        {
            **site_set,
            "site_set_id": site_set["id"],
            "site_set_name": site_set["name"],
            "n_sites_imported": site_set["n_sites"],
            "created_at": site_set.get("uploaded_at"),
            "status": "archived" if site_set.get("is_archived") else "ready",
        }
        for site_set in site_sets
        if include_archived or not site_set.get("is_archived")
    ]


def _site_import_history_rows(uploads, site_sets):
    current = {site_set["id"]: site_set for site_set in site_sets}
    rows = []
    for upload in uploads:
        row = dict(upload)
        site_set = current.get(row.get("site_set_id"))
        if site_set:
            row.update(
                site_set_name=site_set["name"],
                is_archived=site_set.get("is_archived", False),
            )
        else:
            # A deleted set must not offer actions against stale import metadata.
            row["site_set_id"] = None
            row["is_archived"] = False
        rows.append(row)
    return rows


def register_callbacks(app, limiter=None):
    """Register all Dash callbacks on the app instance.

    Parameters
    ----------
    app : dash.Dash
        The Dash application.
    limiter : flask_limiter.Limiter, optional
        Flask-Limiter instance.  Currently unused directly (Dash funnels
        all callbacks through a single POST endpoint) but reserved for
        future use.  Sign-in itself happens on Flask routes (see
        ``auth_routes.py``), which are rate-limited there.
    """

    @app.callback(
        Output("docs-variable-list", "children"),
        Input("docs-variable-search", "value"),
    )
    def search_documentation_variables(query):
        return variable_cards(query)

    @app.callback(
        Output("covariate-metadata-links", "children"),
        Output("exact-match-metadata-links", "children"),
        Input("covariate-selection", "options"),
        Input("exact-match-selection", "options"),
    )
    def refresh_variable_metadata_links(covariates, exact_matches):
        return (
            variable_links([option["value"] for option in covariates or []]),
            variable_links([option["value"] for option in exact_matches or []]),
        )

    # -- Navbar toggle (responsive collapse) ----------------------------------

    @app.callback(
        Output("navbar-collapse", "is_open"),
        Input("navbar-toggler", "n_clicks"),
        State("navbar-collapse", "is_open"),
        prevent_initial_call=True,
    )
    def toggle_navbar(n_clicks, is_open):
        if n_clicks:
            return not is_open
        return is_open

    # -- Session activity check ----------------------------------------------
    # Fires every 5 minutes via dcc.Interval.  The request itself triggers
    # the before_request OIDC token refresh in app.py.  If Trends.Earth
    # rejects the refresh (session ended or app access revoked) the hook
    # signs the user out and this callback redirects to /login.

    @app.callback(
        Output("session-check-output", "children"),
        Input("session-check-interval", "n_intervals"),
        prevent_initial_call=True,
    )
    def check_session_alive(n_intervals):
        if not flask_login.current_user.is_authenticated:
            return dcc.Location(pathname="/login", id="redirect-session-expired")
        raise PreventUpdate

    @app.callback(
        [Output("site-set-selector", "options"), Output("site-set-selector", "value")],
        [
            Input("url", "pathname"),
            Input("site-set-refresh-store", "data"),
            Input("show-archived-site-sets", "value"),
        ],
        [
            State("site-set-selector", "value"),
            State("recompute-config-store", "data"),
        ],
    )
    def refresh_site_set_options(
        pathname, _refresh_token, show_archived, current_value, recompute_config
    ):
        if pathname != "/submit":
            raise PreventUpdate

        user = get_current_user()
        if not user:
            raise PreventUpdate

        site_sets = list_user_site_sets(user.id, include_archived=bool(show_archived))
        options = []
        for s in site_sets:
            label = (
                f"{s['name']} ({s['n_sites']} sites, "
                f"{(s['uploaded_at'] or '')[:19].replace('T', ' ')})"
            )
            if s.get("is_archived"):
                label += " [Archived]"
            options.append({"label": label, "value": s["id"]})

        valid_ids = {s["id"] for s in site_sets}

        # When recomputing, prefer the original task's site set
        recompute_site_set = (
            (recompute_config or {}).get("site_set_id") if not current_value else None
        )
        if recompute_site_set and recompute_site_set in valid_ids:
            value = recompute_site_set
        elif current_value in valid_ids:
            value = current_value
        else:
            value = options[0]["value"] if options else None

        return options, value

    @app.callback(
        Output("covariate-selection", "options"),
        Input("resolution-m", "value"),
        Input("url", "pathname"),
    )
    def refresh_submit_covariate_options(resolution_m_str, pathname):
        if pathname != "/submit":
            raise PreventUpdate

        resolution_m = int(resolution_m_str) if resolution_m_str else 1000
        ready_covariates = get_ready_covariate_names(resolution_m=resolution_m)
        return [{"label": cov, "value": cov} for cov in ready_covariates]

    @app.callback(
        Output("exact-match-selection", "options"),
        Input("resolution-m", "value"),
        Input("url", "pathname"),
    )
    def refresh_exact_match_options(resolution_m_str, pathname):
        if pathname != "/submit":
            raise PreventUpdate

        resolution_m = int(resolution_m_str) if resolution_m_str else 1000
        ready_names = set(get_ready_exact_match_names(resolution_m=resolution_m))
        return [
            {**opt, "disabled": opt["value"] not in ready_names}
            for opt in EXACT_MATCH_OPTIONS
        ]

    @app.callback(
        Output("exact-match-selection", "value", allow_duplicate=True),
        Input("exact-match-selection", "options"),
        State("exact-match-selection", "value"),
        prevent_initial_call=True,
    )
    def filter_exact_match_values(options, current_values):
        """Remove selected exact-match vars that are disabled (unavailable)."""
        if not options or not current_values:
            raise PreventUpdate
        enabled = {o["value"] for o in options if not o.get("disabled")}
        filtered = [v for v in current_values if v in enabled]
        if filtered == current_values:
            raise PreventUpdate
        return filtered

    @app.callback(
        Output("covariate-selection", "value", allow_duplicate=True),
        Input("covariate-selection", "options"),
        State("recompute-config-store", "data"),
        State("covariate-selection", "value"),
        prevent_initial_call=True,
    )
    def sync_covariate_values(options, recompute_config, current_values):
        """Prefill from recompute config, or prune unavailable selections."""
        if not options:
            raise PreventUpdate
        available = {opt["value"] for opt in options}

        # If there is a recompute config, use it as the source of truth.
        if recompute_config:
            covs = [c for c in recompute_config.get("covariates", []) if c in available]
            if covs:
                return covs

        # Otherwise, prune any currently selected covariates that are no
        # longer available (e.g. user switched resolution).
        if current_values:
            filtered = [v for v in current_values if v in available]
            if filtered != current_values:
                return filtered

        raise PreventUpdate

    # ------------------------------------------------------------------
    # Review summary — rendered when the user switches to the Review tab
    # ------------------------------------------------------------------
    _EXACT_MATCH_LABELS = {opt["value"]: opt["label"] for opt in EXACT_MATCH_OPTIONS}

    @app.callback(
        Output("review-summary", "children"),
        Input("submit-tabs", "active_tab"),
        State("task-name", "value"),
        State("task-description", "value"),
        State("parsed-sites-store", "data"),
        State("covariate-selection", "value"),
        State("exact-match-selection", "value"),
        State("resolution-m", "value"),
        State("max-treatment-pixels", "value"),
        State("control-multiplier", "value"),
        State("min-site-area-ha", "value"),
        State("min-glm-treatment-pixels", "value"),
        State("caliper-width", "value"),
        State("max-controls-per-treatment", "value"),
        State("min-control-distance-km", "value"),
        State("separation-fallback-mahalanobis", "value"),
        State("shared-support-controls", "value"),
        State("matching-method", "value"),
        State("n-replicates", "value"),
        State("random-seed", "value"),
        State("match-memory-gb", "value"),
        State("matching-job-queue", "value"),
        prevent_initial_call=True,
    )
    def populate_review_summary(
        active_tab,
        name,
        description,
        sites_data,
        covariates,
        exact_match_vars,
        resolution_m,
        max_treatment_pixels,
        control_multiplier,
        min_site_area_ha,
        min_glm_treatment_pixels,
        caliper_width,
        max_controls_per_treatment,
        min_control_distance_km,
        separation_fallback_mahalanobis,
        shared_support_controls,
        matching_method,
        n_replicates,
        random_seed,
        match_memory_gb,
        matching_job_queue,
    ):
        if active_tab != "tab-submit-review":
            raise PreventUpdate

        warnings = []

        # --- Task info ---
        task_name = name or ""
        if not task_name.strip():
            warnings.append("Task name is empty.")

        # --- Sites ---
        if sites_data:
            n_sites = sites_data.get("n_sites", "?")
            site_set_name = sites_data.get("name", "Uploaded file")
        else:
            n_sites = 0
            site_set_name = None
            warnings.append("No sites have been uploaded or selected.")

        # --- Covariates ---
        cov_list = covariates or []
        if not cov_list:
            warnings.append("No covariates selected.")

        # --- Exact match ---
        em_list = exact_match_vars or []
        if not em_list:
            warnings.append("No exact match variables selected.")

        overlap = set(cov_list) & set(em_list)
        if overlap:
            warnings.append(
                "Overlap between covariates and exact match: "
                + ", ".join(sorted(overlap))
            )

        em_labels = [_EXACT_MATCH_LABELS.get(v, v) for v in em_list]

        # --- Resolution ---
        # Convert from string to int (form returns string)
        try:
            resolution_m = int(resolution_m) if resolution_m else 1000
        except (ValueError, TypeError):
            resolution_m = 1000

        # --- Matching params ---
        mcpt = max_controls_per_treatment
        if mcpt == 0:
            mcpt_label = "No limit (full matching)"
        else:
            mcpt_label = str(mcpt)
            if mcpt == 1:
                mcpt_label += " (pair matching)"

        def _param_row(label, value):
            return html.Tr(
                [
                    html.Td(label, className="text-muted pe-3", style={"width": "50%"}),
                    html.Td(html.Strong(value)),
                ]
            )

        # --- Warnings banner ---
        warning_banner = []
        if warnings:
            warning_banner = [
                dbc.Alert(
                    [
                        html.Strong("Please fix before submitting:"),
                        html.Ul(
                            [html.Li(w) for w in warnings],
                            className="mb-0 mt-1",
                        ),
                    ],
                    color="warning",
                    className="mb-3",
                )
            ]

        # --- Build summary cards ---
        task_card = dbc.Card(
            [
                dbc.CardHeader("Task"),
                dbc.CardBody(
                    html.Table(
                        html.Tbody(
                            [
                                _param_row("Name", task_name or "—"),
                                _param_row("Description", description or "(none)"),
                                _param_row(
                                    "Sites",
                                    (
                                        f"{n_sites} site(s)"
                                        + (
                                            f" \u2014 {site_set_name}"
                                            if site_set_name
                                            else ""
                                        )
                                    )
                                    if sites_data
                                    else "\u2014",
                                ),
                            ]
                        ),
                        className="table table-sm table-borderless mb-0",
                    )
                ),
            ],
            className="ae-section-card mb-3",
        )

        covariates_card = dbc.Card(
            [
                dbc.CardHeader(f"Covariates ({len(cov_list)})"),
                dbc.CardBody(
                    html.Div(
                        [
                            dbc.Badge(c, color="info", className="me-1 mb-1")
                            for c in cov_list
                        ]
                    )
                    if cov_list
                    else html.P("None selected", className="text-muted mb-0"),
                ),
            ],
            className="ae-section-card mb-3",
        )

        exact_match_card = dbc.Card(
            [
                dbc.CardHeader(f"Exact Match Variables ({len(em_list)})"),
                dbc.CardBody(
                    html.Div(
                        [
                            dbc.Badge(lbl, color="secondary", className="me-1 mb-1")
                            for lbl in em_labels
                        ]
                    )
                    if em_list
                    else html.P("None selected", className="text-muted mb-0"),
                ),
            ],
            className="ae-section-card mb-3",
        )

        settings_card = dbc.Card(
            [
                dbc.CardHeader("Matching Settings"),
                dbc.CardBody(
                    html.Table(
                        html.Tbody(
                            [
                                _param_row(
                                    "Resolution",
                                    (
                                        "250 m"
                                        if resolution_m == 250
                                        else "1 km"
                                        if resolution_m == 1000 or resolution_m is None
                                        else f"{resolution_m} m"
                                    ),
                                ),
                                _param_row(
                                    "Max treatment pixels",
                                    str(max_treatment_pixels),
                                ),
                                _param_row(
                                    "Control multiplier",
                                    str(control_multiplier),
                                ),
                                _param_row(
                                    "Min site area (ha)",
                                    str(min_site_area_ha),
                                ),
                                _param_row(
                                    "Min GLM treatment pixels",
                                    str(min_glm_treatment_pixels),
                                ),
                                _param_row(
                                    "Caliper width (SD)",
                                    str(caliper_width),
                                ),
                                _param_row(
                                    "Max controls per treatment",
                                    mcpt_label,
                                ),
                                _param_row(
                                    "Min control distance (km)",
                                    str(min_control_distance_km),
                                ),
                                _param_row(
                                    "Shared-support controls",
                                    "Enabled"
                                    if shared_support_controls
                                    else "Disabled",
                                ),
                                _param_row(
                                    "Separation fallback",
                                    "Mahalanobis"
                                    if separation_fallback_mahalanobis
                                    else "Disabled (GLM fails)",
                                ),
                                _param_row(
                                    "Matching method",
                                    "Nearest neighbour (MatchIt)"
                                    if matching_method == "nearest"
                                    else "Optimal (optmatch)",
                                ),
                                _param_row(
                                    "Replicates",
                                    str(n_replicates or 1),
                                ),
                                _param_row(
                                    "Random seed",
                                    str(random_seed) if random_seed else "—",
                                ),
                                _param_row(
                                    "Matching memory",
                                    f"{match_memory_gb} GB",
                                ),
                                _param_row(
                                    "Batch job queue",
                                    str(matching_job_queue or "—"),
                                ),
                            ]
                        ),
                        className="table table-sm table-borderless mb-0",
                    )
                ),
            ],
            className="ae-section-card mb-3",
        )

        return warning_banner + [
            html.H5("Review Settings", className="mb-3"),
            dbc.Row(
                [
                    dbc.Col(
                        [task_card, covariates_card, exact_match_card],
                        xs=12,
                        lg=6,
                    ),
                    dbc.Col(settings_card, xs=12, lg=6),
                ],
                className="g-3 mb-3",
            ),
        ]

    @app.callback(
        [
            Output("upload-status", "children"),
            Output("site-set-refresh-store", "data", allow_duplicate=True),
            Output("site-upload-mapping-panel", "is_open"),
            Output("site-upload-controls", "is_open"),
            Output("site-upload-columns-store", "data"),
            Output("mapping-site-id", "options"),
            Output("mapping-site-id", "value"),
            Output("mapping-site-name", "options"),
            Output("mapping-site-name", "value"),
            Output("mapping-start-date", "options"),
            Output("mapping-start-date", "value"),
            Output("mapping-end-date", "options"),
            Output("mapping-end-date", "value"),
        ],
        [
            Input("site-upload-stream-payload", "value"),
            Input("confirm-site-upload-mapping-btn", "n_clicks"),
            Input("cancel-site-upload-mapping-btn", "n_clicks"),
        ],
        [
            State("site-upload-columns-store", "data"),
            State("mapping-site-id", "value"),
            State("mapping-site-name", "value"),
            State("mapping-start-date", "value"),
            State("mapping-end-date", "value"),
        ],
        running=[
            (
                Output("confirm-site-upload-mapping-btn", "disabled"),
                True,
                False,
            ),
            (
                Output("cancel-site-upload-mapping-btn", "disabled"),
                True,
                False,
            ),
            (
                Output("confirm-site-upload-mapping-btn", "children"),
                "Starting import...",
                "Confirm Mapping and Start Import",
            ),
            (
                Output("site-upload-mapping-status", "children"),
                dbc.Alert(
                    (
                        "Starting background site import... large uploads may take several minutes."
                    ),
                    color="info",
                    className="mb-0 py-2",
                ),
                None,
            ),
        ],
        prevent_initial_call=True,
    )
    def handle_site_upload_flow(
        stream_payload,
        _confirm_mapping_clicks,
        _cancel_mapping_clicks,
        pending_upload,
        mapped_site_id,
        mapped_site_name,
        mapped_start_date,
        mapped_end_date,
    ):
        ctx = callback_context
        if not ctx.triggered:
            raise PreventUpdate

        trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]
        user = get_current_user()
        if not user:
            return (
                dbc.Alert("Login required.", color="danger"),
                no_update,
                False,
                True,
                None,
                [],
                None,
                [],
                None,
                [],
                None,
                [{"label": "(none)", "value": ""}],
                "",
            )

        if trigger_id == "site-upload-stream-payload":
            if not stream_payload:
                raise PreventUpdate

            try:
                payload = json.loads(stream_payload)
            except Exception:  # noqa: BLE001
                return (
                    dbc.Alert(
                        "Stream upload payload could not be parsed.", color="danger"
                    ),
                    no_update,
                    False,
                    True,
                    None,
                    [],
                    None,
                    [],
                    None,
                    [],
                    None,
                    [{"label": "(none)", "value": ""}],
                    "",
                )

            if payload.get("status") != "ok":
                response = payload.get("response") or {}
                errors = response.get("errors") or ["Large file upload failed."]
                return (
                    dbc.Alert("\n".join(str(e) for e in errors), color="danger"),
                    no_update,
                    False,
                    True,
                    None,
                    [],
                    None,
                    [],
                    None,
                    [],
                    None,
                    [{"label": "(none)", "value": ""}],
                    "",
                )

            response = payload.get("response") or {}
            preview = response.get("preview") or {}
            columns = preview.get("column_info", [])
            suggested = preview.get("suggested_mapping", {})
            options = [
                {
                    "label": (
                        f"{c['name']} ({c['dtype']})"
                        f" — sample: {str(c.get('sample', ''))[:100]}"
                    ),
                    "value": c["name"],
                }
                for c in columns
            ]
            optional_options = [
                {"label": "(none — generated automatically)", "value": ""}
            ]
            optional_options += options
            end_options = [{"label": "(none)", "value": ""}] + options

            pending_data = {
                "filename": response.get("filename") or payload.get("filename"),
                "upload_token": response.get("upload_token"),
                "n_features": preview.get("n_features", 0),
                "available_columns": [c.get("name") for c in columns],
            }
            count = preview.get("n_features")
            count_message = (
                f"Preview ready ({count:,} features). "
                if isinstance(count, int)
                else "Sample preview ready; feature count will be determined during import. "
            )

            return (
                dbc.Alert(
                    (
                        count_message
                        + "Confirm column mapping to start the background import."
                    ),
                    color="info",
                ),
                no_update,
                True,
                False,
                pending_data,
                optional_options,
                suggested.get("site_id") or "",
                optional_options,
                suggested.get("site_name") or "",
                options,
                suggested.get("start_date"),
                end_options,
                suggested.get("end_date") or "",
            )

        if trigger_id == "cancel-site-upload-mapping-btn":
            if pending_upload and pending_upload.get("upload_token"):
                try:
                    discard_staged_site_upload(pending_upload["upload_token"], user.id)
                except Exception:
                    logger.warning(
                        "Failed to discard staged upload on cancel", exc_info=True
                    )

            return (
                dbc.Alert("Upload mapping cancelled.", color="secondary"),
                no_update,
                False,
                True,
                None,
                [],
                None,
                [],
                None,
                [],
                None,
                [{"label": "(none)", "value": ""}],
                "",
            )

        if trigger_id == "confirm-site-upload-mapping-btn":
            if not pending_upload:
                return (
                    dbc.Alert(
                        "No parsed upload is pending. Upload a file first.",
                        color="warning",
                    ),
                    no_update,
                    False,
                    True,
                    None,
                    [],
                    None,
                    [],
                    None,
                    [],
                    None,
                    [{"label": "(none)", "value": ""}],
                    "",
                )

            try:
                upload_token = pending_upload.get("upload_token")
                if not upload_token:
                    return (
                        dbc.Alert(
                            "Upload token missing. Please upload the file again.",
                            color="danger",
                        ),
                        no_update,
                        True,
                        no_update,
                        *([no_update] * 9),
                    )

                mapping = {
                    "site_id": mapped_site_id or None,
                    "site_name": mapped_site_name or None,
                    "start_date": mapped_start_date,
                    "end_date": mapped_end_date or None,
                }
                upload = create_user_site_upload(
                    user.id,
                    pending_upload.get("filename"),
                    upload_token,
                    n_features=pending_upload.get("n_features", 0),
                    column_mapping=mapping,
                )
                status = upload.get("status")
                error = upload.get("error_message")
                if status in {"failed", "cancelled"}:
                    feedback = dbc.Alert(
                        f"Import {status}. {error or 'See Recent Import History for details.'}",
                        color="danger" if status == "failed" else "warning",
                    )
                elif status == "pending" and error:
                    feedback = dbc.Alert(
                        f"Import request saved. Dispatch is delayed: {error} "
                        "Automatic recovery will retry. Track progress in "
                        "Recent Import History below.",
                        color="warning",
                    )
                else:
                    feedback = dbc.Alert(
                        f"Queued background import for '{upload['filename']}'. "
                        "Track progress in Recent Import History below.",
                        color="success",
                    )
                return (
                    feedback,
                    str(_uuid.uuid4()),
                    False,
                    True,
                    None,
                    [],
                    None,
                    [],
                    None,
                    [],
                    None,
                    [{"label": "(none)", "value": ""}],
                    "",
                )
            except ValueError as exc:
                return (
                    dbc.Alert(str(exc), color="danger"),
                    no_update,
                    True,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                )
            except Exception:
                logger.exception("Failed to queue mapped site upload")
                report_exception()
                return (
                    dbc.Alert("Failed to queue mapped site upload.", color="danger"),
                    no_update,
                    True,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                )

        raise PreventUpdate

    @app.callback(
        [
            Output("admin-combined-site-table", "rowData"),
            Output("admin-combined-site-count", "children"),
            Output("admin-site-import-history", "rowData"),
        ],
        [
            Input("admin-refresh-interval", "n_intervals"),
            Input("site-set-refresh-store", "data"),
            Input("admin-show-archived-site-sets", "value"),
        ],
    )
    def refresh_admin_site_upload_views(_n_intervals, _refresh_token, show_archived):
        user = get_current_user()
        if not user:
            raise PreventUpdate

        site_sets = list_user_site_sets(user.id, include_archived=True)
        uploads = list_user_site_uploads(user.id)

        rows = _site_catalog_rows(site_sets, bool(show_archived))
        history = _site_import_history_rows(uploads, site_sets)
        archived_count = sum(1 for r in rows if r.get("is_archived"))
        running_count = sum(1 for r in uploads if r.get("status") == "running")
        queued_count = sum(1 for r in uploads if r.get("status") == "pending")

        return (
            rows,
            (
                f"Site sets: {len(rows)} | Archived: {archived_count}"
                f" | Imports pending: {queued_count} | Running: {running_count}"
            ),
            history,
        )

    @app.callback(
        [
            Output("admin-combined-site-action-status", "children"),
            Output("site-set-refresh-store", "data", allow_duplicate=True),
        ],
        Input("admin-combined-site-table", "cellRendererData"),
        Input("admin-site-import-history", "cellRendererData"),
        prevent_initial_call=True,
    )
    def handle_admin_combined_site_row_actions(catalog_data, history_data):
        triggered = callback_context.triggered_id
        renderer_data = (
            history_data if triggered == "admin-site-import-history" else catalog_data
        )
        if not renderer_data:
            raise PreventUpdate

        value = renderer_data.get("value") or {}
        action = value.get("action")
        if not action:
            raise PreventUpdate

        user = get_current_user()
        if not user:
            return dbc.Alert("Login required.", color="danger"), no_update

        try:
            if action == "cancel_import":
                upload_id = value.get("upload_id")
                if not upload_id:
                    raise PreventUpdate
                success, message = cancel_user_site_upload(upload_id, user.id)
            elif action == "rename_site_set":
                site_set_id = value.get("site_set_id")
                new_name = value.get("new_name")
                if not site_set_id:
                    raise PreventUpdate
                success, message = rename_user_site_set(site_set_id, user.id, new_name)
            elif action == "toggle_archive_site_set":
                site_set_id = value.get("site_set_id")
                if not site_set_id:
                    raise PreventUpdate
                success, message = archive_user_site_set(site_set_id, user.id)
            elif action == "delete_site_set":
                site_set_id = value.get("site_set_id")
                if not site_set_id:
                    raise PreventUpdate
                success, message = delete_user_site_set(site_set_id, user.id)
            elif action == "delete_upload":
                upload_id = value.get("upload_id")
                if not upload_id:
                    raise PreventUpdate
                success, message = delete_user_site_upload(upload_id, user.id)
            else:
                raise PreventUpdate

            color = "success" if success else "warning"
            return dbc.Alert(message, color=color), str(_uuid.uuid4())
        except Exception:
            logger.exception("Failed admin combined site action: %s", action)
            report_exception()
            return dbc.Alert("Failed to update.", color="danger"), no_update

    @app.callback(
        Output("site-upload-mapping-status", "children"),
        Input("mapping-site-id", "value"),
        Input("mapping-site-name", "value"),
        Input("mapping-start-date", "value"),
        Input("mapping-end-date", "value"),
        State("site-upload-columns-store", "data"),
        prevent_initial_call=True,
    )
    def validate_site_upload_mapping_selection(
        mapped_site_id,
        mapped_site_name,
        mapped_start_date,
        mapped_end_date,
        pending_upload,
    ):
        if not pending_upload:
            return None

        required = {
            "start_date": mapped_start_date,
        }
        missing = [key for key, value in required.items() if not value]
        if missing:
            return dbc.Alert(
                f"Select source columns for required fields: {', '.join(missing)}.",
                color="warning",
                className="mb-0 py-2",
            )

        chosen = [
            column
            for column in (
                mapped_site_id,
                mapped_site_name,
                mapped_start_date,
                mapped_end_date,
            )
            if column
        ]
        duplicates = sorted({c for c in chosen if chosen.count(c) > 1})
        if duplicates:
            return dbc.Alert(
                f"Each field must map to a distinct source column. Duplicate selection(s): {', '.join(duplicates)}.",
                color="danger",
                className="mb-0 py-2",
            )

        available = set(pending_upload.get("available_columns") or [])
        unknown = [c for c in chosen if c not in available]
        if unknown:
            return dbc.Alert(
                "Selected column(s) are no longer available in the uploaded file. Please upload again.",
                color="danger",
                className="mb-0 py-2",
            )

        return dbc.Alert(
            (
                "Mapping looks good. "
                "Full validation runs in the background import when you click Confirm Mapping and Start Import."
            ),
            color="success",
            className="mb-0 py-2",
        )

    @app.callback(
        [
            Output("parsed-sites-store", "data"),
            Output("site-preview", "children"),
            Output("site-preview-map", "children"),
            Output("site-set-metadata", "children"),
        ],
        Input("site-set-selector", "value"),
        Input("resolution-m", "value"),
        prevent_initial_call=False,
    )
    def load_selected_site_set(site_set_id, resolution_m_str):
        """Load and display selected site set, with resilience for large datasets.

        For datasets with >5000 features, loads a simplified preview to prevent
        504 Gateway Timeouts. The full dataset is used for task submission.
        """
        resolution_m = int(resolution_m_str) if resolution_m_str else 1000
        if not site_set_id:
            return (
                None,
                html.P(
                    "Upload or select a site set to preview sites.",
                    className="text-muted",
                ),
                html.P("No map to display yet.", className="text-muted small"),
                html.Small("No site set selected.", className="text-muted"),
            )

        user = get_current_user()
        if not user:
            raise PreventUpdate

        try:
            detail = get_user_site_set_detail(site_set_id, user.id)
            if not detail:
                return (
                    None,
                    html.P("Selected site set was not found.", className="text-danger"),
                    html.P("No map to display.", className="text-muted small"),
                    html.Small("Site set unavailable.", className="text-danger"),
                )

            preview_cols = [
                {
                    "field": "feature_id",
                    "hide": True,
                    **server_text_filter(),
                },
                {
                    "headerName": "Site ID",
                    "field": "site_id",
                    "flex": 1,
                    "minWidth": 110,
                    **server_text_filter(),
                    "sortable": True,
                },
                {
                    "headerName": "Site Name",
                    "field": "site_name",
                    "flex": 2,
                    "minWidth": 160,
                    **server_text_filter(),
                    "sortable": True,
                },
                {
                    "headerName": "Start Date",
                    "field": "start_date",
                    "flex": 1,
                    "minWidth": 120,
                    **server_date_filter(),
                    "sortable": True,
                },
                {
                    "headerName": "End Date",
                    "field": "end_date",
                    "flex": 1,
                    "minWidth": 120,
                    **server_date_filter(),
                    "sortable": True,
                },
            ]
            preview_table = _make_ag_grid(
                "site-preview-table",
                preview_cols,
                row_model="infinite",
                height="320px",
                grid_options_extra={
                    "rowSelection": {
                        "mode": "singleRow",
                        "enableClickSelection": True,
                    },
                    "getRowId": {"function": "params.data.feature_id"},
                },
            )

            # Build metadata (no longer shows a "sampled preview" note since
            # the map now uses MVT tiles and shows all sites via centroids).
            metadata_items = [
                html.Small(f"Name: {detail['name']}", className="d-block text-muted"),
                html.Small(
                    f"Source file: {detail['filename']} ({detail['file_size_bytes']:,} bytes)",
                    className="d-block text-muted",
                ),
                html.Small(
                    f"Uploaded: {(detail['uploaded_at'] or '').replace('T', ' ')[:19]} UTC",
                    className="d-block text-muted",
                ),
            ]

            metadata = html.Div(metadata_items)

            store_data = {
                "site_set_id": detail["id"],
                "n_sites": detail["n_sites"],
                "filename": detail["filename"],
                "name": detail["name"],
            }

            tile_url = f"/api/sites-tiles/{site_set_id}/{{z}}/{{x}}/{{y}}"
            return (
                store_data,
                preview_table,
                _openlayers_map_component(
                    "submit-sites-map",
                    None,
                    height="500px",
                    enable_cog_layers=True,
                    resolution_m=resolution_m,
                    tile_url=tile_url,
                    bounds=detail.get("bounds"),
                ),
                metadata,
            )
        except Exception as exc:
            logger.exception("Error loading site set %s", site_set_id)
            error_msg = str(exc)[:200]  # Truncate long error messages
            return (
                None,
                html.P(
                    f"Error loading site set: {error_msg}",
                    className="text-danger",
                ),
                html.P("No map to display.", className="text-muted small"),
                html.Small("Site set load failed.", className="text-danger"),
            )

    # -- Task submission -----------------------------------------------------

    @app.callback(
        Output("site-preview-table", "getRowsResponse"),
        Input("site-preview-table", "getRowsRequest"),
        State("parsed-sites-store", "data"),
        prevent_initial_call=True,
    )
    def supply_preview_rows(request, sites_data):
        """Feed paginated rows to the site-preview AG Grid (Infinite Row Model).

        The grid requests a block of rows as the user scrolls; this callback
        queries only that block from the database so the full 150 K+ row set
        never lands in a single Dash response.  Sort and filter state from
        the AG Grid request are forwarded to the database query.
        """
        if not request or not sites_data:
            raise PreventUpdate
        site_set_id = sites_data.get("site_set_id")
        if not site_set_id:
            raise PreventUpdate
        user = get_current_user()
        if not user:
            raise PreventUpdate

        if not get_user_site_set_detail(site_set_id, user.id):
            return {"rowData": [], "rowCount": 0}

        start = request.get("startRow", 0)
        end = request.get("endRow", 100)
        filter_model = request.get("filterModel") or {}
        sort_model = request.get("sortModel") or []
        rows = get_user_site_set_preview_rows(
            site_set_id,
            start_row=start,
            end_row=end,
            filter_model=filter_model,
            sort_model=sort_model,
        )
        row_count = sites_data.get("n_sites", -1) if not filter_model else -1
        if row_count == -1 and len(rows) < end - start:
            row_count = start + len(rows)
        return {"rowData": rows, "rowCount": row_count}

    @app.callback(
        Output("submit-lock-store", "data", allow_duplicate=True),
        Input("submit-task-button", "n_clicks"),
        prevent_initial_call=True,
    )
    def lock_submit_button(_n_clicks):
        return True

    @app.callback(
        [
            Output("submit-task-button", "disabled"),
            Output("submit-task-button", "children"),
            Output("submit-progress-message", "children"),
        ],
        Input("submit-lock-store", "data"),
        Input({"type": "submit-alert", "scope": ALL}, "is_open"),
    )
    def sync_submit_button_state(is_locked, alert_is_open_values):
        if not is_locked:
            return False, "Submit Task", None

        alert_is_open = any(alert_is_open_values or [])

        if alert_is_open:
            return (
                True,
                "Submit Task",
                html.Small(
                    "Close the message above to enable another submission.",
                    className="text-muted",
                ),
            )

        return (
            True,
            "Submitting\u2026",
            dbc.Alert(
                "Submission in progress. Please wait\u2026",
                color="info",
                className="mb-0 py-2",
            ),
        )

    @app.callback(
        Output("submit-lock-store", "data", allow_duplicate=True),
        Input({"type": "submit-alert", "scope": ALL}, "is_open"),
        State("submit-lock-store", "data"),
        prevent_initial_call=True,
    )
    def unlock_submit_button_when_alert_closed(alert_is_open_values, is_locked):
        if not is_locked:
            raise PreventUpdate
        if not alert_is_open_values:
            raise PreventUpdate
        if any(alert_is_open_values):
            raise PreventUpdate
        return False

    @app.callback(
        [Output("submit-errors", "children"), Output("submit-result", "children")],
        Input("submit-task-button", "n_clicks"),
        State("task-name", "value"),
        State("task-description", "value"),
        State("parsed-sites-store", "data"),
        State("covariate-selection", "value"),
        State("exact-match-selection", "value"),
        State("max-treatment-pixels", "value"),
        State("control-multiplier", "value"),
        State("min-site-area-ha", "value"),
        State("min-glm-treatment-pixels", "value"),
        State("caliper-width", "value"),
        State("max-controls-per-treatment", "value"),
        State("min-control-distance-km", "value"),
        State("separation-fallback-mahalanobis", "value"),
        State("shared-support-controls", "value"),
        State("matching-method", "value"),
        State("n-replicates", "value"),
        State("random-seed", "value"),
        State("match-memory-gb", "value"),
        State("matching-job-queue", "value"),
        State("resolution-m", "value"),
        prevent_initial_call=True,
    )
    def handle_submit(
        n_clicks,
        name,
        description,
        sites_data,
        covariates,
        exact_match_vars,
        max_treatment_pixels,
        control_multiplier,
        min_site_area_ha,
        min_glm_treatment_pixels,
        caliper_width,
        max_controls_per_treatment,
        min_control_distance_km,
        separation_fallback_mahalanobis,
        shared_support_controls,
        matching_method,
        n_replicates,
        random_seed,
        match_memory_gb,
        matching_job_queue,
        resolution_m,
    ):
        def _error_alert(msg):
            return (
                dbc.Alert(
                    msg,
                    id={"type": "submit-alert", "scope": "task-submit"},
                    color="danger",
                    dismissable=True,
                    is_open=True,
                ),
                None,
            )

        if not name:
            return _error_alert("Please enter a task name.")
        if not sites_data:
            return _error_alert("Please upload a sites file.")
        if not covariates:
            return _error_alert("Please select at least one covariate.")
        if not exact_match_vars:
            return _error_alert(
                "Please select at least one exact match variable "
                "(admin boundary, ecoregion, or protected area)."
            )

        overlap = set(covariates) & set(exact_match_vars)
        if overlap:
            return _error_alert(
                "The following variables are selected as both covariates "
                "and exact matches — each must be one or the other: "
                + ", ".join(sorted(overlap))
            )

        user = get_current_user()
        if not user:
            return _error_alert("Please log in first.")

        try:
            site_set_id = sites_data.get("site_set_id") if sites_data else None
            if not site_set_id:
                return _error_alert(
                    "Site set information is missing. Please reload and try again."
                )

            # Server-side bounds validation (mirrors the HTML input
            # min/max attributes so tampered requests are rejected).
            _mtp = int(
                max_treatment_pixels or ANALYSIS_DEFAULTS["max_treatment_pixels"]
            )
            _cm = int(control_multiplier or ANALYSIS_DEFAULTS["control_multiplier"])
            _msa = int(min_site_area_ha or ANALYSIS_DEFAULTS["min_site_area_ha"])
            _mglm = int(
                min_glm_treatment_pixels
                or ANALYSIS_DEFAULTS["min_glm_treatment_pixels"]
            )
            _cw = float(
                caliper_width
                if caliper_width is not None
                else ANALYSIS_DEFAULTS["caliper_width"]
            )
            _mcpt = int(
                max_controls_per_treatment
                if max_controls_per_treatment is not None
                else ANALYSIS_DEFAULTS["max_controls_per_treatment"]
            )
            _mcd = int(
                min_control_distance_km
                if min_control_distance_km is not None
                else ANALYSIS_DEFAULTS["min_control_distance_km"]
            )
            _seed = int(random_seed) if random_seed not in (None, "") else None
            _mmgb = int(match_memory_gb or ANALYSIS_DEFAULTS["match_memory_gb"])
            _nrep = int(n_replicates or ANALYSIS_DEFAULTS["n_replicates"])

            bounds = [
                (_mtp, 1, 100_000, "Max treatment pixels"),
                (_cm, 1, 500, "Control multiplier"),
                (_msa, 0, 100_000, "Minimum site area"),
                (_mglm, 1, 10_000, "Min GLM treatment pixels"),
                (_mcpt, 0, 100, "Max controls per treatment"),
                (_mcd, 0, 500, "Min control distance (km)"),
                (_mmgb, 1, 240, "Matching memory (GB)"),
                (_nrep, 1, 1000, "Number of replicates"),
            ]
            for val, lo, hi, label in bounds:
                if val < lo or val > hi:
                    return _error_alert(f"{label} must be between {lo} and {hi}.")
            if _cw < 0 or _cw > 5.0:
                return _error_alert("Caliper width must be between 0 and 5.0.")
            if _seed is not None and (_seed < 1 or _seed > 2_147_483_647):
                return _error_alert("Random seed must be between 1 and 2147483647.")

            _res = int(resolution_m or ANALYSIS_DEFAULTS["resolution_m"])
            if _res not in (1000, 250):
                return _error_alert("Resolution must be 1000 or 250.")

            task_id = queue_analysis_task(
                task_name=name,
                description=description or "",
                user_id=user.id,
                session_id=user.session_id,
                submitter_email=user.email,
                submitter_name=user.name,
                site_set_id=site_set_id,
                covariates=covariates,
                exact_match_vars=exact_match_vars,
                max_treatment_pixels=_mtp,
                control_multiplier=_cm,
                min_site_area_ha=_msa,
                min_glm_treatment_pixels=_mglm,
                caliper_width=_cw,
                max_controls_per_treatment=_mcpt,
                min_control_distance_km=_mcd,
                separation_fallback_mahalanobis=bool(separation_fallback_mahalanobis),
                shared_support_controls=bool(shared_support_controls),
                matching_method=matching_method or ANALYSIS_DEFAULTS["matching_method"],
                random_seed=_seed,
                n_replicates=_nrep,
                match_memory_mib=_mmgb * 1024,
                matching_job_queue=matching_job_queue,
                resolution_m=_res,
            )

            return None, dbc.Alert(
                [
                    html.P(
                        "Task queued for submission. It will appear as "
                        "\u2018submitting\u2019 within a few seconds."
                    ),
                    dcc.Link(f"View task: {task_id}", href=f"/task/{task_id}"),
                ],
                id={"type": "submit-alert", "scope": "task-submit"},
                color="success",
                dismissable=True,
                is_open=True,
            )

        except ValueError as exc:
            logger.exception("Task submission failed (validation)")
            report_exception()
            return _error_alert(str(exc))

        except Exception:
            logger.exception("Task submission failed")
            report_exception()
            return _error_alert(
                "Submission failed. Please try again or contact support."
            )

    # -- Dashboard task list (AG Grid) ---------------------------------------

    @app.callback(
        [
            Output("task-list-table", "rowData"),
            Output("task-list-table", "columnDefs"),
            Output("task-total-count", "children"),
        ],
        [
            Input("refresh-interval", "n_intervals"),
            Input("refresh-tasks-btn", "n_clicks"),
            Input("show-all-tasks-checkbox", "value"),
            Input("task-list-action-refresh", "data"),
        ],
    )
    def refresh_task_list(_n_intervals, _n_clicks, show_all, _action_refresh=None):
        user = get_current_user()
        if not user:
            raise PreventUpdate

        show_all_users = bool(show_all) and user.is_admin
        user_filter = None if show_all_users else user.id
        tasks = get_task_list(user_id=user_filter)

        columns = list(TASK_LIST_COLUMNS)
        if show_all_users:
            columns = [
                columns[0],
                {
                    "headerName": "Submitted By",
                    "field": "submitted_by_name",
                    "flex": 1.2,
                    "minWidth": 140,
                    **TEXT_FILTER,
                    "filterParams": {
                        "buttons": ["clear", "apply"],
                        "closeOnApply": True,
                    },
                },
                *columns[1:],
            ]

        if not tasks:
            return [], columns, "Total: 0"

        rows = []
        now = datetime.now(UTC)
        for task in tasks:
            cfg = task.config or {}
            covariates = task.covariates or []
            elapsed_end = task.completed_at
            if elapsed_end is None and task.status in ("submitted", "running"):
                elapsed_end = now
            row = {
                "id": str(task.id),
                "name": task.name,
                "status": task.status,
                "can_resume": can_resume_task(task, user),
                "has_logs": bool(
                    (task.extract_job_id or "").startswith("api:")
                    and task.extract_job_id[4:]
                ),
                "progress": task.progress,
                "n_sites": task.n_sites or 0,
                "resolution_m": cfg.get("resolution_m")
                or ANALYSIS_DEFAULTS["resolution_m"],
                "params": {**cfg, "covariates": covariates},
                "matching_method": cfg.get("matching_method", "optimal"),
                "created_at": _fmt_dt(task.created_at),
                "submitted_at": _fmt_dt(task.submitted_at),
                "completed_at": _fmt_dt(task.completed_at),
                "elapsed_seconds": (
                    int((elapsed_end - task.submitted_at).total_seconds())
                    if elapsed_end is not None and task.submitted_at is not None
                    else None
                ),
                "error_message": task.error_message or "",
            }
            if show_all_users:
                row["submitted_by_name"] = (
                    task.submitter_name or task.submitter_email or "Unknown"
                )
            rows.append(row)

        return rows, columns, f"Total: {len(rows)}"

    @app.callback(
        Output("task-params-modal", "is_open"),
        Output("task-params-title", "children"),
        Output("task-params-body", "children"),
        Input("task-list-table", "cellRendererData"),
        Input("task-params-close", "n_clicks"),
        prevent_initial_call=True,
    )
    def show_task_params(renderer_data, _close):
        if callback_context.triggered_id == "task-params-close":
            return False, no_update, no_update
        value = (renderer_data or {}).get("value") or {}
        if value.get("action") != "params":
            raise PreventUpdate
        return (
            True,
            f"Params: {value['task_name']}",
            html.Pre(
                json.dumps(value["params"], indent=2, sort_keys=True),
                className="bg-light border rounded p-3 mb-0",
                style={"whiteSpace": "pre-wrap", "overflowWrap": "anywhere"},
            ),
        )

    # -- Task detail ---------------------------------------------------------

    @app.callback(
        [
            Output("task-title", "children"),
            Output("task-status-badge", "children"),
            Output("task-overview", "children"),
            Output("task-results-content", "children"),
            Output("task-plots", "children"),
            Output("task-raw-results", "children"),
            Output("detail-refresh-interval", "disabled"),
            Output("resume-task-control", "style"),
            Output("cancel-task-control", "style"),
        ],
        [
            Input("detail-refresh-interval", "n_intervals"),
            Input("detail-tabs", "active_tab"),
            Input("task-resume-refresh", "data"),
        ],
        [
            State("task-id-store", "data"),
            State("share-token-store", "data"),
            State("task-plot-selection", "data"),
        ],
    )
    @timed_task_step("task_detail")
    def refresh_task_detail(
        n, active_tab, resume_event, task_id, share_token, plot_selection=None
    ):
        if not task_id:
            raise PreventUpdate

        if not _authorize_task_access(task_id, share_token):
            return (
                "Task Not Found",
                None,
                None,
                None,
                None,
                None,
                True,
                {"display": "none"},
                {"display": "none"},
            )

        # Batch task status is polled by the Celery Beat worker;
        # this callback just reads the current DB state.
        section = {
            "tab-overview": "overview",
            "tab-results": "results",
            "tab-plots": "plots",
            "tab-raw-results": "raw",
        }.get(active_tab, "header")
        detail = get_task_detail(task_id, section=section)
        if not detail:
            return (
                "Task Not Found",
                None,
                None,
                None,
                None,
                None,
                True,
                {"display": "none"},
                {"display": "none"},
            )

        task = detail["task"]
        sites = detail["sites"]
        results = detail["results"]
        totals = detail["totals"]
        is_large = detail.get("is_large", False)
        agg_yearly = detail.get("agg_yearly") or []

        # Disable the refresh interval once the task reaches a terminal
        # state so periodic re-renders don't reset interactive widgets
        # (e.g. the site-level drill-down dropdown).
        terminal_states = {"succeeded", "failed", "cancelled"}
        disable_interval = task.status in terminal_states

        # Title and status badge
        title = task.name
        status_color = {
            "pending": "secondary",
            "submitted": "info",
            "running": "primary",
            "succeeded": "success",
            "failed": "danger",
            "cancelled": "warning",
        }.get(task.status, "secondary")
        badge = dbc.Badge(task.status.upper(), color=status_color, className="fs-5")

        # Quality warnings are shown in the Overview's site status table.
        quality_warnings = []
        if task.status == "succeeded":
            quality_warnings = _compute_quality_warnings(task_id, task, totals)

        # -----------------------------------------------------------
        # Lazy tab rendering: only build content for the *active* tab.
        # This keeps the heavy plots and map lazy.
        # Inactive tabs return ``no_update`` so Dash keeps whatever
        # was last rendered (empty on first visit, populated once the
        # user clicks the tab).
        # -----------------------------------------------------------
        overview = no_update
        results_content = no_update
        plots = no_update
        raw_results = no_update

        if active_tab == "tab-overview":
            overview = _build_overview(
                task, sites, totals, quality_warnings=quality_warnings
            )
        elif active_tab == "tab-results":
            results_content = _build_results_content(
                results,
                totals,
                sites,
                quality_warnings=quality_warnings,
                agg_yearly=agg_yearly,
                is_large=is_large,
                n_sites=task.n_sites or 0,
                defer_yearly=True,
            )
        elif active_tab == "tab-plots":
            if results or agg_yearly:
                plots = _build_plots(
                    results,
                    totals,
                    sites,
                    task=task,
                    quality_warnings=quality_warnings,
                    agg_yearly=agg_yearly,
                    is_large=is_large,
                    defer_map=True,
                    site_filter_attributes=detail.get("site_filter_attributes"),
                    plot_selection=plot_selection,
                    bar_site_data=(
                        get_task_top_site_yearly(task_id, [t.site_id for t in totals])
                        if is_large
                        else None
                    ),
                )
            else:
                plots = html.P("Results not yet available.", className="text-muted")
        elif active_tab == "tab-raw-results":
            raw_results = _build_raw_results(task)

        return (
            title,
            badge,
            overview,
            results_content,
            plots,
            raw_results,
            disable_interval,
            (
                {}
                if not share_token and can_resume_task(task, get_current_user())
                else {"display": "none"}
            ),
            (
                {}
                if not share_token and task.status not in terminal_states
                else {"display": "none"}
            ),
        )

    @app.callback(
        Output(SITE_QUALITY_GRID_ID, "getRowsResponse"),
        Input(SITE_QUALITY_GRID_ID, "getRowsRequest"),
        State("task-id-store", "data"),
        State("share-token-store", "data"),
        prevent_initial_call=True,
    )
    @timed_task_step("task_quality_grid")
    def load_task_quality_rows(request, task_id, share_token):
        if not request or not _authorize_task_access(task_id, share_token):
            raise PreventUpdate
        start, end = request.get("startRow"), request.get("endRow")
        if (
            type(start) is not int
            or type(end) is not int
            or start < 0
            or end <= start
            or end - start > SITE_QUALITY_BLOCK_SIZE
        ):
            logger.warning("Invalid row range requested for the task quality grid")
            raise PreventUpdate
        detail = get_task_detail(task_id, section="totals")
        if not detail or detail["task"].status != "succeeded":
            raise PreventUpdate
        totals = detail["totals"]
        warnings = _compute_quality_warnings(task_id, detail["task"], totals)
        return _site_quality_rows(warnings, totals, start, end)

    @app.callback(
        Output(SITE_STATUS_GRID_ID, "getRowsResponse"),
        Input(SITE_STATUS_GRID_ID, "getRowsRequest"),
        State("task-id-store", "data"),
        State("share-token-store", "data"),
        prevent_initial_call=True,
    )
    @timed_task_step("task_site_status_grid")
    def load_task_site_status_rows(request, task_id, share_token):
        if not request or not _authorize_task_access(task_id, share_token):
            raise PreventUpdate
        start, end = request.get("startRow"), request.get("endRow")
        if (
            type(start) is not int
            or type(end) is not int
            or start < 0
            or end <= start
            or end - start > SITE_STATUS_BLOCK_SIZE
        ):
            logger.warning("Invalid row range requested for the site status grid")
            raise PreventUpdate
        detail = get_task_detail(task_id, section="totals")
        if not detail:
            raise PreventUpdate
        task, totals = detail["task"], detail["totals"]
        succeeded = task.status == "succeeded"
        warnings = _compute_quality_warnings(task_id, task, totals) if succeeded else []
        rows = build_site_status_rows(
            task.extra_metadata or {},
            totals,
            get_task_site_index(task_id),
            warnings,
            has_results=succeeded,
        )
        return site_status_block(rows, request)

    @app.callback(
        Output("task-yearly-results", "children"),
        Input("task-yearly-request", "data"),
        State("task-id-store", "data"),
        State("share-token-store", "data"),
    )
    @timed_task_step("task_yearly_grid")
    def load_task_yearly_results(requested, task_id, share_token):
        if not requested or not _authorize_task_access(task_id, share_token):
            raise PreventUpdate
        detail = get_task_detail(task_id, section="yearly")
        if not detail:
            raise PreventUpdate
        return _build_yearly_results(detail["results"])

    @app.callback(
        Output("task-map-content", "children"),
        Output("site-filter-attributes", "data"),
        Input("task-map-request", "data"),
        State("task-id-store", "data"),
        State("share-token-store", "data"),
    )
    @timed_task_step("task_map")
    def load_task_map(requested, task_id, share_token):
        if not requested or not _authorize_task_access(task_id, share_token):
            raise PreventUpdate
        detail = get_task_detail(task_id, section="map")
        if not detail:
            raise PreventUpdate
        task = detail["task"]
        geojson = detail["sites_geojson"]
        attributes = {}
        for feature in (geojson or {}).get("features", []):
            props = feature.get("properties") or {}
            # The map never reads source attributes; they can be very large.
            source_attributes = props.pop("source_attributes", None)
            if props.get("site_id") is not None:
                attributes[str(props["site_id"])] = filterable_source_attributes(
                    source_attributes
                )
        return (
            _build_map(
                geojson,
                detail["totals"],
                covariates=task.covariates,
                task_id=task_id,
                resolution_m=(task.config or {}).get("resolution_m"),
                share_token=share_token,
                use_tiles=bool(task.site_set_id),
            ),
            no_update if task.site_set_id else attributes,
        )

    # -- Result downloads ----------------------------------------------------

    @app.callback(
        Output("download-replay-links", "data"),
        Output("download-replay-links-status", "children"),
        Input("download-replay-links-btn", "n_clicks"),
        State("task-id-store", "data"),
        State("share-token-store", "data"),
        prevent_initial_call=True,
        running=[(Output("download-replay-links-btn", "disabled"), True, False)],
    )
    def download_matching_replay_links(n_clicks, task_id, share_token):
        if not n_clicks:
            raise PreventUpdate
        if not _authorize_task_access(task_id, share_token):
            return no_update, dbc.Alert(
                "Task not found or access denied.", color="danger"
            )
        task = get_task_header(task_id)
        if not task or task.status != "succeeded":
            return no_update, dbc.Alert(
                "Matching replay files are unavailable.", color="warning"
            )
        try:
            links = "\n".join(matching_replay_links(task.id, task.results_s3_uri))
        except (BotoCoreError, ClientError):
            logger.exception(
                "Could not prepare matching replay links for task %s", task_id
            )
            return no_update, dbc.Alert(
                "Could not prepare matching replay links. Please try again.",
                color="danger",
            )
        if not links:
            return no_update, dbc.Alert(
                "No matching replay files were found.", color="warning"
            )
        return (
            dcc.send_string(
                links + "\n", "matching_replay_links.txt", type="text/plain"
            ),
            None,
        )

    @app.callback(
        Output("download-results", "data"),
        [
            Input("download-by-year", "n_clicks"),
            Input("download-totals", "n_clicks"),
            Input("download-site-identity", "n_clicks"),
        ],
        [
            State("task-id-store", "data"),
            State("share-token-store", "data"),
        ],
        prevent_initial_call=True,
    )
    def handle_download(
        by_year_clicks, total_clicks, identity_clicks, task_id, share_token
    ):
        ctx = callback_context
        if not ctx.triggered:
            raise PreventUpdate

        if not _authorize_task_access(task_id, share_token):
            raise PreventUpdate

        trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]
        downloads = {
            "download-by-year": ("by_site_year", "results_by_site_year.csv"),
            "download-totals": ("by_site_total", "results_by_site_total.csv"),
            "download-site-identity": ("site_identity", "site_identity.csv"),
        }
        if trigger_id not in downloads:
            raise PreventUpdate
        result_type, filename = downloads[trigger_id]
        csv = download_results_csv(task_id, result_type)

        if csv:
            return {"content": csv, "filename": filename}
        return no_update

    # -- Match quality download -----------------------------------------------

    @app.callback(
        Output("download-match-quality", "data"),
        Input("download-match-covariates", "n_clicks"),
        [
            State("task-id-store", "data"),
            State("share-token-store", "data"),
        ],
        prevent_initial_call=True,
    )
    def handle_match_quality_download(n_clicks, task_id, share_token):
        if not n_clicks:
            raise PreventUpdate

        if not _authorize_task_access(task_id, share_token):
            raise PreventUpdate

        csv = download_results_csv(task_id, "match_covariates")
        if csv:
            return {"content": csv, "filename": "results_pixel_covariates.csv"}
        return no_update

    # -- Share modal ----------------------------------------------------------

    @app.callback(
        [
            Output("share-modal", "is_open"),
            Output("share-links-list", "children"),
        ],
        [
            Input("open-share-modal", "n_clicks"),
        ],
        [
            State("share-modal", "is_open"),
            State("task-id-store", "data"),
            State("task-plot-selection", "data"),
            State("detail-tabs", "active_tab"),
        ],
        prevent_initial_call=True,
    )
    def toggle_share_modal(n_clicks, is_open, task_id, selection=None, active_tab=None):
        if not n_clicks:
            raise PreventUpdate

        user = get_current_user()
        if not user or not _check_task_access(task_id, user):
            raise PreventUpdate

        if is_open:
            # Closing — return empty list to avoid stale data
            return False, html.Div()

        # Opening — fetch existing links
        links = list_share_links(task_id, str(user.id), is_admin=user.is_admin)
        return True, _render_share_links_list(links, task_id, selection, active_tab)

    @app.callback(
        [
            Output("share-link-result", "children"),
            Output("share-links-list", "children", allow_duplicate=True),
        ],
        Input("generate-share-link", "n_clicks"),
        [
            State("share-expiry-days", "value"),
            State("task-id-store", "data"),
            State("task-plot-selection", "data"),
            State("detail-tabs", "active_tab"),
        ],
        prevent_initial_call=True,
    )
    def generate_share(n_clicks, expiry_days, task_id, selection=None, active_tab=None):
        if not n_clicks:
            raise PreventUpdate

        user = get_current_user()
        if not user or not _check_task_access(task_id, user):
            raise PreventUpdate

        try:
            result = create_share_link(
                task_id,
                str(user.id),
                expiry_days=int(expiry_days),
                is_admin=user.is_admin,
            )
            from flask import request as flask_request

            base_url = flask_request.host_url.rstrip("/")
            share_url = f"{base_url}/shared/{result['token']}" + plot_selection_query(
                selection, active_tab
            )

            link_display = html.Div(
                [
                    dbc.Alert(
                        [
                            html.Strong("Share link created!"),
                            html.Br(),
                            html.Span(
                                "Copy and share this URL:",
                                className="text-muted",
                            ),
                            dbc.InputGroup(
                                [
                                    dbc.Input(
                                        value=share_url,
                                        id="share-url-input",
                                        readonly=True,
                                        size="sm",
                                    ),
                                    dcc.Clipboard(
                                        target_id="share-url-input",
                                        className="btn btn-outline-secondary btn-sm",
                                        style={"display": "inline-block"},
                                    ),
                                ],
                                className="mt-2",
                                size="sm",
                            ),
                            html.Small(
                                f"Expires: {result['expires_at'][:10]}",
                                className="text-muted mt-1 d-block",
                            ),
                        ],
                        color="success",
                        className="mt-2",
                    ),
                ]
            )

            # Refresh the list
            links = list_share_links(task_id, str(user.id), is_admin=user.is_admin)
            return link_display, _render_share_links_list(
                links, task_id, selection, active_tab
            )
        except Exception:
            logger.exception("Failed to create share link")
            report_exception()
            return (
                dbc.Alert("Failed to create share link.", color="danger"),
                no_update,
            )

    @app.callback(
        Output("share-links-list", "children", allow_duplicate=True),
        Input({"type": "revoke-share-link", "index": ALL}, "n_clicks"),
        State("task-id-store", "data"),
        State("task-plot-selection", "data"),
        State("detail-tabs", "active_tab"),
        prevent_initial_call=True,
    )
    def revoke_share(n_clicks_list, task_id, selection=None, active_tab=None):
        ctx = callback_context
        if not ctx.triggered or not any(n_clicks_list):
            raise PreventUpdate

        user = get_current_user()
        if not user or not _check_task_access(task_id, user):
            raise PreventUpdate

        trigger = ctx.triggered[0]
        import json as _json

        link_id = _json.loads(trigger["prop_id"].rsplit(".", 1)[0])["index"]
        revoke_share_link(
            link_id, str(user.id), task_id=task_id, is_admin=user.is_admin
        )

        links = list_share_links(task_id, str(user.id), is_admin=user.is_admin)
        return _render_share_links_list(links, task_id, selection, active_tab)

    # -- Edit task name/description -------------------------------------------

    @app.callback(
        [
            Output("edit-task-modal", "is_open"),
            Output("edit-task-name", "value"),
            Output("edit-task-description", "value"),
            Output("edit-task-result", "children"),
        ],
        [
            Input("open-edit-modal", "n_clicks"),
            Input("cancel-edit-task", "n_clicks"),
        ],
        [
            State("edit-task-modal", "is_open"),
            State("task-id-store", "data"),
        ],
        prevent_initial_call=True,
    )
    def toggle_edit_modal(open_clicks, cancel_clicks, is_open, task_id):
        if not callback_context.triggered:
            raise PreventUpdate

        # Closing
        if is_open:
            return False, no_update, no_update, html.Div()

        # Opening — populate inputs with current values
        user = get_current_user()
        if not user or not _check_task_access(task_id, user):
            raise PreventUpdate

        task = get_task_header(task_id)
        if not task:
            raise PreventUpdate
        return True, task.name, task.description or "", html.Div()

    @app.callback(
        [
            Output("edit-task-result", "children", allow_duplicate=True),
            Output("edit-task-modal", "is_open", allow_duplicate=True),
            Output("task-title", "children", allow_duplicate=True),
        ],
        Input("save-edit-task", "n_clicks"),
        [
            State("edit-task-name", "value"),
            State("edit-task-description", "value"),
            State("task-id-store", "data"),
        ],
        prevent_initial_call=True,
    )
    def save_task_edits(n_clicks, name, description, task_id):
        if not n_clicks:
            raise PreventUpdate

        user = get_current_user()
        if not user or not _check_task_access(task_id, user):
            raise PreventUpdate

        if not name or not name.strip():
            return (
                dbc.Alert("Name cannot be empty.", color="danger", className="mt-2"),
                no_update,
                no_update,
            )

        try:
            result = update_task_info(
                task_id, name=name, description=description, user_id=user.id
            )
            if not result:
                return (
                    dbc.Alert("Task not found.", color="danger", className="mt-2"),
                    no_update,
                    no_update,
                )
            return html.Div(), False, result["name"]
        except Exception:
            logger.exception("Failed to update task info")
            report_exception()
            return (
                dbc.Alert("Failed to save changes.", color="danger", className="mt-2"),
                no_update,
                no_update,
            )

    # -- Recompute task (detail page) ----------------------------------------

    app.clientside_callback(
        """
        function(n_clicks, task_id) {
            if (!n_clicks || !task_id) {
                return window.dash_clientside.no_update;
            }
            document.getElementById('recompute-task-btn').disabled = true;
            window.location.href = '/submit?recompute=' + task_id;
            return 'Opening the recompute form...';
        }
        """,
        Output("recompute-result", "children"),
        Input("recompute-task-btn", "n_clicks"),
        State("task-id-store", "data"),
        prevent_initial_call=True,
    )

    # -- Sort-order radio for site-level deforestation dropdown --------------

    app.clientside_callback(
        """
        function(sortOrder, filteredIds, storedOptions) {
            if (!storedOptions || !storedOptions[sortOrder]) {
                return window.dash_clientside.no_update;
            }
            if (filteredIds === null || filteredIds === undefined) {
                return storedOptions[sortOrder];
            }
            const allowed = new Set(filteredIds);
            return storedOptions[sortOrder].filter(option => allowed.has(option.value));
        }
        """,
        Output("site-defor-selector", "options"),
        Input("site-defor-sort-order", "value"),
        Input("site-filtered-ids", "data"),
        State("site-defor-sort-options", "data"),
    )

    @app.callback(
        Output("site-filter-field", "options"),
        Input("site-filter-attributes", "data"),
    )
    def update_site_filter_fields(attributes):
        fields = sorted({key for props in (attributes or {}).values() for key in props})
        return [{"label": name, "value": name} for name in fields]

    @app.callback(
        Output("site-filter-value", "options"),
        Output("site-filter-value", "disabled"),
        Input("site-filter-field", "value"),
        Input("site-filter-attributes", "data"),
    )
    def update_site_filter_values(field, attributes):
        return _site_filter_values(field, attributes), not bool(
            field
        ) or attributes is None

    @app.callback(
        Output("site-filtered-ids", "data"),
        Input("task-plot-selection", "data"),
        Input("site-filter-attributes", "data"),
    )
    def filter_site_ids(selection, attributes):
        selection = selection or {}
        if attributes is None and selection.get("field"):
            raise PreventUpdate
        return _filter_site_ids(
            selection.get("field"), selection.get("value"), attributes
        )

    @app.callback(
        Output("task-plot-selection", "data", allow_duplicate=True),
        Output("detail-tabs", "active_tab"),
        Input("task-view-navigation", "data"),
        State("task-id-store", "data"),
        prevent_initial_call=True,
    )
    def restore_task_view(navigation, task_id):
        if not navigation or navigation.get("task_id") != task_id:
            raise PreventUpdate
        search = navigation.get("search")
        return parse_plot_selection(search), parse_task_tab(search)

    @app.callback(
        Output("task-plot-selection", "data"),
        Output("site-filter-field", "value"),
        Output("site-filter-value", "value"),
        Output("site-defor-selector", "value"),
        Output("site-map-selection", "value"),
        Output("site-filter-value", "options", allow_duplicate=True),
        Output("site-filter-value", "disabled", allow_duplicate=True),
        Input("site-filter-field", "value"),
        Input("site-filter-value", "value"),
        Input("site-defor-selector", "value"),
        Input("site-map-selection", "value"),
        Input("site-map-clear", "data"),
        Input("clear-plot-selection-btn", "n_clicks"),
        State("task-plot-selection", "data"),
        Input("task-view-navigation", "data"),
        State("site-filter-attributes", "data"),
        prevent_initial_call=True,
    )
    def update_plot_selection(
        field,
        value,
        selected_site,
        map_site,
        map_clear,
        clear_all,
        selection,
        navigation=None,
        attributes=None,
    ):
        trigger = callback_context.triggered_id
        selection = selection or {}
        if trigger == "task-view-navigation":
            if not navigation:
                raise PreventUpdate
            restored = parse_plot_selection(navigation.get("search"))
            return (
                restored,
                restored.get("field"),
                restored.get("value"),
                restored.get("site"),
                "",
                _site_filter_values(restored.get("field"), attributes),
                not bool(restored.get("field")),
            )
        group_selection = (
            {"field": field, "value": value} if field and value is not None else {}
        )
        if trigger == "clear-plot-selection-btn" and clear_all:
            return {}, None, None, None, "", no_update, no_update
        if trigger == "site-map-clear" and map_clear:
            return group_selection, no_update, no_update, None, "", no_update, no_update
        if trigger == "site-map-selection" and map_site:
            return (
                {**group_selection, "site": map_site},
                no_update,
                no_update,
                map_site,
                no_update,
                no_update,
                no_update,
            )
        if trigger == "site-defor-selector":
            if selected_site == selection.get("site"):
                raise PreventUpdate
            if selected_site:
                group_selection["site"] = selected_site
            return (
                group_selection,
                no_update,
                no_update,
                no_update,
                "",
                no_update,
                no_update,
            )
        if trigger == "site-filter-field":
            if field == selection.get("field"):
                raise PreventUpdate
            return (
                {"field": field} if field else {},
                no_update,
                None,
                None,
                "",
                no_update,
                no_update,
            )
        if trigger == "site-filter-value":
            if field == selection.get("field") and value == selection.get("value"):
                raise PreventUpdate
            selection = (
                {"field": field, "value": value} if field and value is not None else {}
            )
            return selection, no_update, no_update, None, "", no_update, no_update
        raise PreventUpdate

    app.clientside_callback(
        ClientsideFunction(namespace="taskViewHistory", function_name="sync"),
        Output("task-view-navigation", "id"),
        Input("task-plot-selection", "data"),
        Input("detail-tabs", "active_tab"),
        State("task-id-store", "data"),
    )

    app.clientside_callback(
        """
        function(filteredIds, mapChildren) {
            const el = document.getElementById("task-sites-map");
            if (el) {
                el._allowedSiteIds = Array.isArray(filteredIds)
                    ? new Set(filteredIds.map(String)) : null;
                el.dispatchEvent(new CustomEvent("filter-sites", {
                    detail: { siteIds: filteredIds }
                }));
            }
            return window.dash_clientside.no_update;
        }
        """,
        Output("site-filtered-ids", "id"),
        Input("site-filtered-ids", "data"),
        Input("task-map-content", "children"),
    )

    app.clientside_callback(
        """
        function(selection, attributes, rendered) {
            const next = selection || {};
            const previous = rendered || {};
            if ((next.field && attributes == null) ||
                ["site", "field", "value", "error"].every(
                    key => next[key] === previous[key]
                )) {
                return window.dash_clientside.no_update;
            }
            return next;
        }
        """,
        Output("task-graph-selection", "data"),
        Input("task-plot-selection", "data"),
        Input("site-filter-attributes", "data"),
        State("task-graph-selection", "data"),
    )

    @app.callback(
        Output("task-results-graphs", "children"),
        Input("task-graph-selection", "data"),
        State("site-filter-attributes", "data"),
        State("task-id-store", "data"),
        State("share-token-store", "data"),
        State("site-defor-store", "data"),
        prevent_initial_call=True,
    )
    @timed_task_step("task_filtered_plots")
    def update_filtered_aggregate_plots(
        selection, attributes, task_id, share_token, site_store
    ):
        if not _authorize_task_access(task_id, share_token):
            return dbc.Alert("Task not found or access denied.", color="danger")
        selection = selection or {}
        if selection.get("error"):
            return dbc.Alert(selection["error"], color="warning")
        field, value = selection.get("field"), selection.get("value")
        if field and attributes is None:
            raise PreventUpdate
        if field and not any(field in props for props in (attributes or {}).values()):
            return dbc.Alert(
                "The selected source field is not available.", color="warning"
            )
        filtered_ids = _filter_site_ids(field, value, attributes)
        try:
            if selection.get("site"):
                site_id = str(selection["site"])
                if filtered_ids is not None and site_id not in filtered_ids:
                    return dbc.Alert(
                        "The selected site is not in the selected group.",
                        color="warning",
                    )
                site_totals = (site_store or {}).get("summary_totals", {})
                site_total = site_totals.get(site_id)
                if site_total is None:
                    return dbc.Alert(
                        "Summary data is unavailable for the selected site.",
                        color="warning",
                    )
                task = get_task_header(task_id)
                if not task:
                    raise PreventUpdate
                replicate_summary = _filtered_portfolio(task, {site_id}, "total")
                site_name = site_total.get("site_name") or site_id
                summary = _build_selection_summary(
                    [site_total],
                    replicate_summary,
                    f"Selected site: {site_name}",
                )
                site_plots = update_site_deforestation_plot(
                    site_id, site_store, task_id
                )
                plot_children = (
                    site_plots.children
                    if isinstance(site_plots, html.Div)
                    else [site_plots]
                )
                return [summary, *plot_children]

            # Filter attributes are already in the browser; don't reload them.
            detail = get_task_detail(
                task_id, section="plots", include_filter_attributes=False
            )
            if not detail:
                return dbc.Alert("Task not found.", color="danger")
            task = detail["task"]
            totals = detail["totals"]
            results = detail["results"]
            sites = detail["sites"]
            agg_yearly = detail["agg_yearly"]
            if filtered_ids is not None:
                allowed = set(filtered_ids)
                totals = [total for total in totals if str(total.site_id) in allowed]
                result_ids = {str(total.site_id) for total in totals}
                if not result_ids:
                    return dbc.Alert(
                        "No results for the selected group of sites.", color="info"
                    )
                if results is not None:
                    results = [row for row in results if str(row.site_id) in result_ids]
                sites = [site for site in sites if str(site.site_id) in result_ids]
                agg_yearly = _filtered_portfolio(task, result_ids, "yearly")
                replicate_summary = _filtered_portfolio(task, result_ids, "total")
            graphs = _build_plots(
                results,
                totals,
                sites,
                task=task if filtered_ids is None else None,
                agg_yearly=agg_yearly,
                is_large=detail["is_large"],
                graphs_only=True,
                bar_site_data=(
                    get_task_top_site_yearly(task_id, [t.site_id for t in totals])
                    if detail["is_large"]
                    else None
                ),
            )
            if not isinstance(graphs, list):
                return graphs
            if filtered_ids is not None:
                description = "selected group"
                if field and value is not None:
                    label = "(missing)" if value == "null" else str(json.loads(value))
                    description = f"{field} = {label}"
                return [
                    dbc.Alert(
                        f"Filtered to {len(totals)} sites: {description}",
                        color="info",
                    ),
                    _build_selection_summary(
                        totals,
                        replicate_summary,
                        f"Group summary: {description}",
                    ),
                    *graphs,
                ]
            return graphs
        except (ClientError, OSError, ValueError, KeyError) as exc:
            logger.exception("Could not build filtered results for task %s", task_id)
            return dbc.Alert(f"Could not load filtered results: {exc}", color="danger")

    # -- Site selector: zoom the map to the selected site --------------------

    app.clientside_callback(
        ClientsideFunction(namespace="taskViewHistory", function_name="zoomToSite"),
        Output("site-defor-selector", "id"),
        Input("site-defor-selector", "value"),
        Input("site-filter-attributes", "data"),
    )

    # -- Resume task (detail page) -------------------------------------------

    @app.callback(
        Output("resume-task-result", "children"),
        Output("task-resume-refresh", "data"),
        Input("confirm-resume-task", "submit_n_clicks"),
        State("task-id-store", "data"),
        State("share-token-store", "data"),
        prevent_initial_call=True,
        running=[
            (Output("resume-task-btn", "disabled"), True, False),
            (
                Output("resume-task-btn", "children"),
                "Requesting resume...",
                [html.I(className="bi bi-play-circle me-1"), "Resume"],
            ),
        ],
    )
    def handle_resume_task(n_clicks, task_id, share_token):
        if not n_clicks or not task_id or share_token:
            raise PreventUpdate
        user = get_current_user()
        if not user:
            return dbc.Alert("Please log in first.", color="danger"), no_update
        try:
            resume_task(task_id, user)
        except (
            ValueError,
            PermissionError,
            TrendsEarthAPIError,
            SessionExpired,
            SessionUnavailable,
        ) as exc:
            logger.warning("Could not resume task %s: %s", task_id, exc)
            return dbc.Alert(str(exc), color="warning"), no_update
        except Exception:
            logger.exception("Error resuming task %s", task_id)
            report_exception()
            return dbc.Alert(
                "Failed to resume task. Please try again.", color="danger"
            ), no_update
        return (
            dbc.Alert(
                "Resume requested for the same execution. Progress will refresh "
                "automatically; completed work is reused where compatible.",
                color="success",
            ),
            str(_uuid.uuid4()),
        )

    # -- Cancel task (detail page) -------------------------------------------

    @app.callback(
        Output("cancel-task-result", "children"),
        Input("confirm-cancel-task", "submit_n_clicks"),
        State("task-id-store", "data"),
        prevent_initial_call=True,
        running=[
            (Output("cancel-task-btn", "disabled"), True, False),
            (
                Output("cancel-task-btn", "children"),
                "Cancelling...",
                [html.I(className="bi bi-x-circle me-1"), "Cancel"],
            ),
        ],
    )
    def handle_cancel_task(n_clicks, task_id):
        if not n_clicks or not task_id:
            raise PreventUpdate

        user = get_current_user()
        if not user:
            return dbc.Alert("Please log in first.", color="danger", duration=4000)

        try:
            cancel_task(task_id, user)
            return dbc.Alert(
                "Task cancelled successfully. Status will refresh automatically.",
                color="success",
                duration=3000,
            )
        except (ValueError, PermissionError) as e:
            return dbc.Alert(str(e), color="warning", duration=4000)
        except Exception:
            logger.exception("Error cancelling task %s", task_id)
            return dbc.Alert("Failed to cancel task.", color="danger", duration=4000)

    # -- Task actions from task list table ------------------------------------

    @app.callback(
        [
            Output("recompute-from-list-result", "children"),
            Output("cancel-from-list-result", "children"),
            Output("task-list-action-refresh", "data"),
            Output("task-list-action-complete", "data"),
        ],
        Input("task-list-table", "cellRendererData"),
        prevent_initial_call=True,
    )
    def handle_task_list_actions(renderer_data):
        if not renderer_data:
            raise PreventUpdate

        value = renderer_data.get("value") or {}
        action = value.get("action")
        task_id = value.get("task_id")
        if not action or not task_id:
            raise PreventUpdate

        if action == "recompute":
            return (
                dcc.Location(
                    href=f"/submit?recompute={task_id}",
                    id="_recompute-redirect",
                ),
                no_update,
                no_update,
                str(_uuid.uuid4()),
            )

        if action == "resume":
            alert, refresh_event = handle_resume_task(1, task_id, None)
            return no_update, alert, refresh_event, str(_uuid.uuid4())

        if action == "cancel":
            user = get_current_user()
            if not user:
                return (
                    no_update,
                    dbc.Alert("Please log in first.", color="danger", duration=4000),
                    no_update,
                    str(_uuid.uuid4()),
                )
            try:
                cancel_task(task_id, user)
                return (
                    no_update,
                    dbc.Alert("Task cancelled.", color="success", duration=3000),
                    str(_uuid.uuid4()),
                    str(_uuid.uuid4()),
                )
            except (ValueError, PermissionError) as e:
                return (
                    no_update,
                    dbc.Alert(str(e), color="warning", duration=4000),
                    no_update,
                    str(_uuid.uuid4()),
                )
            except Exception:
                logger.exception("Error cancelling task %s from list", task_id)
                return (
                    no_update,
                    dbc.Alert(
                        "Failed to cancel task.",
                        color="danger",
                        duration=4000,
                    ),
                    no_update,
                    str(_uuid.uuid4()),
                )

        raise PreventUpdate

    app.clientside_callback(
        """
        function(completed) {
            if (!completed) return window.dash_clientside.no_update;
            window.taskListAction.setPending(false);
            return window.dash_clientside.no_update;
        }
        """,
        Output("task-list-action-pending", "id"),
        Input("task-list-action-complete", "data"),
        prevent_initial_call=True,
    )

    @app.callback(
        Output("task-logs-modal", "is_open"),
        Output("task-logs-title", "children"),
        Output("task-logs-body", "children"),
        Output("task-logs-task-id", "data"),
        Output("task-logs-interval", "disabled"),
        Input("task-list-table", "cellRendererData"),
        Input("task-logs-refresh", "n_clicks"),
        Input("task-logs-interval", "n_intervals"),
        Input("task-logs-close", "n_clicks"),
        State("task-logs-modal", "is_open"),
        State("task-logs-task-id", "data"),
        prevent_initial_call=True,
    )
    def show_task_logs(renderer_data, _refresh, _interval, _close, is_open, task_id):
        trigger = callback_context.triggered_id
        if trigger == "task-logs-close":
            return False, no_update, no_update, None, True
        if trigger == "task-list-table":
            value = (renderer_data or {}).get("value") or {}
            if value.get("action") != "logs" or not value.get("task_id"):
                raise PreventUpdate
            task_id = value["task_id"]
        elif not is_open or not task_id:
            raise PreventUpdate

        user = get_current_user()
        try:
            if not user:
                raise PermissionError("Please log in first.")
            logs = get_task_execution_logs(task_id, user)
            lines = sorted(
                logs, key=lambda log: log.get("register_date") or "", reverse=True
            )
            body = html.Pre(
                "\n".join(
                    f"{log.get('register_date', '')} - {log.get('level', '')} - {log.get('text', '')}"
                    for log in lines
                )
                or "No logs found for this task.",
                style={"whiteSpace": "pre-wrap", "fontSize": "12px"},
            )
        except (
            ValueError,
            PermissionError,
            TrendsEarthAPIError,
            SessionExpired,
            SessionUnavailable,
        ) as exc:
            logger.warning("Could not fetch logs for task %s: %s", task_id, exc)
            body = dbc.Alert(str(exc), color="warning")
        except Exception:
            logger.exception("Error fetching logs for task %s", task_id)
            report_exception()
            body = dbc.Alert(
                "Failed to fetch task logs. Please try again.", color="danger"
            )
        return True, "Task Logs", body, task_id, False

    # -- Admin: Covariates (unified export + merge) ---------------------------

    @app.callback(
        Output("gee-export-confirm-modal", "is_open"),
        [
            Input("start-gee-export", "n_clicks"),
            Input("gee-export-cancel", "n_clicks"),
            Input("gee-export-confirm", "n_clicks"),
        ],
        State("gee-export-confirm-modal", "is_open"),
        prevent_initial_call=True,
    )
    def toggle_gee_export_confirm_modal(
        open_clicks, cancel_clicks, confirm_clicks, is_open
    ):
        ctx = callback_context
        if not ctx.triggered:
            raise PreventUpdate
        trigger = ctx.triggered[0]["prop_id"].split(".")[0]
        return trigger == "start-gee-export"

    @app.callback(
        Output("gee-export-result", "children"),
        Input("gee-export-confirm", "n_clicks"),
        State("gee-export-category", "value"),
        State("gee-export-resolution", "value"),
        prevent_initial_call=True,
    )
    def handle_gee_export(n_clicks, category, resolution_m_str):
        user = get_current_user()
        if not user or not user.is_admin:
            return dbc.Alert("Admin access required.", color="danger")

        # gee_config already imported at module level
        COVARIATES = gee_config.COVARIATES

        if category == "all":
            names = list(COVARIATES.keys())
        else:
            names = [k for k, v in COVARIATES.items() if v.get("category") == category]

        if not names:
            return dbc.Alert(
                f"No covariates found for category: {category}", color="warning"
            )

        resolution_m = int(resolution_m_str) if resolution_m_str else 1000
        if resolution_m not in (1000, 250):
            return dbc.Alert("Invalid resolution.", color="danger")

        try:
            export_ids = start_gee_export(names, user.id, resolution_m=resolution_m)
            return dbc.Alert(
                f"Started {len(export_ids)} GEE export task(s).",
                color="success",
            )
        except Exception:
            logger.exception("GEE export failed")
            report_exception()
            return dbc.Alert(
                "Export failed. Please try again or contact support.",
                color="danger",
            )

    @app.callback(
        [
            Output("covariates-table", "rowData"),
            Output("covariates-total-count", "children"),
        ],
        [
            Input("admin-refresh-interval", "n_intervals"),
            Input("gee-export-result", "children"),
            Input("covariate-action-result", "children"),
            Input("gee-export-resolution", "value"),
        ],
    )
    def refresh_covariate_inventory(
        n, _export_result, _action_result, resolution_filter
    ):
        user = get_current_user()
        if not user or not user.is_admin:
            raise PreventUpdate

        # GEE export status is polled by the Celery Beat worker;
        # this callback just reads the current DB/S3/GCS state.
        try:
            rows = get_covariate_inventory()
        except Exception:
            logger.exception("Failed to build covariate inventory")
            report_exception()
            rows = []

        # Filter rows to the resolution selected in the export dropdown.
        if resolution_filter:
            res_label = {"1000": "1 km", "250": "250 m"}.get(resolution_filter)
            if res_label:
                rows = [r for r in rows if r.get("resolution") == res_label]

        gcs_count = sum(1 for r in rows if r.get("gcs_tiles", 0) > 0)
        s3_count = sum(1 for r in rows if r.get("on_s3"))
        ready_count = sum(1 for r in rows if r.get("ready_for_analysis"))
        total_label = (
            f"Total: {len(rows)} | On GCS: {gcs_count} | On S3: {s3_count}"
            f" | Ready: {ready_count}"
        )

        return rows, total_label

    # -- Admin: Covariate row action buttons ---------------------------------

    @app.callback(
        Output("covariate-action-result", "children"),
        Input("covariates-table", "cellRendererData"),
        prevent_initial_call=True,
    )
    def handle_covariate_action(renderer_data):
        if not renderer_data:
            raise PreventUpdate

        user = get_current_user()
        if not user or not user.is_admin:
            return dbc.Alert("Admin access required.", color="danger", duration=4000)

        data = renderer_data.get("value", {})
        action = data.get("_action")
        covariate_name = data.get("covariate_name")
        resolution_m = data.get("resolution_m", 1000)

        if not action or not covariate_name:
            raise PreventUpdate

        try:
            if action == "reexport":
                force_reexport(covariate_name, user.id, resolution_m=resolution_m)
                return dbc.Alert(
                    f"Re-export started for '{covariate_name}'. "
                    "Existing GCS tiles and S3 COG have been deleted.",
                    color="success",
                    duration=6000,
                )
            elif action == "remerge":
                force_remerge(covariate_name, user.id, resolution_m=resolution_m)
                return dbc.Alert(
                    f"Re-merge queued for '{covariate_name}'. "
                    "Existing S3 COG has been deleted.",
                    color="success",
                    duration=6000,
                )
            else:
                raise PreventUpdate
        except Exception:
            logger.exception(
                "Covariate action '%s' failed for %s", action, covariate_name
            )
            report_exception(covariate=covariate_name, action=action)
            # Persist a 'failed' record so the table row reflects the error
            try:
                _record_covariate_action_failure(
                    covariate_name,
                    action,
                    user.id,
                    resolution_m=resolution_m,
                )
            except Exception:
                logger.exception(
                    "Failed to persist failure record for %s",
                    covariate_name,
                )
            return dbc.Alert(
                f"Action '{action}' failed for '{covariate_name}'. "
                "Check logs for details.",
                color="danger",
                duration=6000,
            )

    # -- Admin: user access (Trends.Earth app-users API) --------------------

    def _admin_access_token():
        """Return (user, access_token) for an admin, or raise AppUsersError."""
        from session_store import (
            SessionExpired,
            SessionUnavailable,
            get_access_token,
        )

        user = get_current_user()
        if not user or not user.is_admin:
            raise AppUsersError("Admin access required.", status=403)
        try:
            return user, get_access_token(user.session_id)
        except SessionExpired as exc:
            raise AppUsersError(
                "Your sign-in has ended. Reload the page to sign in again.",
                status=401,
            ) from exc
        except SessionUnavailable as exc:
            raise AppUsersError(
                "Could not refresh your sign-in. Please try again shortly."
            ) from exc

    @app.callback(
        [
            Output("user-management-table", "rowData"),
            Output("user-management-total-count", "children"),
            Output("admin-user-action-result", "children", allow_duplicate=True),
        ],
        Input("admin-refresh-interval", "n_intervals"),
        Input("admin-user-status-filter", "value"),
        Input("admin-tabs", "active_tab"),
        prevent_initial_call="initial_duplicate",
    )
    def refresh_user_management(n, status, active_tab):
        if active_tab != "tab-users":
            raise PreventUpdate
        user = get_current_user()
        if not user or not user.is_admin:
            raise PreventUpdate
        try:
            _, token = _admin_access_token()
            users = list_app_users(
                token, status=None if status in (None, "", "all") else status
            )
        except AppUsersError as exc:
            return no_update, no_update, dbc.Alert(str(exc), color="danger")

        rows = [
            {
                "id": u.get("sub"),
                "name": u.get("name") or "",
                "email": u.get("email") or "",
                "app_access": u.get("app_access"),
                "app_role": u.get("app_role") or "",
                "institution": u.get("institution") or "",
                "email_verified": bool(u.get("email_verified")),
                "active": bool(u.get("active", True)),
            }
            for u in users
            if u.get("sub")
        ]
        return rows, f"Total: {len(rows)}", no_update

    # -- Admin: populate user select dropdown --------------------------------

    @app.callback(
        Output("admin-user-select", "options"),
        Input("user-management-table", "rowData"),
    )
    def update_user_select(row_data):
        if not row_data:
            return []
        return [
            {
                "label": f"{r['name'] or r['email']} ({r['email']})"
                + (f" [{r['app_access']}]" if r.get("app_access") != "active" else ""),
                "value": r["id"],
            }
            for r in row_data
        ]

    @app.callback(
        Output("admin-user-select", "value"),
        Input("user-management-table", "cellClicked"),
        prevent_initial_call=True,
    )
    def select_user_from_grid(cell):
        row = (cell or {}).get("rowData") or {}
        if not row.get("id"):
            raise PreventUpdate
        return row["id"]

    def _set_access(user_id, status, role, current_n, success_message):
        if not user_id:
            return dbc.Alert(
                "Please select a user.", color="warning", duration=4000
            ), no_update
        try:
            _, token = _admin_access_token()
            set_app_user_access(token, user_id, status, role=role)
        except AppUsersError as exc:
            return dbc.Alert(str(exc), color="danger", duration=6000), no_update
        # Bump n_intervals to force a refresh of the user table
        return (
            dbc.Alert(success_message, color="success", duration=4000),
            (current_n or 0) + 1,
        )

    @app.callback(
        [
            Output("admin-user-action-result", "children", allow_duplicate=True),
            Output("admin-refresh-interval", "n_intervals", allow_duplicate=True),
        ],
        Input("admin-approve-btn", "n_clicks"),
        State("admin-user-select", "value"),
        State("admin-role-select", "value"),
        State("admin-refresh-interval", "n_intervals"),
        prevent_initial_call=True,
    )
    def handle_approve_user(n_clicks, user_id, role, current_n):
        return _set_access(
            user_id, "active", role or "member", current_n, "Access approved."
        )

    @app.callback(
        [
            Output("admin-user-action-result", "children", allow_duplicate=True),
            Output("admin-refresh-interval", "n_intervals", allow_duplicate=True),
        ],
        Input("admin-role-btn", "n_clicks"),
        State("admin-user-select", "value"),
        State("admin-role-select", "value"),
        State("admin-refresh-interval", "n_intervals"),
        prevent_initial_call=True,
    )
    def handle_change_role(n_clicks, user_id, role, current_n):
        return _set_access(user_id, "active", role, current_n, "Role updated.")

    @app.callback(
        [
            Output("admin-user-action-result", "children", allow_duplicate=True),
            Output("admin-refresh-interval", "n_intervals", allow_duplicate=True),
        ],
        Input("admin-revoke-btn", "n_clicks"),
        State("admin-user-select", "value"),
        State("admin-refresh-interval", "n_intervals"),
        prevent_initial_call=True,
    )
    def handle_revoke_user(n_clicks, user_id, current_n):
        return _set_access(
            user_id,
            "revoked",
            None,
            current_n,
            "Access revoked. Their sign-in ends within a few minutes.",
        )

    # -- Admin: delete a user's app data (modal) -----------------------------

    @app.callback(
        Output("admin-delete-modal", "is_open"),
        [
            Input("admin-delete-btn", "n_clicks"),
            Input("admin-delete-cancel", "n_clicks"),
            Input("admin-delete-confirm", "n_clicks"),
        ],
        State("admin-delete-modal", "is_open"),
        prevent_initial_call=True,
    )
    def toggle_admin_delete_modal(open_clicks, cancel_clicks, confirm_clicks, is_open):
        ctx = callback_context
        if not ctx.triggered:
            raise PreventUpdate
        trigger = ctx.triggered[0]["prop_id"].split(".")[0]
        return trigger == "admin-delete-btn"

    @app.callback(
        Output("admin-user-action-result", "children", allow_duplicate=True),
        Input("admin-delete-confirm", "n_clicks"),
        State("admin-user-select", "value"),
        prevent_initial_call=True,
    )
    def handle_admin_delete_user_data(n_clicks, user_id):
        if not user_id:
            return dbc.Alert("Please select a user.", color="warning", duration=4000)
        user = get_current_user()
        if not user or not user.is_admin:
            return dbc.Alert("Admin access required.", color="danger", duration=4000)
        try:
            counts = delete_user_data(user_id)
        except Exception:
            logger.exception("Failed to delete app data for user %s", user_id)
            return dbc.Alert(
                "Deleting the user's data failed.", color="danger", duration=6000
            )
        return dbc.Alert(
            f"Deleted {counts['tasks']} task(s) and related data.",
            color="success",
            duration=6000,
        )

    # -- Self data deletion (modal) ------------------------------------------

    @app.callback(
        Output("self-delete-modal", "is_open"),
        [
            Input("self-delete-btn", "n_clicks"),
            Input("self-delete-cancel", "n_clicks"),
            Input("self-delete-confirm", "n_clicks"),
        ],
        State("self-delete-modal", "is_open"),
        prevent_initial_call=True,
    )
    def toggle_self_delete_modal(open_clicks, cancel_clicks, confirm_clicks, is_open):
        ctx = callback_context
        if not ctx.triggered:
            raise PreventUpdate
        trigger = ctx.triggered[0]["prop_id"].split(".")[0]
        return trigger == "self-delete-btn"

    @app.callback(
        Output("self-delete-result", "children"),
        Input("self-delete-confirm", "n_clicks"),
        prevent_initial_call=True,
    )
    def handle_self_delete(n_clicks):
        from auth import end_session

        user = get_current_user()
        if not user:
            raise PreventUpdate
        try:
            delete_user_data(user.id)
        except Exception:
            logger.exception("Failed to delete app data for user %s", user.id)
            return dbc.Alert(
                "Deleting your data failed. Please try again.",
                color="danger",
                duration=6000,
            )
        end_session()
        return dcc.Location(href="/login", refresh=True, id="redirect-after-delete")

    # -- AG Grid cell click (task link navigation) ---------------------------

    @app.callback(
        Output("url", "pathname", allow_duplicate=True),
        Input("task-list-table", "cellClicked"),
        prevent_initial_call=True,
    )
    def navigate_to_task(cell):
        if not cell:
            raise PreventUpdate
        row_data = cell.get("rowData", {})
        task_id = row_data.get("id")
        if task_id and cell.get("colId") == "name":
            return f"/task/{task_id}"
        raise PreventUpdate

    # -- Selection-aware Match Quality plots ---------------------------------

    @app.callback(
        Output("match-quality-plots-container", "children"),
        Input("match-quality-section", "active_item"),
        Input("task-plot-selection", "data"),
        Input("site-filter-attributes", "data"),
        State("task-id-store", "data"),
        State("share-token-store", "data"),
        prevent_initial_call=True,
    )
    def update_match_quality_plots(
        active_item, selection, attributes, task_id, share_token
    ):
        """Recompute match-quality diagnostics for the current site selection."""
        if active_item != "match-quality":
            raise PreventUpdate
        if not _authorize_task_access(task_id, share_token):
            return dbc.Alert("Task not found or access denied.", color="danger")

        selection = selection or {}
        if selection.get("error"):
            return dbc.Alert(selection["error"], color="warning")
        field, value = selection.get("field"), selection.get("value")
        if field and not any(field in props for props in (attributes or {}).values()):
            return dbc.Alert(
                "The selected source field is not available.", color="warning"
            )

        try:
            detail = get_task_detail(task_id, section="totals")
            if not detail:
                return dbc.Alert("Task not found.", color="danger")
            task = detail["task"]
            if task.status != "succeeded":
                return html.P("Results not yet available.", className="text-muted")

            available_ids = {str(total.site_id) for total in detail["totals"]}
            filtered_ids = _filter_site_ids(field, value, attributes)
            if selection.get("site"):
                site_id = str(selection["site"])
                if filtered_ids is not None and site_id not in filtered_ids:
                    return dbc.Alert(
                        "The selected site is not in the selected group.",
                        color="warning",
                    )
                selected_ids = {site_id} & available_ids
                selected_site_id = site_id
            else:
                selected_ids = (
                    available_ids & set(filtered_ids)
                    if filtered_ids is not None
                    else available_ids
                )
                selected_site_id = None
            if not selected_ids:
                return dbc.Alert(
                    "No matched sites are available for this selection.",
                    color="info",
                )

            selected_totals = [
                total
                for total in detail["totals"]
                if str(total.site_id) in selected_ids
            ]
            summary_raw = download_results_csv(
                task_id,
                "match_quality_summary",
                results_s3_uri=task.results_s3_uri,
            )
            if not summary_raw:
                return dbc.Alert(
                    "The match quality summary is missing for this task.",
                    color="warning",
                )
            summary = json.loads(summary_raw)
            if not isinstance(summary, dict):
                return dbc.Alert(
                    "The match quality summary has an invalid format.",
                    color="danger",
                )
            pixels = get_filtered_match_quality_pixels(
                task_id,
                selected_ids,
                results_s3_uri=task.results_s3_uri,
            )
            return _build_selected_match_quality(
                pixels,
                summary,
                selected_ids,
                selected_totals,
                selected_site_id,
            )
        except (ClientError, OSError, ValueError, KeyError) as exc:
            logger.exception(
                "Could not build selected match quality for task %s", task_id
            )
            return dbc.Alert(
                f"Could not load match quality data: {exc}",
                color="danger",
            )

    # -- Site-level deforestation drill-down ----------------------------------

    def update_site_deforestation_plot(selected_site, store_data, task_id):
        """Build per-site deforestation and emissions plots with intervention
        date markers when a site is selected from the dropdown."""
        if not selected_site:
            return html.P("Select a site to view its results.", className="text-muted")
        if not store_data:
            raise PreventUpdate

        sites_data = store_data.get("sites", {})
        subsampled_data = store_data.get("subsampled", {})

        # Large-task path: fetch per-site data from DB on demand
        if store_data.get("large_task"):
            if not task_id:
                return html.P(
                    "Cannot load site data: task ID missing.", className="text-muted"
                )
            raw_rows = get_task_site_results(task_id, selected_site)
            if not raw_rows:
                return html.P("No data for selected site.", className="text-muted")
            results_data = raw_rows
            site_rows = results_data
        else:
            results_data = store_data.get("results", [])
            # Filter results for selected site
            site_rows = [r for r in results_data if r["site_id"] == selected_site]
            if not site_rows:
                return html.P("No data for selected site.", className="text-muted")

        site_df = pd.DataFrame(site_rows).sort_values("year")
        site_info = sites_data.get(selected_site, {})
        site_name = site_info.get("site_name", selected_site)
        end_date = site_info.get("end_date")
        site_has_ci = (
            "treatment_defor_ha_ci_lower" in site_df.columns
            and site_df["treatment_defor_ha_ci_lower"].notna().any()
        )
        has_cumulative_ci = (
            "cumulative_treatment_defor_ha_ci_lower" in site_df.columns
            and site_df["cumulative_treatment_defor_ha_ci_lower"].notna().any()
        )

        # Check if this site was subsampled
        sub_info = subsampled_data.get(selected_site)
        sub_note = ""
        if sub_info:
            pct = sub_info.get("sampled_percent", 100)
            sub_note = f" [subsampled {pct:.0f}%]"

        # Determine pre-intervention year range for shading
        pre_years = site_df.loc[
            site_df.get("is_pre_intervention", pd.Series(dtype=bool)), "year"
        ]

        children = []

        # Show subsampling alert if applicable
        if sub_info:
            children.append(
                dbc.Alert(
                    f"This site was subsampled to {pct:.1f}% of pixels for "
                    f"matching. Results are scaled up from the sampled fraction "
                    f"({sub_info.get('sampled_fraction', 1.0):.4f}).",
                    color="info",
                    className="mb-2",
                )
            )

        # --- Deforestation comparison plot ---
        fig_defor = go.Figure()
        fig_defor.add_trace(
            go.Scatter(
                x=site_df["year"],
                y=site_df["treatment_defor_ha"],
                mode="lines+markers",
                name="Project Site",
                line={"color": PROJECT_COLOR, "width": 2},
                marker={"size": 6},
            )
        )
        fig_defor.add_trace(
            go.Scatter(
                x=site_df["year"],
                y=site_df["control_defor_ha"],
                mode="lines+markers",
                name="Matched Controls",
                line={"color": CONTROL_COLOR, "width": 2},
                marker={"size": 6},
            )
        )
        if site_has_ci:
            _add_ci_band(
                fig_defor,
                site_df["year"],
                site_df["treatment_defor_ha_ci_lower"],
                site_df["treatment_defor_ha_ci_upper"],
                color=PROJECT_BAND_COLOR,
                name=f"Project Site ({BOUNDS_NAME.lower()})",
            )
            _add_ci_band(
                fig_defor,
                site_df["year"],
                site_df["control_defor_ha_ci_lower"],
                site_df["control_defor_ha_ci_upper"],
                color=CONTROL_BAND_COLOR,
                name=f"Matched Controls ({BOUNDS_NAME.lower()})",
            )
        fig_defor.update_layout(
            title=f"Annual Deforestation: {site_name}{sub_note}",
            xaxis_title="Year",
            xaxis={"tickformat": "d", "dtick": 1},
            yaxis_title="Deforestation (ha)",
            legend={"yanchor": "top", "y": 0.99, "xanchor": "left", "x": 1.02},
            hovermode="x unified",
        )
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
        if end_date:
            end_year = int(end_date[:4])
            fig_defor.add_vrect(
                x0=end_year + 0.5,
                x1=site_df["year"].max() + 0.5,
                fillcolor="gray",
                opacity=0.12,
                line_width=0,
                annotation_text="Post-intervention",
                annotation_position="top right",
                annotation_font_color="gray",
            )
        children.append(dcc.Graph(figure=fig_defor))

        # --- Cumulative deforestation plot ---
        site_sorted = site_df.sort_values("year")
        has_cumulative_values = (
            "cumulative_treatment_defor_ha" in site_sorted.columns
            and site_sorted["cumulative_treatment_defor_ha"].notna().any()
        )
        cum_treatment = (
            site_sorted["cumulative_treatment_defor_ha"]
            if has_cumulative_values
            else site_sorted["treatment_defor_ha"].cumsum()
        )
        cum_control = (
            site_sorted["cumulative_control_defor_ha"]
            if has_cumulative_values
            else site_sorted["control_defor_ha"].cumsum()
        )
        fig_cum = go.Figure()
        fig_cum.add_trace(
            go.Scatter(
                x=site_sorted["year"],
                y=cum_treatment,
                mode="lines+markers",
                name="Project Site",
                line={"color": PROJECT_COLOR, "width": 2},
                marker={"size": 6},
            )
        )
        fig_cum.add_trace(
            go.Scatter(
                x=site_sorted["year"],
                y=cum_control,
                mode="lines+markers",
                name="Matched Controls",
                line={"color": CONTROL_COLOR, "width": 2},
                marker={"size": 6},
            )
        )
        if has_cumulative_ci:
            _add_ci_band(
                fig_cum,
                site_sorted["year"],
                site_sorted["cumulative_treatment_defor_ha_ci_lower"],
                site_sorted["cumulative_treatment_defor_ha_ci_upper"],
                color=PROJECT_BAND_COLOR,
                name=f"Project Site ({BOUNDS_NAME.lower()})",
            )
            _add_ci_band(
                fig_cum,
                site_sorted["year"],
                site_sorted["cumulative_control_defor_ha_ci_lower"],
                site_sorted["cumulative_control_defor_ha_ci_upper"],
                color=CONTROL_BAND_COLOR,
                name=f"Matched Controls ({BOUNDS_NAME.lower()})",
            )
        fig_cum.update_layout(
            title=f"Cumulative Deforestation: {site_name}{sub_note}",
            xaxis_title="Year",
            xaxis={"tickformat": "d", "dtick": 1},
            yaxis_title="Cumulative Deforestation (ha)",
            legend={"yanchor": "top", "y": 0.99, "xanchor": "left", "x": 1.02},
            hovermode="x unified",
        )
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
        if end_date:
            end_year = int(end_date[:4])
            fig_cum.add_vrect(
                x0=end_year + 0.5,
                x1=site_sorted["year"].max() + 0.5,
                fillcolor="gray",
                opacity=0.12,
                line_width=0,
                annotation_text="Post-intervention",
                annotation_position="top right",
                annotation_font_color="gray",
            )
        children.append(dcc.Graph(figure=fig_cum))

        # --- Emissions comparison plot ---
        has_emissions = (
            site_df["treatment_emissions_mgco2e"].sum() > 0
            or site_df["control_emissions_mgco2e"].sum() > 0
        )
        if has_emissions:
            fig_em = go.Figure()
            fig_em.add_trace(
                go.Scatter(
                    x=site_df["year"],
                    y=site_df["treatment_emissions_mgco2e"],
                    mode="lines+markers",
                    name="Project Site",
                    line={"color": PROJECT_COLOR, "width": 2},
                    marker={"size": 6},
                )
            )
            fig_em.add_trace(
                go.Scatter(
                    x=site_df["year"],
                    y=site_df["control_emissions_mgco2e"],
                    mode="lines+markers",
                    name="Matched Controls",
                    line={"color": CONTROL_COLOR, "width": 2},
                    marker={"size": 6},
                )
            )
            has_emissions_ci = (
                "treatment_emissions_mgco2e_ci_lower" in site_df.columns
                and site_df["treatment_emissions_mgco2e_ci_lower"].notna().any()
            )
            if has_emissions_ci:
                _add_ci_band(
                    fig_em,
                    site_df["year"],
                    site_df["treatment_emissions_mgco2e_ci_lower"],
                    site_df["treatment_emissions_mgco2e_ci_upper"],
                    color=PROJECT_BAND_COLOR,
                    name=f"Project Site ({BOUNDS_NAME.lower()})",
                )
                _add_ci_band(
                    fig_em,
                    site_df["year"],
                    site_df["control_emissions_mgco2e_ci_lower"],
                    site_df["control_emissions_mgco2e_ci_upper"],
                    color=CONTROL_BAND_COLOR,
                    name=f"Matched Controls ({BOUNDS_NAME.lower()})",
                )
            fig_em.update_layout(
                title=f"Annual Emissions: {site_name}{sub_note}",
                xaxis_title="Year",
                xaxis={"tickformat": "d", "dtick": 1},
                yaxis_title="Emissions (MgCO₂e)",
                legend={"yanchor": "top", "y": 0.99, "xanchor": "left", "x": 1.02},
                hovermode="x unified",
            )
            if not pre_years.empty:
                fig_em.add_vrect(
                    x0=pre_years.min() - 0.5,
                    x1=pre_years.max() + 0.5,
                    fillcolor="gray",
                    opacity=0.12,
                    line_width=0,
                    annotation_text="Pre-intervention",
                    annotation_position="top left",
                    annotation_font_color="gray",
                )
            if end_date:
                end_year = int(end_date[:4])
                fig_em.add_vrect(
                    x0=end_year + 0.5,
                    x1=site_df["year"].max() + 0.5,
                    fillcolor="gray",
                    opacity=0.12,
                    line_width=0,
                    annotation_text="Post-intervention",
                    annotation_position="top right",
                    annotation_font_color="gray",
                )
            children.append(dcc.Graph(figure=fig_em))

        # --- Avoided values bar chart ---
        fig_avoided = go.Figure()
        fig_avoided.add_trace(
            go.Bar(
                x=site_df["year"],
                y=site_df["forest_loss_avoided_ha"],
                name="Forest Loss Avoided (ha)",
                marker_color="#17a2b8",
            )
        )
        if (
            "forest_loss_avoided_ha_ci_lower" in site_df.columns
            and site_df["forest_loss_avoided_ha_ci_lower"].notna().any()
        ):
            _add_range_error_bars(
                fig_avoided,
                site_df["year"],
                site_df["forest_loss_avoided_ha"],
                site_df["forest_loss_avoided_ha_ci_lower"],
                site_df["forest_loss_avoided_ha_ci_upper"],
                color="#333333",
                name=BOUNDS_NAME,
            )
        fig_avoided.update_layout(
            title=f"Forest Loss Avoided by Year: {site_name}{sub_note}",
            xaxis_title="Year",
            xaxis={"tickformat": "d", "dtick": 1},
            yaxis_title="Forest Loss Avoided (ha)",
            hovermode="x unified",
        )
        if not pre_years.empty:
            fig_avoided.add_vrect(
                x0=pre_years.min() - 0.5,
                x1=pre_years.max() + 0.5,
                fillcolor="gray",
                opacity=0.12,
                line_width=0,
                annotation_text="Pre-intervention",
                annotation_position="top left",
                annotation_font_color="gray",
            )
        if end_date:
            end_year = int(end_date[:4])
            fig_avoided.add_vrect(
                x0=end_year + 0.5,
                x1=site_df["year"].max() + 0.5,
                fillcolor="gray",
                opacity=0.12,
                line_width=0,
                annotation_text="Post-intervention",
                annotation_position="top right",
                annotation_font_color="gray",
            )
        children.append(dcc.Graph(figure=fig_avoided))

        return html.Div(children)

    # -- Matching presets ----------------------------------------------------

    @app.callback(
        [Output("preset-selector", "options"), Output("presets-store", "data")],
        [Input("url", "pathname"), Input("presets-store", "modified_timestamp")],
    )
    def refresh_presets(_pathname, _ts):
        """Populate the preset dropdown whenever the page loads or the
        store is updated after a save/delete."""
        user = get_current_user()
        if not user:
            raise PreventUpdate

        presets = get_matching_presets(user.id)
        options = [{"label": p["name"], "value": p["id"]} for p in presets]
        return options, presets

    @app.callback(
        [
            Output("covariate-selection", "value"),
            Output("exact-match-selection", "value"),
            Output("max-treatment-pixels", "value"),
            Output("control-multiplier", "value"),
            Output("min-site-area-ha", "value"),
            Output("min-glm-treatment-pixels", "value"),
            Output("caliper-width", "value"),
            Output("max-controls-per-treatment", "value"),
            Output("min-control-distance-km", "value"),
            Output("matching-method", "value"),
            Output("separation-fallback-mahalanobis", "value"),
            Output("shared-support-controls", "value"),
            Output("n-replicates", "value"),
            Output("match-memory-gb", "value"),
            Output("matching-job-queue", "value"),
            Output("preset-feedback", "children", allow_duplicate=True),
        ],
        Input("load-preset-btn", "n_clicks"),
        State("preset-selector", "value"),
        State("presets-store", "data"),
        prevent_initial_call=True,
    )
    def load_preset(_n, preset_id, presets_data):
        """Restore covariates, exact-match variables and matching settings."""
        if not preset_id or not presets_data:
            return (*([no_update] * 15), "Please select a preset to load.")

        for p in presets_data:
            if p["id"] == preset_id:
                s = p["settings"]
                return (
                    p["covariates"],
                    p["exact_match_vars"],
                    s["max_treatment_pixels"],
                    s["control_multiplier"],
                    s["min_site_area_ha"],
                    s["min_glm_treatment_pixels"],
                    s["caliper_width"],
                    s["max_controls_per_treatment"],
                    s["min_control_distance_km"],
                    s["matching_method"],
                    s["separation_fallback_mahalanobis"],
                    s.get("shared_support_controls", False),
                    s["n_replicates"],
                    s["match_memory_gb"],
                    s["matching_job_queue"],
                    dbc.Alert(
                        f'Loaded preset "{p["name"]}".',
                        color="info",
                        duration=3000,
                    ),
                )

        return (
            *([no_update] * 15),
            dbc.Alert(
                "Preset not found.",
                color="warning",
                duration=3000,
            ),
        )

    @app.callback(
        [
            Output("presets-store", "data", allow_duplicate=True),
            Output("preset-feedback", "children", allow_duplicate=True),
            Output("preset-name-input", "value"),
        ],
        Input("save-preset-btn", "n_clicks"),
        State("preset-name-input", "value"),
        State("covariate-selection", "value"),
        State("exact-match-selection", "value"),
        State("max-treatment-pixels", "value"),
        State("control-multiplier", "value"),
        State("min-site-area-ha", "value"),
        State("min-glm-treatment-pixels", "value"),
        State("caliper-width", "value"),
        State("max-controls-per-treatment", "value"),
        State("min-control-distance-km", "value"),
        State("matching-method", "value"),
        State("separation-fallback-mahalanobis", "value"),
        State("shared-support-controls", "value"),
        State("n-replicates", "value"),
        State("match-memory-gb", "value"),
        State("matching-job-queue", "value"),
        prevent_initial_call=True,
    )
    def save_preset(
        _n,
        name,
        covariates,
        exact_match_vars,
        max_treatment_pixels,
        control_multiplier,
        min_site_area_ha,
        min_glm_treatment_pixels,
        caliper_width,
        max_controls_per_treatment,
        min_control_distance_km,
        matching_method,
        separation_fallback_mahalanobis,
        shared_support_controls,
        n_replicates,
        match_memory_gb,
        matching_job_queue,
    ):
        """Save covariate selections and matching settings as one preset."""
        if not name or not name.strip():
            return (
                no_update,
                dbc.Alert(
                    "Please enter a name for the preset.",
                    color="warning",
                    duration=3000,
                ),
                no_update,
            )
        if not covariates:
            return (
                no_update,
                dbc.Alert(
                    "Select at least one covariate before saving.",
                    color="warning",
                    duration=3000,
                ),
                no_update,
            )

        user = get_current_user()
        if not user:
            raise PreventUpdate

        settings = {
            "max_treatment_pixels": max_treatment_pixels,
            "control_multiplier": control_multiplier,
            "min_site_area_ha": min_site_area_ha,
            "min_glm_treatment_pixels": min_glm_treatment_pixels,
            "caliper_width": caliper_width,
            "max_controls_per_treatment": max_controls_per_treatment,
            "min_control_distance_km": min_control_distance_km,
            "matching_method": matching_method,
            "separation_fallback_mahalanobis": separation_fallback_mahalanobis,
            "shared_support_controls": bool(shared_support_controls),
            "n_replicates": n_replicates,
            "match_memory_gb": match_memory_gb,
            "matching_job_queue": matching_job_queue,
        }

        try:
            save_matching_preset(
                user.id, name.strip(), covariates, exact_match_vars, settings
            )
            updated = get_matching_presets(user.id)
            return (
                updated,
                dbc.Alert(
                    f'Preset "{name.strip()}" saved.',
                    color="success",
                    duration=3000,
                ),
                "",
            )
        except Exception:
            logger.exception("Failed to save matching preset")
            report_exception()
            return (
                no_update,
                dbc.Alert(
                    "Failed to save preset.",
                    color="danger",
                    duration=3000,
                ),
                no_update,
            )

    @app.callback(
        [
            Output("presets-store", "data", allow_duplicate=True),
            Output("preset-feedback", "children", allow_duplicate=True),
            Output("preset-selector", "value"),
        ],
        Input("delete-preset-btn", "n_clicks"),
        State("preset-selector", "value"),
        State("presets-store", "data"),
        prevent_initial_call=True,
    )
    def delete_preset(_n, preset_id, presets_data):
        """Delete the currently selected preset."""
        if not preset_id:
            return (
                no_update,
                dbc.Alert(
                    "Please select a preset to delete.",
                    color="warning",
                    duration=3000,
                ),
                no_update,
            )

        user = get_current_user()
        if not user:
            raise PreventUpdate

        preset_name = next(
            (p["name"] for p in (presets_data or []) if p["id"] == preset_id),
            "unknown",
        )

        try:
            deleted = delete_matching_preset(preset_id, user.id)
            if not deleted:
                return (
                    no_update,
                    dbc.Alert(
                        "Preset not found or already deleted.",
                        color="warning",
                        duration=3000,
                    ),
                    no_update,
                )

            updated = get_matching_presets(user.id)
            return (
                updated,
                dbc.Alert(
                    f'Preset "{preset_name}" deleted.',
                    color="info",
                    duration=3000,
                ),
                None,
            )
        except Exception:
            logger.exception("Failed to delete matching preset")
            report_exception()
            return (
                no_update,
                dbc.Alert(
                    "Failed to delete preset.",
                    color="danger",
                    duration=3000,
                ),
                no_update,
            )

    # -- Refresh random seed -------------------------------------------------

    @app.callback(
        Output("random-seed", "value"),
        Input("refresh-random-seed", "n_clicks"),
        prevent_initial_call=True,
    )
    def refresh_random_seed(_n):
        """Generate a new random seed value."""
        return random.SystemRandom().randint(1, 2147483647)
