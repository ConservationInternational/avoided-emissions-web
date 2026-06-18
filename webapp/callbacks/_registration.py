"""Dash callback registration for the avoided emissions web application."""

import json
import logging
import random
import uuid as _uuid

import dash_bootstrap_components as dbc
import flask
import flask_login
import pandas as pd
import plotly.graph_objects as go
from dash import ALL, Input, Output, State, callback_context, dcc, html, no_update
from dash.exceptions import PreventUpdate

from auth import (
    authenticate,
    create_refresh_token,
    get_current_user,
    register_user,
    request_password_reset,
    reset_password_with_token,
    revoke_refresh_token,
)
from config import report_exception
from gee_export import gee_config
from layouts import (
    EXACT_MATCH_OPTIONS,
    TASK_LIST_COLUMNS,
    _make_ag_grid,
)
from services import (
    ANALYSIS_DEFAULTS,
    approve_user,
    cancel_task,
    change_user_role,
    create_share_link,
    delete_covariate_preset,
    delete_matching_settings_preset,
    delete_user,
    download_results_csv,
    force_reexport,
    force_remerge,
    get_covariate_inventory,
    get_covariate_presets,
    get_matching_settings_presets,
    get_ready_covariate_names,
    get_ready_exact_match_names,
    get_task_detail,
    get_task_list,
    get_task_site_results,
    get_user_site_set_detail,
    get_user_site_set_geojson_by_bounds_and_zoom,
    grant_te_script_access,
    list_share_links,
    list_user_site_sets,
    get_user_list,
    revoke_share_link,
    revoke_te_script_access,
    save_covariate_preset,
    save_matching_settings_preset,
    discard_staged_site_upload,
    start_gee_export,
    queue_analysis_task,
    update_task_info,
    archive_user_site_set,
    cancel_user_site_upload,
    create_user_site_upload,
    delete_user_site_set,
    delete_user_site_upload,
    list_user_site_uploads,
    rename_user_site_set,
)
from callbacks._helpers import (
    _authorize_task_access,
    _check_task_access,
    _fmt_dt,
    _openlayers_map_component,
    _record_covariate_action_failure,
    _render_share_links_list,
)
from callbacks._detail_builders import (
    _add_ci_band,
    _build_overview,
    _build_plots,
    _build_raw_results,
    _build_results_content,
)
from callbacks._match_quality import (
    _build_all_match_quality_plots,
    _build_map,
    _build_match_quality,
    _build_plots_from_summary,
    _build_quality_warning_banner,
    _compute_quality_warnings,
)

logger = logging.getLogger(__name__)


def register_callbacks(app, limiter=None):
    """Register all Dash callbacks on the app instance.

    Parameters
    ----------
    app : dash.Dash
        The Dash application.
    limiter : flask_limiter.Limiter, optional
        Flask-Limiter instance.  Currently unused directly (Dash funnels
        all callbacks through a single POST endpoint) but reserved for
        future use.  Auth-related callbacks are rate-limited via a
        lightweight Redis counter (see ``_is_rate_limited``).
    """

    # -- Per-IP rate limiting for auth callbacks -----------------------------
    # Flask-Limiter cannot distinguish individual Dash callbacks because
    # they all share the /_dash-update-component route.  We use a simple
    # Redis INCR + EXPIRE pattern instead.

    def _is_rate_limited(action: str, max_attempts: int = 10, window: int = 300):
        """Return True if the current IP has exceeded *max_attempts* for
        *action* within *window* seconds.  Silently returns False when
        Redis is unavailable."""
        try:
            from flask import request as _req
            import redis as _redis

            from config import Config as _Cfg

            ip = _req.remote_addr or "unknown"
            key = f"rl:{action}:{ip}"
            r = _redis.from_url(_Cfg.CELERY_BROKER_URL, decode_responses=True)
            count = r.incr(key)
            if count == 1:
                r.expire(key, window)
            return count > max_attempts
        except Exception:
            return False

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
    # the before_request refresh-token validation in app.py.  If the user
    # has been inactive for >4 hours the before_request hook invalidates
    # the session and this callback redirects to /login.

    @app.callback(
        Output("session-check-output", "children"),
        Input("session-check-interval", "n_intervals"),
        prevent_initial_call=True,
    )
    def check_session_alive(n_intervals):
        if not flask_login.current_user.is_authenticated:
            return dcc.Location(pathname="/login", id="redirect-session-expired")
        raise PreventUpdate

    # -- Login ---------------------------------------------------------------

    @app.callback(
        Output("login-error", "children"),
        Input("login-button", "n_clicks"),
        State("login-email", "value"),
        State("login-password", "value"),
        prevent_initial_call=True,
    )
    def handle_login(n_clicks, email, password):
        if _is_rate_limited("login", max_attempts=10, window=300):
            return "Too many login attempts. Please try again in a few minutes."

        if not email or not password:
            return "Please enter email and password."

        result = authenticate(email, password)
        if result == "pending_approval":
            return "Your account is pending admin approval."
        if result:
            flask_login.login_user(result, remember=True)
            # Create a refresh token ΓÇö the after_request hook in app.py
            # reads this from the Flask session and sets the cookie.
            token = create_refresh_token(result.id)
            if token:
                flask.session["_pending_refresh_token"] = token
            return dcc.Location(pathname="/", id="redirect-login")
        return "Invalid email or password."

    # -- Registration --------------------------------------------------------

    @app.callback(
        Output("register-message", "children"),
        Input("register-button", "n_clicks"),
        State("register-name", "value"),
        State("register-email", "value"),
        prevent_initial_call=True,
    )
    def handle_register(n_clicks, name, email):
        if _is_rate_limited("register", max_attempts=5, window=600):
            return dbc.Alert(
                "Too many registration attempts. Please try again later.",
                color="danger",
            )

        if not name or not email:
            return dbc.Alert(
                "Please fill in all fields.",
                color="warning",
                duration=5000,
            )

        success, message = register_user(email, name)
        color = "success" if success else "danger"
        return dbc.Alert(message, color=color)

    # -- Forgot password -----------------------------------------------------

    @app.callback(
        Output("forgot-message", "children"),
        Input("forgot-button", "n_clicks"),
        State("forgot-email", "value"),
        prevent_initial_call=True,
    )
    def handle_forgot_password(n_clicks, email):
        if _is_rate_limited("forgot", max_attempts=5, window=600):
            return dbc.Alert(
                "Too many reset requests. Please try again later.",
                color="danger",
            )

        if not email:
            return dbc.Alert(
                "Please enter your email address.",
                color="warning",
                duration=5000,
            )
        request_password_reset(email)
        return dbc.Alert(
            "If an account with that email exists, a password reset "
            "link has been sent. Please check your inbox.",
            color="success",
        )

    # -- Reset password ------------------------------------------------------

    @app.callback(
        Output("reset-message", "children"),
        Input("reset-button", "n_clicks"),
        State("reset-token-store", "data"),
        State("reset-password", "value"),
        State("reset-password-confirm", "value"),
        prevent_initial_call=True,
    )
    def handle_reset_password(n_clicks, token, password, password_confirm):
        if not token:
            return dbc.Alert(
                "Invalid or missing reset token. Please request a new "
                "password reset link.",
                color="danger",
            )
        if not password:
            return dbc.Alert(
                "Please enter a new password.",
                color="warning",
                duration=5000,
            )

        from auth import validate_password

        pw_errors = validate_password(password)
        if pw_errors:
            return dbc.Alert(
                html.Ul([html.Li(e) for e in pw_errors]),
                color="warning",
            )

        if password != password_confirm:
            return dbc.Alert(
                "Passwords do not match.",
                color="danger",
                duration=5000,
            )
        success, message = reset_password_with_token(token, password)
        color = "success" if success else "danger"
        result = [dbc.Alert(message, color=color)]
        if success:
            result.append(
                html.P(
                    dcc.Link(
                        "Go to login",
                        href="/login",
                        className="fw-bold",
                    ),
                    className="text-center mt-2",
                )
            )
        return html.Div(result)

    # -- Real-time password requirements hints -------------------------------

    @app.callback(
        [
            Output("req-length", "className"),
            Output("req-uppercase", "className"),
            Output("req-lowercase", "className"),
            Output("req-number", "className"),
            Output("req-special", "className"),
            Output("req-match", "className"),
        ],
        [
            Input("reset-password", "value"),
            Input("reset-password-confirm", "value"),
        ],
    )
    def validate_password_requirements(password, confirm):
        import re

        pw = password or ""
        conf = confirm or ""

        def _cls(ok: bool) -> str:
            if not pw:
                return "text-muted"
            return "text-success" if ok else "text-danger"

        return (
            _cls(len(pw) >= 12),
            _cls(bool(re.search(r"[A-Z]", pw))),
            _cls(bool(re.search(r"[a-z]", pw))),
            _cls(bool(re.search(r"\d", pw))),
            _cls(bool(re.search(r"[^A-Za-z0-9]", pw))),
            _cls(bool(pw and pw == conf)),
        )

    # -- Reusable site sets --------------------------------------------------

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
    # Review summary ΓÇö rendered when the user switches to the Review tab
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
        State("group-by-exact-matches", "value"),
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
        group_by_exact_matches,
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
                                _param_row("Name", task_name or "ΓÇö"),
                                _param_row("Description", description or "(none)"),
                                _param_row(
                                    "Sites",
                                    (
                                        f"{n_sites} site(s)"
                                        + (
                                            f" ΓÇö {site_set_name}"
                                            if site_set_name
                                            else ""
                                        )
                                    )
                                    if sites_data
                                    else "ΓÇö",
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
                                    "Separation fallback",
                                    "Mahalanobis"
                                    if separation_fallback_mahalanobis
                                    else "Disabled (GLM fails)",
                                ),
                                _param_row(
                                    "Group by exact-match regions",
                                    "Enabled" if group_by_exact_matches else "Disabled",
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
                                    str(random_seed) if random_seed else "ΓÇö",
                                ),
                                _param_row(
                                    "Matching memory",
                                    f"{match_memory_gb} GB",
                                ),
                                _param_row(
                                    "Batch job queue",
                                    str(matching_job_queue or "ΓÇö"),
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
        if not user or not user.is_admin:
            return (
                dbc.Alert("Admin access required.", color="danger"),
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
            except Exception:
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
                {"label": f"{c['name']} ({c['dtype']})", "value": c["name"]}
                for c in columns
            ]
            end_options = [{"label": "(none)", "value": ""}] + options

            pending_data = {
                "filename": response.get("filename") or payload.get("filename"),
                "upload_token": response.get("upload_token"),
                "n_features": preview.get("n_features", 0),
                "available_columns": [c.get("name") for c in columns],
            }

            return (
                dbc.Alert(
                    (
                        f"Parsed {preview.get('n_features', 0)} features. "
                        "Confirm column mapping to start the background import."
                    ),
                    color="info",
                ),
                no_update,
                True,
                False,
                pending_data,
                options,
                suggested.get("site_id"),
                options,
                suggested.get("site_name"),
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
                        False,
                        True,
                        "",
                    )

                mapping = {
                    "site_id": mapped_site_id,
                    "site_name": mapped_site_name,
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
                return (
                    dbc.Alert(
                        (
                            f"Queued background import for '{upload['filename']}'. "
                            "Track progress in the Site Imports table below."
                        ),
                        color="success",
                    ),
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
        ],
        [
            Input("admin-refresh-interval", "n_intervals"),
            Input("site-set-refresh-store", "data"),
            Input("admin-show-archived-site-sets", "value"),
        ],
    )
    def refresh_admin_site_upload_views(_n_intervals, _refresh_token, show_archived):
        user = get_current_user()
        if not user or not user.is_admin:
            raise PreventUpdate

        site_sets = list_user_site_sets(user.id, include_archived=True)
        uploads = list_user_site_uploads(user.id)

        site_set_lookup = {ss["id"]: ss for ss in site_sets}

        rows = []
        for upload in uploads:
            row = dict(upload)
            site_set_id = row.get("site_set_id")
            if site_set_id:
                ss = site_set_lookup.get(site_set_id, {})
                row["is_archived"] = ss.get("is_archived", False)
            else:
                row["is_archived"] = False
            rows.append(row)

        if not bool(show_archived):
            rows = [r for r in rows if not r.get("is_archived")]

        archived_count = sum(1 for r in rows if r.get("is_archived"))
        running_count = sum(1 for r in rows if r.get("status") == "running")
        queued_count = sum(1 for r in rows if r.get("status") == "pending")
        cancelled_count = sum(1 for r in rows if r.get("status") == "cancelled")

        return (
            rows,
            f"Total: {len(rows)} | Archived: {archived_count} | Pending: {queued_count}"
            f" | Running: {running_count} | Cancelled: {cancelled_count}",
        )

    @app.callback(
        [
            Output("admin-combined-site-action-status", "children"),
            Output("site-set-refresh-store", "data", allow_duplicate=True),
        ],
        Input("admin-combined-site-table", "cellRendererData"),
        prevent_initial_call=True,
    )
    def handle_admin_combined_site_row_actions(renderer_data):
        if not renderer_data:
            raise PreventUpdate

        value = renderer_data.get("value") or {}
        action = value.get("action")
        if not action:
            raise PreventUpdate

        user = get_current_user()
        if not user or not user.is_admin:
            return dbc.Alert("Admin access required.", color="danger"), no_update

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
            "site_id": mapped_site_id,
            "site_name": mapped_site_name,
            "start_date": mapped_start_date,
        }
        missing = [key for key, value in required.items() if not value]
        if missing:
            return dbc.Alert(
                f"Select source columns for required fields: {', '.join(missing)}.",
                color="warning",
                className="mb-0 py-2",
            )

        chosen = [mapped_site_id, mapped_site_name, mapped_start_date]
        if mapped_end_date:
            chosen.append(mapped_end_date)
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
                f"Mapping looks good for {pending_upload.get('n_features', 0)} features. "
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
                    "headerName": "Site ID",
                    "field": "site_id",
                    "flex": 1,
                    "minWidth": 110,
                },
                {
                    "headerName": "Site Name",
                    "field": "site_name",
                    "flex": 2,
                    "minWidth": 160,
                },
                {
                    "headerName": "Start Date",
                    "field": "start_date",
                    "flex": 1,
                    "minWidth": 120,
                },
                {
                    "headerName": "End Date",
                    "field": "end_date",
                    "flex": 1,
                    "minWidth": 120,
                },
            ]
            preview_table = _make_ag_grid(
                "site-preview-table",
                preview_cols,
                row_data=detail["preview_rows"],
                height="320px",
                grid_options_extra={
                    "rowSelection": {
                        "mode": "singleRow",
                        "enableClickSelection": True,
                    },
                    "getRowId": {"function": "params.data.preview_row_id"},
                },
            )

            # Build metadata with indicator for sampled data
            is_sampled = detail.get("geojson") and "_is_sample" in detail.get(
                "geojson", "{}"
            )
            sample_note = html.Br() if not is_sampled else None
            if is_sampled:
                sample_note = html.Small(
                    "⚠ Map shows simplified preview. Full dataset will be used for analysis.",
                    className="d-block text-warning mt-2",
                )

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
            if sample_note:
                metadata_items.append(sample_note)

            metadata = html.Div(metadata_items)

            store_data = {
                "site_set_id": detail["id"],
                "geojson": detail["geojson"],
                "n_sites": detail["n_sites"],
                "filename": detail["filename"],
                "name": detail["name"],
            }

            return (
                store_data,
                preview_table,
                _openlayers_map_component(
                    "submit-sites-map",
                    detail["geojson"],
                    height="500px",
                    enable_cog_layers=True,
                    resolution_m=resolution_m,
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

    app.clientside_callback(
        """
        function(n_intervals) {
            if (!window._submitSitesMapState) {
                return dash_clientside.no_update;
            }
            if (!window._submitSitesMapState.hasUpdate) {
                return dash_clientside.no_update;
            }
            
            // Reset the flag so we don't keep firing the callback
            window._submitSitesMapState.hasUpdate = false;
            
            // Return the zoom/bounds data to update the store
            return {
                zoom: window._submitSitesMapState.zoom,
                bounds: window._submitSitesMapState.bounds
            };
        }
        """,
        Output("zoom-bounds-store", "data"),
        Input("zoom-bounds-poll", "n_intervals"),
        prevent_initial_call=True,
    )

    @app.callback(
        Output("site-preview-map", "children", allow_duplicate=True),
        Input("zoom-bounds-store", "data"),
        State("parsed-sites-store", "data"),
        State("resolution-m", "value"),
        prevent_initial_call=True,
    )
    def update_map_on_zoom(zoom_bounds_data, sites_data, resolution_m_str):
        """Update map GeoJSON when user zooms/pans, using zoom-aware sampling.

        This callback is triggered when the map emits a zoom/bounds change event.
        It fetches new sampled data at the appropriate detail level for the current
        zoom, preventing unnecessary rendering of thousands of features at low zoom.
        """
        if not zoom_bounds_data or not sites_data:
            raise PreventUpdate

        site_set_id = sites_data.get("site_set_id")
        if not site_set_id:
            raise PreventUpdate

        try:
            zoom = zoom_bounds_data.get("zoom", 2)
            bounds = zoom_bounds_data.get("bounds", {})
            minx = bounds.get("minx")
            miny = bounds.get("miny")
            maxx = bounds.get("maxx")
            maxy = bounds.get("maxy")

            if None in (minx, miny, maxx, maxy):
                raise PreventUpdate

            resolution_m = int(resolution_m_str) if resolution_m_str else 1000
            geojson_fc = get_user_site_set_geojson_by_bounds_and_zoom(
                site_set_id, zoom, minx, miny, maxx, maxy
            )

            return _openlayers_map_component(
                "submit-sites-map",
                json.dumps(geojson_fc),
                height="500px",
                enable_cog_layers=True,
                resolution_m=resolution_m,
            )
        except Exception:
            logger.exception("Error updating map on zoom")
            raise PreventUpdate

    # -- Task submission -----------------------------------------------------

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
        State("group-by-exact-matches", "value"),
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
        group_by_exact_matches,
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
                "and exact matches ΓÇö each must be one or the other: "
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
                group_by_exact_matches=bool(group_by_exact_matches),
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
                        "\u2018submitted\u2019 within a few seconds."
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
        ],
    )
    def refresh_task_list(_n_intervals, _n_clicks, show_all):
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
                    "filter": "agTextColumnFilter",
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
        for task in tasks:
            cfg = task.config or {}
            covariates = task.covariates or []
            exact_matches = cfg.get("exact_match_vars", [])
            row = {
                "id": str(task.id),
                "name": task.name,
                "status": task.status,
                "n_sites": task.n_sites or 0,
                "covariates_short": ", ".join(covariates),
                "covariates_full": ", ".join(covariates),
                "exact_matches_short": ", ".join(exact_matches),
                "exact_matches_full": ", ".join(exact_matches),
                "max_treatment_pixels": cfg.get("max_treatment_pixels"),
                "control_multiplier": cfg.get("control_multiplier"),
                "caliper_width": cfg.get("caliper_width"),
                "max_controls_per_treatment": cfg.get("max_controls_per_treatment"),
                "matching_method": cfg.get("matching_method", "optimal"),
                "created_at": _fmt_dt(task.created_at),
                "submitted_at": _fmt_dt(task.submitted_at),
                "completed_at": _fmt_dt(task.completed_at),
                "error_message": task.error_message or "",
            }
            if show_all_users:
                row["submitted_by_name"] = task.user.name if task.user else "Unknown"
            rows.append(row)

        return rows, columns, f"Total: {len(rows)}"

    # -- Task detail ---------------------------------------------------------

    @app.callback(
        [
            Output("task-title", "children"),
            Output("task-status-badge", "children"),
            Output("task-overview", "children"),
            Output("task-results-content", "children"),
            Output("task-plots", "children"),
            Output("task-match-quality", "children"),
            Output("task-map", "children"),
            Output("task-raw-results", "children"),
            Output("quality-warning-banner", "children"),
            Output("detail-refresh-interval", "disabled"),
        ],
        [
            Input("detail-refresh-interval", "n_intervals"),
            Input("detail-tabs", "active_tab"),
        ],
        [
            State("task-id-store", "data"),
            State("share-token-store", "data"),
        ],
    )
    def refresh_task_detail(n, active_tab, task_id, share_token):
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
                None,
                None,
                None,
                True,
            )

        # Batch task status is polled by the Celery Beat worker;
        # this callback just reads the current DB state.
        detail = get_task_detail(task_id)
        if not detail:
            return (
                "Task Not Found",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                True,
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

        # Quality warning banner (above tabs) ΓÇö only for succeeded tasks
        quality_banner = html.Div()
        quality_warnings = []
        if task.status == "succeeded":
            quality_warnings = _compute_quality_warnings(task_id, task, totals)
            quality_banner = _build_quality_warning_banner(quality_warnings)

        # -----------------------------------------------------------
        # Lazy tab rendering: only build content for the *active* tab.
        # This dramatically reduces peak memory because heavy tabs
        # (plots, match quality, map) are only built when viewed.
        # Inactive tabs return ``no_update`` so Dash keeps whatever
        # was last rendered (empty on first visit, populated once the
        # user clicks the tab).
        # -----------------------------------------------------------
        overview = no_update
        results_content = no_update
        plots = no_update
        match_quality = no_update
        map_content = no_update
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
            )
        elif active_tab == "tab-plots":
            plots = (
                _build_plots(
                    results,
                    totals,
                    sites,
                    task=task,
                    quality_warnings=quality_warnings,
                    agg_yearly=agg_yearly,
                    is_large=is_large,
                )
                if (results or agg_yearly)
                else html.P("Results not yet available.", className="text-muted")
            )
        elif active_tab == "tab-match-quality":
            match_quality = _build_match_quality(task_id, task, sites, totals)
        elif active_tab == "tab-map":
            task_cfg = task.config or {}
            map_content = _build_map(
                detail.get("sites_geojson"),
                totals,
                covariates=task.covariates,
                task_id=task_id,
                sites=sites,
                resolution_m=task_cfg.get("resolution_m"),
            )
        elif active_tab == "tab-raw-results":
            raw_results = _build_raw_results(task)

        return (
            title,
            badge,
            overview,
            results_content,
            plots,
            match_quality,
            map_content,
            raw_results,
            quality_banner,
            disable_interval,
        )

    # -- Result downloads ----------------------------------------------------

    @app.callback(
        Output("download-results", "data"),
        [Input("download-by-year", "n_clicks"), Input("download-totals", "n_clicks")],
        [
            State("task-id-store", "data"),
            State("share-token-store", "data"),
        ],
        prevent_initial_call=True,
    )
    def handle_download(by_year_clicks, total_clicks, task_id, share_token):
        ctx = callback_context
        if not ctx.triggered:
            raise PreventUpdate

        if not _authorize_task_access(task_id, share_token):
            raise PreventUpdate

        trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]
        if trigger_id == "download-by-year":
            csv = download_results_csv(task_id, "by_site_year")
            filename = "results_by_site_year.csv"
        else:
            csv = download_results_csv(task_id, "by_site_total")
            filename = "results_by_site_total.csv"

        if csv:
            return dict(content=csv, filename=filename)
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
            return dict(content=csv, filename="results_pixel_covariates.csv")
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
        ],
        prevent_initial_call=True,
    )
    def toggle_share_modal(n_clicks, is_open, task_id):
        if not n_clicks:
            raise PreventUpdate

        user = get_current_user()
        if not user or not _check_task_access(task_id, user):
            raise PreventUpdate

        if is_open:
            # Closing ΓÇö return empty list to avoid stale data
            return False, html.Div()

        # Opening ΓÇö fetch existing links
        links = list_share_links(task_id, str(user.id))
        return True, _render_share_links_list(links, task_id)

    @app.callback(
        [
            Output("share-link-result", "children"),
            Output("share-links-list", "children", allow_duplicate=True),
        ],
        Input("generate-share-link", "n_clicks"),
        [
            State("share-expiry-days", "value"),
            State("task-id-store", "data"),
        ],
        prevent_initial_call=True,
    )
    def generate_share(n_clicks, expiry_days, task_id):
        if not n_clicks:
            raise PreventUpdate

        user = get_current_user()
        if not user or not _check_task_access(task_id, user):
            raise PreventUpdate

        try:
            result = create_share_link(
                task_id, str(user.id), expiry_days=int(expiry_days)
            )
            from flask import request as flask_request

            base_url = flask_request.host_url.rstrip("/")
            share_url = f"{base_url}/shared/{result['token']}"

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
            links = list_share_links(task_id, str(user.id))
            return link_display, _render_share_links_list(links, task_id)
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
        prevent_initial_call=True,
    )
    def revoke_share(n_clicks_list, task_id):
        ctx = callback_context
        if not ctx.triggered or not any(n_clicks_list):
            raise PreventUpdate

        user = get_current_user()
        if not user or not _check_task_access(task_id, user):
            raise PreventUpdate

        trigger = ctx.triggered[0]
        import json as _json

        link_id = _json.loads(trigger["prop_id"].rsplit(".", 1)[0])["index"]
        revoke_share_link(link_id, str(user.id), task_id=task_id)

        links = list_share_links(task_id, str(user.id))
        return _render_share_links_list(links, task_id)

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

        # Opening ΓÇö populate inputs with current values
        user = get_current_user()
        if not user or not _check_task_access(task_id, user):
            raise PreventUpdate

        detail = get_task_detail(task_id)
        if not detail:
            raise PreventUpdate

        task = detail["task"]
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
            window.location.href = '/submit?recompute=' + task_id;
            return '';
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
        function(sortOrder, storedOptions) {
            if (!storedOptions || !storedOptions[sortOrder]) {
                return window.dash_clientside.no_update;
            }
            return storedOptions[sortOrder];
        }
        """,
        Output("site-defor-selector", "options"),
        Input("site-defor-sort-order", "value"),
        State("site-defor-sort-options", "data"),
        prevent_initial_call=True,
    )

    # -- Sort-order radio for match quality site dropdown --------------------

    app.clientside_callback(
        """
        function(sortOrder, storedOptions) {
            if (!storedOptions || !storedOptions[sortOrder]) {
                return window.dash_clientside.no_update;
            }
            return storedOptions[sortOrder];
        }
        """,
        Output("match-quality-site-selector", "options"),
        Input("match-quality-sort-order", "value"),
        State("match-quality-sort-options", "data"),
        prevent_initial_call=True,
    )

    # -- Sort-order radio for map site dropdown ------------------------------

    app.clientside_callback(
        """
        function(sortOrder, storedOptions) {
            if (!storedOptions || !storedOptions[sortOrder]) {
                return window.dash_clientside.no_update;
            }
            return storedOptions[sortOrder];
        }
        """,
        Output("map-site-selector", "options"),
        Input("map-site-sort-order", "value"),
        State("map-site-sort-options", "data"),
        prevent_initial_call=True,
    )

    # -- Map site selector: zoom to site and filter pixels -------------------

    app.clientside_callback(
        """
        function(selectedSite) {
            var mapEl = document.getElementById("task-sites-map");
            if (mapEl) {
                mapEl.dispatchEvent(
                    new CustomEvent("zoom-to-site", {
                        detail: { siteId: selectedSite || "" },
                    })
                );
            }
            return window.dash_clientside.no_update;
        }
        """,
        Output("map-site-selector", "id"),
        Input("map-site-selector", "value"),
        prevent_initial_call=True,
    )

    # -- Cancel task (detail page) -------------------------------------------

    @app.callback(
        Output("cancel-task-result", "children"),
        Input("cancel-task-btn", "n_clicks"),
        State("task-id-store", "data"),
        prevent_initial_call=True,
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
                "Task cancelled successfully. RefreshingΓÇª",
                color="success",
                duration=3000,
            )
        except (ValueError, PermissionError) as e:
            return dbc.Alert(str(e), color="warning", duration=4000)
        except Exception:
            logger.exception("Error cancelling task %s", task_id)
            return dbc.Alert("Failed to cancel task.", color="danger", duration=4000)

    # -- Task actions from task list table (recompute / cancel) ---------------

    @app.callback(
        [
            Output("recompute-from-list-result", "children"),
            Output("cancel-from-list-result", "children"),
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
            )

        if action == "cancel":
            user = get_current_user()
            if not user:
                return (
                    no_update,
                    dbc.Alert("Please log in first.", color="danger", duration=4000),
                )
            try:
                cancel_task(task_id, user)
                return (
                    no_update,
                    dbc.Alert("Task cancelled.", color="success", duration=3000),
                )
            except (ValueError, PermissionError) as e:
                return (
                    no_update,
                    dbc.Alert(str(e), color="warning", duration=4000),
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
                )

        raise PreventUpdate

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
        if trigger == "start-gee-export":
            return True
        return False

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
        total_label = f"Total: {len(rows)} | On GCS: {gcs_count} | On S3: {s3_count}"

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

    # -- Admin: User management (AG Grid) ------------------------------------

    @app.callback(
        [
            Output("user-management-table", "rowData"),
            Output("user-management-total-count", "children"),
        ],
        Input("admin-refresh-interval", "n_intervals"),
    )
    def refresh_user_management(n):
        user = get_current_user()
        if not user or not user.is_admin:
            raise PreventUpdate

        users = get_user_list()
        if not users:
            return [], "Total: 0"

        rows = []
        for u in users:
            rows.append(
                {
                    "id": str(u.id),
                    "name": u.name,
                    "email": u.email,
                    "role": u.role,
                    "is_approved": u.is_approved,
                    "created_at": _fmt_dt(u.created_at),
                    "last_login": _fmt_dt(u.last_login),
                    "is_active": u.is_active,
                }
            )

        return rows, f"Total: {len(rows)}"

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
                "label": f"{r['name']} ({r['email']})"
                + (" [pending]" if not r.get("is_approved") else ""),
                "value": r["id"],
            }
            for r in row_data
        ]

    # -- Settings: link trends.earth account ---------------------------------

    @app.callback(
        [Output("te-link-message", "children"), Output("te-link-done-store", "data")],
        Input("te-link-btn", "n_clicks"),
        State("te-link-email", "value"),
        State("te-link-password", "value"),
        prevent_initial_call=True,
    )
    def handle_te_link(n_clicks, email, password):
        """Log in to trends.earth, register an OAuth2 client, store creds."""
        if not email or not password:
            return (
                dbc.Alert(
                    "Please enter both email and password.",
                    color="warning",
                    duration=5000,
                ),
                no_update,
            )

        user = get_current_user()
        if not user:
            return (
                dbc.Alert("Please log in first.", color="danger", duration=5000),
                no_update,
            )

        from config import Config
        from credential_store import save_credential
        from trendsearth_client import TrendsEarthClient

        try:
            # 1. Authenticate with email/password to get a JWT
            client = TrendsEarthClient(
                api_url=Config.TRENDSEARTH_API_URL,
                email=email,
                password=password,
            )
            # Force login to verify credentials
            client._login()

            # 2. Fetch the API-side user profile to capture te_user_id
            te_user_id = None
            try:
                profile = client.get_user_profile()
                te_user_id = (profile.get("data") or {}).get("id")
            except Exception:
                logger.warning("Could not fetch trends.earth user profile")

            # 3. Register an OAuth2 service client
            result = client.create_oauth2_client(
                name=f"avoided-emissions-web ({user.email})",
            )
            data = result.get("data", {})
            client_id = data.get("client_id", "")
            client_secret = data.get("client_secret", "")
            api_client_db_id = data.get("id", "")

            if not client_id or not client_secret:
                return (
                    dbc.Alert(
                        "The API did not return client credentials. Please try again.",
                        color="danger",
                    ),
                    no_update,
                )

            # 4. Store encrypted credentials locally
            save_credential(
                user_id=user.id,
                te_email=email,
                client_id=client_id,
                client_secret=client_secret,
                client_name=f"avoided-emissions-web ({user.email})",
                api_client_db_id=api_client_db_id,
                te_user_id=te_user_id,
            )

            # 5. Grant the user access to the AE script on the TE API
            try:
                grant_te_script_access(user.id)
            except Exception:
                logger.warning(
                    "Linked TE account but failed to grant script "
                    "access for user %s (will retry on next login)",
                    user.id,
                    exc_info=True,
                )

            return (
                dbc.Alert(
                    "Successfully linked to trends.earth! "
                    "Your client credentials have been securely stored.",
                    color="success",
                ),
                True,
            )

        except Exception as e:
            logger.exception("Failed to link trends.earth account")
            report_exception(action="te_link", user_id=str(user.id))
            msg = str(e)
            if "401" in msg or "Unauthorized" in msg:
                msg = "Invalid email or password."
            elif "Max" in msg or "limit" in msg.lower():
                msg = (
                    "Maximum number of OAuth2 clients reached on your "
                    "trends.earth account. Please revoke an existing client "
                    "at trends.earth first."
                )
            else:
                msg = (
                    "Failed to link account. Please try again later "
                    "or contact support if the problem persists."
                )
            return (
                dbc.Alert(msg, color="danger"),
                no_update,
            )

    # -- Settings: test connection -------------------------------------------

    @app.callback(
        Output("te-credential-status", "children"),
        Input("te-test-connection-btn", "n_clicks"),
        prevent_initial_call=True,
    )
    def handle_te_test_connection(n_clicks):
        """Test the stored OAuth2 credentials by requesting a token."""
        user = get_current_user()
        if not user:
            raise PreventUpdate

        from config import Config
        from credential_store import get_decrypted_secret
        from trendsearth_client import TrendsEarthClient

        creds = get_decrypted_secret(user.id)
        if not creds:
            return dbc.Alert(
                "No stored credentials found.",
                color="warning",
                duration=5000,
            )

        client_id, client_secret = creds
        try:
            client = TrendsEarthClient(api_url=Config.TRENDSEARTH_API_URL)
            token_data = client.oauth2_token(client_id, client_secret)
            if token_data.get("access_token"):
                return dbc.Alert(
                    "Connection successful! Access token obtained.",
                    color="success",
                    duration=5000,
                )
            return dbc.Alert(
                "Unexpected response from API.",
                color="warning",
                duration=5000,
            )
        except Exception as e:
            logger.exception("trends.earth connection test failed")
            report_exception(action="te_test_connection", user_id=str(user.id))
            msg = str(e)
            if "401" in msg or "Unauthorized" in msg:
                user_message = (
                    "Connection failed: stored credentials are invalid or expired."
                )
            else:
                user_message = (
                    "Connection test failed. Please try again later "
                    "or relink your account."
                )
            return dbc.Alert(
                user_message,
                color="danger",
                duration=8000,
            )

    # -- Settings: unlink account --------------------------------------------

    @app.callback(
        Output("te-credential-status", "children", allow_duplicate=True),
        Input("te-unlink-btn", "n_clicks"),
        prevent_initial_call=True,
    )
    def handle_te_unlink(n_clicks):
        """Revoke the OAuth2 client on the API and delete local credentials."""
        user = get_current_user()
        if not user:
            raise PreventUpdate

        from config import Config
        from credential_store import (
            delete_credential,
            get_credential,
            get_decrypted_secret,
        )
        from trendsearth_client import TrendsEarthClient

        cred = get_credential(user.id)
        if not cred:
            return dbc.Alert("No linked account.", color="info", duration=4000)

        # Revoke script access (best-effort, before deleting credential)
        try:
            revoke_te_script_access(user.id)
        except Exception:
            logger.warning(
                "Failed to revoke TE script access during unlink "
                "(continuing with local cleanup)",
                exc_info=True,
            )

        # Try to revoke the OAuth2 client on the API side (best-effort)
        if cred.api_client_db_id:
            try:
                creds = get_decrypted_secret(user.id)
                if creds:
                    client_id, client_secret = creds
                    client = TrendsEarthClient.from_oauth2_credentials(
                        api_url=Config.TRENDSEARTH_API_URL,
                        client_id=client_id,
                        client_secret=client_secret,
                    )
                    client.revoke_oauth2_client(cred.api_client_db_id)
            except Exception:
                logger.warning(
                    "Failed to revoke OAuth2 client on API (continuing "
                    "with local cleanup)",
                    exc_info=True,
                )

        delete_credential(user.id)
        return dbc.Alert(
            "Account unlinked. Refresh the page to update the display.",
            color="success",
        )

    # -- Settings: change password -------------------------------------------

    @app.callback(
        Output("change-pw-message", "children"),
        Input("change-pw-btn", "n_clicks"),
        [
            State("change-pw-current", "value"),
            State("change-pw-new", "value"),
            State("change-pw-confirm", "value"),
        ],
        prevent_initial_call=True,
    )
    def handle_change_password(n_clicks, current_pw, new_pw, confirm_pw):
        if _is_rate_limited("change_pw", max_attempts=5, window=300):
            return dbc.Alert(
                "Too many attempts. Please try again later.",
                color="danger",
            )

        user = get_current_user()
        if not user:
            raise PreventUpdate

        if not current_pw:
            return dbc.Alert(
                "Please enter your current password.",
                color="warning",
                duration=5000,
            )
        if not new_pw:
            return dbc.Alert(
                "Please enter a new password.",
                color="warning",
                duration=5000,
            )
        if new_pw != confirm_pw:
            return dbc.Alert(
                "New passwords do not match.",
                color="danger",
                duration=5000,
            )

        from auth import change_password

        success, message = change_password(user.id, current_pw, new_pw)
        color = "success" if success else "danger"
        return dbc.Alert(message, color=color, duration=8000 if success else None)

    # -- Settings: change password real-time hints ---------------------------

    @app.callback(
        [
            Output("cp-req-length", "className"),
            Output("cp-req-uppercase", "className"),
            Output("cp-req-lowercase", "className"),
            Output("cp-req-number", "className"),
            Output("cp-req-special", "className"),
            Output("cp-req-match", "className"),
        ],
        [
            Input("change-pw-new", "value"),
            Input("change-pw-confirm", "value"),
        ],
    )
    def validate_change_pw_requirements(password, confirm):
        import re

        pw = password or ""
        conf = confirm or ""

        def _cls(ok: bool) -> str:
            if not pw:
                return "text-muted"
            return "text-success" if ok else "text-danger"

        return (
            _cls(len(pw) >= 12),
            _cls(bool(re.search(r"[A-Z]", pw))),
            _cls(bool(re.search(r"[a-z]", pw))),
            _cls(bool(re.search(r"\d", pw))),
            _cls(bool(re.search(r"[^A-Za-z0-9]", pw))),
            _cls(bool(pw and pw == conf)),
        )

    # -- Admin: approve user -------------------------------------------------

    @app.callback(
        [
            Output("admin-user-action-result", "children", allow_duplicate=True),
            Output("admin-refresh-interval", "n_intervals", allow_duplicate=True),
        ],
        Input("admin-approve-btn", "n_clicks"),
        State("admin-user-select", "value"),
        State("admin-refresh-interval", "n_intervals"),
        prevent_initial_call=True,
    )
    def handle_approve_user(n_clicks, user_id, current_n):
        if not user_id:
            return dbc.Alert(
                "Please select a user.", color="warning", duration=4000
            ), no_update
        user = get_current_user()
        if not user or not user.is_admin:
            return dbc.Alert(
                "Admin access required.", color="danger", duration=4000
            ), no_update
        success, message = approve_user(user_id)
        color = "success" if success else "danger"
        # Bump n_intervals to force a refresh of the user table
        return dbc.Alert(message, color=color, duration=4000), (current_n or 0) + 1

    # -- Admin: change user role ---------------------------------------------

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
    def handle_change_role(n_clicks, user_id, new_role, current_n):
        if not user_id:
            return dbc.Alert(
                "Please select a user.", color="warning", duration=4000
            ), no_update
        user = get_current_user()
        if not user or not user.is_admin:
            return dbc.Alert(
                "Admin access required.", color="danger", duration=4000
            ), no_update
        success, message = change_user_role(user_id, new_role, user.id)
        color = "success" if success else "danger"
        return dbc.Alert(message, color=color, duration=4000), (current_n or 0) + 1

    # -- Admin: delete user (modal) ------------------------------------------

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
        if trigger == "admin-delete-btn":
            return True
        return False

    @app.callback(
        [
            Output("admin-user-action-result", "children", allow_duplicate=True),
            Output("admin-refresh-interval", "n_intervals", allow_duplicate=True),
        ],
        Input("admin-delete-confirm", "n_clicks"),
        State("admin-user-select", "value"),
        State("admin-refresh-interval", "n_intervals"),
        prevent_initial_call=True,
    )
    def handle_admin_delete_user(n_clicks, user_id, current_n):
        if not user_id:
            return dbc.Alert(
                "Please select a user.", color="warning", duration=4000
            ), no_update
        user = get_current_user()
        if not user or not user.is_admin:
            return dbc.Alert(
                "Admin access required.", color="danger", duration=4000
            ), no_update
        # Prevent admins from deleting themselves
        if str(user.id) == str(user_id):
            return dbc.Alert(
                "You cannot delete your own admin account.",
                color="warning",
                duration=4000,
            ), no_update
        success, message = delete_user(user_id)
        color = "success" if success else "danger"
        return dbc.Alert(message, color=color, duration=4000), (current_n or 0) + 1

    # -- Self account deletion (modal) ---------------------------------------

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
        if trigger == "self-delete-btn":
            return True
        return False

    @app.callback(
        Output("self-delete-result", "children"),
        Input("self-delete-confirm", "n_clicks"),
        prevent_initial_call=True,
    )
    def handle_self_delete(n_clicks):
        user = get_current_user()
        if not user:
            raise PreventUpdate
        success, message = delete_user(user.id)
        if success:
            cookie_token = flask.request.cookies.get("ae_refresh_token")
            revoke_refresh_token(cookie_token)
            flask_login.logout_user()
            flask.session["_clear_refresh_cookie"] = True
            return dcc.Location(pathname="/login", id="redirect-after-delete")
        return dbc.Alert(message, color="danger", duration=4000)

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

    # -- Match quality site filter -------------------------------------------

    @app.callback(
        Output("match-quality-plots-container", "children"),
        Input("match-quality-site-selector", "value"),
        State("match-quality-data-store", "data"),
        prevent_initial_call=True,
    )
    def update_match_quality_plots(selected_site, store_data):
        """Rebuild all match-quality plots when site filter changes.

        Renders summary stat boxes, Love plot (SMD), propensity score
        QQ plot, and covariate distribution histograms for the selected
        site (or aggregate across all sites).
        """
        if not store_data:
            raise PreventUpdate

        site_filter = None
        if selected_site and selected_site != "__all__":
            site_filter = selected_site

        quality_warnings = store_data.get("quality_warnings", [])
        scope = site_filter if site_filter else None
        warning_banner = _build_quality_warning_banner(
            quality_warnings, scope_filter=scope
        )

        if store_data.get("has_summary"):
            # New path: use pre-computed histograms and QQ quantiles
            return html.Div(
                [warning_banner] + _build_plots_from_summary(store_data, site_filter)
            )

        # Legacy path: raw pixel data (kept for backward compatibility
        # with any data still in browser stores from before the
        # pre-computation change).
        df = pd.DataFrame(store_data.get("rows", []))
        covariate_cols = store_data.get("covariate_cols", [])
        site_areas = store_data.get("site_areas", {})

        balance_rows = store_data.get("balance_rows")
        balance_df = pd.DataFrame(balance_rows) if balance_rows else None

        pscore_rows = store_data.get("pscore_rows")
        pscore_df = pd.DataFrame(pscore_rows) if pscore_rows else None

        if df.empty:
            return html.P("No data available.", className="text-muted")

        if site_filter:
            df = df[df["site_id"].astype(str) == str(selected_site)]

        if df.empty:
            return html.P("No data for selected site.", className="text-muted")

        return html.Div(
            [warning_banner]
            + _build_all_match_quality_plots(
                df,
                covariate_cols,
                balance_df,
                pscore_df,
                site_filter,
                site_areas=site_areas,
            )
        )

    # -- Site-level deforestation drill-down ----------------------------------

    @app.callback(
        Output("site-defor-plot-container", "children"),
        Input("site-defor-selector", "value"),
        State("site-defor-store", "data"),
        State("task-id-store", "data"),
        prevent_initial_call=True,
    )
    def update_site_deforestation_plot(selected_site, store_data, task_id):
        """Build per-site deforestation and emissions plots with intervention
        date markers when a site is selected from the dropdown."""
        if not selected_site or not store_data:
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
                line=dict(color="#2ca02c", width=2),
                marker=dict(size=6),
            )
        )
        fig_defor.add_trace(
            go.Scatter(
                x=site_df["year"],
                y=site_df["control_defor_ha"],
                mode="lines+markers",
                name="Matched Controls",
                line=dict(color="#d62728", width=2),
                marker=dict(size=6),
            )
        )
        if site_has_ci:
            _add_ci_band(
                fig_defor,
                site_df["year"],
                site_df["treatment_defor_ha_ci_lower"],
                site_df["treatment_defor_ha_ci_upper"],
                color="rgba(44,160,44,0.15)",
                name="Project Site 95% CI",
            )
            _add_ci_band(
                fig_defor,
                site_df["year"],
                site_df["control_defor_ha_ci_lower"],
                site_df["control_defor_ha_ci_upper"],
                color="rgba(214,39,40,0.15)",
                name="Matched Controls 95% CI",
            )
        fig_defor.update_layout(
            title=f"Annual Deforestation: {site_name}{sub_note}",
            xaxis_title="Year",
            yaxis_title="Deforestation (ha)",
            legend=dict(yanchor="top", y=0.99, xanchor="left", x=1.02),
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
        cum_treatment = site_sorted["treatment_defor_ha"].cumsum()
        cum_control = site_sorted["control_defor_ha"].cumsum()
        fig_cum = go.Figure()
        fig_cum.add_trace(
            go.Scatter(
                x=site_sorted["year"],
                y=cum_treatment,
                mode="lines+markers",
                name="Project Site",
                line=dict(color="#2ca02c", width=2),
                marker=dict(size=6),
            )
        )
        fig_cum.add_trace(
            go.Scatter(
                x=site_sorted["year"],
                y=cum_control,
                mode="lines+markers",
                name="Matched Controls",
                line=dict(color="#d62728", width=2),
                marker=dict(size=6),
            )
        )
        if site_has_ci:
            _add_ci_band(
                fig_cum,
                site_sorted["year"],
                site_sorted["treatment_defor_ha_ci_lower"].cumsum(),
                site_sorted["treatment_defor_ha_ci_upper"].cumsum(),
                color="rgba(44,160,44,0.15)",
                name="Project Site 95% CI",
            )
            _add_ci_band(
                fig_cum,
                site_sorted["year"],
                site_sorted["control_defor_ha_ci_lower"].cumsum(),
                site_sorted["control_defor_ha_ci_upper"].cumsum(),
                color="rgba(214,39,40,0.15)",
                name="Matched Controls 95% CI",
            )
        fig_cum.update_layout(
            title=f"Cumulative Deforestation: {site_name}{sub_note}",
            xaxis_title="Year",
            yaxis_title="Cumulative Deforestation (ha)",
            legend=dict(yanchor="top", y=0.99, xanchor="left", x=1.02),
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
                    line=dict(color="#2ca02c", width=2),
                    marker=dict(size=6),
                )
            )
            fig_em.add_trace(
                go.Scatter(
                    x=site_df["year"],
                    y=site_df["control_emissions_mgco2e"],
                    mode="lines+markers",
                    name="Matched Controls",
                    line=dict(color="#d62728", width=2),
                    marker=dict(size=6),
                )
            )
            if site_has_ci and "treatment_emissions_mgco2e_ci_lower" in site_df.columns:
                _add_ci_band(
                    fig_em,
                    site_df["year"],
                    site_df["treatment_emissions_mgco2e_ci_lower"],
                    site_df["treatment_emissions_mgco2e_ci_upper"],
                    color="rgba(44,160,44,0.15)",
                    name="Project Site 95% CI",
                )
                _add_ci_band(
                    fig_em,
                    site_df["year"],
                    site_df["control_emissions_mgco2e_ci_lower"],
                    site_df["control_emissions_mgco2e_ci_upper"],
                    color="rgba(214,39,40,0.15)",
                    name="Matched Controls 95% CI",
                )
            fig_em.update_layout(
                title=f"Annual Emissions: {site_name}{sub_note}",
                xaxis_title="Year",
                yaxis_title="Emissions (MgCOΓéée)",
                legend=dict(yanchor="top", y=0.99, xanchor="left", x=1.02),
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
        fig_avoided.update_layout(
            title=f"Forest Loss Avoided by Year: {site_name}{sub_note}",
            xaxis_title="Year",
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

    # -- Covariate presets ---------------------------------------------------

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

        presets = get_covariate_presets(user.id)
        options = [{"label": p["name"], "value": p["id"]} for p in presets]
        return options, presets

    @app.callback(
        [
            Output("covariate-selection", "value"),
            Output("exact-match-selection", "value"),
            Output("preset-feedback", "children", allow_duplicate=True),
        ],
        Input("load-preset-btn", "n_clicks"),
        State("preset-selector", "value"),
        State("presets-store", "data"),
        prevent_initial_call=True,
    )
    def load_preset(_n, preset_id, presets_data):
        """Set the checklist values to the covariates and exact match
        variables stored in the selected preset."""
        if not preset_id or not presets_data:
            return no_update, no_update, "Please select a preset to load."

        for p in presets_data:
            if p["id"] == preset_id:
                exact = p.get("exact_match_vars") or no_update
                return (
                    p["covariates"],
                    exact,
                    dbc.Alert(
                        f'Loaded preset "{p["name"]}".',
                        color="info",
                        duration=3000,
                    ),
                )

        return (
            no_update,
            no_update,
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
        prevent_initial_call=True,
    )
    def save_preset(_n, name, covariates, exact_match_vars):
        """Save the current covariate and exact-match selection as a
        named preset."""
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

        try:
            save_covariate_preset(user.id, name.strip(), covariates, exact_match_vars)
            updated = get_covariate_presets(user.id)
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
            logger.exception("Failed to save covariate preset")
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
            deleted = delete_covariate_preset(preset_id, user.id)
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

            updated = get_covariate_presets(user.id)
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
            logger.exception("Failed to delete covariate preset")
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

    # -- Matching settings presets -------------------------------------------

    @app.callback(
        [
            Output("settings-preset-selector", "options"),
            Output("matching-settings-presets-store", "data"),
        ],
        [
            Input("url", "pathname"),
            Input("matching-settings-presets-store", "modified_timestamp"),
        ],
    )
    def refresh_settings_presets(_pathname, _ts):
        """Populate the matching-settings preset dropdown on page load or
        after a save/delete."""
        user = get_current_user()
        if not user:
            raise PreventUpdate

        presets = get_matching_settings_presets(user.id)
        options = [{"label": p["name"], "value": p["id"]} for p in presets]
        return options, presets

    @app.callback(
        [
            Output("max-treatment-pixels", "value"),
            Output("control-multiplier", "value"),
            Output("min-site-area-ha", "value"),
            Output("min-glm-treatment-pixels", "value"),
            Output("caliper-width", "value"),
            Output("max-controls-per-treatment", "value"),
            Output("min-control-distance-km", "value"),
            Output("matching-method", "value"),
            Output("separation-fallback-mahalanobis", "value"),
            Output("group-by-exact-matches", "value"),
            Output("n-replicates", "value"),
            Output("match-memory-gb", "value"),
            Output("matching-job-queue", "value"),
            Output("settings-preset-feedback", "children", allow_duplicate=True),
        ],
        Input("load-settings-preset-btn", "n_clicks"),
        State("settings-preset-selector", "value"),
        State("matching-settings-presets-store", "data"),
        prevent_initial_call=True,
    )
    def load_settings_preset(_n, preset_id, presets_data):
        """Apply the selected matching-settings preset to all form fields."""
        if not preset_id or not presets_data:
            return (*([no_update] * 13), "Please select a preset to load.")

        for p in presets_data:
            if p["id"] == preset_id:
                s = p.get("settings") or {}
                return (
                    s.get("max_treatment_pixels", no_update),
                    s.get("control_multiplier", no_update),
                    s.get("min_site_area_ha", no_update),
                    s.get("min_glm_treatment_pixels", no_update),
                    s.get("caliper_width", no_update),
                    s.get("max_controls_per_treatment", no_update),
                    s.get("min_control_distance_km", no_update),
                    s.get("matching_method", no_update),
                    s.get("separation_fallback_mahalanobis", no_update),
                    s.get("group_by_exact_matches", no_update),
                    s.get("n_replicates", no_update),
                    s.get("match_memory_gb", no_update),
                    s.get("matching_job_queue", no_update),
                    dbc.Alert(
                        f'Loaded preset "{p["name"]}".',
                        color="info",
                        duration=3000,
                    ),
                )

        return (
            *([no_update] * 13),
            dbc.Alert("Preset not found.", color="warning", duration=3000),
        )

    @app.callback(
        [
            Output("matching-settings-presets-store", "data", allow_duplicate=True),
            Output("settings-preset-feedback", "children", allow_duplicate=True),
            Output("settings-preset-name-input", "value"),
        ],
        Input("save-settings-preset-btn", "n_clicks"),
        State("settings-preset-name-input", "value"),
        State("max-treatment-pixels", "value"),
        State("control-multiplier", "value"),
        State("min-site-area-ha", "value"),
        State("min-glm-treatment-pixels", "value"),
        State("caliper-width", "value"),
        State("max-controls-per-treatment", "value"),
        State("min-control-distance-km", "value"),
        State("matching-method", "value"),
        State("separation-fallback-mahalanobis", "value"),
        State("group-by-exact-matches", "value"),
        State("n-replicates", "value"),
        State("match-memory-gb", "value"),
        State("matching-job-queue", "value"),
        prevent_initial_call=True,
    )
    def save_settings_preset(
        _n,
        name,
        max_treatment_pixels,
        control_multiplier,
        min_site_area_ha,
        min_glm_treatment_pixels,
        caliper_width,
        max_controls_per_treatment,
        min_control_distance_km,
        matching_method,
        separation_fallback_mahalanobis,
        group_by_exact_matches,
        n_replicates,
        match_memory_gb,
        matching_job_queue,
    ):
        """Save the current matching settings as a named preset."""
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
            "group_by_exact_matches": group_by_exact_matches,
            "n_replicates": n_replicates,
            "match_memory_gb": match_memory_gb,
            "matching_job_queue": matching_job_queue,
        }

        try:
            save_matching_settings_preset(user.id, name.strip(), settings)
            updated = get_matching_settings_presets(user.id)
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
            logger.exception("Failed to save matching settings preset")
            report_exception()
            return (
                no_update,
                dbc.Alert("Failed to save preset.", color="danger", duration=3000),
                no_update,
            )

    @app.callback(
        [
            Output("matching-settings-presets-store", "data", allow_duplicate=True),
            Output("settings-preset-feedback", "children", allow_duplicate=True),
            Output("settings-preset-selector", "value"),
        ],
        Input("delete-settings-preset-btn", "n_clicks"),
        State("settings-preset-selector", "value"),
        State("matching-settings-presets-store", "data"),
        prevent_initial_call=True,
    )
    def delete_settings_preset(_n, preset_id, presets_data):
        """Delete the currently selected matching settings preset."""
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
            deleted = delete_matching_settings_preset(preset_id, user.id)
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

            updated = get_matching_settings_presets(user.id)
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
            logger.exception("Failed to delete matching settings preset")
            report_exception()
            return (
                no_update,
                dbc.Alert("Failed to delete preset.", color="danger", duration=3000),
                no_update,
            )
