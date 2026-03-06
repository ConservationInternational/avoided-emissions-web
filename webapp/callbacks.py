"""Dash callback definitions for the avoided emissions web application.

Registers all interactive callbacks: login/logout, file upload, task
submission, dashboard refresh (AG Grid), task detail views, result
visualization, and admin panel actions.
"""

import base64
import json
import logging
import os
import uuid as _uuid

import dash_bootstrap_components as dbc
import flask_login
import geopandas as gpd
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import ALL, Input, Output, State, callback_context, dcc, html, no_update
from dash.exceptions import PreventUpdate

from auth import (
    authenticate,
    get_current_user,
    register_user,
    request_password_reset,
    reset_password_with_token,
)
from config import report_exception
from layouts import (
    RESULTS_TOTAL_COLUMNS,
    RESULTS_YEARLY_COLUMNS,
    _make_ag_grid,
)
from services import (
    approve_user,
    change_user_role,
    create_share_link,
    delete_covariate_preset,
    delete_user,
    download_results_csv,
    force_reexport,
    force_remerge,
    get_covariate_inventory,
    get_covariate_presets,
    get_ready_covariate_names,
    get_task_detail,
    get_task_list,
    get_user_site_set_detail,
    list_share_links,
    list_user_site_sets,
    get_user_list,
    revoke_share_link,
    save_covariate_preset,
    save_user_site_set,
    start_gee_export,
    submit_analysis_task,
    update_task_info,
    validate_share_token,
    delete_user_site_set,
)

logger = logging.getLogger(__name__)


def _is_valid_uuid(value):
    """Return True if *value* is a valid UUID string."""
    try:
        _uuid.UUID(str(value))
        return True
    except (ValueError, AttributeError):
        return False


def _check_task_access(task_id, user):
    """Return True if *user* may access the task identified by *task_id*.

    Admins can view any task; regular users can only view their own.
    Returns False (and logs a warning) for invalid UUIDs or ownership
    violations.
    """
    if not _is_valid_uuid(task_id):
        return False
    detail = get_task_detail(task_id)
    if not detail:
        return False
    if user.is_admin:
        return True
    return str(detail["task"].submitted_by) == str(user.id)


def _authorize_task_access(task_id, share_token=None):
    """Check whether the current request may access *task_id*.

    Supports two authentication modes:
    1. **Authenticated user**: checks user login and ownership/admin.
    2. **Share token**: validates the token and confirms it belongs to
       the requested *task_id*.

    Returns the task_id (str) if access is granted, or ``None``.
    """
    # Mode 1: share token (lightweight check — access was already
    # recorded when the page was loaded via display_page)
    if share_token:
        token_task_id = validate_share_token(share_token, record_access=False)
        if token_task_id and str(token_task_id) == str(task_id):
            return str(task_id)
        return None

    # Mode 2: authenticated user
    user = get_current_user()
    if not user:
        return None
    if not _check_task_access(task_id, user):
        return None
    return str(task_id)


def _render_share_links_list(links, task_id):
    """Build the UI list of existing share links for the share modal."""
    if not links:
        return html.P("No share links yet.", className="text-muted")

    from flask import request as flask_request

    base_url = flask_request.host_url.rstrip("/")

    items = []
    for lnk in links:
        expires = (lnk["expires_at"] or "")[:10]
        badge_color = "success" if lnk["is_valid"] else "secondary"
        badge_text = "Active" if lnk["is_valid"] else "Expired / Revoked"
        share_url = f"{base_url}/shared/{lnk['token']}"
        input_id = f"share-link-{lnk['id']}"

        url_display = (
            dbc.InputGroup(
                [
                    dbc.Input(
                        value=share_url,
                        id=input_id,
                        readonly=True,
                        size="sm",
                    ),
                    dcc.Clipboard(
                        target_id=input_id,
                        className="btn btn-outline-secondary btn-sm",
                        style={"display": "inline-block"},
                    ),
                ],
                size="sm",
                className="flex-grow-1 me-2",
            )
            if lnk["is_valid"]
            else html.Code(
                f"...{lnk['token'][-12:]}",
                className="me-2 text-muted",
            )
        )

        row = html.Div(
            [
                html.Div(
                    [
                        dbc.Badge(badge_text, color=badge_color, className="me-2"),
                        url_display,
                        html.Small(
                            f"Expires {expires} · {lnk['access_count']} view(s)",
                            className="text-muted text-nowrap",
                        ),
                    ],
                    className="d-flex align-items-center flex-grow-1",
                ),
                *(
                    [
                        dbc.Button(
                            "Revoke",
                            id={
                                "type": "revoke-share-link",
                                "index": lnk["id"],
                            },
                            color="outline-danger",
                            size="sm",
                            className="ms-2 text-nowrap",
                        )
                    ]
                    if lnk["is_valid"]
                    else []
                ),
            ],
            className="d-flex justify-content-between align-items-center mb-2",
        )
        items.append(row)
    return html.Div(items)


def _fmt_dt(dt):
    """Format a datetime as an ISO 8601 UTC string for client-side
    conversion to the browser's local timezone, or '-' if *None*."""
    if dt is None:
        return "-"
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _record_covariate_action_failure(covariate_name, action, user_id):
    """Create a ``failed`` Covariate record so the table shows the error.

    Called when a reexport/remerge action raises before the GEE task is
    submitted.  Without this, the old DB records are already deleted and
    the table row would show blank status.
    """
    import traceback

    from models import Covariate, get_db

    error_msg = (
        f"Action '{action}' failed before the task was submitted to GEE. "
        f"{traceback.format_exc(limit=3)}"
    )

    db = get_db()
    try:
        rec = Covariate(
            covariate_name=covariate_name,
            status="failed",
            error_message=error_msg,
            started_by=user_id,
        )
        db.add(rec)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _openlayers_map_component(
    map_id,
    geojson_text,
    height="260px",
    enable_cog_layers=False,
    cog_filter_covariates=None,
):
    attrs = {
        "data-geojson": geojson_text or "",
        "data-height": height,
    }
    if enable_cog_layers:
        attrs["data-enable-cog-layers"] = "true"
    if cog_filter_covariates:
        attrs["data-cog-filter"] = ",".join(cog_filter_covariates)
    return html.Div(
        id=map_id,
        className="ol-sites-map",
        **attrs,
    )


def _normalize_metadata_list(value):
    """Ensure a metadata field is a list of dicts.

    The R analysis script serialises some lists (e.g. ``subsampled_sites``)
    as named R lists which ``jsonlite::write_json(auto_unbox=TRUE)`` emits as
    JSON **objects** (``{"1": {...}, "2": {...}}``).  When Python reads them
    they become dicts whose values are the actual records.  This helper
    converts such dicts to a flat list so callers can always iterate over
    dicts.

    A single-element list may also be unboxed to a flat dict — i.e. the
    record itself rather than a dict-of-dicts.  We detect this by checking
    whether the dict values are themselves dicts (nested) or scalars (flat
    record that should be wrapped).
    """
    if isinstance(value, dict):
        # If every value is a dict, it's a dict-of-dicts (named list).
        # Otherwise it's a single flat record that was auto-unboxed.
        if value and all(isinstance(v, dict) for v in value.values()):
            return list(value.values())
        return [value]
    if isinstance(value, list):
        return value
    return []


def _attach_totals_to_geojson(sites_geojson, totals):
    if not sites_geojson:
        return None

    fc = (
        json.loads(sites_geojson)
        if isinstance(sites_geojson, str)
        else dict(sites_geojson)
    )
    totals_by_site = {t.site_id: t for t in totals or []}
    for feature in fc.get("features", []):
        props = feature.setdefault("properties", {})
        site_id = str(props.get("site_id", ""))
        total = totals_by_site.get(site_id)
        if total:
            props["emissions_avoided_mgco2e"] = total.emissions_avoided_mgco2e or 0
            props["forest_loss_avoided_ha"] = total.forest_loss_avoided_ha or 0
            props["total_area_ha"] = total.area_ha or 0
    return json.dumps(fc)


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
            flask_login.login_user(result)
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
        [Input("url", "pathname"), Input("site-set-refresh-store", "data")],
        State("site-set-selector", "value"),
    )
    def refresh_site_set_options(pathname, _refresh_token, current_value):
        if pathname != "/submit":
            raise PreventUpdate

        user = get_current_user()
        if not user:
            raise PreventUpdate

        site_sets = list_user_site_sets(user.id)
        options = [
            {
                "label": (
                    f"{s['name']} ({s['n_sites']} sites, "
                    f"{(s['uploaded_at'] or '')[:19].replace('T', ' ')})"
                ),
                "value": s["id"],
            }
            for s in site_sets
        ]

        valid_ids = {s["id"] for s in site_sets}
        value = (
            current_value
            if current_value in valid_ids
            else (options[0]["value"] if options else None)
        )
        return options, value

    @app.callback(
        Output("covariate-selection", "options"),
        Input("url", "pathname"),
    )
    def refresh_submit_covariate_options(pathname):
        if pathname != "/submit":
            raise PreventUpdate

        ready_covariates = get_ready_covariate_names()
        return [{"label": cov, "value": cov} for cov in ready_covariates]

    @app.callback(
        [
            Output("upload-status", "children"),
            Output("site-set-action-status", "children"),
            Output("site-set-refresh-store", "data"),
            Output("site-set-selector", "value", allow_duplicate=True),
        ],
        [Input("upload-sites", "contents"), Input("delete-site-set-btn", "n_clicks")],
        [State("upload-sites", "filename"), State("site-set-selector", "value")],
        prevent_initial_call=True,
    )
    def handle_site_set_upload_or_delete(
        contents, _delete_clicks, filename, selected_set_id
    ):
        ctx = callback_context
        if not ctx.triggered:
            raise PreventUpdate

        trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]
        user = get_current_user()
        if not user:
            return (
                dbc.Alert("Please log in first.", color="danger"),
                no_update,
                no_update,
                no_update,
            )

        if trigger_id == "upload-sites":
            if contents is None:
                raise PreventUpdate

            _, content_string = contents.split(",")
            decoded = base64.b64decode(content_string)

            try:
                detail = save_user_site_set(user.id, filename, decoded)
                return (
                    dbc.Alert(
                        f"Uploaded and saved {detail['n_sites']} sites as '{detail['name']}'.",
                        color="success",
                    ),
                    no_update,
                    str(_uuid.uuid4()),
                    detail["id"],
                )
            except ValueError as exc:
                return (
                    dbc.Alert(str(exc), color="danger"),
                    no_update,
                    no_update,
                    no_update,
                )
            except Exception:
                logger.exception("Failed to save uploaded site set")
                report_exception()
                return (
                    dbc.Alert("Failed to save uploaded sites.", color="danger"),
                    no_update,
                    no_update,
                    no_update,
                )

        if trigger_id == "delete-site-set-btn":
            if not selected_set_id:
                return (
                    no_update,
                    dbc.Alert("Select a site set to delete.", color="warning"),
                    no_update,
                    no_update,
                )

            try:
                success, message = delete_user_site_set(selected_set_id, user.id)
                color = "success" if success else "warning"
                return (
                    no_update,
                    dbc.Alert(message, color=color),
                    str(_uuid.uuid4()),
                    None,
                )
            except Exception:
                logger.exception("Failed to delete site set")
                report_exception()
                return (
                    no_update,
                    dbc.Alert("Failed to delete site set.", color="danger"),
                    no_update,
                    no_update,
                )

        raise PreventUpdate

    @app.callback(
        [
            Output("parsed-sites-store", "data"),
            Output("site-preview", "children"),
            Output("site-preview-map", "children"),
            Output("site-set-metadata", "children"),
        ],
        Input("site-set-selector", "value"),
        prevent_initial_call=False,
    )
    def load_selected_site_set(site_set_id):
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

        detail = get_user_site_set_detail(site_set_id, user.id)
        if not detail:
            return (
                None,
                html.P("Selected site set was not found.", className="text-danger"),
                html.P("No map to display.", className="text-muted small"),
                html.Small("Site set unavailable.", className="text-danger"),
            )

        preview_cols = [
            {"headerName": "Site ID", "field": "site_id", "flex": 1, "minWidth": 110},
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
            {"headerName": "End Date", "field": "end_date", "flex": 1, "minWidth": 120},
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
                "getRowId": {"function": "params.data.site_id"},
            },
        )

        metadata = html.Div(
            [
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
        )

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
                height="260px",
                enable_cog_layers=True,
            ),
            metadata,
        )

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
        State("random-seed", "value"),
        State("match-memory-gb", "value"),
        State("matching-job-queue", "value"),
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
        random_seed,
        match_memory_gb,
        matching_job_queue,
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
            geojson_fc = json.loads(sites_data["geojson"])
            gdf = gpd.GeoDataFrame.from_features(
                geojson_fc.get("features", []),
                crs="EPSG:4326",
            )

            # Auto-derive forest cover year range from site dates.
            # Need years back to (earliest start_year - 5) for the
            # pre-intervention deforestation covariate, and forward
            # to the latest end_year (or current year if open-ended).
            start_dates = pd.to_datetime(gdf["start_date"])
            fc_min = max(2000, int(start_dates.dt.year.min()) - 5)
            if "end_date" in gdf.columns and gdf["end_date"].notna().any():
                end_years = pd.to_datetime(
                    gdf.loc[gdf["end_date"].notna(), "end_date"]
                ).dt.year
                fc_max = min(2024, int(end_years.max()))
            else:
                fc_max = 2024
            fc_years = list(range(fc_min, fc_max + 1))

            # Server-side bounds validation (mirrors the HTML input
            # min/max attributes so tampered requests are rejected).
            _mtp = int(max_treatment_pixels or 1000)
            _cm = int(control_multiplier or 50)
            _msa = int(min_site_area_ha or 100)
            _mglm = int(min_glm_treatment_pixels or 15)
            _cw = float(caliper_width if caliper_width is not None else 0.2)
            _mcpt = int(
                max_controls_per_treatment
                if max_controls_per_treatment is not None
                else 1
            )
            _seed = int(random_seed) if random_seed not in (None, "") else None
            _mmgb = int(match_memory_gb or 30)

            bounds = [
                (_mtp, 1, 100_000, "Max treatment pixels"),
                (_cm, 1, 500, "Control multiplier"),
                (_msa, 0, 100_000, "Minimum site area"),
                (_mglm, 1, 10_000, "Min GLM treatment pixels"),
                (_mcpt, 0, 100, "Max controls per treatment"),
                (_mmgb, 1, 240, "Matching memory (GB)"),
            ]
            for val, lo, hi, label in bounds:
                if val < lo or val > hi:
                    return _error_alert(f"{label} must be between {lo} and {hi}.")
            if _cw < 0 or _cw > 5.0:
                return _error_alert("Caliper width must be between 0 and 5.0.")
            if _seed is not None and (_seed < 1 or _seed > 2_147_483_647):
                return _error_alert("Random seed must be between 1 and 2147483647.")

            task_id = submit_analysis_task(
                task_name=name,
                description=description or "",
                user_id=user.id,
                gdf=gdf,
                covariates=covariates,
                exact_match_vars=exact_match_vars,
                fc_years=fc_years,
                site_set_id=sites_data.get("site_set_id"),
                max_treatment_pixels=_mtp,
                control_multiplier=_cm,
                min_site_area_ha=_msa,
                min_glm_treatment_pixels=_mglm,
                caliper_width=_cw,
                max_controls_per_treatment=_mcpt,
                random_seed=_seed,
                match_memory_mib=_mmgb * 1024,
                matching_job_queue=matching_job_queue,
            )

            return None, dbc.Alert(
                [
                    html.P("Task submitted successfully."),
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
        [Output("task-list-table", "rowData"), Output("task-total-count", "children")],
        [
            Input("refresh-interval", "n_intervals"),
            Input("refresh-tasks-btn", "n_clicks"),
        ],
    )
    def refresh_task_list(_n_intervals, _n_clicks):
        user = get_current_user()
        if not user:
            raise PreventUpdate

        user_filter = None if user.is_admin else user.id
        tasks = get_task_list(user_id=user_filter)

        if not tasks:
            return [], "Total: 0"

        rows = []
        for task in tasks:
            rows.append(
                {
                    "id": str(task.id),
                    "name": task.name,
                    "status": task.status,
                    "n_sites": task.n_sites or 0,
                    "created_at": _fmt_dt(task.created_at),
                    "submitted_at": _fmt_dt(task.submitted_at),
                    "completed_at": _fmt_dt(task.completed_at),
                }
            )

        return rows, f"Total: {len(rows)}"

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
            return ("Task Not Found", None, None, None, None, None, None, True)

        # Batch task status is polled by the Celery Beat worker;
        # this callback just reads the current DB state.
        detail = get_task_detail(task_id)
        if not detail:
            return ("Task Not Found", None, None, None, None, None, None, True)

        task = detail["task"]
        sites = detail["sites"]
        results = detail["results"]
        totals = detail["totals"]

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

        # Overview tab
        overview = _build_overview(task, sites, totals)

        # Results tab (AG Grid tables)
        results_content = _build_results_content(results, totals, sites)

        # Plots tab
        plots = (
            _build_plots(results, totals, sites, task=task)
            if results
            else html.P("Results not yet available.", className="text-muted")
        )

        # Match Quality tab
        match_quality = _build_match_quality(task_id, task, sites, totals)

        # Map tab
        map_content = _build_map(
            detail.get("sites_geojson"), totals, covariates=task.covariates
        )

        return (
            title,
            badge,
            overview,
            results_content,
            plots,
            match_quality,
            map_content,
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
            return dict(content=csv, filename="results_match_covariates.csv")
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
            # Closing — return empty list to avoid stale data
            return False, html.Div()

        # Opening — fetch existing links
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

        # Opening — populate inputs with current values
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

    # -- Admin: Covariates (unified export + merge) ---------------------------

    @app.callback(
        Output("gee-export-result", "children"),
        Input("start-gee-export", "n_clicks"),
        State("gee-export-category", "value"),
        prevent_initial_call=True,
    )
    def handle_gee_export(n_clicks, category):
        user = get_current_user()
        if not user or not user.is_admin:
            return dbc.Alert("Admin access required.", color="danger")

        import importlib.util

        gee_config_path = os.path.join(
            os.path.dirname(__file__), "gee-export", "config.py"
        )
        spec = importlib.util.spec_from_file_location(
            "gee_export_config", gee_config_path
        )
        gee_config = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(gee_config)
        COVARIATES = gee_config.COVARIATES

        if category == "all":
            names = list(COVARIATES.keys())
        else:
            names = [k for k, v in COVARIATES.items() if v.get("category") == category]

        if not names:
            return dbc.Alert(
                f"No covariates found for category: {category}", color="warning"
            )

        try:
            export_ids = start_gee_export(names, user.id)
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
        ],
    )
    def refresh_covariate_inventory(n, _export_result, _action_result):
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

        if not action or not covariate_name:
            raise PreventUpdate

        try:
            if action == "reexport":
                force_reexport(covariate_name, user.id)
                return dbc.Alert(
                    f"Re-export started for '{covariate_name}'. "
                    "Existing GCS tiles and S3 COG have been deleted.",
                    color="success",
                    duration=6000,
                )
            elif action == "remerge":
                force_remerge(covariate_name, user.id)
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

        # Try to revoke on the API side (best-effort)
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
            flask_login.logout_user()
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

        df = pd.DataFrame(store_data["rows"])
        covariate_cols = store_data["covariate_cols"]
        site_areas = store_data.get("site_areas", {})

        balance_rows = store_data.get("balance_rows")
        balance_df = pd.DataFrame(balance_rows) if balance_rows else None

        pscore_rows = store_data.get("pscore_rows")
        pscore_df = pd.DataFrame(pscore_rows) if pscore_rows else None

        if df.empty:
            return html.P("No data available.", className="text-muted")

        site_filter = None
        if selected_site and selected_site != "__all__":
            site_filter = selected_site
            df = df[df["site_id"].astype(str) == str(selected_site)]

        if df.empty:
            return html.P("No data for selected site.", className="text-muted")

        return html.Div(
            _build_all_match_quality_plots(
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
        prevent_initial_call=True,
    )
    def update_site_deforestation_plot(selected_site, store_data):
        """Build per-site deforestation and emissions plots with intervention
        date markers when a site is selected from the dropdown."""
        if not selected_site or not store_data:
            raise PreventUpdate

        results_data = store_data.get("results", [])
        sites_data = store_data.get("sites", {})
        subsampled_data = store_data.get("subsampled", {})

        # Filter results for selected site
        site_rows = [r for r in results_data if r["site_id"] == selected_site]
        if not site_rows:
            return html.P("No data for selected site.", className="text-muted")

        site_df = pd.DataFrame(site_rows).sort_values("year")
        site_info = sites_data.get(selected_site, {})
        site_name = site_info.get("site_name", selected_site)
        end_date = site_info.get("end_date")

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
            fig_em.update_layout(
                title=f"Annual Emissions: {site_name}{sub_note}",
                xaxis_title="Year",
                yaxis_title="Emissions (MgCO₂e)",
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


# -- Helper functions for building detail page content -----------------------


def _build_overview(task, sites, totals):
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
        "Disabled" if caliper_val == 0 else (caliper_val if caliper_val else "—")
    )
    max_ctrl = config.get("max_controls_per_treatment")
    max_ctrl_display = "No limit" if max_ctrl == 0 else (max_ctrl if max_ctrl else "—")
    mem_mib = config.get("match_memory_mib")
    mem_display = f"{mem_mib / 1024:.1f} GB" if mem_mib else "—"

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
                            ", ".join(task.covariates or []) or "—",
                        ),
                        _detail_row(
                            "Exact match variables",
                            ", ".join(config.get("exact_match_vars", [])) or "—",
                        ),
                        _detail_row(
                            "Max treatment pixels",
                            config.get("max_treatment_pixels", "—"),
                        ),
                        _detail_row(
                            "Control multiplier",
                            config.get("control_multiplier", "—"),
                        ),
                        _detail_row(
                            "Min site area (ha)",
                            config.get("min_site_area_ha", "—"),
                        ),
                        _detail_row(
                            "Min GLM treatment pixels",
                            config.get("min_glm_treatment_pixels", "—"),
                        ),
                        _detail_row("Caliper width (SD)", caliper_display),
                        _detail_row("Max controls per treatment", max_ctrl_display),
                        _detail_row(
                            "Random seed",
                            config.get("random_seed", "Not set"),
                        ),
                        _detail_row("Matching memory", mem_display),
                        _detail_row(
                            "Batch job queue",
                            config.get("matching_job_queue", "—"),
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
        total_emissions = sum(t.emissions_avoided_mgco2e or 0 for t in totals)
        total_forest = sum(t.forest_loss_avoided_ha or 0 for t in totals)
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
                                            "Total Avoided Emissions (MgCO₂e)",
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
            failed_items.append(html.Li(f"{label}: {error}"))
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


def _build_results_content(results, totals, sites=None):
    """Build the results section with sites table, AG Grid tables, and downloads."""
    content = []

    # Sites table — always shown when sites are available
    if sites:
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

    # Totals table
    totals_rows = [
        {
            "site_id": t.site_id,
            "site_name": t.site_name or "-",
            "emissions_avoided_mgco2e": t.emissions_avoided_mgco2e or 0,
            "forest_loss_avoided_ha": t.forest_loss_avoided_ha or 0,
            "area_ha": t.area_ha or 0,
            "period": (f"{t.first_year}-{t.last_year}" if t.first_year else "-"),
            "sampled_percent": ((t.sampled_fraction or 1.0) * 100),
        }
        for t in totals
    ]

    # Yearly results table
    yearly_rows = []
    if results:
        yearly_rows = [
            {
                "site_id": r.site_id,
                "year": r.year,
                "treatment_defor_ha": r.treatment_defor_ha or 0,
                "control_defor_ha": r.control_defor_ha or 0,
                "emissions_avoided_mgco2e": r.emissions_avoided_mgco2e or 0,
                "forest_loss_avoided_ha": r.forest_loss_avoided_ha or 0,
                "n_matched_pixels": r.n_matched_pixels or 0,
            }
            for r in results
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

    if yearly_rows:
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


def _build_plots(results, totals, sites=None, task=None):
    """Build interactive plots for task results.

    Includes aggregate deforestation comparison (project sites vs matched
    controls), existing avoided-emissions/forest-loss bar charts, and a
    site-level drill-down section with intervention date markers.

    Parameters
    ----------
    task : AnalysisTask, optional
        The parent task object, used to read ``extra_metadata`` for
        failed-site and subsampled-site annotations.
    """
    if not results:
        return html.P("No results to plot.", className="text-muted")

    # Extract diagnostic metadata from the task
    meta = (task.extra_metadata or {}) if task else {}
    failed_site_ids = {
        fs.get("site_id") or fs.get("id_numeric")
        for fs in _normalize_metadata_list(meta.get("failed_sites", []))
    }
    subsampled_map = {
        ss.get("site_id") or ss.get("id_numeric"): ss
        for ss in _normalize_metadata_list(meta.get("subsampled_sites", []))
    }

    # Convert to DataFrame
    df = pd.DataFrame(
        [
            {
                "site_id": r.site_id,
                "year": r.year,
                "emissions_avoided_mgco2e": r.emissions_avoided_mgco2e or 0,
                "forest_loss_avoided_ha": r.forest_loss_avoided_ha or 0,
                "treatment_defor_ha": r.treatment_defor_ha or 0,
                "control_defor_ha": r.control_defor_ha or 0,
                "treatment_emissions_mgco2e": r.treatment_emissions_mgco2e or 0,
                "control_emissions_mgco2e": r.control_emissions_mgco2e or 0,
                "is_pre_intervention": bool(r.is_pre_intervention),
            }
            for r in results
        ]
    )

    plots = []

    # --- Aggregate deforestation comparison (treatment vs control) ----------
    has_defor_data = (
        df["treatment_defor_ha"].sum() > 0 or df["control_defor_ha"].sum() > 0
    )
    if has_defor_data:
        agg_df = (
            df.groupby("year")
            .agg(
                treatment_defor_ha=("treatment_defor_ha", "sum"),
                control_defor_ha=("control_defor_ha", "sum"),
            )
            .reset_index()
            .sort_values("year")
        )
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
        n_successful = len(df["site_id"].unique())
        title_suffix = f" ({n_successful} sites"
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
        # Shade the pre-intervention period
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
        # Shade post-intervention period if any site has an end date
        if sites:
            post_start = None
            for s in sites:
                if s.end_date:
                    end_yr = s.end_date.year
                    if post_start is None or end_yr < post_start:
                        post_start = end_yr
            if post_start is not None:
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

    # --- Existing avoided-emissions bar charts -----------------------------

    # Emissions avoided over time (stacked by site)
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
    plots.append(dcc.Graph(figure=fig_emissions))

    # Forest loss avoided over time
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
    plots.append(dcc.Graph(figure=fig_forest))

    # Per-site totals bar chart
    if totals:
        df_totals = pd.DataFrame(
            [
                {
                    "site_id": t.site_id,
                    "site_name": t.site_name or t.site_id,
                    "emissions_avoided_mgco2e": t.emissions_avoided_mgco2e or 0,
                    "forest_loss_avoided_ha": t.forest_loss_avoided_ha or 0,
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
        # Build site options for the dropdown
        site_ids = sorted(df["site_id"].unique())
        site_info_map = {}
        # Build name lookup from totals (always has names from R analysis)
        totals_name_map = {}
        if totals:
            for t in totals:
                if t.site_name:
                    totals_name_map[str(t.site_id)] = t.site_name
        if sites:
            for s in sites:
                sid = str(s.site_id)
                # Use site_name only if it's a real name (not equal to
                # site_id); fall back to the totals lookup from the R
                # analysis results which always has proper names.
                name = s.site_name if s.site_name and s.site_name != sid else None
                site_info_map[sid] = {
                    "site_name": name or totals_name_map.get(sid) or sid,
                    "start_date": (str(s.start_date)[:10] if s.start_date else None),
                    "end_date": str(s.end_date)[:10] if s.end_date else None,
                }

        # Annotate dropdown labels for subsampled sites
        site_options = []
        for sid in site_ids:
            sname = site_info_map.get(str(sid), {}).get("site_name", str(sid))
            if sname and str(sname) != str(sid):
                label = f"{sname} ({sid})"
            else:
                label = str(sid)
            if sid in subsampled_map:
                pct = subsampled_map[sid].get("sampled_percent", 100)
                label += f" (subsampled {pct:.0f}%)"
            site_options.append({"label": label, "value": sid})

        # Include subsampled-site info in the store so the drill-down
        # callback can annotate individual site plots.
        store_data = {
            "results": df.to_dict("records"),
            "sites": site_info_map,
            "subsampled": {sid: sub for sid, sub in subsampled_map.items()},
        }

        plots.append(html.Hr(className="my-4"))
        plots.append(html.H5("Site-Level Deforestation Detail", className="mt-3"))
        plots.append(
            html.P(
                "Select a site to view its deforestation trajectory "
                "compared to matched controls, with intervention dates marked.",
                className="text-muted",
            )
        )
        plots.append(
            dbc.Select(
                id="site-defor-selector",
                options=site_options,
                placeholder="Select a site...",
                className="mb-3",
                style={"maxWidth": "400px"},
            )
        )
        plots.append(dcc.Store(id="site-defor-store", data=store_data))
        plots.append(html.Div(id="site-defor-plot-container"))

    return html.Div(plots)


def _build_match_quality(task_id, task, sites=None, totals=None):
    """Build the match quality assessment section.

    Fetches matched-pixel covariate data, balance statistics, and
    propensity scores from S3.  Produces:

    * **Love plot** — horizontal dot plot of standardized mean differences
      for each covariate with ±0.1 reference lines.
    * **Propensity score QQ plot** — empirical quantile-quantile comparison
      of treatment vs control propensity score distributions.
    * **Covariate histograms** — overlaid treatment/control distributions
      for each covariate (existing functionality).

    Also provides download buttons for the underlying CSVs.
    """
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

    csv_content = download_results_csv(
        task_id, "match_covariates", results_s3_uri=task.results_s3_uri
    )
    if not csv_content:
        return html.Div(
            [
                html.P(
                    "Match covariate data not available for this analysis.",
                    className="text-muted",
                ),
                placeholder_ids,
            ]
        )

    import io

    df = pd.read_csv(io.StringIO(csv_content))

    if df.empty:
        return html.Div(
            [
                html.P("No matched pixels found.", className="text-muted"),
                placeholder_ids,
            ]
        )

    # Identify covariate columns (everything except identifiers/weights)
    id_cols = {"cell", "site_id", "treatment", "match_group", "match_weight"}
    covariate_cols = [c for c in df.columns if c not in id_cols]

    if not covariate_cols:
        return html.Div(
            [
                html.P(
                    "No covariate columns found in match data.",
                    className="text-muted",
                ),
                placeholder_ids,
            ]
        )

    # Fetch balance statistics (Love plot data)
    balance_df = None
    balance_csv = download_results_csv(
        task_id, "balance", results_s3_uri=task.results_s3_uri
    )
    if balance_csv:
        balance_df = pd.read_csv(io.StringIO(balance_csv))
        if balance_df.empty:
            balance_df = None

    # Fetch propensity scores (QQ plot data)
    pscore_df = None
    pscore_csv = download_results_csv(
        task_id, "propensity_scores", results_s3_uri=task.results_s3_uri
    )
    if pscore_csv:
        pscore_df = pd.read_csv(io.StringIO(pscore_csv))
        if pscore_df.empty or "pscore" not in pscore_df.columns:
            pscore_df = None
        elif pscore_df["pscore"].dropna().empty:
            pscore_df = None

    # Build a lookup of site_id -> area_ha from totals (TaskResultTotal),
    # which always has area from the R analysis results.
    site_areas = {}
    for t in totals or []:
        sid = t.site_id if hasattr(t, "site_id") else t.get("site_id")
        area = t.area_ha if hasattr(t, "area_ha") else t.get("area_ha")
        if sid is not None:
            site_areas[str(sid)] = area or 0

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

    # --- Match quality diagnostics (all filterable by site) ----------------
    content.append(html.H5("Match Quality Diagnostics", className="mt-4 mb-2"))

    # Per-site selector for filtering all diagnostic plots
    site_ids = sorted(df["site_id"].unique())
    # Build name map from totals (always has names from R analysis)
    site_name_map = {}
    for t in totals or []:
        sid = t.site_id if hasattr(t, "site_id") else t.get("site_id")
        sname = t.site_name if hasattr(t, "site_name") else t.get("site_name")
        if sid is not None and sname and str(sname) != str(sid):
            site_name_map[str(sid)] = sname
    site_options = [{"label": "All sites (aggregate)", "value": "__all__"}]
    for sid in site_ids:
        sname = site_name_map.get(str(sid))
        label = f"{sname} ({sid})" if sname else str(sid)
        site_options.append({"label": label, "value": sid})

    content.append(
        html.Div(
            [
                html.Label("Filter by site:", className="fw-bold me-2"),
                dbc.Select(
                    id="match-quality-site-selector",
                    options=site_options,
                    value="__all__",
                    style={"maxWidth": "350px", "display": "inline-block"},
                ),
            ],
            className="mb-3",
        )
    )

    # Store the data for client-side filtering via a callback
    store_data = {
        "rows": df.to_dict("records"),
        "covariate_cols": covariate_cols,
        "site_areas": site_areas,
    }
    if balance_df is not None:
        store_data["balance_rows"] = balance_df.to_dict("records")
    if pscore_df is not None:
        store_data["pscore_rows"] = pscore_df.to_dict("records")
    content.append(dcc.Store(id="match-quality-data-store", data=store_data))

    # Render initial plots for all sites (aggregate)
    content.append(
        html.Div(
            _build_all_match_quality_plots(
                df,
                covariate_cols,
                balance_df,
                pscore_df,
                site_filter=None,
                site_areas=site_areas,
            ),
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

    stat_cols = [
        dbc.Col(
            dbc.Card(
                dbc.CardBody(
                    [
                        html.H6("Treatment Pixels", className="text-muted mb-1"),
                        html.H4(f"{n_treatment:,}"),
                    ]
                ),
                className="text-center",
            ),
            md=3,
        ),
        dbc.Col(
            dbc.Card(
                dbc.CardBody(
                    [
                        html.H6("Control Pixels", className="text-muted mb-1"),
                        html.H4(f"{n_control:,}"),
                    ]
                ),
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
                "pixels. Points close to the 45° line indicate similar "
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
    the ±0.1 threshold that is conventionally considered acceptable.

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

    # Colour-code by whether SMD is within the ±0.1 threshold
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
    # Reference lines at ±0.1
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
    # 45° reference line
    q_min = min(t_quantiles.min(), c_quantiles.min())
    q_max = max(t_quantiles.max(), c_quantiles.max())
    fig.add_trace(
        go.Scatter(
            x=[q_min, q_max],
            y=[q_min, q_max],
            mode="lines",
            line=dict(color="gray", dash="dash"),
            name="45° line",
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
                opacity=0.6,
                marker_color="#2ca02c",
                xbins=xbins,
            )
        )
        fig.add_trace(
            go.Histogram(
                x=control_df[col].dropna(),
                name="Control",
                opacity=0.6,
                marker_color="#d62728",
                xbins=xbins,
            )
        )
        fig.update_layout(
            title=f"Covariate: {col}",
            xaxis_title=col,
            yaxis_title="Count",
            barmode="overlay",
            legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99),
            height=350,
            margin=dict(t=40, b=40, l=50, r=20),
        )
        plots.append(dcc.Graph(figure=fig))

    if not plots:
        return [html.P("No numeric covariates to display.", className="text-muted")]

    return plots


def _build_map(sites_geojson, totals, covariates=None):
    """Build an OpenLayers map for task sites and summary values."""
    enriched_geojson = _attach_totals_to_geojson(sites_geojson, totals)
    if not enriched_geojson:
        return html.P("No site geometries available.", className="text-muted")
    return _openlayers_map_component(
        "task-sites-map",
        enriched_geojson,
        height="500px",
        enable_cog_layers=True,
        cog_filter_covariates=covariates,
    )
