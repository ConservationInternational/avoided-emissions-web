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
from dash import Input, Output, State, callback_context, dcc, html, no_update
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
    delete_covariate_preset,
    delete_user,
    download_results_csv,
    force_reexport,
    force_remerge,
    get_covariate_inventory,
    get_covariate_presets,
    get_task_detail,
    get_task_list,
    get_user_site_set_detail,
    list_user_site_sets,
    get_user_list,
    save_covariate_preset,
    save_user_site_set,
    start_gee_export,
    submit_analysis_task,
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


def _openlayers_map_component(map_id, geojson_text, height="260px"):
    return html.Div(
        id=map_id,
        className="ol-sites-map",
        **{
            "data-geojson": geojson_text or "",
            "data-height": height,
        },
    )


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


def register_callbacks(app):
    """Register all Dash callbacks on the app instance."""

    # -- Login ---------------------------------------------------------------

    @app.callback(
        Output("login-error", "children"),
        Input("login-button", "n_clicks"),
        State("login-email", "value"),
        State("login-password", "value"),
        prevent_initial_call=True,
    )
    def handle_login(n_clicks, email, password):
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
        State("register-password", "value"),
        State("register-password-confirm", "value"),
        prevent_initial_call=True,
    )
    def handle_register(n_clicks, name, email, password, password_confirm):
        if not name or not email or not password:
            return dbc.Alert(
                "Please fill in all fields.",
                color="warning",
                duration=5000,
            )

        if len(password) < 8:
            return dbc.Alert(
                "Password must be at least 8 characters.",
                color="warning",
                duration=5000,
            )

        if password != password_confirm:
            return dbc.Alert(
                "Passwords do not match.",
                color="danger",
                duration=5000,
            )

        success, message = register_user(email, password, name)
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
        if len(password) < 8:
            return dbc.Alert(
                "Password must be at least 8 characters.",
                color="warning",
                duration=5000,
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
                "rowSelection": "single",
                "suppressRowClickSelection": False,
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
                "submit-sites-map", detail["geojson"], height="260px"
            ),
            metadata,
        )

    # -- Task submission -----------------------------------------------------

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
        match_memory_gb,
        matching_job_queue,
    ):
        def _error_alert(msg):
            return dbc.Alert(msg, color="danger", dismissable=True), None

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

            task_id = submit_analysis_task(
                task_name=name,
                description=description or "",
                user_id=user.id,
                gdf=gdf,
                covariates=covariates,
                exact_match_vars=exact_match_vars,
                fc_years=fc_years,
                site_set_id=sites_data.get("site_set_id"),
                max_treatment_pixels=int(max_treatment_pixels or 1000),
                control_multiplier=int(control_multiplier or 50),
                min_site_area_ha=int(min_site_area_ha or 100),
                min_glm_treatment_pixels=int(min_glm_treatment_pixels or 15),
                match_memory_mib=int(match_memory_gb or 30) * 1024,
                matching_job_queue=matching_job_queue,
            )

            return None, dbc.Alert(
                [
                    html.P("Task submitted successfully."),
                    dcc.Link(f"View task: {task_id}", href=f"/task/{task_id}"),
                ],
                color="success",
                dismissable=True,
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
            Output("task-map", "children"),
        ],
        [
            Input("detail-refresh-interval", "n_intervals"),
            Input("detail-tabs", "active_tab"),
        ],
        State("task-id-store", "data"),
    )
    def refresh_task_detail(n, active_tab, task_id):
        if not task_id:
            raise PreventUpdate

        user = get_current_user()
        if not user or not _check_task_access(task_id, user):
            return ("Task Not Found", None, None, None, None, None)

        # Batch task status is polled by the Celery Beat worker;
        # this callback just reads the current DB state.
        detail = get_task_detail(task_id)
        if not detail:
            return ("Task Not Found", None, None, None, None, None)

        task = detail["task"]
        sites = detail["sites"]
        results = detail["results"]
        totals = detail["totals"]

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

        # Map tab
        map_content = _build_map(detail.get("sites_geojson"), totals)

        return title, badge, overview, results_content, plots, map_content

    # -- Result downloads ----------------------------------------------------

    @app.callback(
        Output("download-results", "data"),
        [Input("download-by-year", "n_clicks"), Input("download-totals", "n_clicks")],
        State("task-id-store", "data"),
        prevent_initial_call=True,
    )
    def handle_download(by_year_clicks, total_clicks, task_id):
        ctx = callback_context
        if not ctx.triggered:
            raise PreventUpdate

        user = get_current_user()
        if not user or not _check_task_access(task_id, user):
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

            # 2. Register an OAuth2 service client
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

            # 3. Store encrypted credentials locally
            save_credential(
                user_id=user.id,
                te_email=email,
                client_id=client_id,
                client_secret=client_secret,
                client_name=f"avoided-emissions-web ({user.email})",
                api_client_db_id=api_client_db_id,
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
                msg = f"Failed to link account: {msg}"
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
            return dbc.Alert(
                f"Connection failed: {e}",
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
        start_date = site_info.get("start_date")
        end_date = site_info.get("end_date")

        # Check if this site was subsampled
        sub_info = subsampled_data.get(selected_site)
        sub_note = ""
        if sub_info:
            pct = sub_info.get("sampled_percent", 100)
            sub_note = f" [subsampled {pct:.0f}%]"

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
        # Intervention date markers
        if start_date:
            start_year = int(start_date[:4])
            fig_defor.add_vline(
                x=start_year,
                line_dash="dash",
                line_color="blue",
                line_width=1.5,
                annotation_text="Intervention Start",
                annotation_position="top left",
                annotation_font_color="blue",
            )
        if end_date:
            end_year = int(end_date[:4])
            fig_defor.add_vline(
                x=end_year,
                line_dash="dash",
                line_color="orange",
                line_width=1.5,
                annotation_text="Intervention End",
                annotation_position="top right",
                annotation_font_color="orange",
            )
        fig_defor.update_layout(
            title=f"Annual Deforestation: {site_name}{sub_note}",
            xaxis_title="Year",
            yaxis_title="Deforestation (ha)",
            legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
            hovermode="x unified",
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
            if start_date:
                start_year = int(start_date[:4])
                fig_em.add_vline(
                    x=start_year,
                    line_dash="dash",
                    line_color="blue",
                    line_width=1.5,
                    annotation_text="Intervention Start",
                    annotation_position="top left",
                    annotation_font_color="blue",
                )
            if end_date:
                end_year = int(end_date[:4])
                fig_em.add_vline(
                    x=end_year,
                    line_dash="dash",
                    line_color="orange",
                    line_width=1.5,
                    annotation_text="Intervention End",
                    annotation_position="top right",
                    annotation_font_color="orange",
                )
            fig_em.update_layout(
                title=f"Annual Emissions: {site_name}{sub_note}",
                xaxis_title="Year",
                yaxis_title="Emissions (MgCO₂e)",
                legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
                hovermode="x unified",
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
        if start_date:
            start_year = int(start_date[:4])
            fig_avoided.add_vline(
                x=start_year,
                line_dash="dash",
                line_color="blue",
                line_width=1.5,
                annotation_text="Intervention Start",
                annotation_position="top left",
                annotation_font_color="blue",
            )
        if end_date:
            end_year = int(end_date[:4])
            fig_avoided.add_vline(
                x=end_year,
                line_dash="dash",
                line_color="orange",
                line_width=1.5,
                annotation_text="Intervention End",
                annotation_position="top right",
                annotation_font_color="orange",
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
    cards.append(
        dbc.Card(
            [
                dbc.CardHeader("Task Information"),
                dbc.CardBody(
                    [
                        html.P(f"Description: {task.description or 'None'}"),
                        html.P(f"Sites: {task.n_sites or 0}"),
                        html.P(f"Covariates: {', '.join(task.covariates or [])}"),
                        html.P(
                            [
                                "Created: ",
                                html.Span(
                                    _fmt_dt(task.created_at),
                                    className="utc-datetime",
                                    **{"data-utc": _fmt_dt(task.created_at)},
                                ),
                            ]
                        ),
                        html.P(f"Status: {task.status}"),
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
    failed_sites = meta.get("failed_sites", [])
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
    subsampled_sites = meta.get("subsampled_sites", [])
    if subsampled_sites:
        sub_items = []
        for ss in subsampled_sites:
            label = ss.get("site_id") or ss.get("id_numeric", "?")
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
                    "rowSelection": "single",
                    "suppressRowClickSelection": False,
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
        fs.get("site_id") or fs.get("id_numeric") for fs in meta.get("failed_sites", [])
    }
    subsampled_map = {
        ss.get("site_id") or ss.get("id_numeric"): ss
        for ss in meta.get("subsampled_sites", [])
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
            legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
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
        if sites:
            for s in sites:
                site_info_map[s.site_id] = {
                    "site_name": s.site_name or s.site_id,
                    "start_date": (str(s.start_date)[:10] if s.start_date else None),
                    "end_date": str(s.end_date)[:10] if s.end_date else None,
                }

        # Annotate dropdown labels for subsampled sites
        site_options = []
        for sid in site_ids:
            label = site_info_map.get(sid, {}).get("site_name", sid)
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


def _build_map(sites_geojson, totals):
    """Build an OpenLayers map for task sites and summary values."""
    enriched_geojson = _attach_totals_to_geojson(sites_geojson, totals)
    if not enriched_geojson:
        return html.P("No site geometries available.", className="text-muted")
    return _openlayers_map_component("task-sites-map", enriched_geojson, height="500px")
