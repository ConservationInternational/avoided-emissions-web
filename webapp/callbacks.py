"""Dash callback definitions for the avoided emissions web application.

Registers all interactive callbacks: login/logout, file upload, task
submission, dashboard refresh (AG Grid), task detail views, result
visualization, and admin panel actions.
"""

import base64
import io
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

from auth import authenticate, get_current_user, register_user
from layouts import (
    RESULTS_TOTAL_COLUMNS,
    RESULTS_YEARLY_COLUMNS,
    _make_ag_grid,
)
from services import (
    approve_user,
    change_user_role,
    delete_user,
    download_results_csv,
    get_covariate_inventory,
    get_task_detail,
    get_task_list,
    get_user_list,
    parse_sites_file,
    poll_gee_export_status,
    refresh_task_status,
    start_cog_merge,
    start_gee_export,
    submit_analysis_task,
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
    """Format a datetime to a short string, or '-' if None."""
    if dt is None:
        return "-"
    return dt.strftime("%Y-%m-%d %H:%M")


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
                "Please fill in all fields.", color="warning", duration=5000,
            )

        if len(password) < 8:
            return dbc.Alert(
                "Password must be at least 8 characters.", color="warning",
                duration=5000,
            )

        if password != password_confirm:
            return dbc.Alert(
                "Passwords do not match.", color="danger", duration=5000,
            )

        success, message = register_user(email, password, name)
        color = "success" if success else "danger"
        return dbc.Alert(message, color=color)

    # -- File upload ---------------------------------------------------------

    @app.callback(
        [Output("upload-status", "children"),
         Output("parsed-sites-store", "data"),
         Output("site-preview", "children")],
        Input("upload-sites", "contents"),
        State("upload-sites", "filename"),
        prevent_initial_call=True,
    )
    def handle_upload(contents, filename):
        if contents is None:
            raise PreventUpdate

        content_type, content_string = contents.split(",")
        decoded = base64.b64decode(content_string)

        gdf, errors = parse_sites_file(decoded, filename)
        if errors:
            error_elements = []
            for e in errors:
                lines = e.split("\n")
                parts = []
                for i, line in enumerate(lines):
                    if i > 0:
                        parts.append(html.Br())
                    parts.append(line)
                error_elements.append(
                    html.P(parts, className="text-danger")
                )
            return (
                html.Div(error_elements),
                None,
                None,
            )

        # Convert date columns to strings for JSON serialization
        for col in ["start_date", "end_date"]:
            if col in gdf.columns:
                gdf[col] = gdf[col].astype(str)

        # Build AG Grid preview with all sites
        preview_cols = [
            {"headerName": "Site ID", "field": "site_id", "flex": 1,
             "minWidth": 100},
            {"headerName": "Site Name", "field": "site_name", "flex": 2,
             "minWidth": 180},
            {"headerName": "Start Date", "field": "start_date", "flex": 1,
             "minWidth": 120},
        ]
        if "end_date" in gdf.columns:
            preview_cols.append(
                {"headerName": "End Date", "field": "end_date", "flex": 1,
                 "minWidth": 120}
            )

        preview_rows = []
        for _, row in gdf.iterrows():
            r = {
                "site_id": str(row.get("site_id", "")),
                "site_name": str(row.get("site_name", "")),
                "start_date": str(row.get("start_date", ""))[:10],
            }
            if "end_date" in gdf.columns:
                r["end_date"] = str(row.get("end_date", ""))[:10]
            preview_rows.append(r)

        preview_table = _make_ag_grid(
            "site-preview-table", preview_cols,
            row_data=preview_rows, height="400px",
        )

        status_msg = html.Div([
            html.P(
                f"Loaded {len(gdf)} sites from {filename}",
                className="text-success",
            ),
        ])

        store_data = {
            "geojson": gdf.to_json(),
            "n_sites": len(gdf),
            "filename": filename,
        }

        return status_msg, store_data, preview_table

    # -- Task submission -----------------------------------------------------

    @app.callback(
        [Output("submit-errors", "children"),
         Output("submit-result", "children")],
        Input("submit-task-button", "n_clicks"),
        State("task-name", "value"),
        State("task-description", "value"),
        State("parsed-sites-store", "data"),
        State("covariate-selection", "value"),
        State("fc-start-year", "value"),
        State("fc-end-year", "value"),
        prevent_initial_call=True,
    )
    def handle_submit(n_clicks, name, description, sites_data,
                      covariates, fc_start, fc_end):
        if not name:
            return "Please enter a task name.", None
        if not sites_data:
            return "Please upload a sites file.", None
        if not covariates:
            return "Please select at least one covariate.", None

        user = get_current_user()
        if not user:
            return "Please log in first.", None

        try:
            gdf = gpd.read_file(io.StringIO(sites_data["geojson"]))
            fc_years = list(range(int(fc_start), int(fc_end) + 1))

            task_id = submit_analysis_task(
                task_name=name,
                description=description or "",
                user_id=user.id,
                gdf=gdf,
                covariates=covariates,
                fc_years=fc_years,
            )

            return None, dbc.Alert([
                html.P("Task submitted successfully."),
                dcc.Link(f"View task: {task_id}", href=f"/task/{task_id}"),
            ], color="success")

        except Exception as e:
            logger.exception("Task submission failed")
            return "Submission failed. Please try again or contact support.", None

    # -- Dashboard task list (AG Grid) ---------------------------------------

    @app.callback(
        [Output("task-list-table", "rowData"),
         Output("task-total-count", "children")],
        [Input("refresh-interval", "n_intervals"),
         Input("refresh-tasks-btn", "n_clicks")],
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
            rows.append({
                "id": str(task.id),
                "name": task.name,
                "status": task.status,
                "n_sites": task.n_sites or 0,
                "created_at": _fmt_dt(task.created_at),
                "submitted_at": _fmt_dt(task.submitted_at),
                "completed_at": _fmt_dt(task.completed_at),
            })

        return rows, f"Total: {len(rows)}"

    # -- Task detail ---------------------------------------------------------

    @app.callback(
        [Output("task-title", "children"),
         Output("task-status-badge", "children"),
         Output("task-overview", "children"),
         Output("task-results-content", "children"),
         Output("task-plots", "children"),
         Output("task-map", "children")],
        [Input("detail-refresh-interval", "n_intervals"),
         Input("detail-tabs", "active_tab")],
        State("task-id-store", "data"),
    )
    def refresh_task_detail(n, active_tab, task_id):
        if not task_id:
            raise PreventUpdate

        user = get_current_user()
        if not user or not _check_task_access(task_id, user):
            return ("Task Not Found", None, None, None, None, None)

        refresh_task_status(task_id)
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
            "pending": "secondary", "submitted": "info",
            "running": "primary", "succeeded": "success",
            "failed": "danger", "cancelled": "warning",
        }.get(task.status, "secondary")
        badge = dbc.Badge(task.status.upper(), color=status_color,
                          className="fs-5")

        # Overview tab
        overview = _build_overview(task, sites, totals)

        # Results tab (AG Grid tables)
        results_content = _build_results_content(results, totals)

        # Plots tab
        plots = _build_plots(results, totals) if results else html.P(
            "Results not yet available.", className="text-muted"
        )

        # Map tab
        map_content = _build_map(sites, totals)

        return title, badge, overview, results_content, plots, map_content

    # -- Result downloads ----------------------------------------------------

    @app.callback(
        Output("download-results", "data"),
        [Input("download-by-year", "n_clicks"),
         Input("download-totals", "n_clicks")],
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
        import os

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
            names = [k for k, v in COVARIATES.items()
                     if v.get("category") == category]

        if not names:
            return dbc.Alert(f"No covariates found for category: {category}",
                             color="warning")

        try:
            export_ids = start_gee_export(names, user.id)
            return dbc.Alert(
                f"Started {len(export_ids)} GEE export task(s).",
                color="success",
            )
        except Exception:
            logger.exception("GEE export failed")
            return dbc.Alert(
                "Export failed. Please try again or contact support.",
                color="danger",
            )

    @app.callback(
        Output("cog-merge-result", "children"),
        Input("start-cog-merge", "n_clicks"),
        State("covariates-table", "selectedRows"),
        prevent_initial_call=True,
    )
    def handle_cog_merge(n_clicks, selected_rows):
        user = get_current_user()
        if not user or not user.is_admin:
            return dbc.Alert("Admin access required.", color="danger")

        if not selected_rows:
            return dbc.Alert(
                "No covariates selected.  Use the checkboxes to select "
                "rows with GCS tiles, then click 'Merge Selected'.",
                color="warning",
            )

        # Only merge covariates that actually have tiles on GCS
        names = [
            row["covariate_name"]
            for row in selected_rows
            if row.get("gcs_tiles", 0) > 0
        ]

        if not names:
            return dbc.Alert(
                "None of the selected covariates have tiles on GCS.",
                color="warning",
            )

        try:
            merge_ids = start_cog_merge(names, user.id)
            if not merge_ids:
                return dbc.Alert(
                    "All selected covariates are already pending or merging.",
                    color="info",
                    duration=4000,
                )
            return dbc.Alert(
                f"Started COG merge for {len(merge_ids)} covariate(s). "
                "This runs in the background — the table will update "
                "automatically as merges complete.",
                color="success",
            )
        except Exception:
            logger.exception("COG merge request failed")
            return dbc.Alert(
                "Merge failed. Please try again or contact support.",
                color="danger",
            )

    @app.callback(
        [Output("covariates-table", "rowData"),
         Output("covariates-total-count", "children")],
        [Input("admin-refresh-interval", "n_intervals"),
         Input("gee-export-result", "children"),
         Input("cog-merge-result", "children")],
    )
    def refresh_covariate_inventory(n, _export_result, _merge_result):
        # Poll GEE for actual task status before building inventory
        try:
            poll_gee_export_status()
        except Exception:
            pass  # Don't break the UI if polling fails

        try:
            rows = get_covariate_inventory()
        except Exception:
            logger.exception("Failed to build covariate inventory")
            rows = []

        gcs_count = sum(1 for r in rows if r.get("gcs_tiles", 0) > 0)
        s3_count = sum(1 for r in rows if r.get("on_s3"))
        total_label = (
            f"Total: {len(rows)} | On GCS: {gcs_count} | On S3: {s3_count}"
        )

        return rows, total_label

    # -- Admin: User management (AG Grid) ------------------------------------

    @app.callback(
        [Output("user-management-table", "rowData"),
         Output("user-management-total-count", "children")],
        Input("admin-refresh-interval", "n_intervals"),
    )
    def refresh_user_management(n):
        users = get_user_list()
        if not users:
            return [], "Total: 0"

        rows = []
        for u in users:
            rows.append({
                "id": str(u.id),
                "name": u.name,
                "email": u.email,
                "role": u.role,
                "is_approved": u.is_approved,
                "created_at": _fmt_dt(u.created_at),
                "last_login": _fmt_dt(u.last_login),
                "is_active": u.is_active,
            })

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

    # -- Admin: approve user -------------------------------------------------

    @app.callback(
        [Output("admin-user-action-result", "children", allow_duplicate=True),
         Output("admin-refresh-interval", "n_intervals", allow_duplicate=True)],
        Input("admin-approve-btn", "n_clicks"),
        State("admin-user-select", "value"),
        State("admin-refresh-interval", "n_intervals"),
        prevent_initial_call=True,
    )
    def handle_approve_user(n_clicks, user_id, current_n):
        if not user_id:
            return dbc.Alert("Please select a user.", color="warning",
                             duration=4000), no_update
        user = get_current_user()
        if not user or not user.is_admin:
            return dbc.Alert("Admin access required.", color="danger",
                             duration=4000), no_update
        success, message = approve_user(user_id)
        color = "success" if success else "danger"
        # Bump n_intervals to force a refresh of the user table
        return dbc.Alert(message, color=color, duration=4000), (current_n or 0) + 1

    # -- Admin: change user role ---------------------------------------------

    @app.callback(
        [Output("admin-user-action-result", "children", allow_duplicate=True),
         Output("admin-refresh-interval", "n_intervals", allow_duplicate=True)],
        Input("admin-role-btn", "n_clicks"),
        State("admin-user-select", "value"),
        State("admin-role-select", "value"),
        State("admin-refresh-interval", "n_intervals"),
        prevent_initial_call=True,
    )
    def handle_change_role(n_clicks, user_id, new_role, current_n):
        if not user_id:
            return dbc.Alert("Please select a user.", color="warning",
                             duration=4000), no_update
        user = get_current_user()
        if not user or not user.is_admin:
            return dbc.Alert("Admin access required.", color="danger",
                             duration=4000), no_update
        success, message = change_user_role(user_id, new_role)
        color = "success" if success else "danger"
        return dbc.Alert(message, color=color, duration=4000), (current_n or 0) + 1

    # -- Admin: delete user (modal) ------------------------------------------

    @app.callback(
        Output("admin-delete-modal", "is_open"),
        [Input("admin-delete-btn", "n_clicks"),
         Input("admin-delete-cancel", "n_clicks"),
         Input("admin-delete-confirm", "n_clicks")],
        State("admin-delete-modal", "is_open"),
        prevent_initial_call=True,
    )
    def toggle_admin_delete_modal(open_clicks, cancel_clicks, confirm_clicks,
                                  is_open):
        ctx = callback_context
        if not ctx.triggered:
            raise PreventUpdate
        trigger = ctx.triggered[0]["prop_id"].split(".")[0]
        if trigger == "admin-delete-btn":
            return True
        return False

    @app.callback(
        [Output("admin-user-action-result", "children", allow_duplicate=True),
         Output("admin-refresh-interval", "n_intervals", allow_duplicate=True)],
        Input("admin-delete-confirm", "n_clicks"),
        State("admin-user-select", "value"),
        State("admin-refresh-interval", "n_intervals"),
        prevent_initial_call=True,
    )
    def handle_admin_delete_user(n_clicks, user_id, current_n):
        if not user_id:
            return dbc.Alert("Please select a user.", color="warning",
                             duration=4000), no_update
        user = get_current_user()
        if not user or not user.is_admin:
            return dbc.Alert("Admin access required.", color="danger",
                             duration=4000), no_update
        # Prevent admins from deleting themselves
        if str(user.id) == str(user_id):
            return dbc.Alert("You cannot delete your own admin account.",
                             color="warning", duration=4000), no_update
        success, message = delete_user(user_id)
        color = "success" if success else "danger"
        return dbc.Alert(message, color=color, duration=4000), (current_n or 0) + 1

    # -- Self account deletion (modal) ---------------------------------------

    @app.callback(
        Output("self-delete-modal", "is_open"),
        [Input("self-delete-btn", "n_clicks"),
         Input("self-delete-cancel", "n_clicks"),
         Input("self-delete-confirm", "n_clicks")],
        State("self-delete-modal", "is_open"),
        prevent_initial_call=True,
    )
    def toggle_self_delete_modal(open_clicks, cancel_clicks, confirm_clicks,
                                 is_open):
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


# -- Helper functions for building detail page content -----------------------

def _build_overview(task, sites, totals):
    """Build the overview cards for a task detail page."""
    cards = []

    # Task info card
    cards.append(dbc.Card([
        dbc.CardHeader("Task Information"),
        dbc.CardBody([
            html.P(f"Description: {task.description or 'None'}"),
            html.P(f"Sites: {task.n_sites or 0}"),
            html.P(f"Covariates: {', '.join(task.covariates or [])}"),
            html.P(f"Created: {task.created_at}"),
            html.P(f"Status: {task.status}"),
        ]),
    ], className="mb-3"))

    if task.error_message:
        cards.append(dbc.Alert(
            f"Error: {task.error_message}", color="danger"
        ))

    # Summary stats if results exist
    if totals:
        total_emissions = sum(
            t.emissions_avoided_mgco2e or 0 for t in totals
        )
        total_forest = sum(
            t.forest_loss_avoided_ha or 0 for t in totals
        )
        total_area = sum(t.area_ha or 0 for t in totals)

        cards.append(dbc.Row([
            dbc.Col(dbc.Card([
                dbc.CardBody([
                    html.H4(f"{total_emissions:,.0f}",
                             className="text-success"),
                    html.P("Total Avoided Emissions (MgCO₂e)",
                           className="text-muted mb-0"),
                ]),
            ], color="success", outline=True)),
            dbc.Col(dbc.Card([
                dbc.CardBody([
                    html.H4(f"{total_forest:,.0f}",
                             className="text-info"),
                    html.P("Forest Loss Avoided (ha)",
                           className="text-muted mb-0"),
                ]),
            ], color="info", outline=True)),
            dbc.Col(dbc.Card([
                dbc.CardBody([
                    html.H4(f"{total_area:,.0f}"),
                    html.P("Total Site Area (ha)",
                           className="text-muted mb-0"),
                ]),
            ], color="secondary", outline=True)),
        ], className="mb-3"))

    # Sites table (AG Grid)
    if sites:
        site_rows = [{
            "site_id": s.site_id,
            "site_name": s.site_name or "-",
            "start_date": str(s.start_date)[:10] if s.start_date else "-",
            "end_date": str(s.end_date)[:10] if s.end_date else "Ongoing",
            "area_ha": s.area_ha,
        } for s in sites]

        site_cols = [
            {"headerName": "Site ID", "field": "site_id", "flex": 1,
             "minWidth": 120},
            {"headerName": "Name", "field": "site_name", "flex": 1.5,
             "minWidth": 150},
            {"headerName": "Start", "field": "start_date", "flex": 1,
             "minWidth": 110},
            {"headerName": "End", "field": "end_date", "flex": 1,
             "minWidth": 110},
            {"headerName": "Area (ha)", "field": "area_ha", "flex": 1,
             "minWidth": 100, "type": "numericColumn",
             "valueFormatter": {"function": "d3.format(',.0f')(params.value)"}},
        ]

        cards.append(dbc.Card([
            dbc.CardHeader("Sites"),
            dbc.CardBody(
                _make_ag_grid(
                    "overview-sites-table", site_cols,
                    row_data=site_rows, height="300px",
                ),
            ),
        ]))

    return html.Div(cards)


def _build_results_content(results, totals):
    """Build the results section with AG Grid tables and download buttons."""
    if not totals:
        return html.P("Results not yet available.", className="text-muted")

    # Totals table
    totals_rows = [{
        "site_id": t.site_id,
        "site_name": t.site_name or "-",
        "emissions_avoided_mgco2e": t.emissions_avoided_mgco2e or 0,
        "forest_loss_avoided_ha": t.forest_loss_avoided_ha or 0,
        "area_ha": t.area_ha or 0,
        "period": (f"{t.first_year}-{t.last_year}"
                   if t.first_year else "-"),
    } for t in totals]

    # Yearly results table
    yearly_rows = []
    if results:
        yearly_rows = [{
            "site_id": r.site_id,
            "year": r.year,
            "emissions_avoided_mgco2e": r.emissions_avoided_mgco2e or 0,
            "forest_loss_avoided_ha": r.forest_loss_avoided_ha or 0,
            "n_matched_pixels": r.n_matched_pixels or 0,
        } for r in results]

    content = [
        html.H5("Totals by Site"),
        _make_ag_grid(
            "results-totals-table", RESULTS_TOTAL_COLUMNS,
            row_data=totals_rows, height="350px",
        ),
    ]

    if yearly_rows:
        content.extend([
            html.H5("Results by Year", className="mt-4"),
            _make_ag_grid(
                "results-yearly-table", RESULTS_YEARLY_COLUMNS,
                row_data=yearly_rows, height="400px",
            ),
        ])

    content.extend([
        dbc.ButtonGroup([
            dbc.Button("Download CSV (by year)",
                       id="download-by-year", color="secondary",
                       size="sm"),
            dbc.Button("Download CSV (totals)",
                       id="download-totals", color="secondary",
                       size="sm"),
        ], className="mt-3"),
        dcc.Download(id="download-results"),
    ])

    return html.Div(content)


def _build_plots(results, totals):
    """Build interactive plots for task results."""
    if not results:
        return html.P("No results to plot.", className="text-muted")

    # Convert to DataFrame
    df = pd.DataFrame([{
        "site_id": r.site_id,
        "year": r.year,
        "emissions_avoided_mgco2e": r.emissions_avoided_mgco2e or 0,
        "forest_loss_avoided_ha": r.forest_loss_avoided_ha or 0,
    } for r in results])

    plots = []

    # Emissions avoided over time (stacked by site)
    fig_emissions = px.bar(
        df, x="year", y="emissions_avoided_mgco2e", color="site_id",
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
        df, x="year", y="forest_loss_avoided_ha", color="site_id",
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
        df_totals = pd.DataFrame([{
            "site_id": t.site_id,
            "site_name": t.site_name or t.site_id,
            "emissions_avoided_mgco2e": t.emissions_avoided_mgco2e or 0,
            "forest_loss_avoided_ha": t.forest_loss_avoided_ha or 0,
        } for t in totals])

        fig_site_totals = px.bar(
            df_totals, x="site_name", y="emissions_avoided_mgco2e",
            title="Total Avoided Emissions by Site",
            labels={
                "emissions_avoided_mgco2e": "Emissions Avoided (MgCO₂e)",
                "site_name": "Site",
            },
        )
        plots.append(dcc.Graph(figure=fig_site_totals))

    return html.Div(plots)


def _build_map(sites, totals):
    """Build a Leaflet map showing site locations with result overlays."""
    if not sites:
        return html.P("No site geometries available.", className="text-muted")

    totals_dict = {}
    if totals:
        totals_dict = {t.site_id: t for t in totals}

    lats, lons, texts, colors = [], [], [], []
    for s in sites:
        lats.append(0)
        lons.append(0)
        t = totals_dict.get(s.site_id)
        emissions = t.emissions_avoided_mgco2e if t else 0
        texts.append(
            f"{s.site_name or s.site_id}<br>"
            f"Emissions avoided: {emissions:,.0f} MgCO₂e"
        )
        colors.append(emissions or 0)

    fig = go.Figure(go.Scattermapbox(
        lat=lats, lon=lons, text=texts,
        marker=dict(size=10, color=colors, colorscale="Greens",
                    showscale=True),
        hoverinfo="text",
    ))
    fig.update_layout(
        mapbox=dict(style="open-street-map", zoom=2,
                    center=dict(lat=0, lon=0)),
        margin=dict(r=0, t=0, l=0, b=0),
        height=500,
    )

    return dcc.Graph(figure=fig)
