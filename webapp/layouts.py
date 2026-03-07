"""Dash application layout definitions.

Defines the page layouts for login, dashboard, task submission, task detail,
admin panel, and navigation components. Uses AG Grid for sortable/filterable
tables following the same patterns as the trends.earth-api-ui.
"""

import importlib.util
import os
import random

import dash_ag_grid as dag
import dash_bootstrap_components as dbc
from dash import dcc, html

# Default covariates for the matching formula
DEFAULT_COVARIATES = [
    "lc_2015_agriculture",
    "precip",
    "temp",
    "elev",
    "slope",
    "dist_cities",
    "friction_surface",
    "pop_2015",
    "pop_growth",
    "total_biomass",
]

# All available covariates (matching + additional options)
ALL_COVARIATES = DEFAULT_COVARIATES + [
    "lc_2015_forest",
    "lc_2015_grassland",
    "lc_2015_wetlands",
    "lc_2015_artificial",
    "lc_2015_other",
    "lc_2015_water",
    "pop_2000",
    "pop_2005",
    "pop_2010",
    "pop_2020",
    "cropland_2003",
    "cropland_2007",
    "cropland_2011",
    "cropland_2015",
    "cropland_2019",
    "ecoregion",
    "pa",
]

# Exact match variables — at least one must be selected for each task.
# Names must match the output_name values in webapp/rasterize_vectors.py.
EXACT_MATCH_OPTIONS = [
    {"label": "Admin 0 (Country)", "value": "admin0"},
    {"label": "Admin 1 (Province / State)", "value": "admin1"},
    {"label": "Admin 2 (District)", "value": "admin2"},
    {"label": "Ecoregion", "value": "ecoregion"},
    {"label": "Protected Area (WDPA)", "value": "pa"},
]

DEFAULT_EXACT_MATCH = ["admin1", "ecoregion", "pa"]

# Variables that can be used as *either* exact match *or* covariates
# (but never both simultaneously).  When used as a covariate they are
# included in the propensity score formula; when used as an exact match
# they define stratification groups.
DUAL_PURPOSE_VARS = ["ecoregion", "pa"]

MATCHING_JOB_QUEUE_OPTIONS = [
    {
        "label": "spot_fleet_1TB-io2-disk (default)",
        "value": "spot_fleet_1TB-io2-disk",
    },
    {
        "label": "ondemand_fleet_1TB-io2-disk",
        "value": "ondemand_fleet_1TB-io2-disk",
    },
]

DEFAULT_MATCHING_JOB_QUEUE = "spot_fleet_1TB-io2-disk"

# -- Column definitions (AG Grid) -------------------------------------------

TRUNCATED_CELL = {
    "whiteSpace": "nowrap",
    "overflow": "hidden",
    "textOverflow": "ellipsis",
}

TASK_LIST_COLUMNS = [
    {
        "headerName": "Name",
        "field": "name",
        "flex": 2,
        "minWidth": 200,
        "pinned": "left",
        "cellStyle": {**TRUNCATED_CELL, "cursor": "pointer"},
        "tooltipField": "name",
        "cellRenderer": "TaskLink",
    },
    {
        "headerName": "Status",
        "field": "status",
        "flex": 1,
        "minWidth": 110,
        "cellStyle": {"fontSize": "12px"},
        "filter": "agTextColumnFilter",
        "filterParams": {
            "buttons": ["clear", "apply"],
            "closeOnApply": True,
        },
        "cellRenderer": "StatusBadge",
    },
    {
        "headerName": "Sites",
        "field": "n_sites",
        "flex": 0.6,
        "minWidth": 80,
        "filter": "agNumberColumnFilter",
    },
    {
        "headerName": "Created",
        "field": "created_at",
        "flex": 1.5,
        "minWidth": 160,
        "sort": "desc",
        "sortIndex": 0,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
        "cellRenderer": "LocalDateTime",
    },
    {
        "headerName": "Submitted",
        "field": "submitted_at",
        "flex": 1.5,
        "minWidth": 160,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
        "cellRenderer": "LocalDateTime",
    },
    {
        "headerName": "Completed",
        "field": "completed_at",
        "flex": 1.5,
        "minWidth": 160,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
        "cellRenderer": "LocalDateTime",
    },
    {
        "headerName": "Actions",
        "field": "actions",
        "flex": 0.8,
        "minWidth": 100,
        "cellRenderer": "TaskActions",
        "sortable": False,
        "filter": False,
        "pinned": "right",
    },
]

COVARIATE_COLUMNS = [
    {
        "headerName": "Covariate",
        "field": "covariate_name",
        "checkboxSelection": True,
        "headerCheckboxSelection": True,
        "headerCheckboxSelectionFilteredOnly": True,
        "flex": 2,
        "minWidth": 200,
        "pinned": "left",
        "cellStyle": {**TRUNCATED_CELL},
        "tooltipField": "description",
    },
    {
        "headerName": "Category",
        "field": "category",
        "flex": 1.2,
        "minWidth": 120,
        "filter": "agTextColumnFilter",
    },
    {
        "headerName": "Status",
        "field": "status",
        "flex": 1,
        "minWidth": 110,
        "cellRenderer": "StatusBadge",
        "filter": "agTextColumnFilter",
    },
    {
        "headerName": "GEE Task ID",
        "field": "gee_task_id",
        "flex": 1.5,
        "minWidth": 150,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "11px"},
        "tooltipField": "gee_task_id",
    },
    {
        "headerName": "GCS Tiles",
        "field": "gcs_tiles",
        "flex": 0.7,
        "minWidth": 85,
        "cellRenderer": "TileCount",
    },
    {
        "headerName": "On S3",
        "field": "on_s3",
        "flex": 0.5,
        "minWidth": 65,
        "cellRenderer": "S3Status",
    },
    {
        "headerName": "Size (MB)",
        "field": "size_mb",
        "flex": 0.8,
        "minWidth": 90,
        "filter": "agNumberColumnFilter",
        "valueFormatter": {
            "function": "params.value ? d3.format(',.1f')(params.value) : ''"
        },
        "type": "numericColumn",
    },
    {
        "headerName": "Merged URL",
        "field": "merged_url",
        "flex": 2.5,
        "minWidth": 250,
        "cellRenderer": "CogLink",
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "11px"},
        "tooltipField": "merged_url",
    },
    {
        "headerName": "Error",
        "field": "error_message",
        "flex": 2,
        "minWidth": 200,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "11px", "color": "#721C24"},
        "tooltipField": "error_message",
    },
    {
        "headerName": "Actions",
        "field": "actions",
        "flex": 1.5,
        "minWidth": 170,
        "cellRenderer": "CovariateActions",
        "sortable": False,
        "filter": False,
        "pinned": "right",
    },
]

RESULTS_TOTAL_COLUMNS = [
    {
        "headerName": "Site ID",
        "field": "site_id",
        "flex": 1,
        "minWidth": 120,
        "pinned": "left",
        "cellStyle": {**TRUNCATED_CELL},
        "tooltipField": "site_id",
    },
    {
        "headerName": "Name",
        "field": "site_name",
        "flex": 1.5,
        "minWidth": 150,
        "cellStyle": {**TRUNCATED_CELL},
        "tooltipField": "site_name",
    },
    {
        "headerName": "Emissions Avoided (MgCO₂e)",
        "field": "emissions_avoided_mgco2e",
        "flex": 1.5,
        "minWidth": 180,
        "filter": "agNumberColumnFilter",
        "valueFormatter": {"function": "d3.format(',.1f')(params.value)"},
        "type": "numericColumn",
        "sort": "desc",
        "sortIndex": 0,
    },
    {
        "headerName": "Forest Loss Avoided (ha)",
        "field": "forest_loss_avoided_ha",
        "flex": 1.5,
        "minWidth": 170,
        "filter": "agNumberColumnFilter",
        "valueFormatter": {"function": "d3.format(',.1f')(params.value)"},
        "type": "numericColumn",
    },
    {
        "headerName": "Area (ha)",
        "field": "area_ha",
        "flex": 1,
        "minWidth": 110,
        "filter": "agNumberColumnFilter",
        "valueFormatter": {"function": "d3.format(',.0f')(params.value)"},
        "type": "numericColumn",
    },
    {
        "headerName": "Period",
        "field": "period",
        "flex": 1,
        "minWidth": 110,
    },
    {
        "headerName": "Sampled %",
        "field": "sampled_percent",
        "flex": 0.8,
        "minWidth": 100,
        "filter": "agNumberColumnFilter",
        "valueFormatter": {"function": "d3.format('.1f')(params.value)"},
        "type": "numericColumn",
    },
]

RESULTS_YEARLY_COLUMNS = [
    {
        "headerName": "Site ID",
        "field": "site_id",
        "flex": 1,
        "minWidth": 120,
        "pinned": "left",
        "cellStyle": {**TRUNCATED_CELL},
    },
    {
        "headerName": "Year",
        "field": "year",
        "flex": 0.6,
        "minWidth": 80,
        "filter": "agNumberColumnFilter",
        "sort": "asc",
        "sortIndex": 0,
    },
    {
        "headerName": "Site Defor. (ha)",
        "field": "treatment_defor_ha",
        "flex": 1.2,
        "minWidth": 140,
        "filter": "agNumberColumnFilter",
        "valueFormatter": {"function": "d3.format(',.1f')(params.value)"},
        "type": "numericColumn",
    },
    {
        "headerName": "Control Defor. (ha)",
        "field": "control_defor_ha",
        "flex": 1.2,
        "minWidth": 150,
        "filter": "agNumberColumnFilter",
        "valueFormatter": {"function": "d3.format(',.1f')(params.value)"},
        "type": "numericColumn",
    },
    {
        "headerName": "Emissions Avoided (MgCO₂e)",
        "field": "emissions_avoided_mgco2e",
        "flex": 1.5,
        "minWidth": 180,
        "filter": "agNumberColumnFilter",
        "valueFormatter": {"function": "d3.format(',.1f')(params.value)"},
        "type": "numericColumn",
    },
    {
        "headerName": "Forest Loss Avoided (ha)",
        "field": "forest_loss_avoided_ha",
        "flex": 1.5,
        "minWidth": 170,
        "filter": "agNumberColumnFilter",
        "valueFormatter": {"function": "d3.format(',.1f')(params.value)"},
        "type": "numericColumn",
    },
    {
        "headerName": "Matched Pixels",
        "field": "n_matched_pixels",
        "flex": 1,
        "minWidth": 120,
        "filter": "agNumberColumnFilter",
        "valueFormatter": {"function": "d3.format(',')(params.value)"},
        "type": "numericColumn",
    },
]

USER_MANAGEMENT_COLUMNS = [
    {
        "headerName": "Name",
        "field": "name",
        "flex": 1.5,
        "minWidth": 150,
        "cellStyle": {**TRUNCATED_CELL},
        "tooltipField": "name",
    },
    {
        "headerName": "Email",
        "field": "email",
        "flex": 2,
        "minWidth": 200,
        "cellStyle": {**TRUNCATED_CELL},
        "tooltipField": "email",
    },
    {
        "headerName": "Role",
        "field": "role",
        "flex": 0.8,
        "minWidth": 90,
        "filter": "agTextColumnFilter",
    },
    {
        "headerName": "Approved",
        "field": "is_approved",
        "flex": 0.7,
        "minWidth": 90,
        "cellRenderer": "ApprovalBadge",
        "filter": "agTextColumnFilter",
    },
    {
        "headerName": "Created",
        "field": "created_at",
        "flex": 1.5,
        "minWidth": 160,
        "sort": "desc",
        "sortIndex": 0,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
        "cellRenderer": "LocalDateTime",
    },
    {
        "headerName": "Last Login",
        "field": "last_login",
        "flex": 1.5,
        "minWidth": 160,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
        "cellRenderer": "LocalDateTime",
    },
    {
        "headerName": "Active",
        "field": "is_active",
        "flex": 0.6,
        "minWidth": 80,
    },
]


# -- AG Grid defaults (mirroring api-ui patterns) ---------------------------

DEFAULT_GRID_OPTIONS = {
    "enableCellTextSelection": True,
    "ensureDomOrder": True,
    "animateRows": False,
    "suppressMenuHide": True,
    "suppressHorizontalScroll": False,
    "alwaysShowHorizontalScroll": True,
    "rowHeight": 32,
    "headerHeight": 32,
}

DEFAULT_COL_DEF = {
    "resizable": True,
    "sortable": True,
    "filter": True,
    "minWidth": 50,
    "suppressSizeToFit": True,
    "wrapText": True,
    "autoHeight": False,
}

TASK_STATUS_ROW_STYLES = [
    {
        "condition": "params.data.status === 'failed'",
        "style": {"backgroundColor": "#F8D7DA", "color": "#721C24"},
    },
    {
        "condition": "params.data.status === 'succeeded'",
        "style": {"backgroundColor": "#D1E7DD", "color": "#0F5132"},
    },
    {
        "condition": "params.data.status === 'running'",
        "style": {"backgroundColor": "#CCE5FF", "color": "#084298"},
    },
    {
        "condition": "params.data.status === 'submitted'",
        "style": {"backgroundColor": "#FFF3CD", "color": "#664D03"},
    },
    {
        "condition": "params.data.status === 'pending'",
        "style": {"backgroundColor": "#E2E3E5", "color": "#495057"},
    },
]

COVARIATE_STATUS_ROW_STYLES = [
    # Greyed-out: nothing anywhere
    {
        "condition": "(!params.data.gcs_tiles || params.data.gcs_tiles === 0) && !params.data.on_s3 && !params.data.status",
        "style": {"backgroundColor": "#F5F5F5", "color": "#AAAAAA"},
    },
    # Export phase
    {
        "condition": "params.data.status === 'pending_export'",
        "style": {"backgroundColor": "#E2E3E5", "color": "#495057"},
    },
    {
        "condition": "params.data.status === 'exporting'",
        "style": {"backgroundColor": "#CCE5FF", "color": "#084298"},
    },
    {
        "condition": "params.data.status === 'exported'",
        "style": {"backgroundColor": "#FFF3CD", "color": "#664D03"},
    },
    # Merge phase
    {
        "condition": "params.data.status === 'pending_merge'",
        "style": {"backgroundColor": "#E2E3E5", "color": "#495057"},
    },
    {
        "condition": "params.data.status === 'merging'",
        "style": {"backgroundColor": "#CCE5FF", "color": "#084298"},
    },
    # Merged / on S3
    {
        "condition": "params.data.on_s3 && !params.data.status",
        "style": {"backgroundColor": "#D1E7DD", "color": "#0F5132"},
    },
    {
        "condition": "params.data.status === 'merged'",
        "style": {"backgroundColor": "#D1E7DD", "color": "#0F5132"},
    },
    # Failed / cancelled
    {
        "condition": "params.data.status === 'failed'",
        "style": {"backgroundColor": "#F8D7DA", "color": "#721C24"},
    },
    {
        "condition": "params.data.status === 'cancelled'",
        "style": {"backgroundColor": "#F8D7DA", "color": "#721C24"},
    },
]


def _make_ag_grid(
    table_id,
    column_defs,
    *,
    row_model="clientSide",
    height="600px",
    style_conditions=None,
    grid_options_extra=None,
    row_data=None,
):
    """Create an AG Grid component using api-ui conventions.

    Args:
        table_id: HTML id for the grid component.
        column_defs: list of AG-Grid column definitions.
        row_model: 'clientSide' or 'infinite'.
        height: CSS height string.
        style_conditions: optional row-style conditions list.
        grid_options_extra: dict merged into DEFAULT_GRID_OPTIONS.
        row_data: initial row data (clientSide mode only).
    """
    grid_opts = {**DEFAULT_GRID_OPTIONS}
    if grid_options_extra:
        grid_opts.update(grid_options_extra)

    kwargs = {
        "id": table_id,
        "columnDefs": column_defs,
        "defaultColDef": DEFAULT_COL_DEF,
        "rowModelType": row_model,
        "dashGridOptions": grid_opts,
        "style": {"height": height, "width": "100%"},
        "className": "ag-theme-alpine",
    }

    if style_conditions:
        kwargs["getRowStyle"] = {"styleConditions": style_conditions}

    if row_data is not None and row_model == "clientSide":
        kwargs["rowData"] = row_data

    return dag.AgGrid(**kwargs)


# -- Navigation bar ----------------------------------------------------------


def navbar(user=None, active_page=None):
    """Top navigation bar.

    Parameters
    ----------
    active_page : str | None
        One of ``"/submit"``, ``"/"``, ``"/admin"``, ``"/settings"``.
        The matching nav link is rendered bold to indicate the current page.
    """
    nav_items = []
    if user:
        nav_items = [
            dbc.NavItem(
                dbc.NavLink(
                    "Submit Task",
                    href="/submit",
                    active=(active_page == "/submit"),
                    className="fw-bold" if active_page == "/submit" else "",
                )
            ),
            dbc.NavItem(
                dbc.NavLink(
                    "View Tasks",
                    href="/",
                    active=(active_page == "/"),
                    className="fw-bold" if active_page == "/" else "",
                )
            ),
        ]
        if user.is_admin:
            nav_items.append(
                dbc.NavItem(
                    dbc.NavLink(
                        "Admin",
                        href="/admin",
                        active=(active_page == "/admin"),
                        className="fw-bold" if active_page == "/admin" else "",
                    )
                )
            )
        nav_items.append(
            dbc.NavItem(
                dbc.NavLink(
                    "Profile",
                    href="/settings",
                    active=(active_page == "/settings"),
                    className="fw-bold" if active_page == "/settings" else "",
                )
            )
        )

    right_items = []
    if user:
        right_items = [
            dbc.NavItem(dbc.NavLink(user.name, disabled=True, className="text-light")),
            dbc.NavItem(dbc.NavLink("Logout", href="/logout")),
        ]
    else:
        right_items = [
            dbc.NavItem(dbc.NavLink("Login", href="/login")),
            dbc.NavItem(dbc.NavLink("Register", href="/register")),
        ]

    return dbc.Navbar(
        dbc.Container(
            [
                dbc.NavbarBrand(
                    [
                        html.Img(
                            src="/assets/CI_Logo.png",
                            height="36",
                            className="me-3",
                            alt="Conservation International",
                        ),
                        html.Span(
                            "Avoided Emissions",
                            style={
                                "borderLeft": "1px solid rgba(255,255,255,0.3)",
                                "paddingLeft": "0.75rem",
                            },
                        ),
                    ],
                    href="/",
                    className="fw-bold d-flex align-items-center",
                ),
                dbc.NavbarToggler(id="navbar-toggler", n_clicks=0),
                dbc.Collapse(
                    [
                        dbc.Nav(nav_items, className="me-auto", navbar=True),
                        dbc.Nav(right_items, navbar=True),
                    ],
                    id="navbar-collapse",
                    is_open=False,
                    navbar=True,
                ),
            ],
            fluid="lg",
        ),
        color="dark",
        dark=True,
        className="mb-4 ae-navbar shadow-sm",
    )


def footer():
    """Footer bar with legal links — shown on authenticated pages."""
    return html.Footer(
        dbc.Container(
            [
                dbc.Row(
                    dbc.Col(
                        html.Div(
                            [
                                html.Span(
                                    "Powered by",
                                    className="ae-footer-powered-text",
                                ),
                                html.Img(
                                    src="/assets/trends_earth_bl_print.png",
                                    alt="Trends.Earth",
                                    className="ae-footer-powered-logo",
                                ),
                            ],
                            className="ae-footer-powered",
                        ),
                        className="text-center",
                    ),
                ),
                dbc.Row(
                    dbc.Col(
                        [
                            html.A(
                                "Privacy Policy",
                                href="https://www.conservation.org/privacy-policy",
                                target="_blank",
                                rel="noopener noreferrer",
                                className="ae-footer-link",
                            ),
                            html.Span("·", className="ae-footer-separator"),
                            html.A(
                                "Terms of Use",
                                href="https://www.conservation.org/terms-of-use",
                                target="_blank",
                                rel="noopener noreferrer",
                                className="ae-footer-link",
                            ),
                        ],
                        className="text-center",
                    ),
                ),
            ],
            fluid="lg",
        ),
        className="ae-footer",
    )


# -- Page layouts ------------------------------------------------------------


def login_layout():
    """Login page layout."""
    return dbc.Container(
        [
            navbar(),
            dbc.Row(
                dbc.Col(
                    [
                        dbc.Card(
                            [
                                dbc.CardHeader(
                                    html.Div(
                                        [
                                            html.H4(
                                                "Avoided Emissions",
                                                className="text-center mb-1",
                                                style={"color": "white"},
                                            ),
                                            html.H6(
                                                "Login",
                                                className="text-center",
                                                style={"color": "#ffffffcc"},
                                            ),
                                        ]
                                    ),
                                    style={"backgroundColor": "#2c3e50"},
                                ),
                                dbc.CardBody(
                                    [
                                        dbc.Label("Email"),
                                        dbc.Input(
                                            id="login-email",
                                            type="email",
                                            placeholder="user@example.com",
                                            className="mb-2",
                                        ),
                                        dbc.Label("Password"),
                                        dbc.Input(
                                            id="login-password",
                                            type="password",
                                            className="mb-2",
                                        ),
                                        html.Div(
                                            id="login-error",
                                            className="text-danger mb-2",
                                        ),
                                        dbc.Button(
                                            "Login",
                                            id="login-button",
                                            color="primary",
                                            className="w-100",
                                        ),
                                        html.Hr(),
                                        html.P(
                                            [
                                                "Don't have an account? ",
                                                dcc.Link(
                                                    "Register here",
                                                    href="/register",
                                                    className="fw-bold",
                                                ),
                                            ],
                                            className="text-center mb-1 small",
                                        ),
                                        html.Div(
                                            dcc.Link(
                                                "Forgot password?",
                                                href="/forgot-password",
                                                className="small",
                                            ),
                                            className="text-center mb-0",
                                        ),
                                    ]
                                ),
                            ],
                            className="mt-5 shadow-sm ae-auth-card",
                        ),
                        html.Div(
                            [
                                html.Span(
                                    "Powered by",
                                    className="ae-footer-powered-text",
                                ),
                                html.Img(
                                    src="/assets/trends_earth_bl_print.png",
                                    alt="Trends.Earth",
                                    className="ae-footer-powered-logo",
                                ),
                            ],
                            className="ae-footer-powered mt-3",
                        ),
                        html.Div(
                            [
                                html.A(
                                    "Privacy Policy",
                                    href="https://www.conservation.org/policies/privacy",
                                    target="_blank",
                                    className="text-muted",
                                    style={
                                        "textDecoration": "none",
                                        "fontSize": "12px",
                                    },
                                ),
                                html.Span(
                                    " | ",
                                    className="text-muted",
                                    style={"fontSize": "12px"},
                                ),
                                html.A(
                                    "Terms of Use",
                                    href="https://www.conservation.org/policies/terms-of-use",
                                    target="_blank",
                                    className="text-muted",
                                    style={
                                        "textDecoration": "none",
                                        "fontSize": "12px",
                                    },
                                ),
                            ],
                            className="text-center mt-1",
                        ),
                    ],
                    xs=12,
                    sm={"size": 10, "offset": 1},
                    md={"size": 6, "offset": 3},
                    lg={"size": 4, "offset": 4},
                )
            ),
        ]
    )


def register_layout():
    """Registration page layout.

    Collects only name and email.  Once an admin approves the account
    the user receives an email with a link to set their password.
    """
    return dbc.Container(
        [
            navbar(),
            dbc.Row(
                dbc.Col(
                    [
                        dbc.Card(
                            [
                                dbc.CardHeader(
                                    html.Div(
                                        [
                                            html.H4(
                                                "Avoided Emissions",
                                                className="text-center mb-1",
                                                style={"color": "white"},
                                            ),
                                            html.H6(
                                                "Create Account",
                                                className="text-center",
                                                style={"color": "#ffffffcc"},
                                            ),
                                        ]
                                    ),
                                    style={"backgroundColor": "#2c3e50"},
                                ),
                                dbc.CardBody(
                                    [
                                        dbc.Label("Full Name"),
                                        dbc.Input(
                                            id="register-name",
                                            type="text",
                                            className="mb-2",
                                        ),
                                        dbc.Label("Email"),
                                        dbc.Input(
                                            id="register-email",
                                            type="email",
                                            placeholder="user@example.com",
                                            className="mb-3",
                                        ),
                                        html.Small(
                                            "After registration, an administrator "
                                            "will review your request. Once "
                                            "approved, you'll receive an email "
                                            "with a link to set your password.",
                                            className="text-muted d-block mb-3",
                                        ),
                                        html.Div(
                                            id="register-message", className="mb-2"
                                        ),
                                        dbc.Button(
                                            "Register",
                                            id="register-button",
                                            color="primary",
                                            className="w-100",
                                        ),
                                        html.Hr(),
                                        html.P(
                                            [
                                                "Already have an account? ",
                                                dcc.Link(
                                                    "Login here",
                                                    href="/login",
                                                    className="fw-bold",
                                                ),
                                            ],
                                            className="text-center mb-0 small",
                                        ),
                                    ]
                                ),
                            ],
                            className="mt-5 shadow-sm ae-auth-card",
                        ),
                    ],
                    xs=12,
                    sm={"size": 10, "offset": 1},
                    md={"size": 6, "offset": 3},
                    lg={"size": 4, "offset": 4},
                )
            ),
        ]
    )


def forgot_password_layout():
    """Forgot-password page — accepts an email and sends a reset link."""
    return dbc.Container(
        [
            navbar(),
            dbc.Row(
                dbc.Col(
                    [
                        dbc.Card(
                            [
                                dbc.CardHeader(
                                    html.Div(
                                        [
                                            html.H4(
                                                "Avoided Emissions",
                                                className="text-center mb-1",
                                                style={"color": "white"},
                                            ),
                                            html.H6(
                                                "Reset Password",
                                                className="text-center",
                                                style={"color": "#ffffffcc"},
                                            ),
                                        ]
                                    ),
                                    style={"backgroundColor": "#2c3e50"},
                                ),
                                dbc.CardBody(
                                    [
                                        html.P(
                                            "Enter the email address associated "
                                            "with your account and we'll send you "
                                            "a link to reset your password.",
                                            className="mb-3",
                                        ),
                                        dbc.Label("Email"),
                                        dbc.Input(
                                            id="forgot-email",
                                            type="email",
                                            placeholder="user@example.com",
                                            className="mb-3",
                                        ),
                                        html.Div(id="forgot-message", className="mb-2"),
                                        dbc.Button(
                                            "Send Reset Link",
                                            id="forgot-button",
                                            color="primary",
                                            className="w-100",
                                        ),
                                        html.Hr(),
                                        html.P(
                                            [
                                                "Remember your password? ",
                                                dcc.Link(
                                                    "Login here",
                                                    href="/login",
                                                    className="fw-bold",
                                                ),
                                            ],
                                            className="text-center mb-0 small",
                                        ),
                                    ]
                                ),
                            ],
                            className="mt-5 shadow-sm ae-auth-card",
                        ),
                    ],
                    xs=12,
                    sm={"size": 10, "offset": 1},
                    md={"size": 6, "offset": 3},
                    lg={"size": 4, "offset": 4},
                )
            ),
        ]
    )


def reset_password_layout(token=""):
    """Reset-password page — sets a new password using the emailed token.

    Includes real-time password requirements hints that update as the
    user types (driven by a Dash callback).
    """
    req_item_style = {"fontSize": "0.85rem", "lineHeight": "1.6"}

    return dbc.Container(
        [
            navbar(),
            # Hidden store carries the token from the URL query string
            dcc.Store(id="reset-token-store", data=token),
            dbc.Row(
                dbc.Col(
                    [
                        dbc.Card(
                            [
                                dbc.CardHeader(
                                    html.Div(
                                        [
                                            html.H4(
                                                "Avoided Emissions",
                                                className="text-center mb-1",
                                                style={"color": "white"},
                                            ),
                                            html.H6(
                                                "Set New Password",
                                                className="text-center",
                                                style={"color": "#ffffffcc"},
                                            ),
                                        ]
                                    ),
                                    style={"backgroundColor": "#2c3e50"},
                                ),
                                dbc.CardBody(
                                    [
                                        dbc.Label("New Password"),
                                        dbc.Input(
                                            id="reset-password",
                                            type="password",
                                            className="mb-1",
                                            debounce=False,
                                        ),
                                        dbc.Label("Confirm New Password"),
                                        dbc.Input(
                                            id="reset-password-confirm",
                                            type="password",
                                            className="mb-2",
                                            debounce=False,
                                        ),
                                        # Password requirements checklist
                                        html.Div(
                                            [
                                                html.Small(
                                                    "Password requirements:",
                                                    className="fw-bold",
                                                ),
                                                html.Ul(
                                                    [
                                                        html.Li(
                                                            "At least 12 characters",
                                                            id="req-length",
                                                            className="text-muted",
                                                            style=req_item_style,
                                                        ),
                                                        html.Li(
                                                            "One uppercase letter",
                                                            id="req-uppercase",
                                                            className="text-muted",
                                                            style=req_item_style,
                                                        ),
                                                        html.Li(
                                                            "One lowercase letter",
                                                            id="req-lowercase",
                                                            className="text-muted",
                                                            style=req_item_style,
                                                        ),
                                                        html.Li(
                                                            "One number",
                                                            id="req-number",
                                                            className="text-muted",
                                                            style=req_item_style,
                                                        ),
                                                        html.Li(
                                                            "One special character",
                                                            id="req-special",
                                                            className="text-muted",
                                                            style=req_item_style,
                                                        ),
                                                        html.Li(
                                                            "Passwords match",
                                                            id="req-match",
                                                            className="text-muted",
                                                            style=req_item_style,
                                                        ),
                                                    ],
                                                    className="mb-2",
                                                    style={
                                                        "listStyleType": "none",
                                                        "paddingLeft": "0.5rem",
                                                    },
                                                ),
                                            ],
                                            className="mb-2",
                                        ),
                                        html.Div(id="reset-message", className="mb-2"),
                                        dbc.Button(
                                            "Set Password",
                                            id="reset-button",
                                            color="primary",
                                            className="w-100",
                                        ),
                                        html.Hr(),
                                        html.P(
                                            [
                                                dcc.Link(
                                                    "Back to login",
                                                    href="/login",
                                                    className="fw-bold",
                                                ),
                                            ],
                                            className="text-center mb-0 small",
                                        ),
                                    ]
                                ),
                            ],
                            className="mt-5 shadow-sm ae-auth-card",
                        ),
                    ],
                    xs=12,
                    sm={"size": 10, "offset": 1},
                    md={"size": 6, "offset": 3},
                    lg={"size": 4, "offset": 4},
                )
            ),
        ]
    )


def dashboard_layout(user):
    """Main dashboard showing task list with AG Grid and status overview."""
    show_all_checkbox = (
        dbc.Col(
            dbc.Checkbox(
                id="show-all-tasks-checkbox",
                label="Show all users' tasks",
                value=False,
                className="ms-3",
            ),
            width="auto",
            className="d-flex align-items-center",
        )
        if user and user.is_admin
        else html.Div(id="show-all-tasks-checkbox", hidden=True)
    )

    return dbc.Container(
        [
            navbar(user, active_page="/"),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H2("Analysis Tasks", className="mb-1"),
                            html.P(
                                "Track submitted analyses and quickly start new tasks. "
                                "Click the name in the table to view results for a "
                                "completed task.",
                                className="text-muted mb-0",
                            ),
                        ],
                        width=True,
                    )
                ],
                className="mb-3",
            ),
            dbc.Card(
                [
                    dbc.CardBody(
                        [
                            dbc.Row(
                                [
                                    dbc.Col(
                                        html.Span(
                                            id="task-total-count",
                                            children="Total: 0",
                                            className="text-muted fw-bold",
                                        ),
                                        width=True,
                                    ),
                                    show_all_checkbox,
                                    dbc.Col(
                                        [
                                            dbc.Button(
                                                "Refresh",
                                                id="refresh-tasks-btn",
                                                color="primary",
                                                size="sm",
                                                className="me-2",
                                            ),
                                            dbc.Button(
                                                "New Task",
                                                href="/submit",
                                                color="success",
                                                size="sm",
                                            ),
                                        ],
                                        width="auto",
                                        className="ae-action-buttons",
                                    ),
                                ],
                                className="ae-action-bar align-items-center mb-3",
                            ),
                            _make_ag_grid(
                                table_id="task-list-table",
                                column_defs=TASK_LIST_COLUMNS,
                                row_model="clientSide",
                                height="700px",
                                style_conditions=TASK_STATUS_ROW_STYLES,
                            ),
                        ]
                    )
                ],
                className="ae-section-card mb-4",
            ),
            # Stores & intervals
            html.Div(id="recompute-from-list-result"),
            dcc.Store(id="task-list-store"),
            dcc.Interval(id="refresh-interval", interval=30000, n_intervals=0),
        ]
    )


def submit_layout(user):
    """Task submission form with file upload and covariate selection."""
    default_random_seed = random.SystemRandom().randint(1, 2147483647)

    return dbc.Container(
        [
            navbar(user, active_page="/submit"),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H2("Submit Analysis Task", className="mb-1"),
                            html.P(
                                "Use the guided tabs to upload sites, configure matching, and submit.",
                                className="text-muted mb-0",
                            ),
                        ],
                        width=True,
                    )
                ],
                className="mb-3",
            ),
            html.Div(
                [
                    html.Div(id="submit-errors"),
                    html.Div(id="submit-result", className="mt-2"),
                ],
                className="submit-feedback-sticky mb-3",
            ),
            dbc.Form(
                [
                    dbc.Tabs(
                        [
                            dbc.Tab(
                                label="1. Task & Sites",
                                tab_id="tab-submit-sites",
                                children=[
                                    html.Div(
                                        [
                                            dbc.Row(
                                                [
                                                    dbc.Col(
                                                        [
                                                            dbc.Label("Task Name"),
                                                            dbc.Input(
                                                                id="task-name",
                                                                type="text",
                                                                placeholder="My analysis",
                                                            ),
                                                        ],
                                                        xs=12,
                                                        md=6,
                                                    ),
                                                    dbc.Col(
                                                        [
                                                            dbc.Label(
                                                                "Description (optional)"
                                                            ),
                                                            dbc.Input(
                                                                id="task-description",
                                                                type="text",
                                                                placeholder="Brief description",
                                                            ),
                                                        ],
                                                        xs=12,
                                                        md=6,
                                                    ),
                                                ],
                                                className="g-3 mb-3",
                                            ),
                                            dbc.Card(
                                                [
                                                    dbc.CardHeader(
                                                        "Use Previously Uploaded Sites"
                                                    ),
                                                    dbc.CardBody(
                                                        [
                                                            dbc.Row(
                                                                [
                                                                    dbc.Col(
                                                                        dbc.Select(
                                                                            id="site-set-selector",
                                                                            options=[],
                                                                            placeholder="Select a saved site set...",
                                                                        ),
                                                                        xs=12,
                                                                        md=9,
                                                                    ),
                                                                    dbc.Col(
                                                                        dbc.Button(
                                                                            "Delete",
                                                                            id="delete-site-set-btn",
                                                                            color="danger",
                                                                            outline=True,
                                                                            className="w-100",
                                                                        ),
                                                                        xs=12,
                                                                        md=3,
                                                                    ),
                                                                ],
                                                                className="g-2 mb-2",
                                                            ),
                                                            html.Div(
                                                                id="site-set-metadata"
                                                            ),
                                                            html.Div(
                                                                id="site-set-action-status",
                                                                className="mt-2",
                                                            ),
                                                        ]
                                                    ),
                                                ],
                                                className="ae-section-card mb-3",
                                            ),
                                            dbc.Card(
                                                [
                                                    dbc.CardHeader("Site Preview"),
                                                    dbc.CardBody(
                                                        [
                                                            html.Div(
                                                                id="site-preview-map",
                                                                className="mb-3",
                                                            ),
                                                            html.Div(id="site-preview"),
                                                        ]
                                                    ),
                                                ],
                                                className="ae-section-card mb-3",
                                            ),
                                            dbc.Card(
                                                [
                                                    dbc.CardHeader(
                                                        "Upload New Sites (GeoJSON, GeoPackage, or Archive)"
                                                    ),
                                                    dbc.CardBody(
                                                        [
                                                            html.P(
                                                                [
                                                                    "Upload a ",
                                                                    html.Strong(
                                                                        "GeoJSON"
                                                                    ),
                                                                    " or ",
                                                                    html.Strong(
                                                                        "GeoPackage"
                                                                    ),
                                                                    " file, or a ",
                                                                    html.Strong(
                                                                        ".zip/.tar.gz"
                                                                    ),
                                                                    " archive containing exactly one ",
                                                                    html.Strong(
                                                                        "Shapefile"
                                                                    ),
                                                                    ", ",
                                                                    html.Strong(
                                                                        "GeoJSON"
                                                                    ),
                                                                    ", or ",
                                                                    html.Strong(
                                                                        "GeoPackage"
                                                                    ),
                                                                    " dataset with site polygons. "
                                                                    "Geometries must be valid Polygons or "
                                                                    "MultiPolygons in EPSG:4326 (WGS 84).",
                                                                ],
                                                                className="mb-2 small",
                                                            ),
                                                            dbc.Table(
                                                                [
                                                                    html.Thead(
                                                                        html.Tr(
                                                                            [
                                                                                html.Th(
                                                                                    "Field"
                                                                                ),
                                                                                html.Th(
                                                                                    "Type"
                                                                                ),
                                                                                html.Th(
                                                                                    "Required"
                                                                                ),
                                                                                html.Th(
                                                                                    "Description"
                                                                                ),
                                                                            ]
                                                                        )
                                                                    ),
                                                                    html.Tbody(
                                                                        [
                                                                            html.Tr(
                                                                                [
                                                                                    html.Td(
                                                                                        html.Code(
                                                                                            "site_id"
                                                                                        )
                                                                                    ),
                                                                                    html.Td(
                                                                                        "string"
                                                                                    ),
                                                                                    html.Td(
                                                                                        "Yes"
                                                                                    ),
                                                                                    html.Td(
                                                                                        "Unique site identifier"
                                                                                    ),
                                                                                ]
                                                                            ),
                                                                            html.Tr(
                                                                                [
                                                                                    html.Td(
                                                                                        html.Code(
                                                                                            "site_name"
                                                                                        )
                                                                                    ),
                                                                                    html.Td(
                                                                                        "string"
                                                                                    ),
                                                                                    html.Td(
                                                                                        "Yes"
                                                                                    ),
                                                                                    html.Td(
                                                                                        "Human-readable site name"
                                                                                    ),
                                                                                ]
                                                                            ),
                                                                            html.Tr(
                                                                                [
                                                                                    html.Td(
                                                                                        html.Code(
                                                                                            "start_date"
                                                                                        )
                                                                                    ),
                                                                                    html.Td(
                                                                                        "date"
                                                                                    ),
                                                                                    html.Td(
                                                                                        "Yes"
                                                                                    ),
                                                                                    html.Td(
                                                                                        "Intervention start date (YYYY-MM-DD)"
                                                                                    ),
                                                                                ]
                                                                            ),
                                                                            html.Tr(
                                                                                [
                                                                                    html.Td(
                                                                                        html.Code(
                                                                                            "end_date"
                                                                                        )
                                                                                    ),
                                                                                    html.Td(
                                                                                        "date"
                                                                                    ),
                                                                                    html.Td(
                                                                                        "No"
                                                                                    ),
                                                                                    html.Td(
                                                                                        "Intervention end date (optional; omit if ongoing)"
                                                                                    ),
                                                                                ]
                                                                            ),
                                                                        ]
                                                                    ),
                                                                ],
                                                                bordered=True,
                                                                hover=True,
                                                                size="sm",
                                                                className="mb-3",
                                                            ),
                                                            dcc.Upload(
                                                                id="upload-sites",
                                                                children=dbc.Button(
                                                                    "Drag & Drop or Click to Upload",
                                                                    color="secondary",
                                                                    outline=True,
                                                                    className="w-100",
                                                                ),
                                                                multiple=False,
                                                                accept=".geojson,.json,.gpkg,.zip,.tar.gz,.tgz",
                                                                className="mb-2",
                                                            ),
                                                            html.Div(
                                                                id="upload-status"
                                                            ),
                                                        ]
                                                    ),
                                                    dbc.CardFooter(
                                                        html.Small(
                                                            "After selecting or uploading a site set, continue to the Matching Setup tab.",
                                                            className="text-muted",
                                                        )
                                                    ),
                                                ],
                                                className="ae-section-card",
                                            ),
                                        ],
                                        className="pt-3",
                                    )
                                ],
                            ),
                            dbc.Tab(
                                label="2. Matching Setup",
                                tab_id="tab-submit-matching",
                                children=[
                                    html.Div(
                                        [
                                            dbc.Row(
                                                [
                                                    dbc.Col(
                                                        [
                                                            dbc.Label(
                                                                "Matching Covariates"
                                                            ),
                                                            dbc.Card(
                                                                [
                                                                    dbc.CardBody(
                                                                        [
                                                                            dbc.Row(
                                                                                [
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Select(
                                                                                                id="preset-selector",
                                                                                                placeholder="Load a saved preset…",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        md=5,
                                                                                    ),
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Button(
                                                                                                "Load",
                                                                                                id="load-preset-btn",
                                                                                                color="primary",
                                                                                                size="sm",
                                                                                                className="me-1",
                                                                                            ),
                                                                                            dbc.Button(
                                                                                                "Delete",
                                                                                                id="delete-preset-btn",
                                                                                                color="danger",
                                                                                                outline=True,
                                                                                                size="sm",
                                                                                            ),
                                                                                        ],
                                                                                        xs="auto",
                                                                                        md=3,
                                                                                        className="d-flex align-items-center",
                                                                                    ),
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.InputGroup(
                                                                                                [
                                                                                                    dbc.Input(
                                                                                                        id="preset-name-input",
                                                                                                        type="text",
                                                                                                        placeholder="Preset name",
                                                                                                        size="sm",
                                                                                                    ),
                                                                                                    dbc.Button(
                                                                                                        "Save",
                                                                                                        id="save-preset-btn",
                                                                                                        color="success",
                                                                                                        size="sm",
                                                                                                    ),
                                                                                                ],
                                                                                                size="sm",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        md=4,
                                                                                    ),
                                                                                ]
                                                                            ),
                                                                            html.Div(
                                                                                id="preset-feedback",
                                                                                className="mt-2 small",
                                                                            ),
                                                                        ],
                                                                        className="py-2 px-3",
                                                                    ),
                                                                ],
                                                                className="mb-3 ae-section-card",
                                                            ),
                                                            dbc.Card(
                                                                dbc.CardBody(
                                                                    dbc.Checklist(
                                                                        id="covariate-selection",
                                                                        options=[],
                                                                        value=[],
                                                                        inline=False,
                                                                        className="ms-2",
                                                                    ),
                                                                    className="ae-scroll-panel",
                                                                ),
                                                                className="ae-section-card",
                                                            ),
                                                        ],
                                                        xs=12,
                                                        lg=6,
                                                    ),
                                                    dbc.Col(
                                                        [
                                                            dbc.Card(
                                                                [
                                                                    dbc.CardHeader(
                                                                        "Exact Match Variables"
                                                                    ),
                                                                    dbc.CardBody(
                                                                        [
                                                                            html.Small(
                                                                                "At least one must be selected. Controls are drawn only from areas sharing these attributes with treatment sites.",
                                                                                className="text-muted d-block mb-2",
                                                                            ),
                                                                            dbc.Checklist(
                                                                                id="exact-match-selection",
                                                                                options=EXACT_MATCH_OPTIONS,
                                                                                value=DEFAULT_EXACT_MATCH,
                                                                                inline=False,
                                                                                className="ms-2",
                                                                            ),
                                                                        ]
                                                                    ),
                                                                ],
                                                                className="mb-3 ae-section-card",
                                                            ),
                                                            dbc.Card(
                                                                [
                                                                    dbc.CardHeader(
                                                                        "Other Matching Settings"
                                                                    ),
                                                                    dbc.CardBody(
                                                                        [
                                                                            dbc.Row(
                                                                                [
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Label(
                                                                                                "Max treatment pixels"
                                                                                            ),
                                                                                            dbc.Input(
                                                                                                id="max-treatment-pixels",
                                                                                                type="number",
                                                                                                min=1,
                                                                                                max=100000,
                                                                                                step=1,
                                                                                                value=1000,
                                                                                            ),
                                                                                            html.Small(
                                                                                                "Maximum treatment pixels sampled per group/site.",
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        sm=6,
                                                                                    ),
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Label(
                                                                                                "Control multiplier"
                                                                                            ),
                                                                                            dbc.Input(
                                                                                                id="control-multiplier",
                                                                                                type="number",
                                                                                                min=1,
                                                                                                max=500,
                                                                                                step=1,
                                                                                                value=50,
                                                                                            ),
                                                                                            html.Small(
                                                                                                "Maximum controls sampled per treatment pixel.",
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        sm=6,
                                                                                    ),
                                                                                ],
                                                                                className="g-3 mb-3",
                                                                            ),
                                                                            dbc.Row(
                                                                                [
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Label(
                                                                                                "Minimum site area (ha)"
                                                                                            ),
                                                                                            dbc.Input(
                                                                                                id="min-site-area-ha",
                                                                                                type="number",
                                                                                                min=0,
                                                                                                max=100000,
                                                                                                step=1,
                                                                                                value=100,
                                                                                            ),
                                                                                            html.Small(
                                                                                                "Sites smaller than this are filtered before extraction.",
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        sm=6,
                                                                                    ),
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Label(
                                                                                                "Min GLM treatment pixels"
                                                                                            ),
                                                                                            dbc.Input(
                                                                                                id="min-glm-treatment-pixels",
                                                                                                type="number",
                                                                                                min=1,
                                                                                                max=10000,
                                                                                                step=1,
                                                                                                value=15,
                                                                                            ),
                                                                                            html.Small(
                                                                                                "Below this, matching uses Mahalanobis distance instead of GLM.",
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        sm=6,
                                                                                    ),
                                                                                ],
                                                                                className="g-3",
                                                                            ),
                                                                            dbc.Row(
                                                                                [
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Label(
                                                                                                "Caliper width"
                                                                                            ),
                                                                                            dbc.Input(
                                                                                                id="caliper-width",
                                                                                                type="number",
                                                                                                min=0,
                                                                                                max=5.0,
                                                                                                step=0.05,
                                                                                                value=0.2,
                                                                                            ),
                                                                                            html.Small(
                                                                                                "Maximum distance (in SD) for a valid match. "
                                                                                                "Tighter values improve balance but reduce matched pairs. "
                                                                                                "Set to 0 to disable the caliper.",
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        sm=6,
                                                                                    ),
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Label(
                                                                                                "Max controls per treatment pixel"
                                                                                            ),
                                                                                            dcc.Dropdown(
                                                                                                id="max-controls-per-treatment",
                                                                                                options=[
                                                                                                    {
                                                                                                        "label": "1 (pair matching)",
                                                                                                        "value": 1,
                                                                                                    },
                                                                                                    {
                                                                                                        "label": "3",
                                                                                                        "value": 3,
                                                                                                    },
                                                                                                    {
                                                                                                        "label": "5",
                                                                                                        "value": 5,
                                                                                                    },
                                                                                                    {
                                                                                                        "label": "No limit (full matching)",
                                                                                                        "value": 0,
                                                                                                    },
                                                                                                ],
                                                                                                value=1,
                                                                                                clearable=False,
                                                                                            ),
                                                                                            html.Small(
                                                                                                "More controls per treatment reduces variance "
                                                                                                "but may increase bias. Controls are weighted "
                                                                                                "inversely by group size.",
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        sm=6,
                                                                                    ),
                                                                                ],
                                                                                className="g-3 mt-1",
                                                                            ),
                                                                            dbc.Row(
                                                                                [
                                                                                    dbc.Col(
                                                                                        [
                                                                                            html.Div(
                                                                                                [
                                                                                                    dbc.Label(
                                                                                                        "Random seed (optional)",
                                                                                                        className="me-2 mb-0",
                                                                                                    ),
                                                                                                    dbc.Button(
                                                                                                        "\u21bb",
                                                                                                        id="refresh-random-seed",
                                                                                                        size="sm",
                                                                                                        color="link",
                                                                                                        title="Generate new random seed",
                                                                                                        className="p-0 ms-1",
                                                                                                        style={
                                                                                                            "fontSize": "1.1rem",
                                                                                                            "lineHeight": "1",
                                                                                                        },
                                                                                                    ),
                                                                                                ],
                                                                                                className="d-flex align-items-center",
                                                                                            ),
                                                                                            dbc.Input(
                                                                                                id="random-seed",
                                                                                                type="number",
                                                                                                min=1,
                                                                                                max=2147483647,
                                                                                                step=1,
                                                                                                value=default_random_seed,
                                                                                            ),
                                                                                            html.Small(
                                                                                                "Prefilled with a random value. ",
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        sm=6,
                                                                                    ),
                                                                                ],
                                                                                className="g-3 mt-1",
                                                                            ),
                                                                            dbc.Row(
                                                                                [
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Label(
                                                                                                "Matching memory (GB)"
                                                                                            ),
                                                                                            dcc.Dropdown(
                                                                                                id="match-memory-gb",
                                                                                                options=[
                                                                                                    {
                                                                                                        "label": "30 GB (default)",
                                                                                                        "value": 30,
                                                                                                    },
                                                                                                    {
                                                                                                        "label": "60 GB",
                                                                                                        "value": 60,
                                                                                                    },
                                                                                                    {
                                                                                                        "label": "120 GB",
                                                                                                        "value": 120,
                                                                                                    },
                                                                                                    {
                                                                                                        "label": "240 GB",
                                                                                                        "value": 240,
                                                                                                    },
                                                                                                ],
                                                                                                value=30,
                                                                                                clearable=False,
                                                                                            ),
                                                                                            html.Small(
                                                                                                "Increase if matching jobs fail with exit code -9 (out of memory). "
                                                                                                "Costs increase if more memory is requested",
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        sm=6,
                                                                                    ),
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Label(
                                                                                                "Batch job queue"
                                                                                            ),
                                                                                            dcc.Dropdown(
                                                                                                id="matching-job-queue",
                                                                                                options=MATCHING_JOB_QUEUE_OPTIONS,
                                                                                                value=DEFAULT_MATCHING_JOB_QUEUE,
                                                                                                clearable=False,
                                                                                            ),
                                                                                            html.Small(
                                                                                                [
                                                                                                    "Use ",
                                                                                                    html.Code(
                                                                                                        "ondemand_fleet_1TB-io2-disk"
                                                                                                    ),
                                                                                                    " only when needed — it incurs much higher costs.",
                                                                                                ],
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        sm=6,
                                                                                    ),
                                                                                ],
                                                                                className="g-3 mt-1",
                                                                            ),
                                                                        ]
                                                                    ),
                                                                ],
                                                                className="ae-section-card",
                                                            ),
                                                        ],
                                                        xs=12,
                                                        lg=6,
                                                    ),
                                                ],
                                                className="g-3",
                                            ),
                                        ],
                                        className="pt-3",
                                    )
                                ],
                            ),
                            dbc.Tab(
                                label="3. Review & Submit",
                                tab_id="tab-submit-review",
                                children=[
                                    html.Div(
                                        [
                                            dbc.Card(
                                                [
                                                    dbc.CardBody(
                                                        [
                                                            html.H5(
                                                                "Ready to submit",
                                                                className="mb-2",
                                                            ),
                                                            html.P(
                                                                "Confirm your uploaded sites and matching settings in the previous tabs, then submit the task.",
                                                                className="text-muted mb-3",
                                                            ),
                                                            dcc.Loading(
                                                                dbc.Button(
                                                                    "Submit Task",
                                                                    id="submit-task-button",
                                                                    color="primary",
                                                                    size="lg",
                                                                    className="w-100",
                                                                ),
                                                                type="circle",
                                                            ),
                                                            html.Div(
                                                                id="submit-progress-message",
                                                                className="mt-2",
                                                            ),
                                                        ]
                                                    )
                                                ],
                                                className="ae-section-card",
                                            ),
                                        ],
                                        className="pt-3",
                                    )
                                ],
                            ),
                        ],
                        id="submit-tabs",
                        active_tab="tab-submit-sites",
                        className="ae-content-tabs",
                    ),
                ]
            ),
            # Hidden stores
            dcc.Store(id="parsed-sites-store"),
            dcc.Store(id="presets-store"),
            dcc.Store(id="site-set-refresh-store"),
            dcc.Store(id="submit-lock-store", data=False),
        ]
    )


def task_detail_layout(user, task_id, shared_token=None):
    """Task detail page with status, results, plots, and map.

    Parameters
    ----------
    user : User or None
        The logged-in user, or *None* when rendering a shared view.
    task_id : str
        UUID of the task to display.
    shared_token : str or None
        When set, the page is rendered in read-only shared mode: the
        share button is hidden and a banner is shown instead.
    """
    is_shared = shared_token is not None

    # -- Header row: title, badge, and (for authenticated users) share button -
    header_children = [
        html.Div(
            [
                html.H2(id="task-title", className="mb-1"),
                html.Span(id="task-status-badge", className="ms-2"),
            ],
            className="d-flex align-items-center",
        ),
        html.P(
            "Review progress, outputs, plots, and map layers for this analysis task.",
            className="text-muted mb-0",
        ),
    ]

    header_row = dbc.Row(
        [
            dbc.Col(header_children, width=True),
            # Edit / Share buttons — only shown for authenticated users
            *(
                [
                    dbc.Col(
                        html.Div(
                            [
                                dbc.Button(
                                    [html.I(className="bi bi-pencil me-1"), "Edit"],
                                    id="open-edit-modal",
                                    color="outline-secondary",
                                    size="sm",
                                    className="mt-1 me-2",
                                ),
                                dbc.Button(
                                    [
                                        html.I(className="bi bi-arrow-repeat me-1"),
                                        "Recompute",
                                    ],
                                    id="recompute-task-btn",
                                    color="outline-warning",
                                    size="sm",
                                    className="mt-1 me-2",
                                    title="Resubmit this task with a new random seed",
                                ),
                                dbc.Button(
                                    [
                                        html.I(className="bi bi-share me-1"),
                                        "Share",
                                    ],
                                    id="open-share-modal",
                                    color="outline-primary",
                                    size="sm",
                                    className="mt-1",
                                ),
                            ],
                            className="d-flex",
                        ),
                        width="auto",
                        className="d-flex align-items-start",
                    )
                ]
                if not is_shared
                else []
            ),
        ],
        className="mb-3",
    )

    # -- Shared-view banner ---------------------------------------------------
    shared_banner = (
        dbc.Alert(
            [
                html.I(className="bi bi-link-45deg me-2"),
                "You are viewing a shared link. Results are read-only.",
            ],
            color="info",
            className="mb-3 py-2",
            dismissable=False,
        )
        if is_shared
        else html.Div()
    )

    # -- Edit modal (only in authenticated mode) ------------------------------
    edit_modal = (
        dbc.Modal(
            [
                dbc.ModalHeader(dbc.ModalTitle("Edit Task")),
                dbc.ModalBody(
                    [
                        dbc.Label("Name"),
                        dbc.Input(
                            id="edit-task-name",
                            type="text",
                            maxLength=255,
                            className="mb-3",
                        ),
                        dbc.Label("Description"),
                        dbc.Textarea(
                            id="edit-task-description",
                            className="mb-3",
                            style={"height": "100px"},
                        ),
                        html.Div(id="edit-task-result"),
                    ]
                ),
                dbc.ModalFooter(
                    [
                        dbc.Button(
                            "Cancel",
                            id="cancel-edit-task",
                            color="secondary",
                            className="me-2",
                        ),
                        dbc.Button(
                            "Save",
                            id="save-edit-task",
                            color="primary",
                        ),
                    ]
                ),
            ],
            id="edit-task-modal",
            is_open=False,
        )
        if not is_shared
        else html.Div()
    )

    # -- Share modal (only in authenticated mode) -----------------------------
    share_modal = (
        dbc.Modal(
            [
                dbc.ModalHeader(dbc.ModalTitle("Share Task Results")),
                dbc.ModalBody(
                    [
                        html.P(
                            "Generate a link that allows anyone to view this "
                            "task's results, plots, and downloads without "
                            "logging in.",
                            className="text-muted",
                        ),
                        dbc.Label("Link expires after"),
                        dbc.Select(
                            id="share-expiry-days",
                            options=[
                                {"label": "1 day", "value": "1"},
                                {"label": "7 days", "value": "7"},
                                {"label": "30 days", "value": "30"},
                                {"label": "90 days", "value": "90"},
                            ],
                            value="7",
                            className="mb-3",
                        ),
                        dbc.Button(
                            "Generate Link",
                            id="generate-share-link",
                            color="primary",
                            className="mb-3",
                        ),
                        html.Div(id="share-link-result"),
                        html.Hr(),
                        html.H6("Active Share Links"),
                        html.Div(id="share-links-list"),
                    ]
                ),
            ],
            id="share-modal",
            is_open=False,
            size="lg",
        )
        if not is_shared
        else html.Div()
    )

    # -- Tab pane (shared by both modes) --------------------------------------
    tabs = dbc.Tabs(
        [
            dbc.Tab(
                label="Overview",
                tab_id="tab-overview",
                children=[
                    html.Div(
                        dbc.Card(
                            dbc.CardBody(html.Div(id="task-overview")),
                            className="ae-section-card",
                        ),
                        className="p-3",
                    ),
                ],
            ),
            dbc.Tab(
                label="Results Tables",
                tab_id="tab-results",
                children=[
                    html.Div(
                        dbc.Card(
                            dbc.CardBody(html.Div(id="task-results-content")),
                            className="ae-section-card",
                        ),
                        className="p-3",
                    ),
                ],
            ),
            dbc.Tab(
                label="Results Plots",
                tab_id="tab-plots",
                children=[
                    html.Div(
                        dbc.Card(
                            dbc.CardBody(html.Div(id="task-plots")),
                            className="ae-section-card",
                        ),
                        className="p-3",
                    ),
                ],
            ),
            dbc.Tab(
                label="Match Quality",
                tab_id="tab-match-quality",
                children=[
                    html.Div(
                        dbc.Card(
                            dbc.CardBody(html.Div(id="task-match-quality")),
                            className="ae-section-card",
                        ),
                        className="p-3",
                    ),
                ],
            ),
            dbc.Tab(
                label="Map",
                tab_id="tab-map",
                children=[
                    html.Div(
                        dbc.Card(
                            dbc.CardBody(
                                html.Div(id="task-map"),
                                className="p-0",
                            ),
                            className="ae-section-card ae-map-card",
                        ),
                        className="p-3",
                    ),
                ],
            ),
        ],
        id="detail-tabs",
        active_tab="tab-overview",
        className="ae-content-tabs",
    )

    return dbc.Container(
        [
            navbar(user, active_page="/"),
            shared_banner,
            header_row,
            html.Div(id="recompute-result"),
            html.Div(id="quality-warning-banner"),
            tabs,
            edit_modal,
            share_modal,
            dcc.Store(id="task-id-store", data=task_id),
            dcc.Store(id="share-token-store", data=shared_token),
            dcc.Interval(id="detail-refresh-interval", interval=15000, n_intervals=0),
        ]
    )


def _build_category_options():
    """Build dropdown options with variable names per category from config."""
    gee_config_path = os.path.join(os.path.dirname(__file__), "gee-export", "config.py")
    spec = importlib.util.spec_from_file_location("gee_export_config", gee_config_path)
    gee_config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gee_config)
    covariates = gee_config.COVARIATES

    # Group variable names by category
    cats = {}
    for name, cfg in covariates.items():
        cat = cfg.get("category", "other")
        cats.setdefault(cat, []).append(name)

    # Pretty labels for categories
    cat_labels = {
        "climate": "Climate",
        "terrain": "Terrain",
        "accessibility": "Accessibility",
        "demographics": "Demographics",
        "biomass": "Biomass",
        "land_cover": "Land Cover",
        "forest_cover": "Forest Cover",
        "ecological": "Ecological",
        "administrative": "Administrative",
    }

    # Build "All" option with total count
    total = sum(len(v) for v in cats.values())
    options = [{"label": f"All ({total} layers)", "value": "all"}]

    # Build per-category options in display order
    for cat_key, cat_label in cat_labels.items():
        names = cats.get(cat_key, [])
        if not names:
            continue
        # Abbreviate forest_cover list (24 layers)
        if len(names) > 6:
            shown = ", ".join(names[:3]) + f", ... +{len(names) - 3} more"
        else:
            shown = ", ".join(names)
        options.append(
            {
                "label": f"{cat_label} ({shown})",
                "value": cat_key,
            }
        )

    return options


def admin_layout(user):
    """Admin panel for covariate management and users."""
    category_options = _build_category_options()

    return dbc.Container(
        [
            navbar(user, active_page="/admin"),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H2("Admin Panel", className="mb-1"),
                            html.P(
                                "Manage covariate inventory and user accounts.",
                                className="text-muted mb-0",
                            ),
                        ],
                        width=True,
                    )
                ],
                className="mb-3",
            ),
            dbc.Tabs(
                [
                    dbc.Tab(
                        label="Covariates",
                        tab_id="tab-covariates",
                        children=[
                            html.Div(
                                [
                                    dbc.Card(
                                        [
                                            dbc.CardHeader(
                                                "Export Covariate Layers from GEE"
                                            ),
                                            dbc.CardBody(
                                                [
                                                    dbc.Row(
                                                        [
                                                            dbc.Col(
                                                                [
                                                                    dbc.Label(
                                                                        "Category"
                                                                    ),
                                                                    dbc.Select(
                                                                        id="gee-export-category",
                                                                        options=category_options,
                                                                        value="all",
                                                                    ),
                                                                ],
                                                                xs=12,
                                                                sm=6,
                                                            ),
                                                            dbc.Col(
                                                                [
                                                                    html.Div(
                                                                        style={
                                                                            "height": "32px"
                                                                        }
                                                                    ),
                                                                    dbc.Button(
                                                                        "Start Export",
                                                                        id="start-gee-export",
                                                                        color="warning",
                                                                    ),
                                                                ],
                                                                width="auto",
                                                                className="d-flex align-items-end",
                                                            ),
                                                        ]
                                                    ),
                                                    html.Div(
                                                        id="gee-export-result",
                                                        className="mt-2",
                                                    ),
                                                ]
                                            ),
                                        ],
                                        className="ae-section-card mb-3",
                                    ),
                                    dbc.Card(
                                        [
                                            dbc.CardBody(
                                                [
                                                    dbc.Row(
                                                        [
                                                            dbc.Col(
                                                                html.H5(
                                                                    "Covariate Inventory",
                                                                    className="mb-0",
                                                                ),
                                                                width="auto",
                                                            ),
                                                            dbc.Col(
                                                                html.Span(
                                                                    id="covariates-total-count",
                                                                    children="Total: 0",
                                                                    className="text-muted fw-bold",
                                                                ),
                                                                width=True,
                                                                className="text-end",
                                                            ),
                                                        ],
                                                        className="ae-action-bar align-items-center mb-3",
                                                    ),
                                                    _make_ag_grid(
                                                        table_id="covariates-table",
                                                        column_defs=COVARIATE_COLUMNS,
                                                        row_model="clientSide",
                                                        height="500px",
                                                        style_conditions=COVARIATE_STATUS_ROW_STYLES,
                                                        grid_options_extra={
                                                            "rowSelection": "multiple",
                                                            "suppressRowClickSelection": True,
                                                            "isRowSelectable": {
                                                                "function": (
                                                                    "!!params.data"
                                                                    " && params.data.gcs_tiles > 0"
                                                                    " && params.data.status !== 'merging'"
                                                                    " && params.data.status !== 'pending_merge'"
                                                                    " && params.data.status !== 'exporting'"
                                                                    " && params.data.status !== 'pending_export'"
                                                                )
                                                            },
                                                        },
                                                    ),
                                                    html.Div(
                                                        id="covariate-action-result",
                                                        className="mt-2",
                                                    ),
                                                ]
                                            )
                                        ],
                                        className="ae-section-card",
                                    ),
                                ],
                                className="pt-3",
                            ),
                        ],
                    ),
                    dbc.Tab(
                        label="Users",
                        tab_id="tab-users",
                        children=[
                            html.Div(
                                [
                                    dbc.Card(
                                        [
                                            dbc.CardHeader("User Actions"),
                                            dbc.CardBody(
                                                [
                                                    html.P(
                                                        "Select a user from the table below, then use these actions.",
                                                        className="text-muted small mb-3",
                                                    ),
                                                    dbc.Row(
                                                        [
                                                            dbc.Col(
                                                                [
                                                                    dbc.Label(
                                                                        "Selected User",
                                                                        size="sm",
                                                                    ),
                                                                    dbc.Select(
                                                                        id="admin-user-select",
                                                                        options=[],
                                                                        placeholder="Select a user...",
                                                                    ),
                                                                ],
                                                                xs=12,
                                                                md=4,
                                                            ),
                                                            dbc.Col(
                                                                [
                                                                    dbc.Label(
                                                                        "Change Role",
                                                                        size="sm",
                                                                    ),
                                                                    dbc.Select(
                                                                        id="admin-role-select",
                                                                        options=[
                                                                            {
                                                                                "label": "User",
                                                                                "value": "user",
                                                                            },
                                                                            {
                                                                                "label": "Admin",
                                                                                "value": "admin",
                                                                            },
                                                                        ],
                                                                        value="user",
                                                                    ),
                                                                ],
                                                                xs=6,
                                                                md=2,
                                                            ),
                                                            dbc.Col(
                                                                [
                                                                    html.Div(
                                                                        style={
                                                                            "height": "32px"
                                                                        }
                                                                    ),
                                                                    dbc.ButtonGroup(
                                                                        [
                                                                            dbc.Button(
                                                                                "Approve",
                                                                                id="admin-approve-btn",
                                                                                color="success",
                                                                                size="sm",
                                                                            ),
                                                                            dbc.Button(
                                                                                "Change Role",
                                                                                id="admin-role-btn",
                                                                                color="info",
                                                                                size="sm",
                                                                            ),
                                                                            dbc.Button(
                                                                                "Delete",
                                                                                id="admin-delete-btn",
                                                                                color="danger",
                                                                                size="sm",
                                                                            ),
                                                                        ]
                                                                    ),
                                                                ],
                                                                width="auto",
                                                                className="d-flex align-items-end",
                                                            ),
                                                        ]
                                                    ),
                                                    html.Div(
                                                        id="admin-user-action-result",
                                                        className="mt-2",
                                                    ),
                                                    dbc.Modal(
                                                        [
                                                            dbc.ModalHeader(
                                                                dbc.ModalTitle(
                                                                    "Confirm Delete User"
                                                                )
                                                            ),
                                                            dbc.ModalBody(
                                                                "Are you sure you want to delete this user and all their analysis tasks? This cannot be undone."
                                                            ),
                                                            dbc.ModalFooter(
                                                                [
                                                                    dbc.Button(
                                                                        "Cancel",
                                                                        id="admin-delete-cancel",
                                                                        color="secondary",
                                                                        className="me-2",
                                                                    ),
                                                                    dbc.Button(
                                                                        "Delete User",
                                                                        id="admin-delete-confirm",
                                                                        color="danger",
                                                                    ),
                                                                ]
                                                            ),
                                                        ],
                                                        id="admin-delete-modal",
                                                        is_open=False,
                                                        centered=True,
                                                    ),
                                                ]
                                            ),
                                        ],
                                        className="ae-section-card mb-3",
                                    ),
                                    dbc.Card(
                                        [
                                            dbc.CardBody(
                                                [
                                                    dbc.Row(
                                                        [
                                                            dbc.Col(
                                                                html.H5(
                                                                    "User Management",
                                                                    className="mb-0",
                                                                ),
                                                                width="auto",
                                                            ),
                                                            dbc.Col(
                                                                html.Span(
                                                                    id="user-management-total-count",
                                                                    children="Total: 0",
                                                                    className="text-muted fw-bold",
                                                                ),
                                                                width=True,
                                                                className="text-end",
                                                            ),
                                                        ],
                                                        className="ae-action-bar align-items-center mb-3",
                                                    ),
                                                    _make_ag_grid(
                                                        table_id="user-management-table",
                                                        column_defs=USER_MANAGEMENT_COLUMNS,
                                                        row_model="clientSide",
                                                        height="500px",
                                                    ),
                                                ]
                                            )
                                        ],
                                        className="ae-section-card",
                                    ),
                                ],
                                className="pt-3",
                            ),
                        ],
                    ),
                ],
                id="admin-tabs",
                active_tab="tab-covariates",
                className="ae-content-tabs",
            ),
            dcc.Interval(id="admin-refresh-interval", interval=30000, n_intervals=0),
        ]
    )


def settings_layout(user):
    """User profile page with account and trends.earth API management."""
    from credential_store import get_credential

    cred = get_credential(user.id)

    if cred:
        # Show current credential status
        credential_card = dbc.Card(
            [
                dbc.CardHeader(
                    html.H5("Linked Account", className="mb-0"),
                    style={"backgroundColor": "#d1e7dd"},
                ),
                dbc.CardBody(
                    [
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        html.P(
                                            [
                                                html.Strong("trends.earth email: "),
                                                cred.te_email,
                                            ]
                                        ),
                                        html.P(
                                            [
                                                html.Strong("Client ID: "),
                                                html.Code(cred.client_id),
                                            ]
                                        ),
                                        html.P(
                                            [
                                                html.Strong("Linked: "),
                                                html.Span(
                                                    cred.created_at.strftime(
                                                        "%Y-%m-%dT%H:%M:%SZ"
                                                    )
                                                    if cred.created_at
                                                    else "—",
                                                    className="utc-datetime"
                                                    if cred.created_at
                                                    else "",
                                                    **(
                                                        {
                                                            "data-utc": cred.created_at.strftime(
                                                                "%Y-%m-%dT%H:%M:%SZ"
                                                            )
                                                        }
                                                        if cred.created_at
                                                        else {}
                                                    ),
                                                ),
                                            ]
                                        ),
                                    ]
                                ),
                            ]
                        ),
                        html.Hr(),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        dbc.Button(
                                            "Test Connection",
                                            id="te-test-connection-btn",
                                            color="info",
                                            outline=True,
                                            className="me-2",
                                        ),
                                        dbc.Button(
                                            "Unlink Account",
                                            id="te-unlink-btn",
                                            color="danger",
                                            outline=True,
                                        ),
                                    ]
                                ),
                            ]
                        ),
                        html.Div(id="te-credential-status", className="mt-3"),
                    ]
                ),
            ],
            className="mb-4 shadow-sm",
        )
    else:
        credential_card = None

    link_card = dbc.Card(
        [
            dbc.CardHeader(
                html.H5(
                    "Link to trends.earth" if not cred else "Re-link Account",
                    className="mb-0",
                ),
                style={"backgroundColor": "#2c3e50", "color": "white"},
            ),
            dbc.CardBody(
                [
                    html.P(
                        "Enter your trends.earth account credentials to register "
                        "this application as an authorized client.",
                        className="text-muted",
                    ),
                    dbc.Label("trends.earth Email"),
                    dbc.Input(
                        id="te-link-email",
                        type="email",
                        placeholder="you@example.com",
                        className="mb-2",
                    ),
                    dbc.Label("trends.earth Password"),
                    dbc.Input(
                        id="te-link-password",
                        type="password",
                        className="mb-3",
                    ),
                ]
                + (
                    [
                        dbc.Alert(
                            [
                                "Don't have a trends.earth account yet? ",
                                html.A(
                                    "Register at api.trends.earth",
                                    href="https://api.trends.earth",
                                    target="_blank",
                                    rel="noopener noreferrer",
                                    className="alert-link",
                                ),
                                ".",
                            ],
                            color="info",
                            className="mb-3",
                        ),
                    ]
                    if not cred
                    else []
                )
                + [
                    html.Div(id="te-link-message", className="mb-2"),
                    dbc.Button(
                        "Link Account",
                        id="te-link-btn",
                        color="primary",
                        className="w-100",
                    ),
                ]
            ),
        ],
        className="mb-4 shadow-sm",
    )

    children = [
        navbar(user, active_page="/settings"),
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H2("Profile", className="mb-1"),
                        html.P(
                            "Manage your trends.earth connection and account settings.",
                            className="text-muted mb-0",
                        ),
                    ],
                    width=True,
                )
            ],
            className="mb-3",
        ),
    ]

    # -- trends.earth API Integration card (groups linked account + link form) --
    te_integration_contents = [
        html.P(
            "Link your trends.earth account to submit analysis tasks "
            "through the trends.earth API.",
            className="text-muted",
        ),
    ]
    if credential_card:
        te_integration_contents.append(credential_card)
    te_integration_contents.append(link_card)

    te_integration_card = dbc.Card(
        [
            dbc.CardHeader(
                html.H5("trends.earth API Integration", className="mb-0"),
            ),
            dbc.CardBody(te_integration_contents),
        ],
        className="mb-4 shadow-sm",
    )
    children.append(dbc.Row(dbc.Col(te_integration_card, xs=12, lg=8)))

    # -- Change Password card -----------------------------------------------
    req_item_style = {"fontSize": "0.85rem", "lineHeight": "1.6"}
    change_pw_card = dbc.Card(
        [
            dbc.CardHeader(
                html.H5("Change Password", className="mb-0"),
            ),
            dbc.CardBody(
                [
                    dbc.Label("Current Password"),
                    dbc.Input(
                        id="change-pw-current",
                        type="password",
                        className="mb-2",
                    ),
                    dbc.Label("New Password"),
                    dbc.Input(
                        id="change-pw-new",
                        type="password",
                        className="mb-1",
                        debounce=False,
                    ),
                    dbc.Label("Confirm New Password"),
                    dbc.Input(
                        id="change-pw-confirm",
                        type="password",
                        className="mb-2",
                        debounce=False,
                    ),
                    # Real-time password requirements checklist
                    html.Div(
                        [
                            html.Small(
                                "Password requirements:",
                                className="fw-bold",
                            ),
                            html.Ul(
                                [
                                    html.Li(
                                        "At least 12 characters",
                                        id="cp-req-length",
                                        className="text-muted",
                                        style=req_item_style,
                                    ),
                                    html.Li(
                                        "One uppercase letter",
                                        id="cp-req-uppercase",
                                        className="text-muted",
                                        style=req_item_style,
                                    ),
                                    html.Li(
                                        "One lowercase letter",
                                        id="cp-req-lowercase",
                                        className="text-muted",
                                        style=req_item_style,
                                    ),
                                    html.Li(
                                        "One number",
                                        id="cp-req-number",
                                        className="text-muted",
                                        style=req_item_style,
                                    ),
                                    html.Li(
                                        "One special character",
                                        id="cp-req-special",
                                        className="text-muted",
                                        style=req_item_style,
                                    ),
                                    html.Li(
                                        "Passwords match",
                                        id="cp-req-match",
                                        className="text-muted",
                                        style=req_item_style,
                                    ),
                                ],
                                className="mb-2",
                                style={
                                    "listStyleType": "none",
                                    "paddingLeft": "0.5rem",
                                },
                            ),
                        ],
                        className="mb-2",
                    ),
                    html.Div(id="change-pw-message", className="mb-2"),
                    dbc.Button(
                        "Change Password",
                        id="change-pw-btn",
                        color="primary",
                        className="w-100",
                    ),
                ]
            ),
        ],
        className="mb-4 shadow-sm",
    )
    children.append(dbc.Row(dbc.Col(change_pw_card, xs=12, lg=8)))

    children.append(
        dbc.Row(
            dbc.Col(
                [
                    dbc.Card(
                        [
                            dbc.CardHeader(
                                html.H5("Delete account", className="mb-0"),
                            ),
                            dbc.CardBody(
                                [
                                    html.P(
                                        "Delete your account and all associated analysis tasks. This action cannot be undone. This does not affect your trends.earth account.",
                                        className="text-muted",
                                    ),
                                    dbc.Button(
                                        "Delete My Account",
                                        id="self-delete-btn",
                                        color="danger",
                                        outline=True,
                                        size="sm",
                                    ),
                                    dbc.Modal(
                                        [
                                            dbc.ModalHeader(
                                                dbc.ModalTitle("Delete Account")
                                            ),
                                            dbc.ModalBody(
                                                [
                                                    html.P(
                                                        "Are you sure you want to delete your account? "
                                                        "This will permanently remove your account and all "
                                                        "associated analysis tasks. This action cannot be undone.",
                                                        className="text-danger",
                                                    ),
                                                ]
                                            ),
                                            dbc.ModalFooter(
                                                [
                                                    dbc.Button(
                                                        "Cancel",
                                                        id="self-delete-cancel",
                                                        color="secondary",
                                                        className="me-2",
                                                    ),
                                                    dbc.Button(
                                                        "Delete My Account",
                                                        id="self-delete-confirm",
                                                        color="danger",
                                                    ),
                                                ]
                                            ),
                                        ],
                                        id="self-delete-modal",
                                        is_open=False,
                                        centered=True,
                                    ),
                                    html.Div(id="self-delete-result", className="mt-2"),
                                ]
                            ),
                        ],
                        className="mb-4 shadow-sm ae-section-card",
                    )
                ],
                xs=12,
                lg=8,
            )
        )
    )

    # Hidden stores for callback coordination
    children.append(dcc.Store(id="te-link-done-store"))

    return dbc.Container(children)


def not_found_layout(user=None):
    """404 page."""
    return dbc.Container(
        [
            navbar(user, active_page=None),
            dbc.Row(
                dbc.Col(
                    [
                        html.H2("Page Not Found"),
                        html.P("The requested page does not exist."),
                        dbc.Button("Go to Dashboard", href="/", color="primary"),
                    ],
                    className="text-center mt-5",
                )
            ),
        ]
    )
