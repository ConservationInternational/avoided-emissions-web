"""Dash application layout definitions.

Defines the page layouts for login, dashboard, task submission, task detail,
admin panel, and navigation components. Uses AG Grid for sortable/filterable
tables following the same patterns as the trends.earth-api-ui.
"""

import importlib.util
import os

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
    "dist_roads",
    "crop_suitability",
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
]

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
        "tooltipField": "created_at",
    },
    {
        "headerName": "Submitted",
        "field": "submitted_at",
        "flex": 1.5,
        "minWidth": 160,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
        "tooltipField": "submitted_at",
    },
    {
        "headerName": "Completed",
        "field": "completed_at",
        "flex": 1.5,
        "minWidth": 160,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
        "tooltipField": "completed_at",
    },
]

GEE_EXPORT_COLUMNS = [
    {
        "headerName": "Covariate",
        "field": "covariate_name",
        "flex": 2,
        "minWidth": 180,
        "pinned": "left",
        "cellStyle": {**TRUNCATED_CELL},
        "tooltipField": "covariate_name",
    },
    {
        "headerName": "Category",
        "field": "category",
        "flex": 1,
        "minWidth": 120,
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
        "headerName": "Status",
        "field": "status",
        "flex": 1,
        "minWidth": 110,
        "cellRenderer": "StatusBadge",
        "filter": "agTextColumnFilter",
    },
    {
        "headerName": "Started",
        "field": "started_at",
        "flex": 1.5,
        "minWidth": 160,
        "sort": "desc",
        "sortIndex": 0,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
    },
    {
        "headerName": "Completed",
        "field": "completed_at",
        "flex": 1.5,
        "minWidth": 160,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
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
        "headerName": "Tiles",
        "field": "tile_urls",
        "flex": 2,
        "minWidth": 200,
        "cellRenderer": "TileLinks",
        "autoHeight": True,
        "cellStyle": {"whiteSpace": "normal", "lineHeight": "1.4"},
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
        "headerName": "Created",
        "field": "created_at",
        "flex": 1.5,
        "minWidth": 160,
        "sort": "desc",
        "sortIndex": 0,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
    },
    {
        "headerName": "Last Login",
        "field": "last_login",
        "flex": 1.5,
        "minWidth": 160,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
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
    "cacheBlockSize": 50,
    "maxBlocksInCache": 3,
    "purgeClosedRowNodes": True,
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

GEE_STATUS_ROW_STYLES = [
    {
        "condition": "params.data.status === 'failed'",
        "style": {"backgroundColor": "#F8D7DA", "color": "#721C24"},
    },
    {
        "condition": "params.data.status === 'completed'",
        "style": {"backgroundColor": "#D1E7DD", "color": "#0F5132"},
    },
    {
        "condition": "params.data.status === 'running'",
        "style": {"backgroundColor": "#CCE5FF", "color": "#084298"},
    },
    {
        "condition": "params.data.status === 'pending'",
        "style": {"backgroundColor": "#E2E3E5", "color": "#495057"},
    },
]


def _make_ag_grid(table_id, column_defs, *, row_model="clientSide",
                  height="600px", style_conditions=None,
                  grid_options_extra=None, row_data=None):
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

def navbar(user=None):
    """Top navigation bar."""
    nav_items = [
        dbc.NavItem(dbc.NavLink("Dashboard", href="/")),
        dbc.NavItem(dbc.NavLink("Submit Task", href="/submit")),
    ]
    if user and user.is_admin:
        nav_items.append(
            dbc.NavItem(dbc.NavLink("Admin", href="/admin"))
        )

    right_items = []
    if user:
        right_items = [
            dbc.NavItem(
                dbc.NavLink(user.name, disabled=True, className="text-light")
            ),
            dbc.NavItem(dbc.NavLink("Logout", href="/logout")),
        ]
    else:
        right_items = [dbc.NavItem(dbc.NavLink("Login", href="/login"))]

    return dbc.Navbar(
        dbc.Container([
            dbc.NavbarBrand("Avoided Emissions", href="/",
                            className="fw-bold"),
            dbc.Nav(nav_items, className="me-auto", navbar=True),
            dbc.Nav(right_items, navbar=True),
        ]),
        color="dark",
        dark=True,
        className="mb-4",
    )


# -- Page layouts ------------------------------------------------------------

def login_layout():
    """Login page layout."""
    return dbc.Container([
        navbar(),
        dbc.Row(dbc.Col([
            dbc.Card([
                dbc.CardHeader(
                    html.Div([
                        html.H4("Avoided Emissions",
                                className="text-center mb-1",
                                style={"color": "white"}),
                        html.H6("Login",
                                className="text-center",
                                style={"color": "#ffffffcc"}),
                    ]),
                    style={"backgroundColor": "#2c3e50"},
                ),
                dbc.CardBody([
                    dbc.Label("Email"),
                    dbc.Input(id="login-email", type="email",
                              placeholder="user@example.com",
                              className="mb-2"),
                    dbc.Label("Password"),
                    dbc.Input(id="login-password", type="password",
                              className="mb-3"),
                    html.Div(id="login-error", className="text-danger mb-2"),
                    dbc.Button("Login", id="login-button", color="primary",
                               className="w-100"),
                ]),
            ], className="mt-5 shadow-sm"),
        ], width={"size": 4, "offset": 4})),
    ])


def dashboard_layout(user):
    """Main dashboard showing task list with AG Grid and status overview."""
    return dbc.Container([
        navbar(user),
        dbc.Row([
            dbc.Col(html.H2("Analysis Tasks"), width="auto"),
            dbc.Col(
                html.Div([
                    html.Span(id="task-total-count", children="Total: 0",
                              className="text-muted fw-bold me-3"),
                    dbc.Button("Refresh", id="refresh-tasks-btn",
                               color="primary", size="sm",
                               className="me-2"),
                    dbc.Button("New Task", href="/submit", color="success",
                               size="sm"),
                ], className="d-flex align-items-center justify-content-end"),
                width=True,
            ),
        ], className="align-items-center mb-3"),
        html.Hr(className="mt-0"),
        _make_ag_grid(
            table_id="task-list-table",
            column_defs=TASK_LIST_COLUMNS,
            row_model="clientSide",
            height="700px",
            style_conditions=TASK_STATUS_ROW_STYLES,
        ),
        # Stores & intervals
        dcc.Store(id="task-list-store"),
        dcc.Interval(id="refresh-interval", interval=30000, n_intervals=0),
    ])


def submit_layout(user):
    """Task submission form with file upload and covariate selection."""
    return dbc.Container([
        navbar(user),
        html.H2("Submit Analysis Task"),
        html.Hr(),
        dbc.Form([
            dbc.Row([
                dbc.Col([
                    dbc.Label("Task Name"),
                    dbc.Input(id="task-name", type="text",
                              placeholder="My analysis"),
                ], width=6),
                dbc.Col([
                    dbc.Label("Description (optional)"),
                    dbc.Input(id="task-description", type="text",
                              placeholder="Brief description"),
                ], width=6),
            ], className="mb-3"),

            dbc.Row([
                dbc.Col([
                    dbc.Label("Upload Sites (GeoJSON or GeoPackage)"),
                    dbc.Card([
                        dbc.CardBody([
                            html.P([
                                "Upload a ",
                                html.Strong("GeoJSON"),
                                " or ",
                                html.Strong("GeoPackage"),
                                " file containing site polygons. "
                                "Geometries must be valid Polygons or "
                                "MultiPolygons in EPSG:4326 (WGS 84).",
                            ], className="mb-2 small"),
                            dbc.Table([
                                html.Thead(html.Tr([
                                    html.Th("Field"),
                                    html.Th("Type"),
                                    html.Th("Required"),
                                    html.Th("Description"),
                                ])),
                                html.Tbody([
                                    html.Tr([
                                        html.Td(html.Code("site_id")),
                                        html.Td("string"),
                                        html.Td("Yes"),
                                        html.Td("Unique site identifier"),
                                    ]),
                                    html.Tr([
                                        html.Td(html.Code("site_name")),
                                        html.Td("string"),
                                        html.Td("Yes"),
                                        html.Td("Human-readable site name"),
                                    ]),
                                    html.Tr([
                                        html.Td(html.Code("start_date")),
                                        html.Td("date"),
                                        html.Td("Yes"),
                                        html.Td("Intervention start date "
                                                 "(YYYY-MM-DD)"),
                                    ]),
                                    html.Tr([
                                        html.Td(html.Code("end_date")),
                                        html.Td("date"),
                                        html.Td("No"),
                                        html.Td("Intervention end date "
                                                 "(optional; omit if ongoing)"),
                                    ]),
                                ]),
                            ], bordered=True, hover=True, size="sm",
                               className="mb-0"),
                        ]),
                    ], color="light", className="mb-2"),
                    dcc.Upload(
                        id="upload-sites",
                        children=dbc.Button(
                            "Drag & Drop or Click to Upload",
                            color="secondary", outline=True,
                            className="w-100",
                        ),
                        multiple=False,
                        accept=".geojson,.json,.gpkg",
                        className="mb-2",
                    ),
                    html.Div(id="upload-status"),
                ], width=12),
            ], className="mb-3"),

            dbc.Row([
                dbc.Col([
                    dbc.Label("Matching Covariates"),
                    dbc.Checklist(
                        id="covariate-selection",
                        options=[{"label": c, "value": c}
                                 for c in ALL_COVARIATES],
                        value=DEFAULT_COVARIATES,
                        inline=False,
                        className="ms-2",
                    ),
                ], width=6),
                dbc.Col([
                    dbc.Label("Forest Cover Years"),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("Start Year", size="sm"),
                            dbc.Input(id="fc-start-year", type="number",
                                      value=2000, min=2000, max=2023),
                        ], width=6),
                        dbc.Col([
                            dbc.Label("End Year", size="sm"),
                            dbc.Input(id="fc-end-year", type="number",
                                      value=2023, min=2000, max=2023),
                        ], width=6),
                    ]),
                    html.Hr(),
                    dbc.Label("Site Preview"),
                    html.Div(id="site-preview"),
                ], width=6),
            ], className="mb-3"),

            html.Div(id="submit-errors", className="text-danger mb-2"),
            dbc.Button("Submit Task", id="submit-task-button",
                       color="primary", size="lg", className="w-100"),
            html.Div(id="submit-result"),
        ]),
        # Hidden store for parsed sites data
        dcc.Store(id="parsed-sites-store"),
    ])


def task_detail_layout(user, task_id):
    """Task detail page with status, results, plots, and map."""
    return dbc.Container([
        navbar(user),
        html.Div([
            html.H2(id="task-title", className="d-inline"),
            html.Span(id="task-status-badge", className="ms-2"),
        ], className="mb-3"),
        html.Hr(),

        dbc.Tabs([
            dbc.Tab(label="Overview", tab_id="tab-overview", children=[
                html.Div(id="task-overview", className="mt-3"),
            ]),
            dbc.Tab(label="Results", tab_id="tab-results", children=[
                html.Div(id="task-results-content", className="mt-3"),
            ]),
            dbc.Tab(label="Plots", tab_id="tab-plots", children=[
                html.Div(id="task-plots", className="mt-3"),
            ]),
            dbc.Tab(label="Map", tab_id="tab-map", children=[
                html.Div(id="task-map", className="mt-3",
                         style={"height": "500px"}),
            ]),
        ], id="detail-tabs", active_tab="tab-overview"),

        dcc.Store(id="task-id-store", data=task_id),
        dcc.Interval(id="detail-refresh-interval", interval=15000,
                     n_intervals=0),
    ])


def _build_category_options():
    """Build dropdown options with variable names per category from config."""
    gee_config_path = os.path.join(
        os.path.dirname(__file__), "gee-export", "config.py"
    )
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
        options.append({
            "label": f"{cat_label} ({shown})",
            "value": cat_key,
        })

    return options


def admin_layout(user):
    """Admin panel for GEE export management and users."""
    category_options = _build_category_options()

    return dbc.Container([
        navbar(user),
        html.H2("Admin Panel"),
        html.Hr(),

        dbc.Tabs([
            dbc.Tab(label="GEE Exports", tab_id="tab-gee", children=[
                dbc.Card([
                    dbc.CardHeader("Export Covariate Layers from GEE"),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                dbc.Label("Category"),
                                dbc.Select(
                                    id="gee-export-category",
                                    options=category_options,
                                    value="all",
                                ),
                            ], width=6),
                            dbc.Col([
                                html.Div(style={"height": "32px"}),
                                dbc.Button("Start Export",
                                           id="start-gee-export",
                                           color="warning"),
                            ], width="auto", className="d-flex align-items-end"),
                        ]),
                        html.Div(id="gee-export-result", className="mt-2"),
                    ]),
                ], className="mt-3 mb-4"),

                dbc.Row([
                    dbc.Col(html.H5("Export History"), width="auto"),
                    dbc.Col(
                        html.Span(id="gee-export-total-count",
                                  children="Total: 0",
                                  className="text-muted fw-bold"),
                        width=True,
                        className="text-end",
                    ),
                ], className="align-items-center mb-2"),
                _make_ag_grid(
                    table_id="gee-exports-table",
                    column_defs=GEE_EXPORT_COLUMNS,
                    row_model="clientSide",
                    height="500px",
                    style_conditions=GEE_STATUS_ROW_STYLES,
                ),
            ]),
            dbc.Tab(label="Users", tab_id="tab-users", children=[
                dbc.Row([
                    dbc.Col(html.H5("User Management", className="mt-3"),
                            width="auto"),
                    dbc.Col(
                        html.Span(id="user-management-total-count",
                                  children="Total: 0",
                                  className="text-muted fw-bold mt-3"),
                        width=True,
                        className="text-end",
                    ),
                ], className="align-items-center mb-2"),
                _make_ag_grid(
                    table_id="user-management-table",
                    column_defs=USER_MANAGEMENT_COLUMNS,
                    row_model="clientSide",
                    height="500px",
                ),
            ]),
        ], id="admin-tabs", active_tab="tab-gee"),

        dcc.Interval(id="admin-refresh-interval", interval=30000,
                     n_intervals=0),
    ])


def not_found_layout(user=None):
    """404 page."""
    return dbc.Container([
        navbar(user),
        dbc.Row(dbc.Col([
            html.H2("Page Not Found"),
            html.P("The requested page does not exist."),
            dbc.Button("Go to Dashboard", href="/", color="primary"),
        ], className="text-center mt-5")),
    ])
