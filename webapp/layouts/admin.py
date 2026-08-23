"""Admin page layout including site upload card and category options."""

import dash_bootstrap_components as dbc
from dash import dcc, html

from gee_export import gee_config
from layouts.common import (
    COMBINED_SITE_UPLOAD_COLUMNS,
    COVARIATE_COLUMNS,
    COVARIATE_STATUS_ROW_STYLES,
    USER_MANAGEMENT_COLUMNS,
    USER_SITE_UPLOAD_ROW_STYLES,
    _make_ag_grid,
    navbar,
)


def _build_category_options():
    """Build dropdown options with variable names per category from config."""
    # gee_config already imported at module level
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
        "soil": "Soil",
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


def _site_upload_card(footer_text):
    """Build the shared admin site-upload card.

    Parameters
    ----------
    footer_text : str
        Footer copy shown beneath the upload controls.

    Returns
    -------
    dbc.Card
        Card containing the streamed upload controls and mapping UI.
    """
    return dbc.Card(
        [
            dbc.CardHeader("Upload New Sites (GeoJSON, GeoPackage, or Archive)"),
            dbc.CardBody(
                [
                    dbc.Collapse(
                        [
                            html.P(
                                [
                                    "Upload a ",
                                    html.Strong("GeoJSON"),
                                    " or ",
                                    html.Strong("GeoPackage"),
                                    " file, or a ",
                                    html.Strong(".zip/.tar.gz"),
                                    " archive containing exactly one ",
                                    html.Strong("Shapefile"),
                                    ", ",
                                    html.Strong("GeoJSON"),
                                    ", or ",
                                    html.Strong("GeoPackage"),
                                    (
                                        " dataset with site polygons. Geometries must be valid "
                                        "Polygons or MultiPolygons in EPSG:4326 (WGS 84)."
                                    ),
                                ],
                                className="mb-2 small",
                            ),
                            dbc.Button(
                                "Click to Upload",
                                id="upload-sites-stream-btn",
                                color="secondary",
                                outline=True,
                                className="w-100 mb-1",
                            ),
                        ],
                        id="site-upload-controls",
                        is_open=True,
                    ),
                    dcc.Input(
                        id="site-upload-stream-payload",
                        type="text",
                        value="",
                        style={"display": "none"},
                    ),
                    html.Div(id="upload-status"),
                    dbc.Collapse(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H6("Map Uploaded Columns", className="mb-2"),
                                    html.P(
                                        "Confirm which columns should be used for each required field. "
                                        "start_date must be parseable as dates; end_date is optional.",
                                        className="small text-muted mb-3",
                                    ),
                                    dbc.Row(
                                        [
                                            dbc.Col(
                                                [
                                                    dbc.Label(
                                                        "site_id", className="mb-1"
                                                    ),
                                                    dbc.Select(
                                                        id="mapping-site-id",
                                                        options=[],
                                                        placeholder="Select source column...",
                                                    ),
                                                ],
                                                xs=12,
                                                md=6,
                                                className="mb-2",
                                            ),
                                            dbc.Col(
                                                [
                                                    dbc.Label(
                                                        "site_name", className="mb-1"
                                                    ),
                                                    dbc.Select(
                                                        id="mapping-site-name",
                                                        options=[],
                                                        placeholder="Select source column...",
                                                    ),
                                                ],
                                                xs=12,
                                                md=6,
                                                className="mb-2",
                                            ),
                                            dbc.Col(
                                                [
                                                    dbc.Label(
                                                        "start_date", className="mb-1"
                                                    ),
                                                    dbc.Select(
                                                        id="mapping-start-date",
                                                        options=[],
                                                        placeholder="Select source column...",
                                                    ),
                                                ],
                                                xs=12,
                                                md=6,
                                                className="mb-2",
                                            ),
                                            dbc.Col(
                                                [
                                                    dbc.Label(
                                                        "end_date (optional)",
                                                        className="mb-1",
                                                    ),
                                                    dbc.Select(
                                                        id="mapping-end-date",
                                                        options=[],
                                                        placeholder="Leave empty for ongoing interventions",
                                                    ),
                                                ],
                                                xs=12,
                                                md=6,
                                                className="mb-2",
                                            ),
                                        ],
                                        className="g-2",
                                    ),
                                    html.Div(
                                        id="site-upload-mapping-status",
                                        className="mt-2",
                                    ),
                                    dbc.Row(
                                        [
                                            dbc.Col(
                                                dbc.Button(
                                                    "Confirm Mapping and Start Import",
                                                    id="confirm-site-upload-mapping-btn",
                                                    color="primary",
                                                    className="w-100",
                                                ),
                                                xs=12,
                                                md=7,
                                            ),
                                            dbc.Col(
                                                dbc.Button(
                                                    "Cancel",
                                                    id="cancel-site-upload-mapping-btn",
                                                    color="secondary",
                                                    outline=True,
                                                    className="w-100",
                                                ),
                                                xs=12,
                                                md=5,
                                            ),
                                        ],
                                        className="g-2 mt-2",
                                    ),
                                ]
                            )
                        ),
                        id="site-upload-mapping-panel",
                        is_open=False,
                        className="mt-2",
                    ),
                ]
            ),
            dbc.CardFooter(html.Small(footer_text, className="text-muted")),
        ],
        className="ae-section-card mb-3",
    )


def admin_layout(user):
    """Admin panel for covariate management, site uploads, and users."""
    category_options = _build_category_options()
    admin_tab_style = {} if user.is_admin else {"display": "none"}

    return dbc.Container(
        [
            navbar(user, active_page="/admin"),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H2("Admin Panel", className="mb-1"),
                            html.P(
                                "Manage covariates, asynchronous site uploads, and user accounts.",
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
                        label="Site Uploads",
                        tab_id="tab-site-uploads",
                        children=[
                            html.Div(
                                [
                                    _site_upload_card(
                                        "After the mapping is confirmed, the staged site file is imported into the database in the background."
                                    ),
                                    dbc.Card(
                                        [
                                            dbc.CardBody(
                                                [
                                                    dbc.Row(
                                                        [
                                                            dbc.Col(
                                                                html.H5(
                                                                    "Site Imports",
                                                                    className="mb-0",
                                                                ),
                                                                width="auto",
                                                            ),
                                                            dbc.Col(
                                                                html.Span(
                                                                    id="admin-combined-site-count",
                                                                    children="Total: 0",
                                                                    className="text-muted fw-bold",
                                                                ),
                                                                width=True,
                                                                className="text-end",
                                                            ),
                                                        ],
                                                        className="ae-action-bar align-items-center mb-3",
                                                    ),
                                                    dbc.Checkbox(
                                                        id="admin-show-archived-site-sets",
                                                        label="Show archived site sets",
                                                        value=False,
                                                        className="mb-2",
                                                    ),
                                                    _make_ag_grid(
                                                        table_id="admin-combined-site-table",
                                                        column_defs=COMBINED_SITE_UPLOAD_COLUMNS,
                                                        row_model="clientSide",
                                                        height="420px",
                                                        style_conditions=USER_SITE_UPLOAD_ROW_STYLES,
                                                    ),
                                                    html.Div(
                                                        id="admin-combined-site-action-status",
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
                        label="Covariates",
                        tab_id="tab-covariates",
                        tab_style=admin_tab_style,
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
                                                                    dbc.Label(
                                                                        "Resolution"
                                                                    ),
                                                                    dbc.Select(
                                                                        id="gee-export-resolution",
                                                                        options=[
                                                                            {
                                                                                "label": "1 km",
                                                                                "value": "1000",
                                                                            },
                                                                            {
                                                                                "label": "250 m",
                                                                                "value": "250",
                                                                            },
                                                                        ],
                                                                        value="1000",
                                                                    ),
                                                                ],
                                                                xs=12,
                                                                sm=3,
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
                                                    dbc.Modal(
                                                        [
                                                            dbc.ModalHeader(
                                                                dbc.ModalTitle(
                                                                    "Confirm Covariate Export"
                                                                )
                                                            ),
                                                            dbc.ModalBody(
                                                                "Start export will launch export tasks for all covariates in the selected category and resolution. Continue?"
                                                            ),
                                                            dbc.ModalFooter(
                                                                [
                                                                    dbc.Button(
                                                                        "Cancel",
                                                                        id="gee-export-cancel",
                                                                        color="secondary",
                                                                        className="me-2",
                                                                    ),
                                                                    dbc.Button(
                                                                        "Start Export",
                                                                        id="gee-export-confirm",
                                                                        color="warning",
                                                                    ),
                                                                ]
                                                            ),
                                                        ],
                                                        id="gee-export-confirm-modal",
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
                        tab_style=admin_tab_style,
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
                active_tab="tab-site-uploads",
                className="ae-content-tabs",
            ),
            dcc.Interval(id="admin-refresh-interval", interval=30000, n_intervals=0),
            dcc.Store(id="site-set-refresh-store"),
            dcc.Store(id="site-upload-columns-store"),
        ]
    )
