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

SITE_CATALOG_COLUMNS = [
    {
        **column,
        "headerName": (
            "Sites"
            if column["field"] == "n_sites_imported"
            else "Uploaded"
            if column["field"] == "created_at"
            else column["headerName"]
        ),
    }
    for column in COMBINED_SITE_UPLOAD_COLUMNS
    if column["field"]
    in (
        "site_set_name",
        "filename",
        "n_sites_imported",
        "is_archived",
        "created_at",
        "id",
    )
]
IMPORT_HISTORY_COLUMNS = [
    *COMBINED_SITE_UPLOAD_COLUMNS[:3],
    {"headerName": "Phase", "field": "phase", "minWidth": 140},
    {
        "headerName": "Scanned",
        "field": "rows_scanned",
        "type": "numericColumn",
        "minWidth": 110,
    },
    {
        "headerName": "Accepted",
        "field": "rows_accepted",
        "type": "numericColumn",
        "minWidth": 110,
    },
    {
        "headerName": "Skipped",
        "field": "rows_skipped",
        "type": "numericColumn",
        "minWidth": 100,
    },
    *[
        {
            **column,
            **(
                {
                    "wrapText": True,
                    "autoHeight": True,
                    "cellStyle": {"whiteSpace": "normal"},
                }
                if column["field"] == "error_message"
                else {}
            ),
        }
        for column in COMBINED_SITE_UPLOAD_COLUMNS[3:]
        if column["field"] not in {"is_archived", "ingest_stats"}
    ],
]


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
                                        " dataset with site polygons. Polygons and MultiPolygons "
                                        "are validated and repaired. GeoJSON uses WGS 84; other "
                                        "datasets must declare their CRS and are transformed "
                                        "to EPSG:4326."
                                    ),
                                ],
                                className="mb-2 small",
                            ),
                            html.P(
                                "Upload request limit: 800 MiB including form data. "
                                "Archive limits: 2,000 files, 512 MiB per file, "
                                "1 GiB expanded total and 200:1 compression.",
                                id="site-upload-limits",
                                className="mb-2 small text-muted",
                            ),
                            dbc.Button(
                                "Click to Upload",
                                id="upload-sites-stream-btn",
                                color="secondary",
                                outline=True,
                                className="w-100 mb-1",
                            ),
                            html.Button(
                                "Cancel transfer",
                                id="upload-sites-stream-cancel-btn",
                                type="button",
                                className="btn btn-outline-secondary btn-sm mb-2",
                                style={"display": "none"},
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
                    html.Div(
                        id="upload-status",
                        role="status",
                        **{"aria-live": "polite", "aria-atomic": "true"},
                    ),
                    dbc.Collapse(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H6("Map Uploaded Columns", className="mb-2"),
                                    html.P(
                                        "Only start_date is required (for example 2000-01-01). "
                                        "Site ID and name are optional display labels and are generated "
                                        "when omitted; duplicate display IDs are allowed. "
                                        "Numeric years/serial dates must be converted to explicit dates. "
                                        "The choices below include sampled source values.",
                                        className="small text-muted mb-3",
                                    ),
                                    dbc.Row(
                                        [
                                            dbc.Col(
                                                [
                                                    dbc.Label(
                                                        "site_id (optional display ID)",
                                                        className="mb-1",
                                                        html_for="mapping-site-id",
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
                                                        "site_name (optional)",
                                                        className="mb-1",
                                                        html_for="mapping-site-name",
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
                                                        "start_date (required)",
                                                        className="mb-1",
                                                        html_for="mapping-start-date",
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
                                                        html_for="mapping-end-date",
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
                                        role="status",
                                        **{"aria-live": "polite"},
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


_USER_STATUS_OPTIONS = [
    {"label": "Pending requests", "value": "pending"},
    {"label": "Active", "value": "active"},
    {"label": "Revoked", "value": "revoked"},
    {"label": "All", "value": "all"},
]


def _users_tab(user_status=None):
    """App-access management backed by the Trends.Earth app-users API."""
    valid = {o["value"] for o in _USER_STATUS_OPTIONS}
    status = user_status if user_status in valid else "pending"
    return html.Div(
        [
            dbc.Card(
                [
                    dbc.CardHeader("User Access"),
                    dbc.CardBody(
                        [
                            html.P(
                                "Users sign in with their Trends.Earth account. New "
                                "users appear here as pending requests; app admins "
                                "who can approve users are emailed by Trends.Earth "
                                "when a request arrives. Select a user from the "
                                "table, then choose an action.",
                                className="text-muted small mb-3",
                            ),
                            dbc.Row(
                                [
                                    dbc.Col(
                                        [
                                            dbc.Label("Selected user", size="sm"),
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
                                            dbc.Label("Role", size="sm"),
                                            dbc.Select(
                                                id="admin-role-select",
                                                options=[
                                                    {
                                                        "label": "Member",
                                                        "value": "member",
                                                    },
                                                    {
                                                        "label": "Admin",
                                                        "value": "admin",
                                                    },
                                                ],
                                                value="member",
                                            ),
                                        ],
                                        xs=6,
                                        md=2,
                                    ),
                                    dbc.Col(
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
                                                    "Revoke Access",
                                                    id="admin-revoke-btn",
                                                    color="warning",
                                                    size="sm",
                                                ),
                                                dbc.Button(
                                                    "Delete App Data",
                                                    id="admin-delete-btn",
                                                    color="danger",
                                                    size="sm",
                                                ),
                                            ]
                                        ),
                                        width="auto",
                                        className="d-flex align-items-end",
                                    ),
                                ]
                            ),
                            html.Div(id="admin-user-action-result", className="mt-2"),
                            dbc.Modal(
                                [
                                    dbc.ModalHeader(dbc.ModalTitle("Delete user data")),
                                    dbc.ModalBody(
                                        "Delete all of this user's analysis tasks, "
                                        "results and other data from the Avoided "
                                        "Emissions app? This cannot be undone. It does "
                                        "not change their access or their Trends.Earth "
                                        "account; use Revoke Access for that."
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
                                                "Delete Data",
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
                                        html.H5("Users", className="mb-0"),
                                        width="auto",
                                    ),
                                    dbc.Col(
                                        dbc.Select(
                                            id="admin-user-status-filter",
                                            options=_USER_STATUS_OPTIONS,
                                            value=status,
                                            size="sm",
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
    )


def admin_layout(user, user_status=None):
    """Admin panel for covariate management, site uploads, and users.

    ``user_status`` (from ``/admin?status=pending``, the link in Trends.Earth
    access-request emails) opens the Users tab filtered to that status.
    """
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
                                "Manage covariates, asynchronous site uploads, and user access.",
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
                                                                    "Site Set Catalog",
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
                                                        column_defs=SITE_CATALOG_COLUMNS,
                                                        row_model="clientSide",
                                                        height="420px",
                                                        style_conditions=USER_SITE_UPLOAD_ROW_STYLES,
                                                        grid_options_extra={
                                                            "pagination": True,
                                                            "paginationPageSize": 25,
                                                            "paginationPageSizeSelector": [
                                                                25,
                                                                50,
                                                                100,
                                                            ],
                                                            "getRowId": {
                                                                "function": "params.data.id"
                                                            },
                                                        },
                                                    ),
                                                    html.Div(
                                                        id="admin-combined-site-action-status",
                                                        className="mt-2",
                                                    ),
                                                    html.Hr(),
                                                    html.H5("Recent Import History"),
                                                    html.P(
                                                        "The latest 50 imports are shown here. "
                                                        "All reusable site sets remain in the catalog above, "
                                                        "including legacy uploads.",
                                                        className="small text-muted",
                                                    ),
                                                    _make_ag_grid(
                                                        "admin-site-import-history",
                                                        IMPORT_HISTORY_COLUMNS,
                                                        height="320px",
                                                        grid_options_extra={
                                                            "pagination": True,
                                                            "paginationPageSize": 10,
                                                            "paginationPageSizeSelector": [
                                                                10,
                                                                25,
                                                                50,
                                                            ],
                                                        },
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
                        children=[_users_tab(user_status)] if user.is_admin else [],
                    ),
                ],
                id="admin-tabs",
                active_tab=(
                    "tab-users" if user.is_admin and user_status else "tab-site-uploads"
                ),
                className="ae-content-tabs",
            ),
            dcc.Interval(id="admin-refresh-interval", interval=30000, n_intervals=0),
            dcc.Store(id="site-set-refresh-store"),
            dcc.Store(id="site-upload-columns-store"),
        ]
    )
