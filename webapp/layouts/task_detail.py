"""Task detail page layout."""

import dash_bootstrap_components as dbc
from dash import dcc, html

from layouts.common import (
    navbar,
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
                                    title="Open submission form pre-filled with this task's settings",
                                ),
                                dbc.Button(
                                    [
                                        html.I(className="bi bi-x-circle me-1"),
                                        "Cancel",
                                    ],
                                    id="cancel-task-btn",
                                    color="outline-danger",
                                    size="sm",
                                    className="mt-1 me-2",
                                    title="Cancel this running task",
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
            dbc.Tab(
                label="Raw Results",
                tab_id="tab-raw-results",
                children=[
                    html.Div(
                        dbc.Card(
                            dbc.CardBody(html.Div(id="task-raw-results")),
                            className="ae-section-card",
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
            html.Div(id="cancel-task-result"),
            html.Div(id="quality-warning-banner"),
            tabs,
            edit_modal,
            share_modal,
            dcc.Store(id="task-id-store", data=task_id),
            dcc.Store(id="share-token-store", data=shared_token),
            dcc.Interval(id="detail-refresh-interval", interval=15000, n_intervals=0),
        ]
    )
