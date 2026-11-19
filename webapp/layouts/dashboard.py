"""Dashboard page layout."""

import dash_bootstrap_components as dbc
from dash import dcc, html

from layouts.common import (
    TASK_LIST_COLUMNS,
    TASK_STATUS_ROW_STYLES,
    _make_ag_grid,
    navbar,
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
                            html.Div(id="task-list-action-pending", role="status"),
                            html.Div(id="recompute-from-list-result"),
                            html.Div(id="cancel-from-list-result"),
                            _make_ag_grid(
                                table_id="task-list-table",
                                column_defs=TASK_LIST_COLUMNS,
                                row_model="clientSide",
                                height="calc(100vh - 280px)",
                                style_conditions=TASK_STATUS_ROW_STYLES,
                            ),
                        ]
                    )
                ],
                className="ae-section-card mb-4",
            ),
            # Stores & intervals
            dcc.Store(id="task-list-action-refresh"),
            dcc.Store(id="task-list-action-complete"),
            dbc.Modal(
                [
                    dbc.ModalHeader(dbc.ModalTitle(id="task-params-title")),
                    dbc.ModalBody(id="task-params-body"),
                    dbc.ModalFooter(
                        dbc.Button("Close", id="task-params-close", color="secondary")
                    ),
                ],
                id="task-params-modal",
                size="lg",
                scrollable=True,
                is_open=False,
            ),
            dcc.Store(id="task-logs-task-id"),
            dcc.Interval(id="task-logs-interval", interval=30000, disabled=True),
            dbc.Modal(
                [
                    dbc.ModalHeader(
                        [
                            dbc.ModalTitle(id="task-logs-title"),
                            html.Div(
                                [
                                    dbc.Button(
                                        "Refresh",
                                        id="task-logs-refresh",
                                        color="primary",
                                        size="sm",
                                    ),
                                    dbc.Button(
                                        "Close",
                                        id="task-logs-close",
                                        color="secondary",
                                        size="sm",
                                    ),
                                ],
                                className="d-flex gap-2 ms-auto",
                            ),
                        ],
                        close_button=False,
                    ),
                    dbc.ModalBody(id="task-logs-body"),
                ],
                id="task-logs-modal",
                size="xl",
                scrollable=True,
                is_open=False,
                backdrop="static",
                keyboard=False,
            ),
            dcc.Store(id="task-list-store"),
            dcc.Interval(id="refresh-interval", interval=30000, n_intervals=0),
        ],
        fluid=True,
    )
