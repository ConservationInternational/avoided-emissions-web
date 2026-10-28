"""Settings and 404 page layouts."""

import dash_bootstrap_components as dbc
from dash import html

from layouts.common import (
    navbar,
)


def settings_layout(user):
    """User profile page: Trends.Earth identity and app data management."""
    from auth import te_password_reset_url

    role_label = "Administrator" if user.is_admin else "Member"
    profile_card = dbc.Card(
        [
            dbc.CardHeader(html.H5("Trends.Earth account", className="mb-0")),
            dbc.CardBody(
                [
                    html.P(
                        "You are signed in with your Trends.Earth account, which "
                        "is also used to run your analyses on Trends.Earth.",
                        className="text-muted",
                    ),
                    html.Dl(
                        [
                            html.Dt("Name", className="col-sm-3"),
                            html.Dd(user.name or "—", className="col-sm-9"),
                            html.Dt("Email", className="col-sm-3"),
                            html.Dd(user.email or "—", className="col-sm-9"),
                            html.Dt("Role in this app", className="col-sm-3"),
                            html.Dd(role_label, className="col-sm-9"),
                        ],
                        className="row mb-2",
                    ),
                    html.P(
                        [
                            (
                                "Your name, email and password are managed by "
                                "Trends.Earth (for example in the Trends.Earth QGIS "
                                "plugin). "
                            ),
                            html.A(
                                "Reset your Trends.Earth password",
                                href=te_password_reset_url(),
                                target="_blank",
                                rel="noopener noreferrer",
                            ),
                            ".",
                        ],
                        className="small mb-0",
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
                            "Your account and Avoided Emissions data.",
                            className="text-muted mb-0",
                        ),
                    ],
                    width=True,
                )
            ],
            className="mb-3",
        ),
        dbc.Row(dbc.Col(profile_card, xs=12, lg=8)),
    ]

    children.append(
        dbc.Row(
            dbc.Col(
                [
                    dbc.Card(
                        [
                            dbc.CardHeader(
                                html.H5(
                                    "Delete my Avoided Emissions data", className="mb-0"
                                ),
                            ),
                            dbc.CardBody(
                                [
                                    html.P(
                                        "Delete all of your analysis tasks, results, share links, site "
                                        "sets and presets from this app, and sign out. This cannot be "
                                        "undone. Your Trends.Earth account and your access to this app "
                                        "are not affected.",
                                        className="text-muted",
                                    ),
                                    dbc.Button(
                                        "Delete My Data",
                                        id="self-delete-btn",
                                        color="danger",
                                        outline=True,
                                        size="sm",
                                    ),
                                    dbc.Modal(
                                        [
                                            dbc.ModalHeader(
                                                dbc.ModalTitle("Delete my data")
                                            ),
                                            dbc.ModalBody(
                                                [
                                                    html.P(
                                                        "Are you sure? This permanently removes all of "
                                                        "your analysis tasks and other data from the "
                                                        "Avoided Emissions app. This cannot be undone.",
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
                                                        "Delete My Data",
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
