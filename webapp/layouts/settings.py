"""Settings and 404 page layouts."""

import dash_bootstrap_components as dbc
from dash import dcc, html

from layouts.common import (
    navbar,
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
                                    "Register at api.trends.earth/register",
                                    href="https://api.trends.earth/register",
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
