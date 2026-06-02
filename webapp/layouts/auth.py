"""Authentication page layouts: login, register, forgot/reset password."""

import dash_bootstrap_components as dbc
from dash import dcc, html

from layouts.common import (
    navbar,
)


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
