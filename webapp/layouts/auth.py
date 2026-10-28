"""Sign-in page layouts: login landing, app-access status and sign-out."""

from urllib.parse import urlencode

import dash_bootstrap_components as dbc
from dash import html

from layouts.common import logout_form, navbar

_LOGIN_ERRORS = {
    "unavailable": (
        "The Trends.Earth sign-in service could not be reached. Please try "
        "again in a few minutes."
    ),
    "failed": "Sign-in did not complete. Please try again.",
}

_ACCESS_MESSAGES = {
    "pending": (
        "Access requested",
        "info",
        (
            "Your request to use the Avoided Emissions app has been sent to the "
            "app administrators. You will receive an email from Trends.Earth "
            "once it has been reviewed."
        ),
    ),
    "revoked": (
        "Access removed",
        "warning",
        (
            "Your access to the Avoided Emissions app has been removed. Contact "
            "an administrator if you believe this is a mistake."
        ),
    ),
    "unavailable": (
        "Access could not be checked",
        "warning",
        (
            "Trends.Earth could not check your access to this app right now. "
            "Please try again later."
        ),
    ),
}


def _auth_card(title, body):
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
                                                title,
                                                className="text-center",
                                                style={"color": "#ffffffcc"},
                                            ),
                                        ]
                                    ),
                                    style={"backgroundColor": "#2c3e50"},
                                ),
                                dbc.CardBody(body),
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


def _sign_in_url(next_url=""):
    if next_url:
        return "/auth/login?" + urlencode({"next": next_url})
    return "/auth/login"


def login_layout(error="", next_url=""):
    """Landing page offering sign-in with Trends.Earth."""
    body = []
    if error:
        body.append(
            dbc.Alert(
                _LOGIN_ERRORS.get(error, _LOGIN_ERRORS["failed"]),
                color="danger",
                className="small",
            )
        )
    body += [
        html.P(
            "Sign in with your Trends.Earth account. The first time you sign "
            "in, an access request is sent to the app administrators.",
            className="small",
        ),
        # A plain link (not dcc.Link) so the browser leaves the Dash app.
        html.A(
            "Sign in with Trends.Earth",
            href=_sign_in_url(next_url),
            className="btn btn-primary w-100",
            id="login-button",
        ),
        html.Hr(),
        html.P(
            "No Trends.Earth account?",
            className="text-center small",
        ),
        html.A(
            "Register with Trends.Earth",
            href="https://api.trends.earth/register",
            className="btn btn-outline-primary w-100",
            id="register-button",
        ),
    ]
    return _auth_card("Sign in", body)


def access_layout(status=""):
    """Shown when Trends.Earth declined access to this app."""
    title, color, message = _ACCESS_MESSAGES.get(
        status,
        (
            "Access denied",
            "warning",
            "Your Trends.Earth account does not have access to this app.",
        ),
    )
    return _auth_card(
        title,
        [
            dbc.Alert(message, color=color, className="small"),
            html.A(
                "Back to sign in",
                href="/login",
                className="btn btn-outline-primary w-100",
            ),
        ],
    )


def logout_layout(user=None):
    """Sign-out confirmation (the navbar posts directly; this is a fallback)."""
    if not user:
        return _auth_card(
            "Signed out",
            [
                html.P("You are signed out.", className="small"),
                html.A("Sign in", href="/login", className="btn btn-primary w-100"),
            ],
        )
    return _auth_card(
        "Sign out",
        [
            html.P(
                "Signing out also ends your Trends.Earth sign-in in this browser.",
                className="small",
            ),
            logout_form("Sign out", button_class="btn btn-primary w-100"),
        ],
    )
