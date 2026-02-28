"""Main Dash application entry point.

Creates the Dash app, configures Flask-Login authentication, registers
callbacks, and sets up URL routing between pages.
"""

import logging
import uuid as _uuid

import dash
import dash_bootstrap_components as dbc
import flask
import flask_login
import rollbar
import rollbar.contrib.flask
from dash import Input, Output, dcc, html
from flask import got_request_exception
from flask_wtf.csrf import CSRFProtect

from auth import login_manager
from callbacks import register_callbacks
from config import Config
from layouts import (
    admin_layout,
    dashboard_layout,
    login_layout,
    not_found_layout,
    register_layout,
    settings_layout,
    submit_layout,
    task_detail_layout,
)

logger = logging.getLogger(__name__)

# Create Dash app with Bootstrap theme
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.FLATLY],
    suppress_callback_exceptions=True,
    title="Avoided Emissions",
)
server = app.server

# Configure Flask
if not Config.DEBUG and Config.SECRET_KEY in ("change-me-in-production", ""):
    raise RuntimeError(
        "SECRET_KEY is not set. Refusing to start in production with the "
        "default key. Set SECRET_KEY in your environment."
    )
server.config["SECRET_KEY"] = Config.SECRET_KEY
server.config["SESSION_COOKIE_HTTPONLY"] = True
server.config["SESSION_COOKIE_SAMESITE"] = "Lax"
if not Config.DEBUG:
    server.config["SESSION_COOKIE_SECURE"] = True

# Initialize CSRF protection.
# Dash submits all interactions as same-origin XHR/JSON requests which are
# already guarded by SameSite cookies and the browser same-origin policy,
# so we disable the automatic check and rely on those built-in protections.
# If standalone Flask form routes are added later, decorate them with
# @csrf.protect to opt in.
server.config["WTF_CSRF_CHECK_DEFAULT"] = False
csrf = CSRFProtect(server)

# Initialize Rollbar error tracking
if Config.ROLLBAR_ACCESS_TOKEN:
    _rollbar_kwargs = dict(
        access_token=Config.ROLLBAR_ACCESS_TOKEN,
        environment=Config.ROLLBAR_ENVIRONMENT,
        root=__name__,
        allow_logging_basic_config=False,
    )
    if Config.GIT_REVISION:
        _rollbar_kwargs["code_version"] = Config.GIT_REVISION
    with server.app_context():
        rollbar.init(**_rollbar_kwargs)
        got_request_exception.connect(
            rollbar.contrib.flask.report_exception, server
        )
    logger.info("Rollbar initialized (environment=%s)", Config.ROLLBAR_ENVIRONMENT)
else:
    logger.warning("ROLLBAR_ACCESS_TOKEN not set — error tracking disabled")

# Health endpoint (used by Docker healthcheck to confirm app + migrations are ready)
@server.route("/health")
def health_check():
    return "ok", 200


# Initialize Flask-Login
login_manager.init_app(server)
login_manager.login_view = "/login"

# Root layout with URL routing
app.layout = html.Div([
    dcc.Location(id="url", refresh=True),
    html.Div(id="page-content"),
])


@app.callback(
    Output("page-content", "children"),
    Input("url", "pathname"),
)
def display_page(pathname):
    """Route URLs to page layouts."""
    user = None
    if flask_login.current_user.is_authenticated:
        user = flask_login.current_user

    if pathname == "/login":
        return login_layout()

    if pathname == "/register":
        return register_layout()

    if pathname == "/logout":
        flask_login.logout_user()
        return dcc.Location(pathname="/login", id="redirect-logout")

    # All other pages require login
    if not user:
        return dcc.Location(pathname="/login", id="redirect-to-login")

    if pathname == "/" or pathname == "/dashboard":
        return dashboard_layout(user)

    if pathname == "/submit":
        return submit_layout(user)

    if pathname == "/settings":
        return settings_layout(user)

    if pathname == "/admin":
        if not user.is_admin:
            return not_found_layout(user)
        return admin_layout(user)

    if pathname and pathname.startswith("/task/"):
        task_id = pathname.split("/task/")[1]
        # Validate task_id is a proper UUID to prevent injection
        try:
            _uuid.UUID(task_id)
        except (ValueError, AttributeError):
            return not_found_layout(user)
        return task_detail_layout(user, task_id)

    return not_found_layout(user)


# Register all interactive callbacks
register_callbacks(app)


if __name__ == "__main__":
    app.run(debug=Config.DEBUG, host="0.0.0.0", port=8050)
