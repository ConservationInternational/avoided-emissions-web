"""Main Dash application entry point.

Creates the Dash app, configures Flask-Login authentication, registers
callbacks, and sets up URL routing between pages.
"""

import logging

import dash
import dash_bootstrap_components as dbc
import flask
import flask_login
import rollbar
import rollbar.contrib.flask
from dash import Input, Output, dcc, html
from flask import got_request_exception

from auth import login_manager
from callbacks import register_callbacks
from config import Config
from layouts import (
    admin_layout,
    dashboard_layout,
    login_layout,
    not_found_layout,
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
server.config["SECRET_KEY"] = Config.SECRET_KEY

# Initialize Rollbar error tracking
if Config.ROLLBAR_ACCESS_TOKEN:
    with server.app_context():
        rollbar.init(
            access_token=Config.ROLLBAR_ACCESS_TOKEN,
            environment=Config.ROLLBAR_ENVIRONMENT,
            root=__name__,
            allow_logging_basic_config=False,
        )
        got_request_exception.connect(
            rollbar.contrib.flask.report_exception, server
        )
    logger.info("Rollbar initialized (environment=%s)", Config.ROLLBAR_ENVIRONMENT)
else:
    logger.warning("ROLLBAR_ACCESS_TOKEN not set — error tracking disabled")

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

    if pathname == "/admin":
        if not user.is_admin:
            return not_found_layout(user)
        return admin_layout(user)

    if pathname and pathname.startswith("/task/"):
        task_id = pathname.split("/task/")[1]
        return task_detail_layout(user, task_id)

    return not_found_layout(user)


# Register all interactive callbacks
register_callbacks(app)


if __name__ == "__main__":
    app.run(debug=Config.DEBUG, host="0.0.0.0", port=8050)
