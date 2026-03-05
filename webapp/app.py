"""Main Dash application entry point.

Creates the Dash app, configures Flask-Login authentication, registers
callbacks, and sets up URL routing between pages.
"""

import logging
import os
import sys
import uuid as _uuid
from urllib.parse import parse_qs

import dash
import dash_bootstrap_components as dbc
import flask_login
import rollbar
import rollbar.contrib.flask
from dash import Input, Output, State, dcc, html
from flask import got_request_exception, jsonify
from flask_wtf.csrf import CSRFProtect

from auth import login_manager
from callbacks import register_callbacks
from config import Config
from layouts import (
    admin_layout,
    dashboard_layout,
    forgot_password_layout,
    login_layout,
    not_found_layout,
    register_layout,
    reset_password_layout,
    settings_layout,
    submit_layout,
    task_detail_layout,
)

# ---------------------------------------------------------------------------
# Logging — configure the root logger so that all application loggers (auth,
# email_service, services, tasks, etc.) emit to stderr.  Gunicorn captures
# stderr and writes it to the container log, making messages visible in
# ``docker service logs``.  ``basicConfig`` is a no-op if the root logger
# already has handlers (e.g. when running under ``python app.py``), so this
# is safe to call unconditionally.
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr,
)

logger = logging.getLogger(__name__)

# Create Dash app with Bootstrap theme
app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.FLATLY,
        "https://cdn.jsdelivr.net/npm/ol@10.6.1/ol.css",
    ],
    external_scripts=[
        "https://cdn.jsdelivr.net/npm/geotiff@2.1.3/dist-browser/geotiff.js",
        "https://cdn.jsdelivr.net/npm/ol@10.6.1/dist/ol.js",
    ],
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
        got_request_exception.connect(rollbar.contrib.flask.report_exception, server)
    logger.info("Rollbar initialized (environment=%s)", Config.ROLLBAR_ENVIRONMENT)
else:
    logger.warning("ROLLBAR_ACCESS_TOKEN not set — error tracking disabled")


# Health endpoint (used by Docker healthcheck to confirm app + migrations are ready)
@server.route("/health")
def health_check():
    return "ok", 200


# -- COG layer API -----------------------------------------------------------
# Returns available covariate COG layers with pre-signed S3 URLs and style
# config so the OpenLayers map can render them as toggleable overlays.


@server.route("/api/cog-layers")
@flask_login.login_required
def cog_layers():
    """Return merged covariate layers with pre-signed URLs and styles."""
    import importlib.util

    import boto3

    from layer_config import get_style
    from models import Covariate, get_db

    # Load gee-export config for descriptions and categories
    gee_config_path = os.path.join(os.path.dirname(__file__), "gee-export", "config.py")
    spec = importlib.util.spec_from_file_location("gee_export_config", gee_config_path)
    gee_config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gee_config)

    cog_prefix = f"{Config.S3_PREFIX}/cog"

    # Get latest merged covariates from DB
    db = get_db()
    try:
        latest: dict[str, Covariate] = {}
        for rec in db.query(Covariate).filter(Covariate.status == "merged").all():
            existing = latest.get(rec.covariate_name)
            if existing is None or (
                rec.started_at
                and (
                    existing.started_at is None or rec.started_at > existing.started_at
                )
            ):
                latest[rec.covariate_name] = rec
    finally:
        db.close()

    if not Config.S3_BUCKET:
        return jsonify({"layers": []})

    s3 = boto3.client("s3", region_name=Config.AWS_REGION)
    layers = []

    for name, rec in sorted(latest.items()):
        if not rec.merged_url:
            continue
        cfg = gee_config.COVARIATES.get(name, {})
        category = cfg.get("category", "")

        # Generate a 1-hour pre-signed URL for the COG
        s3_key = f"{cog_prefix}/{name}.tif"
        try:
            url = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": Config.S3_BUCKET, "Key": s3_key},
                ExpiresIn=3600,
            )
        except Exception:
            continue

        style = get_style(name, category)
        layers.append(
            {
                "name": name,
                "description": cfg.get("description", name),
                "category": category,
                "url": url,
                "style": style,
            }
        )

    return jsonify({"layers": layers})


# Initialize Flask-Login
login_manager.init_app(server)
login_manager.login_view = "/login"

# Root layout with URL routing
app.layout = html.Div(
    [
        dcc.Location(id="url", refresh=True),
        html.Div(id="page-content"),
    ]
)


@app.callback(
    Output("page-content", "children"),
    Input("url", "pathname"),
    State("url", "search"),
)
def display_page(pathname, search):
    """Route URLs to page layouts."""
    user = None
    if flask_login.current_user.is_authenticated:
        user = flask_login.current_user

    if pathname == "/login":
        return login_layout()

    if pathname == "/register":
        return register_layout()

    if pathname == "/forgot-password":
        return forgot_password_layout()

    if pathname == "/reset-password":
        # Token is passed as a query parameter; extract from dcc.Location
        # search string (e.g. "?token=abc123")
        token = parse_qs((search or "").lstrip("?")).get("token", [""])[0]
        return reset_password_layout(token)

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
