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
import flask
import flask_login
import rollbar
import rollbar.contrib.flask
from dash import Input, Output, State, dcc, html
from flask import got_request_exception, request
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_wtf.csrf import CSRFProtect

from auth import (
    REFRESH_TOKEN_COOKIE,
    clear_refresh_cookie,
    login_manager,
    touch_refresh_token,
    validate_and_refresh,
)

# Import webapp's tasks module before other local modules are imported so that
# sys.modules['tasks'] is populated with the Celery task registry before any
# other import has a chance to bind a different module to that name.
import tasks as _webapp_tasks  # noqa: F401 — side-effect: registers Celery tasks

from callbacks import register_callbacks
from config import Config
from layouts import (
    admin_layout,
    dashboard_layout,
    footer,
    forgot_password_layout,
    login_layout,
    not_found_layout,
    register_layout,
    reset_password_layout,
    settings_layout,
    submit_layout,
    task_detail_layout,
)
from api_routes import create_api_blueprint

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


def _extra_csp_sources(env_var_name: str) -> list[str]:
    raw = (os.environ.get(env_var_name, "") or "").strip()
    if not raw:
        return []
    return [item for item in raw.split() if item]


_CSP_SCRIPT_SRC = [
    "'self'",
    "'unsafe-inline'",
    "'unsafe-eval'",
    "https://cdn.jsdelivr.net",
]
_CSP_STYLE_SRC = [
    "'self'",
    "'unsafe-inline'",
    "https://cdn.jsdelivr.net",
    "https://fonts.googleapis.com",
]
_CSP_FONT_SRC = [
    "'self'",
    "data:",
    "https://fonts.gstatic.com",
]
_CSP_IMG_SRC = [
    "'self'",
    "data:",
    "blob:",
    "https://*.amazonaws.com",
    "https://storage.googleapis.com",
    "https://*.storage.googleapis.com",
    "https://s3.amazonaws.com",
    "https://*.s3.amazonaws.com",
    "https://tile.openstreetmap.org",
    "https://*.tile.openstreetmap.org",
]
_CSP_CONNECT_SRC = [
    "'self'",
    "blob:",
    "https://*.amazonaws.com",
    "https://storage.googleapis.com",
    "https://*.storage.googleapis.com",
    "https://s3.amazonaws.com",
    "https://*.s3.amazonaws.com",
    "https://cdn.jsdelivr.net",
]

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
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1"},
    ],
)
server = app.server

# Configure Flask
if not Config.DEBUG and Config.SECRET_KEY in ("change-me-in-production", ""):
    raise RuntimeError(
        "SECRET_KEY is not set. Refusing to start in production with the "
        "default key. Set SECRET_KEY in your environment."
    )
if not Config.DEBUG and not (Config.APP_URL or "").strip().lower().startswith(
    "https://"
):
    raise RuntimeError(
        "APP_URL must use HTTPS when DEBUG is false. "
        "Set APP_URL to an https:// URL in your environment."
    )
server.config["SECRET_KEY"] = Config.SECRET_KEY
server.config["SESSION_COOKIE_HTTPONLY"] = True
server.config["SESSION_COOKIE_SAMESITE"] = "Lax"
if not Config.DEBUG:
    server.config["SESSION_COOKIE_SECURE"] = True

# --- Upload size limit (800 MB) ---
# Supports large site geometry files (up to several hundred MB).
server.config["MAX_CONTENT_LENGTH"] = 800 * 1024 * 1024  # 800 MB

# Initialize CSRF protection.
# SECURITY NOTE: WTF_CSRF_CHECK_DEFAULT is intentionally disabled.
# Dash submits all interactions as same-origin XHR/JSON requests which are
# already guarded by SameSite cookies and the browser same-origin policy.
# Most Flask API routes (/api/*) are read-only GET endpoints behind
# @flask_login.login_required. The streamed upload endpoint is POST but it
# accepts only multipart file bytes and does not perform state-changing account
# actions.
# WARNING: If you add Flask routes that accept POST/PUT/DELETE with
# form data or cookies, decorate them with @csrf.protect to opt in.
server.config["WTF_CSRF_CHECK_DEFAULT"] = False
csrf = CSRFProtect(server)

# -- Rate limiting -----------------------------------------------------------
# Protects authentication endpoints against brute-force and credential-
# stuffing attacks.  Uses the Redis instance already available for Celery;
# falls back to in-memory storage when Redis is unreachable.
limiter = Limiter(
    get_remote_address,
    app=server,
    default_limits=[],  # No blanket limit — applied selectively below
    storage_uri=Config.CELERY_BROKER_URL,  # Redis
)

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

# Register API Blueprint (all /health, /api/* routes)
server.register_blueprint(create_api_blueprint(limiter))


# -- Refresh-token based session management -----------------------------------
# On every request:
#   1. If the user already has a valid Flask-Login session *and* a refresh
#      token cookie, touch the token's ``last_activity`` so the 4-hour
#      inactivity window keeps rolling.
#   2. If the Flask-Login session has expired but a valid refresh token
#      exists (last activity < 4 h ago), transparently re-login the user.
#   3. If the token has expired or been revoked, force logout.

_REFRESH_SKIP_PREFIXES = ("/health", "/_dash-", "/assets/")


@server.before_request
def _refresh_token_check():
    cookie_token = request.cookies.get(REFRESH_TOKEN_COOKIE)

    # Skip for paths that don't need auth checks
    path = request.path
    if any(path.startswith(p) for p in _REFRESH_SKIP_PREFIXES):
        return None

    if flask_login.current_user.is_authenticated:
        # User has a live session — just keep the refresh token alive
        if cookie_token:
            touch_refresh_token(cookie_token)
        return None

    # No active session — try to restore from refresh token
    if not cookie_token:
        return None

    session_user, new_token = validate_and_refresh(cookie_token)
    if session_user and new_token:
        flask_login.login_user(session_user)
        # Store the rotated token so the after_request hook sets the
        # new cookie (replacing the old one).
        flask.session["_pending_refresh_token"] = new_token
    else:
        # Token invalid/expired — clear the stale cookie on this response
        @flask.after_this_request
        def _clear_cookie(response):
            clear_refresh_cookie(response)
            return response

    return None


# -- Security headers -------------------------------------------------------
# Applied to every response.  CSP is intentionally permissive for the CDN
# assets loaded by Dash/OpenLayers; tighten further when possible.


@server.after_request
def _set_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    if not Config.DEBUG:
        response.headers["Strict-Transport-Security"] = (
            "max-age=63072000; includeSubDomains"
        )

    # -- Refresh token cookie management ------------------------------------
    # The login callback stores a plaintext token in the Flask session;
    # we move it to a dedicated HTTP-only cookie here so it persists
    # across browser sessions.  Logout sets a clear flag instead.
    from auth import set_refresh_cookie

    pending_token = flask.session.pop("_pending_refresh_token", None)
    if pending_token:
        set_refresh_cookie(response, pending_token)

    if flask.session.pop("_clear_refresh_cookie", None):
        clear_refresh_cookie(response)
    # CSP: allow Dash inline scripts/styles, CDN assets (OL, GeoTIFF),
    # Google Fonts, and S3-hosted resources used by presigned URLs.
    # Keep sources centralised in module-level lists for easier updates.
    img_src = _CSP_IMG_SRC + _extra_csp_sources("CSP_EXTRA_IMG_SRC")
    connect_src = _CSP_CONNECT_SRC + _extra_csp_sources("CSP_EXTRA_CONNECT_SRC")

    csp_parts = [
        "default-src 'self'",
        f"script-src {' '.join(_CSP_SCRIPT_SRC)}",
        f"script-src-elem {' '.join(_CSP_SCRIPT_SRC)}",
        f"style-src {' '.join(_CSP_STYLE_SRC)}",
        f"style-src-elem {' '.join(_CSP_STYLE_SRC)}",
        "style-src-attr 'unsafe-inline'",
        f"img-src {' '.join(img_src)}",
        f"font-src {' '.join(_CSP_FONT_SRC)}",
        f"connect-src {' '.join(connect_src)}",
        "worker-src 'self' blob:",
        "frame-ancestors 'none'",
    ]
    response.headers["Content-Security-Policy"] = "; ".join(csp_parts)
    return response


# Initialize Flask-Login
login_manager.init_app(server)
login_manager.login_view = "/login"

# Root layout with URL routing
app.layout = html.Div(
    [
        dcc.Location(id="url", refresh=True),
        html.Div(id="page-content"),
        # Fires every 5 minutes to detect inactivity-based logouts.
        # The callback hits /api/session-check which triggers the
        # before_request refresh-token validation.
        dcc.Interval(
            id="session-check-interval",
            interval=5 * 60 * 1000,  # 5 minutes in ms
            n_intervals=0,
        ),
        html.Div(id="session-check-output", style={"display": "none"}),
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
        from auth import revoke_refresh_token

        cookie_token = request.cookies.get(REFRESH_TOKEN_COOKIE)
        revoke_refresh_token(cookie_token)
        flask_login.logout_user()
        flask.session["_clear_refresh_cookie"] = True
        return dcc.Location(pathname="/login", id="redirect-logout")

    # Shared task view — no login required
    if pathname and pathname.startswith("/shared/"):
        from services import validate_share_token

        share_token = pathname.split("/shared/", 1)[1]
        if not share_token:
            return html.Div([not_found_layout(user), footer()])
        task_id = validate_share_token(share_token)
        if not task_id:
            return html.Div(
                [
                    dbc.Container(
                        html.Div(
                            [
                                html.H3("Link Expired or Invalid"),
                                html.P(
                                    "This share link is no longer valid. "
                                    "It may have expired or been revoked."
                                ),
                                dbc.Button(
                                    "Go to Login", href="/login", color="primary"
                                ),
                            ],
                            className="text-center mt-5",
                        )
                    ),
                    footer(),
                ]
            )
        return html.Div(
            [task_detail_layout(user, task_id, shared_token=share_token), footer()]
        )

    # All other pages require login
    if not user:
        return dcc.Location(pathname="/login", id="redirect-to-login")

    if pathname == "/" or pathname == "/dashboard":
        page = dashboard_layout(user)
    elif pathname == "/submit":
        recompute_config = None
        recompute_id = parse_qs((search or "").lstrip("?")).get("recompute", [""])[0]
        if recompute_id:
            try:
                _uuid.UUID(recompute_id)
                from services import get_recompute_config

                recompute_config = get_recompute_config(recompute_id, str(user.id))
            except (ValueError, AttributeError):
                pass
        page = submit_layout(user, recompute_config=recompute_config)
    elif pathname == "/settings":
        page = settings_layout(user)
    elif pathname == "/admin":
        page = admin_layout(user)
    elif pathname and pathname.startswith("/task/"):
        task_id = pathname.split("/task/")[1]
        # Validate task_id is a proper UUID to prevent injection
        try:
            _uuid.UUID(task_id)
        except (ValueError, AttributeError):
            return html.Div([not_found_layout(user), footer()])
        page = task_detail_layout(user, task_id)
    else:
        page = not_found_layout(user)

    return html.Div([page, footer()])


# Register all interactive callbacks
register_callbacks(app, limiter=limiter)


if __name__ == "__main__":
    app.run(debug=Config.DEBUG, host="0.0.0.0", port=8050)
