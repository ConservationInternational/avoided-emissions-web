"""Authentication for the Dash application via Trends.Earth OIDC.

Users sign in with their Trends.Earth account.  Identity, app access and the
user's role in this app come from Trends.Earth tokens; there is no local user
table.  The Flask session holds only an opaque id pointing at a server-side
session record (see :mod:`session_store`).
"""

import functools
import logging

import flask
import flask_login
import oidc
import session_store
from config import Config

logger = logging.getLogger(__name__)

SESSION_ID_KEY = "ae_sid"

login_manager = flask_login.LoginManager()


class SessionUser(flask_login.UserMixin):
    """The signed-in Trends.Earth user, built from the session record."""

    def __init__(self, record, session_id=None):
        self.id = str(record["sub"])
        self.email = record.get("email") or ""
        self.name = record.get("name") or self.email
        self.role = record.get("app_role") or "member"
        self.te_role = record.get("te_role")
        self.session_id = session_id

    @property
    def is_admin(self):
        return self.role == "admin"

    def get_id(self):
        return self.id


@login_manager.user_loader
def load_user(user_id):
    sid = flask.session.get(SESSION_ID_KEY)
    if not sid:
        return None
    try:
        record = session_store.get(sid)
    except session_store.SessionUnavailable:
        logger.warning("Session storage unavailable — treating as anonymous")
        return None
    if not record or str(record.get("sub")) != str(user_id):
        return None
    return SessionUser(record, session_id=sid)


def start_session(record):
    """Persist *record* and sign the user in for this browser."""
    sid = session_store.create(record)
    flask.session.clear()
    flask.session[SESSION_ID_KEY] = sid
    flask.session.permanent = True
    flask_login.login_user(SessionUser(record, session_id=sid), remember=False)
    return sid


def end_session():
    """Sign out locally and revoke the Trends.Earth refresh token.

    Returns the ID token (if any) for use as ``id_token_hint`` at the
    Trends.Earth logout endpoint.
    """
    sid = flask.session.get(SESSION_ID_KEY)
    id_token = None
    if sid:
        try:
            record = session_store.get(sid)
        except session_store.SessionUnavailable:
            record = None
        if record:
            id_token = record.get("id_token")
            oidc.revoke(record.get("refresh_token"))
        session_store.delete(sid)
    flask_login.logout_user()
    flask.session.clear()
    return id_token


def refresh_current_session():
    """Keep the current session's access token fresh.

    Returns ``False`` if the session has ended (the caller should treat the
    request as anonymous), ``True`` otherwise.  Transient provider or storage
    failures keep the session.
    """
    sid = flask.session.get(SESSION_ID_KEY)
    if not sid:
        return True
    try:
        record = session_store.ensure_fresh(sid)
    except session_store.SessionExpired:
        flask_login.logout_user()
        flask.session.clear()
        return False
    except session_store.SessionUnavailable:
        logger.warning("Could not refresh session; keeping it for now")
        return True
    user = flask_login.current_user
    if getattr(user, "is_authenticated", False):
        # Pick up role changes made in Trends.Earth since the last refresh.
        user.role = record.get("app_role") or "member"
    return True


def get_current_user():
    """Get the current logged-in user, or None."""
    if flask_login.current_user.is_authenticated:
        return flask_login.current_user
    return None


def te_password_reset_url():
    """Trends.Earth page where users can reset their password."""
    return f"{Config.OIDC_ISSUER}/oauth/forgot-password"


def require_login(func):
    """Decorator that returns a login redirect for unauthenticated users."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not flask_login.current_user.is_authenticated:
            return flask.redirect("/login")
        return func(*args, **kwargs)

    return wrapper


def require_admin(func):
    """Decorator that restricts access to admin users."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not flask_login.current_user.is_authenticated:
            return flask.redirect("/login")
        if not flask_login.current_user.is_admin:
            return flask.redirect("/")
        return func(*args, **kwargs)

    return wrapper
