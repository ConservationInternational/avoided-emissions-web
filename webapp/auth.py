"""Authentication helpers for the Dash application.

Provides password hashing/verification and session-based user management
using Flask-Login integrated with the Dash server.
"""

import functools
from datetime import datetime, timezone

import bcrypt
import flask
import flask_login

from models import User, get_db


login_manager = flask_login.LoginManager()


class SessionUser(flask_login.UserMixin):
    """Flask-Login user wrapper around the database User model."""

    def __init__(self, user_record):
        self.id = str(user_record.id)
        self.email = user_record.email
        self.name = user_record.name
        self.role = user_record.role
        self.is_approved = user_record.is_approved

    @property
    def is_admin(self):
        return self.role == "admin"


@login_manager.user_loader
def load_user(user_id):
    db = get_db()
    try:
        user = db.query(User).filter(User.id == user_id).first()
        if user and user.is_active and user.is_approved:
            return SessionUser(user)
    finally:
        db.close()
    return None


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))


def authenticate(email: str, password: str):
    """Authenticate a user by email and password.

    Returns a SessionUser on success, None on failure.  Updates last_login.
    Returns the string ``"pending_approval"`` when the credentials are
    correct but the account has not yet been approved by an admin.
    """
    db = get_db()
    try:
        user = db.query(User).filter(User.email == email).first()
        if user and user.is_active and verify_password(password, user.password_hash):
            if not user.is_approved:
                return "pending_approval"
            user.last_login = datetime.now(timezone.utc)
            db.commit()
            return SessionUser(user)
    finally:
        db.close()
    return None


def register_user(email: str, password: str, name: str):
    """Create a new user account pending admin approval.

    Returns ``(True, message)`` on success or ``(False, error)`` on failure.
    """
    db = get_db()
    try:
        existing = db.query(User).filter(User.email == email).first()
        if existing:
            return False, "An account with this email already exists."
        user = User(
            email=email,
            password_hash=hash_password(password),
            name=name,
            role="user",
            is_approved=False,
        )
        db.add(user)
        db.commit()
        return True, "Account created. An administrator must approve your account before you can log in."
    except Exception:
        db.rollback()
        return False, "Registration failed. Please try again."
    finally:
        db.close()


def get_current_user():
    """Get the current logged-in user, or None."""
    if flask_login.current_user.is_authenticated:
        return flask_login.current_user
    return None


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
