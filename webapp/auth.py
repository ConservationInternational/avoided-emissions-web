"""Authentication helpers for the Dash application.

Provides password hashing/verification, session-based user management
using Flask-Login integrated with the Dash server, and password reset
via email.
"""

import functools
import logging
from datetime import datetime, timezone

import bcrypt
import flask
import flask_login

from config import Config
from models import PasswordResetToken, User, get_db

logger = logging.getLogger(__name__)


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
        return (
            True,
            "Account created. An administrator must approve your account before you can log in.",
        )
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


def request_password_reset(email: str) -> bool:
    """Create a password-reset token and email it to the user.

    Always returns ``True`` regardless of whether the email exists so that
    the caller can show a generic message (prevents user enumeration).
    """
    db = get_db()
    try:
        user = db.query(User).filter(User.email == email).first()
        if not user or not user.is_active:
            # Don't reveal whether the account exists
            logger.info("Password reset requested for unknown email: %s", email)
            return True

        # Invalidate any previous tokens for this user
        PasswordResetToken.invalidate_user_tokens(user.id, db)

        # Create a new token
        reset_token = PasswordResetToken(user_id=user.id)
        db.add(reset_token)
        db.commit()

        # Build the reset link
        reset_url = f"{Config.APP_URL}/reset-password?token={reset_token.token}"

        html = f"""
        <p>Hello {user.name},</p>

        <p>A password reset was requested for your Avoided Emissions account.</p>

        <p>Click the link below to reset your password. This link will expire
        in 1 hour.</p>

        <p><a href="{reset_url}">Reset Your Password</a></p>

        <p>If you cannot click the link, copy and paste this URL into your
        browser:</p>
        <p>{reset_url}</p>

        <p>If you did not request this password reset, please ignore this email.
        Your password will remain unchanged.</p>
        """

        try:
            from email_service import send_html_email

            send_html_email(
                recipients=[user.email],
                html=html,
                subject="[Avoided Emissions] Password Reset Request",
            )
        except Exception:
            logger.exception("Failed to send password reset email to %s", user.email)
    except Exception:
        db.rollback()
        logger.exception("Error during password reset request for %s", email)
    finally:
        db.close()
    return True


def reset_password_with_token(token_string: str, new_password: str):
    """Reset a user's password using a valid reset token.

    Returns ``(True, message)`` on success or ``(False, error)`` on failure.
    """
    if not token_string or not new_password:
        return False, "Token and new password are required."

    if len(new_password) < 8:
        return False, "Password must be at least 8 characters."

    db = get_db()
    try:
        reset_token = PasswordResetToken.get_valid_token(token_string, db)
        if not reset_token:
            return False, "This reset link is invalid or has expired."

        user = db.query(User).filter(User.id == reset_token.user_id).first()
        if not user:
            return False, "User not found."

        user.password_hash = hash_password(new_password)
        reset_token.mark_used()
        db.add(user)
        db.add(reset_token)
        db.commit()

        logger.info("Password reset successful for %s", user.email)
        return True, "Your password has been reset. You can now log in."
    except Exception:
        db.rollback()
        logger.exception("Error during password reset with token")
        return False, "An error occurred. Please try again."
    finally:
        db.close()


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
