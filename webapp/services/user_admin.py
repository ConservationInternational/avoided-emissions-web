"""User management, trends.earth admin integration, and result exports."""

import logging
from datetime import datetime, timezone

from config import Config, report_exception
from models import (
    AnalysisTask,
    get_db,
)

logger = logging.getLogger(__name__)


def get_user_list():
    """Return all users ordered by creation date (admin only)."""
    db = get_db()
    try:
        from models import User

        return db.query(User).order_by(User.created_at.desc()).all()
    finally:
        db.close()


def approve_user(user_id):
    """Approve a pending user account and email them a set-password link.

    Returns (success, message).
    """
    db = get_db()
    try:
        from models import PasswordResetToken, User

        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return False, "User not found."
        if user.is_approved:
            return False, "User is already approved."
        user.is_approved = True
        user.updated_at = datetime.now(timezone.utc)
        db.commit()

        # Send the newly-approved user a link to set their password.
        try:
            PasswordResetToken.invalidate_user_tokens(user.id, db)
            reset_token = PasswordResetToken(user_id=user.id)
            db.add(reset_token)
            db.commit()

            from config import Config

            set_pw_url = f"{Config.APP_URL}/reset-password?token={reset_token.token}"
            html_body = f"""
            <p>Hello {user.name},</p>

            <p>Your Avoided Emissions account has been approved! To get
            started, please set your password by clicking the link below.
            This link will expire in 1 hour.</p>

            <p><a href=\"{set_pw_url}\">Set Your Password</a></p>

            <p>If you cannot click the link, copy and paste this URL into
            your browser:</p>
            <p>{set_pw_url}</p>
            """
            from email_service import send_html_email

            send_html_email(
                recipients=[user.email],
                html=html_body,
                subject="[Avoided Emissions] Account Approved — Set Your Password",
            )
        except Exception:
            logger.exception(
                "Failed to send set-password email to newly approved user %s",
                user.email,
            )
            report_exception(approved_user_email=user.email)

        return True, f"User {user.email} approved."
    except Exception:  # noqa: BLE001
        db.rollback()
        return False, "Failed to approve user."
    finally:
        db.close()


def change_user_role(user_id, new_role, acting_user_id=None):
    """Change a user's role. Returns (success, message)."""
    if new_role not in ("admin", "user"):
        return False, "Invalid role."
    if acting_user_id and str(acting_user_id) == str(user_id) and new_role == "user":
        return False, "You cannot change your own role to user."
    db = get_db()
    try:
        from models import User

        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return False, "User not found."
        user.role = new_role
        user.updated_at = datetime.now(timezone.utc)
        db.commit()
        return True, f"User {user.email} role changed to {new_role}."
    except Exception:  # noqa: BLE001
        db.rollback()
        return False, "Failed to change role."
    finally:
        db.close()


# ---------------------------------------------------------------------------
# trends.earth script access management
# ---------------------------------------------------------------------------


def _get_te_admin_client():
    """Return a ``TrendsEarthClient`` authenticated with the webapp's
    service-account credentials (``TRENDSEARTH_CLIENT_ID`` /
    ``TRENDSEARTH_CLIENT_SECRET``).

    These credentials must belong to a trends.earth ADMIN or SUPERADMIN
    user so that the script access endpoints are accessible.

    Returns ``None`` if the service-account credentials or script ID are
    not configured.
    """
    from trendsearth_client import TrendsEarthClient

    client_id = Config.TRENDSEARTH_CLIENT_ID
    client_secret = Config.TRENDSEARTH_CLIENT_SECRET
    script_id = Config.TRENDSEARTH_SCRIPT_ID

    if not client_id or not client_secret or not script_id:
        logger.debug(
            "TE admin client not available — TRENDSEARTH_CLIENT_ID, "
            "TRENDSEARTH_CLIENT_SECRET, or TRENDSEARTH_SCRIPT_ID is not set."
        )
        return None

    return TrendsEarthClient.from_oauth2_credentials(
        api_url=Config.TRENDSEARTH_API_URL,
        client_id=client_id,
        client_secret=client_secret,
    )


def grant_te_script_access(user_id):
    """Grant a webapp user access to the avoided-emissions TE API script.

    Looks up the user's ``te_user_id`` from the stored credential and
    adds that ID to the script's allowed-users list on the TE API using
    the webapp's service-account credentials.

    Does nothing (and logs a warning) if:
    - The user has no linked TE credential or no ``te_user_id``.
    - The webapp service-account credentials are not configured.

    Raises on HTTP errors so callers can decide whether to treat the
    failure as blocking or best-effort.
    """
    from credential_store import get_credential

    cred = get_credential(user_id)
    if not cred or not cred.te_user_id:
        logger.warning(
            "Cannot grant TE script access for user %s — no te_user_id", user_id
        )
        return

    client = _get_te_admin_client()
    if not client:
        logger.warning(
            "Cannot grant TE script access — webapp service-account "
            "credentials are not configured."
        )
        return

    script_id = Config.TRENDSEARTH_SCRIPT_ID
    logger.info(
        "Granting TE script %s access to TE user %s (webapp user %s)",
        script_id,
        cred.te_user_id,
        user_id,
    )
    client.add_user_to_script(script_id, cred.te_user_id)


def revoke_te_script_access(user_id):
    """Revoke a webapp user's access to the avoided-emissions TE API script.

    Looks up the user's ``te_user_id`` from the stored credential and
    removes that ID from the script's allowed-users list on the TE API
    using the webapp's service-account credentials.

    Does nothing (and logs a warning) if:
    - The user has no linked TE credential or no ``te_user_id``.
    - The webapp service-account credentials are not configured.

    Raises on HTTP errors so callers can decide whether to treat the
    failure as blocking or best-effort.
    """
    from credential_store import get_credential

    cred = get_credential(user_id)
    if not cred or not cred.te_user_id:
        logger.warning(
            "Cannot revoke TE script access for user %s — no te_user_id", user_id
        )
        return

    client = _get_te_admin_client()
    if not client:
        logger.warning(
            "Cannot revoke TE script access — webapp service-account "
            "credentials are not configured."
        )
        return

    script_id = Config.TRENDSEARTH_SCRIPT_ID
    logger.info(
        "Revoking TE script %s access from TE user %s (webapp user %s)",
        script_id,
        cred.te_user_id,
        user_id,
    )
    client.remove_user_from_script(script_id, cred.te_user_id)


def delete_user(user_id):
    """Delete a user account and their analysis tasks. Returns (success, message)."""
    # Revoke TE script access *before* deleting the DB row so that
    # the credential lookup still works.
    try:
        revoke_te_script_access(user_id)
    except Exception:
        logger.warning(
            "Failed to revoke TE script access for user %s during deletion "
            "(continuing with deletion)",
            user_id,
            exc_info=True,
        )

    db = get_db()
    try:
        from models import User

        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return False, "User not found."
        email = user.email
        # Delete the user's analysis tasks (cascades to sites/results via DB)
        tasks = (
            db.query(AnalysisTask).filter(AnalysisTask.submitted_by == user_id).all()
        )
        for task in tasks:
            db.delete(task)
        db.delete(user)
        db.commit()
        return True, f"User {email} deleted."
    except Exception:  # noqa: BLE001
        db.rollback()
        return False, "Failed to delete user."
    finally:
        db.close()
