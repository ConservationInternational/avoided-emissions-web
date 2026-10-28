"""App user administration through Trends.Earth, and local user-data removal.

Access to this app is managed in Trends.Earth: users request access when they
first sign in, and app admins approve, revoke or change roles through the
Trends.Earth app-users API using their own access token.  This app keeps no
user table; analysis data is keyed by the Trends.Earth subject (``sub``).
"""

import logging

import requests
from config import Config
from models import (
    AnalysisTask,
    CovariatePreset,
    MatchingSettingsPreset,
    TaskShareLink,
    UserSiteSet,
    UserSiteUpload,
    get_db,
)

logger = logging.getLogger(__name__)

_HTTP_TIMEOUT = 30
APP_ROLES = ("member", "admin")
ACCESS_STATUSES = ("pending", "active", "revoked")

_ERROR_MESSAGES = {
    "app_role_required": "You need the admin role in this app to manage users.",
    "insufficient_scope": (
        "Your sign-in does not permit managing users. Sign out and sign in again."
    ),
    "superadmin_protected": (
        "Trends.Earth superadmins always have admin access and cannot be "
        "revoked or demoted."
    ),
    "self_demotion_forbidden": "You cannot revoke or demote your own access.",
    "email_unverified": "Your Trends.Earth email address is not verified.",
    "not_found": "User not found.",
}


class AppUsersError(Exception):
    """A Trends.Earth app-users API call failed."""

    def __init__(self, message, status=None, code=None):
        super().__init__(message)
        self.status = status
        self.code = code


def _app_url(path=""):
    base = Config.TRENDSEARTH_API_URL.rstrip("/")
    return f"{base}/apps/{Config.OIDC_APP_KEY}{path}"


def _call(method, path, access_token, **kwargs):
    try:
        resp = requests.request(
            method,
            _app_url(path),
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=_HTTP_TIMEOUT,
            **kwargs,
        )
    except requests.RequestException as exc:
        raise AppUsersError("Could not reach Trends.Earth.") from exc
    try:
        body = resp.json()
    except ValueError:
        body = {}
    if resp.status_code >= 400:
        code = body.get("error") if isinstance(body, dict) else None
        detail = body.get("detail") if isinstance(body, dict) else None
        message = (
            _ERROR_MESSAGES.get(code)
            or detail
            or (f"Trends.Earth returned an error ({resp.status_code}).")
        )
        raise AppUsersError(message, status=resp.status_code, code=code)
    return body


def list_app_users(access_token, status=None, max_pages=20):
    """Return every user with a grant for this app (optionally by status)."""
    if status is not None and status not in ACCESS_STATUSES:
        raise ValueError(f"status must be one of: {', '.join(ACCESS_STATUSES)}")
    users = []
    page = 1
    while page <= max_pages:
        params = {"page": page, "per_page": 100}
        if status:
            params["status"] = status
        body = _call("GET", "/users", access_token, params=params)
        batch = body.get("data") or []
        users.extend(batch)
        if not batch or len(users) >= int(body.get("total") or 0):
            break
        page += 1
    return users


def set_app_user_access(access_token, subject, status, role=None, note=None):
    """Approve, revoke or change the role of a user; returns their profile."""
    if status not in ACCESS_STATUSES:
        raise ValueError(f"status must be one of: {', '.join(ACCESS_STATUSES)}")
    if role is not None and role not in APP_ROLES:
        raise ValueError(f"role must be one of: {', '.join(APP_ROLES)}")
    payload = {"status": status}
    if role is not None:
        payload["role"] = role
    if note:
        payload["note"] = note
    body = _call("PUT", f"/users/{subject}/access", access_token, json=payload)
    return body.get("data") or {}


def delete_user_data(user_id):
    """Delete all of a user's analysis data from this app.

    Removes the user's analysis tasks (with their sites and results), share
    links, site sets, site uploads and presets.  The Trends.Earth account
    and its access to this app are not affected.  Returns a dict of counts.
    """
    db = get_db()
    try:
        tasks = db.query(AnalysisTask).filter(AnalysisTask.submitted_by == user_id)
        task_ids = [t.id for t in tasks]
        site_sets = db.query(UserSiteSet).filter(UserSiteSet.user_id == user_id).all()
        site_set_ids = [s.id for s in site_sets]

        n_links = (
            db.query(TaskShareLink)
            .filter(
                (TaskShareLink.created_by == user_id)
                | TaskShareLink.task_id.in_(task_ids)
            )
            .delete(synchronize_session=False)
        )
        n_tasks = 0
        for task in tasks.all():
            db.delete(task)
            n_tasks += 1
        db.flush()
        if site_set_ids:
            # Other users' tasks (e.g. admin recomputes) may reference these
            # site sets; keep those tasks and drop the reference.
            db.query(AnalysisTask).filter(
                AnalysisTask.site_set_id.in_(site_set_ids)
            ).update({AnalysisTask.site_set_id: None}, synchronize_session=False)
        for site_set in site_sets:
            db.delete(site_set)
        n_uploads = (
            db.query(UserSiteUpload)
            .filter(UserSiteUpload.user_id == user_id)
            .delete(synchronize_session=False)
        )
        n_presets = 0
        for model in (CovariatePreset, MatchingSettingsPreset):
            n_presets += (
                db.query(model)
                .filter(model.user_id == user_id)
                .delete(synchronize_session=False)
            )
        db.commit()
        counts = {
            "tasks": n_tasks,
            "share_links": n_links,
            "site_sets": len(site_sets),
            "site_uploads": n_uploads,
            "presets": n_presets,
        }
        logger.info("Deleted app data for user %s: %s", user_id, counts)
        return counts
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
