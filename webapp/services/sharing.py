"""Task share link management and task resubmission."""

import logging
from datetime import datetime, timedelta, timezone

from models import (
    AnalysisTask,
    TaskShareLink,
    get_db,
)

from services.analysis_task import ANALYSIS_DEFAULTS, queue_analysis_task

logger = logging.getLogger(__name__)


def create_share_link(task_id, user_id, expiry_days=7):
    """Create a shareable link for a task.

    Parameters
    ----------
    task_id : str
        UUID of the ``AnalysisTask``.
    user_id : str
        UUID of the user creating the link.
    expiry_days : int
        Number of days until the link expires (default 7, max 90).

    Returns
    -------
    dict
        ``{"token": ..., "expires_at": ..., "id": ...}`` on success.
    """

    # Clamp expiry to a reasonable range (1–90 days)
    expiry_days = max(1, min(int(expiry_days), 90))

    db = get_db()
    try:
        from models import User

        task = db.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
        if not task:
            raise ValueError("Task not found.")

        user = (
            db.query(User).filter(User.id == user_id, User.is_active.is_(True)).first()
        )
        if not user:
            raise PermissionError("User is not authorized to manage share links.")

        if user.role != "admin" and str(task.submitted_by) != str(user_id):
            raise PermissionError("User is not authorized to manage share links.")

        link = TaskShareLink(
            task_id=task_id,
            created_by=user_id,
            expires_at=datetime.now(timezone.utc) + timedelta(days=expiry_days),
        )
        db.add(link)
        db.commit()
        return {
            "token": link.token,
            "expires_at": link.expires_at.isoformat(),
            "id": str(link.id),
        }
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def list_share_links(task_id, user_id=None):
    """Return active share links for a task.

    Returns
    -------
    list[dict]
        Each dict has ``id``, ``token``, ``created_at``, ``expires_at``,
        ``is_active``, ``access_count``.
    """

    db = get_db()
    try:
        if user_id is not None:
            from models import User

            task = db.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
            if not task:
                return []

            user = (
                db.query(User)
                .filter(User.id == user_id, User.is_active.is_(True))
                .first()
            )
            if not user:
                return []
            if user.role != "admin" and str(task.submitted_by) != str(user_id):
                return []

        links = (
            db.query(TaskShareLink)
            .filter(TaskShareLink.task_id == task_id)
            .order_by(TaskShareLink.created_at.desc())
            .all()
        )
        return [
            {
                "id": str(lnk.id),
                "token": lnk.token,
                "created_at": lnk.created_at.isoformat() if lnk.created_at else None,
                "expires_at": lnk.expires_at.isoformat() if lnk.expires_at else None,
                "is_active": lnk.is_active,
                "is_valid": lnk.is_valid,
                "access_count": lnk.access_count or 0,
            }
            for lnk in links
        ]
    finally:
        db.close()


def revoke_share_link(link_id, user_id, task_id=None):
    """Revoke a share link by setting ``is_active`` to False.

    Validates that the link belongs to the expected *task_id* (if
    provided) to prevent cross-object attacks where a forged request
    pairs a valid task_id with a foreign link_id.

    Returns ``True`` if the link was found and revoked.
    """

    db = get_db()
    try:
        from models import User

        link = db.query(TaskShareLink).filter(TaskShareLink.id == link_id).first()
        if not link:
            return False

        user = (
            db.query(User).filter(User.id == user_id, User.is_active.is_(True)).first()
        )
        if not user:
            return False

        task = db.query(AnalysisTask).filter(AnalysisTask.id == link.task_id).first()
        if not task:
            return False

        if user.role != "admin" and str(task.submitted_by) != str(user_id):
            return False

        # Cross-validate: link must belong to the task the caller has
        # access to.  Without this check a user who can view task A
        # could revoke a link belonging to task B.
        if task_id is not None and str(link.task_id) != str(task_id):
            return False
        link.is_active = False
        db.commit()
        return True
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def validate_share_token(token, record_access=True):
    """Validate a share token and return the associated task_id.

    Parameters
    ----------
    token : str
        The URL-safe share token.
    record_access : bool
        When *True* (the default), increments the access counter on the
        link.  Pass *False* for lightweight authorisation checks that
        should not inflate the counter (e.g. periodic callback ticks).

    Returns
    -------
    str or None
        The task UUID as a string, or ``None``.
    """

    db = get_db()
    try:
        link = TaskShareLink.get_valid_link(token, db)
        if not link:
            return None
        if record_access:
            link.record_access()
            db.commit()
        return str(link.task_id)
    except Exception:  # noqa: BLE001
        db.rollback()
        return None
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Recompute (resubmit with new random seed)
# ---------------------------------------------------------------------------


def get_recompute_config(task_id, user_id):
    """Return the configuration of a task for pre-filling the submit form.

    Loads the original task's settings and returns them as a plain dict
    suitable for pre-populating the task-submission page.  A new random
    seed is generated so the user starts with a fresh value.

    Parameters
    ----------
    task_id : str
        UUID of the ``AnalysisTask`` to recompute.
    user_id : str
        UUID of the user requesting the recompute.

    Returns
    -------
    dict
        Keys: ``task_name``, ``description``, ``covariates``,
        ``exact_match_vars``, ``max_treatment_pixels``,
        ``control_multiplier``, ``min_site_area_ha``,
        ``min_glm_treatment_pixels``, ``caliper_width``,
        ``max_controls_per_treatment``, ``random_seed``,
        ``match_memory_gb``, ``matching_job_queue``, ``site_set_id``.

    Raises
    ------
    ValueError
        If the task is not found or the user is not authorised.
    """
    import random as _random

    db = get_db()
    try:
        task = db.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
        if not task:
            raise ValueError("Task not found.")

        from models import User

        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            raise ValueError("User not found.")
        if not user.is_admin and str(task.submitted_by) != str(user_id):
            raise ValueError("You can only recompute your own tasks.")

        config = task.config or {}
        match_memory_mib = config.get("match_memory_mib", 30720)

        return {
            "task_name": f"{task.name} (recompute)",
            "description": task.description or "",
            "covariates": list(task.covariates or []),
            "exact_match_vars": config.get("exact_match_vars", []),
            "max_treatment_pixels": config.get("max_treatment_pixels", 1000),
            "control_multiplier": config.get("control_multiplier", 50),
            "min_site_area_ha": config.get("min_site_area_ha", 100),
            "min_glm_treatment_pixels": config.get("min_glm_treatment_pixels", 15),
            "caliper_width": config.get("caliper_width", 0.2),
            "max_controls_per_treatment": config.get("max_controls_per_treatment", 1),
            "min_control_distance_km": config.get("min_control_distance_km", 10),
            "separation_fallback_mahalanobis": config.get(
                "separation_fallback_mahalanobis", False
            ),
            "group_by_exact_matches": config.get("group_by_exact_matches", False),
            "matching_method": config.get("matching_method", "optimal"),
            "n_replicates": config.get("n_replicates", 1),
            "random_seed": _random.randint(1, 2_147_483_647),
            "match_memory_gb": max(1, match_memory_mib // 1024),
            "matching_job_queue": config.get("matching_job_queue", "ae-spot-gp3"),
            "site_set_id": str(task.site_set_id) if task.site_set_id else None,
        }
    finally:
        db.close()


def resubmit_analysis_task(task_id, user_id):
    """Resubmit a previously submitted task with a new random seed.

    Looks up the original task's configuration, generates a fresh random
    seed, and creates a brand-new ``AnalysisTask`` via
    :func:`queue_analysis_task`.  The new task starts in
    ``status='submitting'`` and a Celery worker handles the slow parts
    (PostGIS computations, S3 upload, API call) asynchronously.

    Parameters
    ----------
    task_id : str
        UUID of the original ``AnalysisTask`` to recompute.
    user_id : str
        UUID of the user requesting the recompute.

    Returns
    -------
    str
        UUID of the newly created task.

    Raises
    ------
    ValueError
        If the task is not found, not owned by the user, or its sites
        cannot be recovered.
    """
    import random as _random

    db = get_db()
    try:
        task = db.query(AnalysisTask).filter(AnalysisTask.id == task_id).first()
        if not task:
            raise ValueError("Task not found.")

        # Ownership check (admins bypass)
        from models import User

        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            raise ValueError("User not found.")
        if not user.is_admin and str(task.submitted_by) != str(user_id):
            raise ValueError("You can only recompute your own tasks.")

        # Verify that site data can be recovered — either via the local
        # site set or an S3 URI.  We don't load the GDF here; the Celery
        # worker will do that.
        source_sites_s3_uri = None
        source_sites_parquet_s3_uri = (task.config or {}).get("sites_parquet_s3_uri")
        if not task.site_set_id:
            source_sites_s3_uri = task.sites_s3_uri or (task.config or {}).get(
                "sites_s3_uri"
            )
            if not source_sites_s3_uri and not source_sites_parquet_s3_uri:
                raise ValueError(
                    "Cannot recover sites for this task. The original site "
                    "data is no longer available."
                )

        config = task.config or {}
        new_seed = _random.randint(1, 2_147_483_647)
        match_memory_mib = config.get("match_memory_mib", 30720)
        new_task_name = f"{task.name} (recompute)"

        return queue_analysis_task(
            task_name=new_task_name,
            description=task.description or "",
            user_id=user_id,
            site_set_id=str(task.site_set_id) if task.site_set_id else None,
            covariates=list(task.covariates or []),
            exact_match_vars=config.get("exact_match_vars", []),
            max_treatment_pixels=config.get("max_treatment_pixels", 1000),
            control_multiplier=config.get("control_multiplier", 50),
            min_site_area_ha=config.get("min_site_area_ha", 100),
            min_glm_treatment_pixels=config.get("min_glm_treatment_pixels", 15),
            caliper_width=config.get("caliper_width", 0.2),
            max_controls_per_treatment=config.get("max_controls_per_treatment", 1),
            min_control_distance_km=config.get("min_control_distance_km", 10),
            separation_fallback_mahalanobis=config.get(
                "separation_fallback_mahalanobis", False
            ),
            group_by_exact_matches=config.get("group_by_exact_matches", False),
            matching_method=config.get("matching_method", "optimal"),
            n_replicates=config.get("n_replicates", 1),
            match_batch_size=config.get(
                "match_batch_size", ANALYSIS_DEFAULTS["match_batch_size"]
            ),
            random_seed=new_seed,
            match_memory_mib=match_memory_mib,
            matching_job_queue=config.get("matching_job_queue", "ae-spot-gp3"),
            resolution_m=config.get("resolution_m", ANALYSIS_DEFAULTS["resolution_m"]),
            source_sites_s3_uri=source_sites_s3_uri,
            source_sites_parquet_s3_uri=source_sites_parquet_s3_uri,
        )
    finally:
        db.close()
