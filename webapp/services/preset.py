"""User-saved covariate and matching settings preset management."""

import logging
from datetime import datetime, timezone


from models import (
    CovariatePreset,
    MatchingSettingsPreset,
    get_db,
)

logger = logging.getLogger(__name__)


def get_covariate_presets(user_id):
    """Return all covariate presets for the given user, ordered by name.

    Each item is a dict with keys ``id``, ``name``, ``covariates``, and
    ``exact_match_vars``.
    """
    db = get_db()
    try:
        presets = (
            db.query(CovariatePreset)
            .filter(CovariatePreset.user_id == user_id)
            .order_by(CovariatePreset.name)
            .all()
        )
        return [
            {
                "id": str(p.id),
                "name": p.name,
                "covariates": list(p.covariates),
                "exact_match_vars": list(p.exact_match_vars)
                if p.exact_match_vars
                else [],
            }
            for p in presets
        ]
    finally:
        db.close()


def save_covariate_preset(user_id, name, covariates, exact_match_vars=None):
    """Create or update a covariate preset for the given user.

    If a preset with the same *name* already exists for this user it is
    updated in-place; otherwise a new row is inserted.  Returns the
    preset ``id`` as a string.
    """
    db = get_db()
    try:
        existing = (
            db.query(CovariatePreset)
            .filter(
                CovariatePreset.user_id == user_id,
                CovariatePreset.name == name,
            )
            .first()
        )
        if existing:
            existing.covariates = list(covariates)
            existing.exact_match_vars = (
                list(exact_match_vars) if exact_match_vars else []
            )
            existing.updated_at = datetime.now(timezone.utc)
            db.commit()
            return str(existing.id)

        preset = CovariatePreset(
            user_id=user_id,
            name=name,
            covariates=list(covariates),
            exact_match_vars=list(exact_match_vars) if exact_match_vars else [],
        )
        db.add(preset)
        db.commit()
        return str(preset.id)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def delete_covariate_preset(preset_id, user_id):
    """Delete a covariate preset by id, scoped to the owning user.

    Returns ``True`` if a row was deleted, ``False`` otherwise.
    """
    db = get_db()
    try:
        preset = (
            db.query(CovariatePreset)
            .filter(
                CovariatePreset.id == preset_id,
                CovariatePreset.user_id == user_id,
            )
            .first()
        )
        if not preset:
            return False
        db.delete(preset)
        db.commit()
        return True
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Matching settings presets
# ---------------------------------------------------------------------------


def get_matching_settings_presets(user_id):
    """Return all matching settings presets for the given user, ordered by name.

    Each item is a dict with keys ``id``, ``name``, and ``settings``.
    """
    db = get_db()
    try:
        presets = (
            db.query(MatchingSettingsPreset)
            .filter(MatchingSettingsPreset.user_id == user_id)
            .order_by(MatchingSettingsPreset.name)
            .all()
        )
        return [
            {
                "id": str(p.id),
                "name": p.name,
                "settings": dict(p.settings) if p.settings else {},
            }
            for p in presets
        ]
    finally:
        db.close()


def save_matching_settings_preset(user_id, name, settings):
    """Create or update a matching settings preset for the given user.

    If a preset with the same *name* already exists for this user it is
    updated in-place; otherwise a new row is inserted.  Returns the
    preset ``id`` as a string.
    """
    db = get_db()
    try:
        existing = (
            db.query(MatchingSettingsPreset)
            .filter(
                MatchingSettingsPreset.user_id == user_id,
                MatchingSettingsPreset.name == name,
            )
            .first()
        )
        if existing:
            existing.settings = dict(settings)
            existing.updated_at = datetime.now(timezone.utc)
            db.commit()
            return str(existing.id)

        preset = MatchingSettingsPreset(
            user_id=user_id,
            name=name,
            settings=dict(settings),
        )
        db.add(preset)
        db.commit()
        return str(preset.id)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def delete_matching_settings_preset(preset_id, user_id):
    """Delete a matching settings preset by id, scoped to the owning user.

    Returns ``True`` if a row was deleted, ``False`` otherwise.
    """
    db = get_db()
    try:
        preset = (
            db.query(MatchingSettingsPreset)
            .filter(
                MatchingSettingsPreset.id == preset_id,
                MatchingSettingsPreset.user_id == user_id,
            )
            .first()
        )
        if not preset:
            return False
        db.delete(preset)
        db.commit()
        return True
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Task share links
# ---------------------------------------------------------------------------
