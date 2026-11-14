"""User-saved matching preset management."""

from datetime import UTC, datetime

from models import MatchingPreset, get_db


def get_matching_presets(user_id):
    """Return the user's named covariate selections and matching settings."""
    db = get_db()
    try:
        presets = (
            db.query(MatchingPreset)
            .filter(MatchingPreset.user_id == user_id)
            .order_by(MatchingPreset.name)
            .all()
        )
        return [
            {
                "id": str(p.id),
                "name": p.name,
                "covariates": list(p.covariates),
                "exact_match_vars": list(p.exact_match_vars or []),
                "settings": dict(p.settings),
            }
            for p in presets
        ]
    finally:
        db.close()


def save_matching_preset(user_id, name, covariates, exact_match_vars, settings):
    """Save both selections and settings, replacing the user's same-named preset.

    Returns the preset ``id`` as a string.
    """
    db = get_db()
    try:
        preset = (
            db.query(MatchingPreset)
            .filter(
                MatchingPreset.user_id == user_id,
                MatchingPreset.name == name,
            )
            .first()
        )
        if preset is None:
            preset = MatchingPreset(user_id=user_id, name=name)
            db.add(preset)

        preset.covariates = list(covariates)
        preset.exact_match_vars = list(exact_match_vars or [])
        preset.settings = dict(settings)
        preset.updated_at = datetime.now(UTC)
        db.commit()
        return str(preset.id)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def delete_matching_preset(preset_id, user_id):
    """Delete an owned preset, returning whether a row was deleted."""
    db = get_db()
    try:
        preset = (
            db.query(MatchingPreset)
            .filter(
                MatchingPreset.id == preset_id,
                MatchingPreset.user_id == user_id,
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
