"""Credential management for stored trends.earth OAuth2 client secrets.

Uses Fernet symmetric encryption (from the ``cryptography`` package)
keyed by the application's ``SECRET_KEY`` to encrypt the client_secret
at rest in the database.
"""

import base64
import hashlib
import logging
from datetime import datetime, timezone

from cryptography.fernet import Fernet

from config import Config
from models import TrendsEarthCredential, get_db

logger = logging.getLogger(__name__)


def _fernet() -> Fernet:
    """Derive a Fernet key from ``Config.SECRET_KEY``."""
    # Fernet requires a 32-byte url-safe base64-encoded key.
    # Derive one deterministically from the app secret.
    key_bytes = hashlib.sha256(Config.SECRET_KEY.encode()).digest()
    return Fernet(base64.urlsafe_b64encode(key_bytes))


def encrypt_secret(raw_secret: str) -> str:
    """Encrypt *raw_secret* and return a base64-encoded ciphertext string."""
    return _fernet().encrypt(raw_secret.encode()).decode()


def decrypt_secret(encrypted: str) -> str:
    """Decrypt a previously encrypted secret."""
    return _fernet().decrypt(encrypted.encode()).decode()


# ------------------------------------------------------------------
# CRUD helpers
# ------------------------------------------------------------------


def get_credential(user_id) -> TrendsEarthCredential | None:
    """Return the stored credential for *user_id*, or ``None``."""
    db = get_db()
    try:
        return (
            db.query(TrendsEarthCredential)
            .filter(TrendsEarthCredential.user_id == user_id)
            .first()
        )
    finally:
        db.close()


def save_credential(
    user_id,
    te_email: str,
    client_id: str,
    client_secret: str,
    client_name: str = "avoided-emissions-web",
    api_client_db_id: str | None = None,
) -> TrendsEarthCredential:
    """Store (or replace) the user's trends.earth OAuth2 credential.

    The *client_secret* is encrypted before being written to the database.
    """
    encrypted = encrypt_secret(client_secret)
    db = get_db()
    try:
        existing = (
            db.query(TrendsEarthCredential)
            .filter(TrendsEarthCredential.user_id == user_id)
            .first()
        )
        if existing:
            existing.te_email = te_email
            existing.client_id = client_id
            existing.client_secret_encrypted = encrypted
            existing.client_name = client_name
            existing.api_client_db_id = api_client_db_id
            existing.updated_at = datetime.now(timezone.utc)
            db.commit()
            db.refresh(existing)
            return existing

        cred = TrendsEarthCredential(
            user_id=user_id,
            te_email=te_email,
            client_id=client_id,
            client_secret_encrypted=encrypted,
            client_name=client_name,
            api_client_db_id=api_client_db_id,
        )
        db.add(cred)
        db.commit()
        db.refresh(cred)
        return cred
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def delete_credential(user_id) -> bool:
    """Delete the stored credential for *user_id*.  Returns True if deleted."""
    db = get_db()
    try:
        cred = (
            db.query(TrendsEarthCredential)
            .filter(TrendsEarthCredential.user_id == user_id)
            .first()
        )
        if cred is None:
            return False
        db.delete(cred)
        db.commit()
        return True
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def get_decrypted_secret(user_id) -> tuple[str, str] | None:
    """Return ``(client_id, client_secret)`` for *user_id*, or ``None``.

    The client_secret is decrypted from the database.
    """
    cred = get_credential(user_id)
    if cred is None:
        return None
    try:
        secret = decrypt_secret(cred.client_secret_encrypted)
        return cred.client_id, secret
    except Exception:
        logger.exception("Failed to decrypt credential for user %s", user_id)
        return None
