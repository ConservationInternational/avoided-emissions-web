"""Server-side storage for Trends.Earth OIDC sessions.

Tokens never reach the browser.  The Flask session cookie only carries an
opaque session id; the record holding the user's identity and Trends.Earth
tokens lives in Redis so that both the web process and the Celery workers
(which submit analyses on the user's behalf) can use it.

Trends.Earth rotates refresh tokens on every use and revokes the whole token
family if a rotated token is presented again, so refreshes are serialised
per session with a Redis lock that is shared across processes.
"""

import json
import logging
import secrets
import time

import redis
from config import Config

logger = logging.getLogger(__name__)

_KEY_PREFIX = "ae:sess:"
# Refresh when the access token has less than this many seconds left.
REFRESH_MARGIN_SECONDS = 30
_LOCK_TIMEOUT_SECONDS = 20
_LOCK_WAIT_SECONDS = 15

_client = None


class SessionExpired(Exception):
    """The session is gone or Trends.Earth no longer honours its tokens."""


class SessionUnavailable(Exception):
    """Session storage or the identity provider is temporarily unavailable."""


def get_redis():
    global _client
    if _client is None:
        _client = redis.Redis.from_url(Config.SESSION_REDIS_URL)
    return _client


def set_redis(client):
    """Override the Redis client (used by tests)."""
    global _client
    _client = client


def _key(sid):
    return f"{_KEY_PREFIX}{sid}"


def create(record):
    """Store a new session record and return its opaque id."""
    sid = secrets.token_urlsafe(32)
    record = dict(record)
    record["created_at"] = int(time.time())
    try:
        get_redis().set(_key(sid), json.dumps(record), ex=Config.OIDC_SESSION_MAX_AGE)
    except redis.RedisError as exc:
        raise SessionUnavailable("Session storage is unavailable") from exc
    return sid


def get(sid):
    """Return the session record for *sid*, or ``None``."""
    if not sid:
        return None
    try:
        raw = get_redis().get(_key(sid))
    except redis.RedisError as exc:
        raise SessionUnavailable("Session storage is unavailable") from exc
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        logger.warning("Discarding unreadable session record")
        delete(sid)
        return None


def _save(sid, record):
    client = get_redis()
    ttl = client.ttl(_key(sid))
    if ttl is None or ttl <= 0:
        ttl = Config.OIDC_SESSION_MAX_AGE
    client.set(_key(sid), json.dumps(record), ex=ttl)


def delete(sid):
    if not sid:
        return
    try:
        get_redis().delete(_key(sid))
    except redis.RedisError:
        logger.warning("Could not delete session record", exc_info=True)


def _needs_refresh(record, margin):
    return int(record.get("access_expires_at") or 0) - margin <= time.time()


def ensure_fresh(sid, margin=REFRESH_MARGIN_SECONDS):
    """Return a session record whose access token is valid for *margin* s.

    Refreshes through Trends.Earth when needed.  Raises
    :class:`SessionExpired` when the session no longer exists or the
    provider refuses to refresh it (account disabled, app access removed,
    refresh token revoked), and :class:`SessionUnavailable` for transient
    failures that should not end the session.
    """
    record = get(sid)
    if record is None:
        raise SessionExpired("Session not found")
    if not _needs_refresh(record, margin):
        return record

    client = get_redis()
    lock = client.lock(
        f"{_key(sid)}:lock",
        timeout=_LOCK_TIMEOUT_SECONDS,
        blocking_timeout=_LOCK_WAIT_SECONDS,
    )
    try:
        acquired = lock.acquire()
    except redis.RedisError as exc:
        raise SessionUnavailable("Session storage is unavailable") from exc
    if not acquired:
        raise SessionUnavailable("Timed out waiting for a session refresh")
    try:
        # Another process may have refreshed while we waited for the lock.
        record = get(sid)
        if record is None:
            raise SessionExpired("Session not found")
        if not _needs_refresh(record, margin):
            return record

        import oidc

        try:
            tokens = oidc.refresh(record["refresh_token"])
            claims = oidc.verify_access_token(tokens["access_token"])
        except oidc.OIDCTokenRejected as exc:
            logger.info("Trends.Earth refused to refresh session: %s", exc)
            delete(sid)
            raise SessionExpired(str(exc)) from exc
        except oidc.OIDCProviderUnavailable as exc:
            raise SessionUnavailable(str(exc)) from exc

        record.update(oidc.session_fields_from_access(tokens, claims))
        try:
            _save(sid, record)
        except redis.RedisError as exc:
            raise SessionUnavailable("Session storage is unavailable") from exc
        return record
    finally:
        try:
            lock.release()
        except redis.exceptions.LockError:
            pass
        except redis.RedisError:
            logger.warning("Could not release session lock", exc_info=True)


def get_access_token(sid, margin=REFRESH_MARGIN_SECONDS):
    """Return a currently valid Trends.Earth access token for *sid*."""
    return ensure_fresh(sid, margin=margin)["access_token"]
