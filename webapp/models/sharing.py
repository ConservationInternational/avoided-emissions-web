"""Sharing, credentials, and authentication token models."""

import secrets
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from .base import Base


class TaskShareLink(Base):
    """Shareable link granting read-only access to a task's results.

    Authenticated users create these from the task detail page. Each link
    carries a unique URL-safe token and an expiration timestamp. Anyone
    with the link can view all results, plots, and downloads without
    logging in until the link expires or is explicitly revoked.
    """

    __tablename__ = "task_share_links"

    DEFAULT_EXPIRY_DAYS = 7

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_id = Column(
        UUID(as_uuid=True), ForeignKey("analysis_tasks.id"), nullable=False, index=True
    )
    token = Column(
        String(128),
        unique=True,
        nullable=False,
        default=lambda: secrets.token_urlsafe(48),
    )
    created_by = Column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True
    )
    created_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    expires_at = Column(DateTime(timezone=True), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    access_count = Column(Integer, default=0, nullable=False)
    last_accessed_at = Column(DateTime(timezone=True))

    task = relationship("AnalysisTask")
    creator = relationship("User")

    @property
    def is_valid(self):
        """Return True if the link is active and has not expired."""
        now = datetime.now(timezone.utc)
        return self.is_active and now < self.expires_at

    def record_access(self):
        """Increment access counter and update last-accessed timestamp."""
        self.access_count = (self.access_count or 0) + 1
        self.last_accessed_at = datetime.now(timezone.utc)

    @classmethod
    def get_valid_link(cls, token_string, db):
        """Look up a share link by token and return it only if still valid."""
        link = db.query(cls).filter(cls.token == token_string).first()
        if link and link.is_valid:
            return link
        return None


class TrendsEarthCredential(Base):
    """Stored OAuth2 client credentials for the trends.earth API.

    Each user may link their trends.earth account, which registers an
    OAuth2 service client on the API and stores the ``client_id`` and
    encrypted ``client_secret`` here.  The webapp uses these credentials
    to obtain short-lived access tokens on behalf of the user.
    """

    __tablename__ = "trendsearth_credentials"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, unique=True
    )
    # The trends.earth user email used to create the client
    te_email = Column(String(255), nullable=False)
    # The UUID of the user on the trends.earth API side
    te_user_id = Column(String(128), nullable=True, index=True)
    # OAuth2 client_id (public, non-secret)
    client_id = Column(String(128), nullable=False)
    # OAuth2 client_secret (encrypted with Fernet using SECRET_KEY)
    client_secret_encrypted = Column(Text, nullable=False)
    # Optional human-readable label used when registering the client
    client_name = Column(String(255), nullable=False, default="avoided-emissions-web")
    # The database UUID of the client on the API side (for revocation)
    api_client_db_id = Column(String(128))
    created_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    user = relationship(
        "User",
        backref="trendsearth_credential",
    )


class PasswordResetToken(Base):
    """Time-limited token for password reset requests.

    Tokens expire after 1 hour and can only be used once.
    """

    __tablename__ = "password_reset_tokens"

    TOKEN_EXPIRY_HOURS = 1

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True
    )
    token = Column(
        String(128),
        unique=True,
        nullable=False,
        default=lambda: secrets.token_urlsafe(64),
    )
    created_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    expires_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc) + timedelta(hours=1),
    )
    used_at = Column(DateTime(timezone=True))

    user = relationship("User")

    @property
    def is_valid(self):
        """Return True if the token has not expired and has not been used."""
        now = datetime.now(timezone.utc)
        return self.used_at is None and now < self.expires_at

    def mark_used(self):
        """Mark this token as consumed."""
        self.used_at = datetime.now(timezone.utc)

    @classmethod
    def get_valid_token(cls, token_string, db):
        """Look up a token and return it only if still valid."""
        reset_token = db.query(cls).filter(cls.token == token_string).first()
        if reset_token and reset_token.is_valid:
            return reset_token
        return None

    @classmethod
    def invalidate_user_tokens(cls, user_id, db):
        """Mark all existing tokens for a user as used."""
        now = datetime.now(timezone.utc)
        db.query(cls).filter(cls.user_id == user_id, cls.used_at.is_(None)).update(
            {cls.used_at: now}
        )
        db.flush()


class RefreshToken(Base):
    """Database-backed refresh token for persistent login.

    Each token has an absolute expiry (30 days) and an inactivity
    timeout (4 hours).  ``last_activity`` is updated on authenticated
    requests so the token stays valid while the user is active.
    """

    __tablename__ = "refresh_tokens"

    INACTIVITY_TIMEOUT_HOURS = 4
    ABSOLUTE_EXPIRY_DAYS = 30
    # Only update last_activity once per minute to reduce DB writes.
    ACTIVITY_UPDATE_INTERVAL_SECONDS = 60

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True
    )
    token_hash = Column(String(255), unique=True, nullable=False)
    created_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    expires_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc) + timedelta(days=30),
    )
    last_activity = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    revoked_at = Column(DateTime(timezone=True))

    user = relationship("User")

    @property
    def is_valid(self):
        """Return True if the token has not expired, not been revoked,
        and was used within the inactivity window."""
        now = datetime.now(timezone.utc)
        if self.revoked_at is not None:
            return False
        if now >= self.expires_at:
            return False
        inactivity_deadline = self.last_activity + timedelta(
            hours=self.INACTIVITY_TIMEOUT_HOURS
        )
        return now < inactivity_deadline

    def touch_activity(self):
        """Update last_activity if enough time has elapsed.

        Returns True if the timestamp was actually updated (caller
        should flush/commit), False if skipped to reduce DB writes.
        """
        now = datetime.now(timezone.utc)
        elapsed = (now - self.last_activity).total_seconds()
        if elapsed >= self.ACTIVITY_UPDATE_INTERVAL_SECONDS:
            self.last_activity = now
            return True
        return False

    def revoke(self):
        """Mark this token as revoked."""
        self.revoked_at = datetime.now(timezone.utc)

    @classmethod
    def create_token(cls, user_id, db):
        """Create a new refresh token for *user_id*.

        Returns ``(RefreshToken, plaintext_token)`` tuple.  The plaintext
        token is what gets stored in the cookie; only its SHA-256 hash
        is persisted in the database.
        """
        import hashlib

        plaintext = secrets.token_urlsafe(64)
        token_hash = hashlib.sha256(plaintext.encode()).hexdigest()
        refresh = cls(user_id=user_id, token_hash=token_hash)
        db.add(refresh)
        db.flush()
        return refresh, plaintext

    @classmethod
    def get_by_plaintext(cls, plaintext, db):
        """Look up a token by its plaintext value (hashes first)."""
        import hashlib

        token_hash = hashlib.sha256(plaintext.encode()).hexdigest()
        return db.query(cls).filter(cls.token_hash == token_hash).first()

    @classmethod
    def revoke_all_for_user(cls, user_id, db):
        """Revoke all active refresh tokens for a user."""
        now = datetime.now(timezone.utc)
        db.query(cls).filter(cls.user_id == user_id, cls.revoked_at.is_(None)).update(
            {cls.revoked_at: now}
        )
        db.flush()
