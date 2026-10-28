"""Task share-link model."""

import secrets
import uuid
from datetime import UTC, datetime

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String
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
    created_by = Column(UUID(as_uuid=True), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    expires_at = Column(DateTime(timezone=True), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    access_count = Column(Integer, default=0, nullable=False)
    last_accessed_at = Column(DateTime(timezone=True))

    task = relationship("AnalysisTask")

    @property
    def is_valid(self):
        """Return True if the link is active and has not expired."""
        now = datetime.now(UTC)
        return self.is_active and now < self.expires_at

    def record_access(self):
        """Increment access counter and update last-accessed timestamp."""
        self.access_count = (self.access_count or 0) + 1
        self.last_accessed_at = datetime.now(UTC)

    @classmethod
    def get_valid_link(cls, token_string, db):
        """Look up a share link by token and return it only if still valid."""
        link = db.query(cls).filter(cls.token == token_string).first()
        if link and link.is_valid:
            return link
        return None
