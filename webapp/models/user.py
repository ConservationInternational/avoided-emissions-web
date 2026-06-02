"""User account model."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, Enum, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from .base import Base


class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    name = Column(String(255), nullable=False)
    role = Column(
        Enum("admin", "user", name="user_role"),
        nullable=False,
        default="user",
    )
    created_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    last_login = Column(DateTime(timezone=True))
    is_active = Column(Boolean, default=True)
    is_approved = Column(Boolean, default=False)

    tasks = relationship("AnalysisTask", back_populates="user")
    site_sets = relationship(
        "UserSiteSet", back_populates="user", cascade="all, delete-orphan"
    )
    site_uploads = relationship(
        "UserSiteUpload", back_populates="user", cascade="all, delete-orphan"
    )
    covariate_presets = relationship(
        "CovariatePreset", back_populates="user", cascade="all, delete-orphan"
    )
    matching_settings_presets = relationship(
        "MatchingSettingsPreset",
        back_populates="user",
        cascade="all, delete-orphan",
    )

    @property
    def is_admin(self):
        return self.role == "admin"
