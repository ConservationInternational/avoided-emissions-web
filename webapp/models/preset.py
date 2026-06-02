"""User-saved covariate and matching settings preset models."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, ForeignKey, String
from sqlalchemy.dialects.postgresql import ARRAY, JSON, UUID
from sqlalchemy.orm import relationship
from sqlalchemy import Text

from .base import Base


class CovariatePreset(Base):
    """Named set of covariates that a user can save and restore."""

    __tablename__ = "covariate_presets"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True
    )
    name = Column(String(255), nullable=False)
    covariates = Column(ARRAY(Text), nullable=False)
    exact_match_vars = Column(ARRAY(Text), nullable=True)
    created_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    user = relationship("User", back_populates="covariate_presets")


class MatchingSettingsPreset(Base):
    """Named set of matching settings that a user can save and restore."""

    __tablename__ = "matching_settings_presets"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True
    )
    name = Column(String(255), nullable=False)
    settings = Column(JSON, nullable=False)
    created_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    user = relationship("User", back_populates="matching_settings_presets")
