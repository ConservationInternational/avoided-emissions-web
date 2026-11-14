"""User-saved matching preset model."""

import uuid
from datetime import UTC, datetime

from sqlalchemy import Column, DateTime, Index, String, Text
from sqlalchemy.dialects.postgresql import ARRAY, JSON, UUID

from .base import Base


class MatchingPreset(Base):
    """Named covariate selection and matching settings saved together."""

    __tablename__ = "matching_presets"
    __table_args__ = (
        Index("ix_matching_presets_user_name", "user_id", "name", unique=True),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    covariates = Column(ARRAY(Text), nullable=False)
    exact_match_vars = Column(ARRAY(Text), nullable=True)
    settings = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
