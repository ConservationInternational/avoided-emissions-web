"""User site set, feature, and upload models."""

import uuid
from datetime import datetime, timezone

from geoalchemy2 import Geometry
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import relationship

from .base import Base


class UserSiteSet(Base):
    """User-uploaded site collections stored in PostGIS for reuse."""

    __tablename__ = "user_site_sets"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True
    )
    name = Column(String(255), nullable=False)
    original_filename = Column(String(500), nullable=False)
    file_size_bytes = Column(BigInteger, nullable=False)
    file_format = Column(String(20), nullable=False)
    uploaded_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    n_sites = Column(Integer, nullable=False)
    bounds = Column(JSON)
    extra_metadata = Column("metadata", JSON, default=dict)
    is_archived = Column(Boolean, default=False, nullable=False, server_default="false")

    user = relationship("User", back_populates="site_sets")
    sites = relationship(
        "UserSiteFeature", back_populates="site_set", cascade="all, delete-orphan"
    )
    tasks = relationship("AnalysisTask", back_populates="site_set")


class UserSiteFeature(Base):
    """Individual site polygons belonging to a :class:`UserSiteSet`."""

    __tablename__ = "user_site_features"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    site_set_id = Column(
        UUID(as_uuid=True),
        ForeignKey("user_site_sets.id"),
        nullable=False,
        index=True,
    )
    site_id = Column(String(100), nullable=False)
    site_name = Column(String(255), nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date)
    area_ha = Column(Float)
    geom = Column(
        Geometry("MULTIPOLYGON", srid=4326, spatial_index=True), nullable=False
    )

    site_set = relationship("UserSiteSet", back_populates="sites")


class UserSiteUpload(Base):
    """Background site-upload job metadata for asynchronous imports.

    Stores the owning ``user_id``, source ``original_filename``, Celery
    ``celery_task_id``, optional resulting ``site_set_id`` / ``site_set_name``,
    lifecycle ``status``, timestamps, and any extra metadata needed to render
    upload status in the admin UI.
    """

    __tablename__ = "user_site_uploads"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True
    )
    original_filename = Column(String(500), nullable=False)
    celery_task_id = Column(String(255), index=True)
    site_set_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    site_set_name = Column(String(255))
    n_features = Column(Integer)
    n_sites_imported = Column(Integer)
    status = Column(
        Enum(
            "pending",
            "running",
            "completed",
            "failed",
            "cancelled",
            name="site_upload_status",
        ),
        nullable=False,
        default="pending",
    )
    created_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    error_message = Column(Text)
    extra_metadata = Column("metadata", JSON, default=dict)

    user = relationship("User", back_populates="site_uploads")
