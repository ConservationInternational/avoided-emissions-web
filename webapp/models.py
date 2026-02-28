"""SQLAlchemy database models for the avoided emissions web application."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    create_engine,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSON, UUID
from sqlalchemy.orm import DeclarativeBase, Session, relationship, sessionmaker

from config import Config


class Base(DeclarativeBase):
    pass


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
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    last_login = Column(DateTime(timezone=True))
    is_active = Column(Boolean, default=True)
    is_approved = Column(Boolean, default=False)

    tasks = relationship("AnalysisTask", back_populates="user")

    @property
    def is_admin(self):
        return self.role == "admin"


class Covariate(Base):
    """Unified covariate lifecycle tracking.

    Each row tracks a covariate through export (GEE → GCS) and merge
    (GCS tiles → single COG on S3).  Multiple rows per covariate are
    allowed to preserve history; the inventory view uses the most recent.
    """

    __tablename__ = "covariates"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    covariate_name = Column(String(100), nullable=False)

    # GEE export fields
    gee_task_id = Column(String(255))
    gcs_bucket = Column(String(255))
    gcs_prefix = Column(String(500))

    # COG merge / output fields
    output_bucket = Column(String(255))
    output_prefix = Column(String(500))
    n_tiles = Column(Integer)
    merged_url = Column(String(1000))
    size_bytes = Column(Float)

    # Lifecycle
    status = Column(
        Enum(
            "pending_export",
            "exporting",
            "exported",
            "pending_merge",
            "merging",
            "merged",
            "failed",
            "cancelled",
            name="covariate_status",
        ),
        nullable=False,
        default="pending_export",
    )
    started_by = Column(UUID(as_uuid=True), ForeignKey("users.id"))
    started_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    completed_at = Column(DateTime(timezone=True))
    error_message = Column(Text)
    extra_metadata = Column("metadata", JSON, default=dict)


class AnalysisTask(Base):
    __tablename__ = "analysis_tasks"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    submitted_by = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    status = Column(
        Enum("pending", "submitted", "running", "succeeded", "failed",
             "cancelled", name="task_status"),
        nullable=False,
        default="pending",
    )
    extract_job_id = Column(String(255))
    match_job_id = Column(String(255))
    summarize_job_id = Column(String(255))
    config = Column(JSON, nullable=False, default=dict)
    covariates = Column(ARRAY(Text), nullable=False)
    n_sites = Column(Integer)
    sites_s3_uri = Column(String(500))
    config_s3_uri = Column(String(500))
    results_s3_uri = Column(String(500))
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    submitted_at = Column(DateTime(timezone=True))
    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    error_message = Column(Text)
    extra_metadata = Column("metadata", JSON, default=dict)

    user = relationship("User", back_populates="tasks")
    sites = relationship("TaskSite", back_populates="task",
                         cascade="all, delete-orphan")
    results = relationship("TaskResult", back_populates="task",
                           cascade="all, delete-orphan")
    results_total = relationship("TaskResultTotal", back_populates="task",
                                 cascade="all, delete-orphan")


class TaskSite(Base):
    __tablename__ = "task_sites"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_id = Column(UUID(as_uuid=True), ForeignKey("analysis_tasks.id"), nullable=False)
    site_id = Column(String(100), nullable=False)
    site_name = Column(String(255))
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    area_ha = Column(Float)

    task = relationship("AnalysisTask", back_populates="sites")


class TaskResult(Base):
    __tablename__ = "task_results"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_id = Column(UUID(as_uuid=True), ForeignKey("analysis_tasks.id"), nullable=False)
    site_id = Column(String(100), nullable=False)
    year = Column(Integer, nullable=False)
    forest_loss_avoided_ha = Column(Float)
    emissions_avoided_mgco2e = Column(Float)
    n_matched_pixels = Column(Integer)
    sampled_fraction = Column(Float)

    task = relationship("AnalysisTask", back_populates="results")


class TaskResultTotal(Base):
    __tablename__ = "task_results_total"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_id = Column(UUID(as_uuid=True), ForeignKey("analysis_tasks.id"), nullable=False)
    site_id = Column(String(100), nullable=False)
    site_name = Column(String(255))
    forest_loss_avoided_ha = Column(Float)
    emissions_avoided_mgco2e = Column(Float)
    area_ha = Column(Float)
    n_matched_pixels = Column(Integer)
    sampled_fraction = Column(Float)
    first_year = Column(Integer)
    last_year = Column(Integer)
    n_years = Column(Integer)

    task = relationship("AnalysisTask", back_populates="results_total")


# Database session management
engine = create_engine(Config.DATABASE_URL)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


def get_db() -> Session:
    """Get a database session. Caller must close it."""
    return SessionLocal()
