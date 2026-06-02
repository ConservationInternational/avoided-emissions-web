"""Analysis task, site, and result models."""

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
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSON, UUID
from sqlalchemy.orm import relationship

from .base import Base


class AnalysisTask(Base):
    __tablename__ = "analysis_tasks"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    submitted_by = Column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True
    )
    site_set_id = Column(
        UUID(as_uuid=True), ForeignKey("user_site_sets.id"), index=True
    )
    status = Column(
        Enum(
            "pending",
            "submitting",
            "submitted",
            "running",
            "succeeded",
            "failed",
            "cancelled",
            name="task_status",
        ),
        nullable=False,
        default="pending",
    )
    extract_job_id = Column(String(255), index=True)
    match_job_id = Column(String(255))
    summarize_job_id = Column(String(255))
    config = Column(JSON, nullable=False, default=dict)
    covariates = Column(ARRAY(Text), nullable=False)
    n_sites = Column(Integer)
    sites_s3_uri = Column(String(500))
    config_s3_uri = Column(String(500))
    results_s3_uri = Column(String(500))
    created_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    submitted_at = Column(DateTime(timezone=True))
    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    error_message = Column(Text)
    extra_metadata = Column("metadata", JSON, default=dict)

    user = relationship("User", back_populates="tasks")
    site_set = relationship("UserSiteSet", back_populates="tasks")
    sites = relationship(
        "TaskSite", back_populates="task", cascade="all, delete-orphan"
    )
    results = relationship(
        "TaskResult", back_populates="task", cascade="all, delete-orphan"
    )
    results_total = relationship(
        "TaskResultTotal", back_populates="task", cascade="all, delete-orphan"
    )


class TaskSite(Base):
    __tablename__ = "task_sites"
    __table_args__ = (
        UniqueConstraint(
            "task_id",
            "site_id",
            "sub_site_index",
            name="task_sites_task_id_site_id_sub_site_index_key",
        ),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_id = Column(
        UUID(as_uuid=True),
        ForeignKey("analysis_tasks.id"),
        nullable=False,
        index=True,
    )
    site_id = Column(String(100), nullable=False)
    site_name = Column(String(255))
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    area_ha = Column(Float)
    # Sub-site support for sites spanning multiple exact-match groups
    sub_site_index = Column(Integer, default=0, nullable=False)
    is_sub_site = Column(Boolean, default=False, nullable=False)
    original_area_ha = Column(Float, nullable=True)

    task = relationship("AnalysisTask", back_populates="sites")


class TaskResult(Base):
    __tablename__ = "task_results"
    __table_args__ = (
        UniqueConstraint(
            "task_id",
            "site_id",
            "sub_site_index",
            "year",
            name="task_results_task_id_site_id_sub_site_index_year_key",
        ),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_id = Column(
        UUID(as_uuid=True),
        ForeignKey("analysis_tasks.id"),
        nullable=False,
        index=True,
    )
    site_id = Column(String(100), nullable=False)
    # 0 = no sub-site (ordinary site); 1+ = sub-site index within a
    # cross-site grouping group (set by step 2 of the R analysis).
    sub_site_index = Column(Integer, nullable=False, default=0)
    year = Column(Integer, nullable=False)
    extrapolated_forest_loss_avoided_ha = Column(Float)
    extrapolated_emissions_avoided_mgco2e = Column(Float)
    extrapolated_treatment_defor_ha = Column(Float)
    extrapolated_control_defor_ha = Column(Float)
    extrapolated_treatment_emissions_mgco2e = Column(Float)
    extrapolated_control_emissions_mgco2e = Column(Float)
    is_pre_intervention = Column(Boolean, default=False)
    is_post_intervention = Column(Boolean, default=False)
    n_sample_pixels = Column(Integer)
    sampled_fraction = Column(Float)

    # Confidence interval bounds (populated when n_replicates > 1)
    extrapolated_treatment_defor_ha_ci_lower = Column(Float)
    extrapolated_treatment_defor_ha_ci_upper = Column(Float)
    extrapolated_control_defor_ha_ci_lower = Column(Float)
    extrapolated_control_defor_ha_ci_upper = Column(Float)
    extrapolated_forest_loss_avoided_ha_ci_lower = Column(Float)
    extrapolated_forest_loss_avoided_ha_ci_upper = Column(Float)
    extrapolated_treatment_emissions_mgco2e_ci_lower = Column(Float)
    extrapolated_treatment_emissions_mgco2e_ci_upper = Column(Float)
    extrapolated_control_emissions_mgco2e_ci_lower = Column(Float)
    extrapolated_control_emissions_mgco2e_ci_upper = Column(Float)
    extrapolated_emissions_avoided_mgco2e_ci_lower = Column(Float)
    extrapolated_emissions_avoided_mgco2e_ci_upper = Column(Float)

    task = relationship("AnalysisTask", back_populates="results")


class TaskResultTotal(Base):
    __tablename__ = "task_results_total"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_id = Column(
        UUID(as_uuid=True),
        ForeignKey("analysis_tasks.id"),
        nullable=False,
        index=True,
    )
    site_id = Column(String(100), nullable=False)
    site_name = Column(String(255))
    extrapolated_forest_loss_avoided_ha = Column(Float)
    extrapolated_emissions_avoided_mgco2e = Column(Float)
    area_ha = Column(Float)
    n_sample_pixels = Column(Integer)
    n_treatment_pixels = Column(Integer)
    sampled_fraction = Column(Float)
    first_year = Column(Integer)
    last_year = Column(Integer)
    n_years = Column(Integer)

    # Confidence interval bounds (populated when n_replicates > 1)
    extrapolated_forest_loss_avoided_ha_ci_lower = Column(Float)
    extrapolated_forest_loss_avoided_ha_ci_upper = Column(Float)
    extrapolated_emissions_avoided_mgco2e_ci_lower = Column(Float)
    extrapolated_emissions_avoided_mgco2e_ci_upper = Column(Float)

    task = relationship("AnalysisTask", back_populates="results_total")
