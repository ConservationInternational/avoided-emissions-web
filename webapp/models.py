"""SQLAlchemy database models for the avoided emissions web application."""

import uuid
from datetime import datetime, timezone

from geoalchemy2 import Geometry
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
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
    covariate_presets = relationship(
        "CovariatePreset", back_populates="user", cascade="all, delete-orphan"
    )

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


class CovariatePreset(Base):
    """Named set of covariates that a user can save and restore."""

    __tablename__ = "covariate_presets"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    name = Column(String(255), nullable=False)
    covariates = Column(ARRAY(Text), nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    user = relationship("User", back_populates="covariate_presets")


# ---------------------------------------------------------------------------
# Vector reference data tables (PostGIS)
# ---------------------------------------------------------------------------


class GeoBoundaryADM0(Base):
    """Country-level administrative boundaries from geoBoundaries CGAZ."""

    __tablename__ = "geoboundaries_adm0"

    id = Column(Integer, primary_key=True, autoincrement=True)
    shape_group = Column(String(10), nullable=False, index=True)
    shape_name = Column(String(255), nullable=False)
    shape_iso = Column(String(10))
    shape_id = Column(String(100))
    shape_type = Column(String(20))
    geom = Column(
        Geometry("MULTIPOLYGON", srid=4326, spatial_index=True), nullable=False
    )


class GeoBoundaryADM1(Base):
    """First-level administrative boundaries from geoBoundaries CGAZ."""

    __tablename__ = "geoboundaries_adm1"

    id = Column(Integer, primary_key=True, autoincrement=True)
    shape_group = Column(String(10), nullable=False, index=True)
    shape_name = Column(String(255), nullable=False)
    shape_iso = Column(String(10))
    shape_id = Column(String(100))
    shape_type = Column(String(20))
    geom = Column(
        Geometry("MULTIPOLYGON", srid=4326, spatial_index=True), nullable=False
    )


class GeoBoundaryADM2(Base):
    """Second-level administrative boundaries from geoBoundaries CGAZ."""

    __tablename__ = "geoboundaries_adm2"

    id = Column(Integer, primary_key=True, autoincrement=True)
    shape_group = Column(String(10), nullable=False, index=True)
    shape_name = Column(String(255), nullable=False)
    shape_iso = Column(String(10))
    shape_id = Column(String(100))
    shape_type = Column(String(20))
    geom = Column(
        Geometry("MULTIPOLYGON", srid=4326, spatial_index=True), nullable=False
    )


class Ecoregion(Base):
    """RESOLVE ecoregions (2017)."""

    __tablename__ = "ecoregions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    eco_id = Column(Integer, nullable=False, index=True)
    eco_name = Column(String(255))
    biome_num = Column(Integer)
    biome_name = Column(String(255))
    realm = Column(String(100))
    nnh = Column(Float)
    color = Column(String(10))
    color_bio = Column(String(10))
    color_nnh = Column(String(10))
    area_km2 = Column(Float)
    geom = Column(
        Geometry("MULTIPOLYGON", srid=4326, spatial_index=True), nullable=False
    )


class ProtectedArea(Base):
    """WDPA protected areas."""

    __tablename__ = "wdpa"

    id = Column(Integer, primary_key=True, autoincrement=True)
    wdpaid = Column(Integer, nullable=False, index=True)
    name = Column(String(500))
    orig_name = Column(String(500))
    desig = Column(String(500))
    desig_type = Column(String(100))
    iucn_cat = Column(String(20))
    int_crit = Column(String(100))
    marine = Column(String(10))
    rep_m_area = Column(Float)
    gis_m_area = Column(Float)
    rep_area = Column(Float)
    gis_area = Column(Float)
    no_take = Column(String(50))
    no_tk_area = Column(Float)
    status = Column(String(100))
    status_yr = Column(Integer)
    gov_type = Column(String(255))
    own_type = Column(String(100))
    mang_auth = Column(String(500))
    mang_plan = Column(String(500))
    verif = Column(String(100))
    iso3 = Column(String(10), index=True)
    parent_iso3 = Column(String(10))
    geom = Column(
        Geometry("MULTIPOLYGON", srid=4326, spatial_index=True), nullable=False
    )


# Database session management
engine = create_engine(Config.DATABASE_URL)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


def get_db() -> Session:
    """Get a database session. Caller must close it."""
    return SessionLocal()
