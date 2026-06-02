"""Covariate lifecycle and GEE export metadata models."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    Column,
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


class Covariate(Base):
    """Unified covariate lifecycle tracking.

    Each row tracks a covariate through export (GEE → GCS) and merge
    (GCS tiles → single COG on S3).  Multiple rows per covariate are
    allowed to preserve history; the inventory view uses the most recent.
    """

    __tablename__ = "covariates"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    covariate_name = Column(String(100), nullable=False)
    resolution_m = Column(Integer, nullable=False, default=1000)

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
            "rasterizing",
            "failed",
            "cancelled",
            name="covariate_status",
        ),
        nullable=False,
        default="pending_export",
    )
    started_by = Column(UUID(as_uuid=True), ForeignKey("users.id"), index=True)
    started_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    completed_at = Column(DateTime(timezone=True))
    error_message = Column(Text)
    extra_metadata = Column("metadata", JSON, default=dict)

    export_snapshots = relationship(
        "GeeExportMetadata", back_populates="covariate", cascade="all, delete-orphan"
    )


class GeeExportMetadata(Base):
    """Snapshot of GEE-exported tiles on GCS and their merge into a COG.

    Each row captures the state of the GCS tiles for a single covariate
    at a point in time.  Comparing :attr:`tile_etag_hash` between
    snapshots lets the system determine whether tiles have changed
    since the last merge, avoiding redundant work.

    The ``tile_details`` JSON column stores per-tile metadata returned
    by the GCS JSON API (name, etag, size, md5Hash, updated), while
    ``tile_etag_hash`` is a compact SHA-256 fingerprint of those values
    suitable for quick equality checks.
    """

    __tablename__ = "gee_export_metadata"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    covariate_id = Column(
        UUID(as_uuid=True), ForeignKey("covariates.id"), nullable=True, index=True
    )
    covariate_name = Column(String(100), nullable=False, index=True)

    # GCS tile snapshot
    gcs_bucket = Column(String(255))
    gcs_prefix = Column(String(500))
    tile_count = Column(Integer)
    tile_total_bytes = Column(BigInteger)
    # JSON list of {name, etag, size_bytes, md5, updated} per tile
    tile_details = Column(JSON)
    # SHA-256 fingerprint of sorted (name, etag, size_bytes) tuples
    tile_etag_hash = Column(String(64), index=True)
    tiles_detected_at = Column(DateTime(timezone=True))

    # GEE export info (populated when the originating GEE task is known)
    gee_task_id = Column(String(255))
    gee_completed_at = Column(DateTime(timezone=True))

    # Merge lifecycle
    merge_started_at = Column(DateTime(timezone=True))
    merge_completed_at = Column(DateTime(timezone=True))
    merge_duration_seconds = Column(Float)

    # Merged COG on S3
    merged_cog_key = Column(String(1000))
    merged_cog_url = Column(String(1000))
    merged_cog_bytes = Column(BigInteger)
    merged_cog_etag = Column(String(255))

    # Status
    status = Column(
        Enum(
            "detected",
            "pending_merge",
            "merging",
            "merged",
            "skipped_existing",
            "failed",
            name="gee_export_meta_status",
        ),
        nullable=False,
        default="detected",
    )
    error_message = Column(Text)
    created_at = Column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    covariate = relationship("Covariate", back_populates="export_snapshots")


class ReferenceLayerExport(Base):
    """Tracks S3 GeoParquet exports of PostGIS reference layers.

    One row per layer name (unique constraint).  Updated in place each
    time the export task re-runs so there is at most one current
    artifact per layer.
    """

    __tablename__ = "reference_layer_exports"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # Matches the keys of _EXTENT_TABLE_MAP in services.py
    # (e.g. "admin0", "admin1", "admin2", "ecoregion")
    layer_name = Column(String(100), nullable=False, unique=True, index=True)
    s3_uri = Column(String(500), nullable=False)
    feature_count = Column(Integer)
    # Monotonically increasing schema version — bump when the GeoParquet
    # schema changes so downstream consumers can detect stale artifacts.
    schema_version = Column(Integer, nullable=False, default=1)
    exported_at = Column(DateTime(timezone=True), nullable=False)
