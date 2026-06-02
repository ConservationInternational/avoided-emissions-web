"""PostGIS vector reference data models (boundaries, ecoregions, protected areas)."""

from geoalchemy2 import Geometry
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Float,
    Integer,
    String,
    Text,
)

from .base import Base


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
    """WDPA protected areas.

    Column names match the WDPA Feb 2026 GDB field names (lowercased).
    """

    __tablename__ = "wdpa"

    id = Column(Integer, primary_key=True, autoincrement=True)
    site_id = Column(Integer, nullable=False, index=True)
    site_pid = Column(String(100))
    site_type = Column(String(50))
    name_eng = Column(String(500))
    name = Column(String(500))
    desig = Column(String(500))
    desig_eng = Column(String(500))
    desig_type = Column(String(100))
    iucn_cat = Column(String(20))
    int_crit = Column(String(100))
    realm = Column(String(50))
    rep_m_area = Column(Float)
    gis_m_area = Column(Float)
    rep_area = Column(Float)
    gis_area = Column(Float)
    no_take = Column(String(50))
    no_tk_area = Column(Float)
    status = Column(String(100))
    status_yr = Column(Integer)
    gov_type = Column(String(255))
    govsubtype = Column(String(255))
    own_type = Column(String(100))
    ownsubtype = Column(String(255))
    mang_auth = Column(String(500))
    mang_plan = Column(String(500))
    verif = Column(String(100))
    metadataid = Column(Integer)
    prnt_iso3 = Column(String(100))
    iso3 = Column(String(100), index=True)
    supp_info = Column(Text)
    cons_obj = Column(Text)
    inlnd_wtrs = Column(String(50))
    oecm_asmt = Column(String(50))
    geom = Column(
        Geometry("MULTIPOLYGON", srid=4326, spatial_index=True), nullable=False
    )


class VectorImportMetadata(Base):
    """Tracks completion and provenance of vector reference data imports.

    Each row records whether a particular vector table was fully imported,
    along with details about the source file and timing.  The import
    pipeline uses this to detect incomplete imports (e.g. after an OOM
    kill) and automatically truncate & retry.
    """

    __tablename__ = "_vector_import_metadata"

    table_name = Column(String(100), primary_key=True)
    row_count = Column(Integer, nullable=False)
    source_url = Column(Text)
    source_filename = Column(String(500))
    file_size_bytes = Column(BigInteger)
    import_duration_seconds = Column(Float)
    started_at = Column(
        DateTime(timezone=True),
    )
    completed_at = Column(
        DateTime(timezone=True),
    )
