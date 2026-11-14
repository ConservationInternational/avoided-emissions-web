"""SQLAlchemy database models for the avoided emissions web application.

This package re-exports every model and utility so that all existing
``from models import X`` statements continue to work unchanged.
"""

# Import all submodules first so every model class is registered with
# Base.metadata before any mapper configuration or query runs.
from models.base import Base, SessionLocal, engine, get_db
from models.covariate import (
    Covariate,
    GeeExportMetadata,
    ReferenceLayerExport,
    ReferenceRegionPiece,
)
from models.preset import MatchingPreset
from models.sharing import TaskShareLink
from models.site import UserSiteFeature, UserSiteSet, UserSiteUpload
from models.task import AnalysisTask, TaskResult, TaskResultTotal, TaskSite
from models.vector import (
    Ecoregion,
    GeoBoundaryADM0,
    GeoBoundaryADM1,
    GeoBoundaryADM2,
    ProtectedArea,
    VectorImportMetadata,
)

__all__ = [
    # task
    "AnalysisTask",
    # base
    "Base",
    # covariate
    "Covariate",
    # preset
    "Ecoregion",
    "GeeExportMetadata",
    # vector
    "GeoBoundaryADM0",
    "GeoBoundaryADM1",
    "GeoBoundaryADM2",
    "MatchingPreset",
    "ProtectedArea",
    "ReferenceLayerExport",
    "ReferenceRegionPiece",
    "SessionLocal",
    "TaskResult",
    "TaskResultTotal",
    # sharing
    "TaskShareLink",
    "TaskSite",
    "UserSiteFeature",
    # site
    "UserSiteSet",
    "UserSiteUpload",
    "VectorImportMetadata",
    "engine",
    "get_db",
]
