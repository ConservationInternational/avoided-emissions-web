"""SQLAlchemy database models for the avoided emissions web application.

This package re-exports every model and utility so that all existing
``from models import X`` statements continue to work unchanged.
"""

# Import all submodules first so every model class is registered with
# Base.metadata before any mapper configuration or query runs.
from models.base import Base, SessionLocal, engine, get_db
from models.covariate import Covariate, GeeExportMetadata, ReferenceLayerExport
from models.preset import CovariatePreset, MatchingSettingsPreset
from models.sharing import (
    PasswordResetToken,
    RefreshToken,
    TaskShareLink,
    TrendsEarthCredential,
)
from models.site import UserSiteFeature, UserSiteSet, UserSiteUpload
from models.task import AnalysisTask, TaskResult, TaskResultTotal, TaskSite
from models.user import User
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
    "CovariatePreset",
    "Ecoregion",
    "GeeExportMetadata",
    # vector
    "GeoBoundaryADM0",
    "GeoBoundaryADM1",
    "GeoBoundaryADM2",
    "MatchingSettingsPreset",
    "PasswordResetToken",
    "ProtectedArea",
    "ReferenceLayerExport",
    "RefreshToken",
    "SessionLocal",
    "TaskResult",
    "TaskResultTotal",
    # sharing
    "TaskShareLink",
    "TaskSite",
    "TrendsEarthCredential",
    # user
    "User",
    "UserSiteFeature",
    # site
    "UserSiteSet",
    "UserSiteUpload",
    "VectorImportMetadata",
    "engine",
    "get_db",
]
