"""
Avoided emissions analysis schemas.

This module defines dataclass schemas for the avoided deforestation /
avoided emissions analysis pipeline.  The pipeline has three steps:

1. **Extract** - extract covariate raster values for treatment (site)
   and control pixels.
2. **Match** - propensity-score matching of treatment to control pixels.
3. **Summarise** - compute forest-cover trajectories for matched pairs
   and derive avoided emissions (MgCO2e).

These schemas are used:
* As the ``params`` payload when submitting an avoided-emissions execution
  to the Trends.Earth API.
* As the ``results`` payload returned when an execution finishes.

They deliberately mirror the JSON structures already consumed /
produced by the R scripts in ``scripts/``.
"""

import dataclasses
import json
import typing


# =========================================================================
# Parameters - sent as execution params when the job is submitted
# =========================================================================


@dataclasses.dataclass
class AvoidedEmissionsSite:
    """A single conservation site to be analysed."""

    site_id: str
    site_name: str = ""
    start_date: typing.Optional[str] = None  # ISO-8601 date string
    end_date: typing.Optional[str] = None
    area_ha: typing.Optional[float] = None


@dataclasses.dataclass
class AvoidedEmissionsParams:
    """Parameters for an avoided-emissions analysis execution.

    Mirrors the JSON config consumed by the R analysis scripts.
    """

    task_id: str
    sites_s3_uri: str  # S3 URI to GeoJSON / GeoPackage of sites
    cog_bucket: str  # S3 bucket with covariate COGs
    cog_prefix: str  # S3 key prefix for COGs
    covariates: typing.List[str] = dataclasses.field(default_factory=list)
    exact_match_vars: typing.List[str] = dataclasses.field(
        default_factory=lambda: ["region", "ecoregion", "pa"]
    )
    fc_years: typing.List[int] = dataclasses.field(
        default_factory=lambda: list(range(2000, 2024))
    )
    # --- optional tuning knobs (match the R defaults) ---
    max_treatment_pixels: int = 1000
    control_multiplier: int = 50
    min_site_area_ha: float = 100.0
    min_glm_treatment_pixels: int = 15
    # S3 location where results will be written
    results_s3_uri: typing.Optional[str] = None
    # Pipeline step to run: "all" | "extract" | "match" | "summarize"
    step: str = "all"
    # Optional: specific site_id to match (array-job element override)
    site_id: typing.Optional[str] = None

    def to_dict(self):
        """Serialize to a plain dictionary."""
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, data):
        """Deserialize from a dictionary, ignoring unknown keys."""
        known = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})


# =========================================================================
# Results - stored in the execution's ``results`` JSONB column
# =========================================================================


@dataclasses.dataclass
class AvoidedEmissionsSiteYearResult:
    """Per-site per-year avoided emission result row."""

    site_id: str
    year: int
    forest_loss_avoided_ha: float = 0.0
    emissions_avoided_mgco2e: float = 0.0
    n_matched_pixels: int = 0
    sampled_fraction: float = 1.0
    site_name: typing.Optional[str] = None


@dataclasses.dataclass
class AvoidedEmissionsSiteTotalResult:
    """Aggregated totals for one site across all years."""

    site_id: str
    site_name: typing.Optional[str] = None
    forest_loss_avoided_ha: float = 0.0
    emissions_avoided_mgco2e: float = 0.0
    area_ha: typing.Optional[float] = None
    n_matched_pixels: int = 0
    sampled_fraction: float = 1.0
    first_year: typing.Optional[int] = None
    last_year: typing.Optional[int] = None
    n_years: int = 0


@dataclasses.dataclass
class AvoidedEmissionsResults:
    """Top-level result structure for an avoided-emissions execution.

    Stored in ``Execution.results`` on the API side.
    """

    type: str = "AvoidedEmissionsResults"
    task_id: typing.Optional[str] = None
    n_sites: int = 0
    total_emissions_avoided_mgco2e: float = 0.0
    total_forest_loss_avoided_ha: float = 0.0
    total_area_ha: float = 0.0
    year_range_min: typing.Optional[int] = None
    year_range_max: typing.Optional[int] = None
    # Detailed per-site-year breakdown
    by_site_year: typing.List[AvoidedEmissionsSiteYearResult] = dataclasses.field(
        default_factory=list
    )
    # Per-site totals
    by_site_total: typing.List[AvoidedEmissionsSiteTotalResult] = dataclasses.field(
        default_factory=list
    )
    # S3 URIs to additional output artefacts
    results_s3_uri: typing.Optional[str] = None
    pixel_level_csv_s3_uri: typing.Optional[str] = None

    def to_dict(self):
        """Serialize to a plain dictionary (for JSON storage)."""
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, data):
        """Deserialize from a dictionary, reconstructing nested objects."""
        if data is None:
            return cls()
        d = dict(data)
        d.pop("type", None)  # handled by default

        by_year_raw = d.pop("by_site_year", [])
        by_total_raw = d.pop("by_site_total", [])

        known = {f.name for f in dataclasses.fields(cls)}
        obj = cls(**{k: v for k, v in d.items() if k in known})

        obj.by_site_year = [
            _from_dict(AvoidedEmissionsSiteYearResult, r)
            for r in (by_year_raw or [])
        ]
        obj.by_site_total = [
            _from_dict(AvoidedEmissionsSiteTotalResult, r)
            for r in (by_total_raw or [])
        ]
        return obj

    def to_json(self, **kwargs):
        """Serialize to a JSON string."""
        return json.dumps(self.to_dict(), **kwargs)


def _from_dict(cls, data):
    """Create a dataclass instance from *data*, ignoring unknown keys."""
    known = {f.name for f in dataclasses.fields(cls)}
    return cls(**{k: v for k, v in data.items() if k in known})
