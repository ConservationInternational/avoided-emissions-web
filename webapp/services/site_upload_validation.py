"""Shared validation for uploaded site attributes and WGS84 geometries."""

import math
import numbers
import re
from datetime import date, datetime

import pandas as pd
from shapely import get_coordinates
from shapely.geometry.base import BaseGeometry

MAX_SITE_ID_LENGTH = 100
MAX_SITE_NAME_LENGTH = 255


def _is_missing_scalar(value, *, field, row_number):
    if value is None:
        return True
    missing = pd.isna(value)
    if hasattr(missing, "__len__"):
        raise TypeError(f"row {row_number}: {field} must be a scalar value")
    return bool(missing)


def normalize_site_text(value, *, field, row_number):
    """Return a trimmed bounded display value, or None when blank."""
    if _is_missing_scalar(value, field=field, row_number=row_number):
        return None
    result = str(value).strip()
    if not result:
        return None
    limit = MAX_SITE_ID_LENGTH if field == "site_id" else MAX_SITE_NAME_LENGTH
    if len(result) > limit:
        raise ValueError(f"row {row_number}: {field} exceeds {limit} characters")
    return result


def normalize_site_date(value, *, field, row_number, required):
    """Parse only unambiguous ISO dates; never interpret numbers as epochs."""
    if _is_missing_scalar(value, field=field, row_number=row_number):
        if required:
            raise ValueError(f"row {row_number}: {field} is required")
        return None

    if isinstance(value, numbers.Number):
        raise TypeError(
            f"row {row_number}: {field} must be an ISO date, not a numeric value"
        )

    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    raw = str(value).strip()
    if not raw:
        if required:
            raise ValueError(f"row {row_number}: {field} is required")
        return None

    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}(?:[Tt ].*)?", raw):
        raise ValueError(f"row {row_number}: {field} must use ISO date format")

    try:
        if len(raw) == 10:
            return date.fromisoformat(raw)
        timestamp = f"{raw.removesuffix('Z')}+00:00" if raw.endswith("Z") else raw
        return datetime.fromisoformat(timestamp).date()
    except ValueError as exc:
        raise ValueError(f"row {row_number}: {field} must use ISO date format") from exc


def validate_wgs84_polygon(geometry, *, row_number):
    """Reject non-polygon, empty, nonfinite, or out-of-range WGS84 geometry."""
    if not isinstance(geometry, BaseGeometry):
        raise TypeError(f"row {row_number}: geometry is malformed")
    if geometry.is_empty:
        raise ValueError(f"row {row_number}: geometry is empty after repair")
    if geometry.geom_type not in {"Polygon", "MultiPolygon"}:
        raise ValueError(
            f"row {row_number}: expected polygon geometry, got {geometry.geom_type}"
        )
    coordinates = get_coordinates(geometry)
    if not coordinates.size or not all(
        math.isfinite(float(value)) for value in coordinates.flat
    ):
        raise ValueError(f"row {row_number}: geometry has non-finite coordinates")
    if not geometry.is_valid:
        raise ValueError(f"row {row_number}: geometry is invalid after repair")

    bounds = geometry.bounds
    if len(bounds) != 4 or not all(math.isfinite(value) for value in bounds):
        raise ValueError(f"row {row_number}: geometry has non-finite coordinates")
    min_x, min_y, max_x, max_y = bounds
    if (
        min_x < -180
        or max_x > 180
        or min_y < -90
        or max_y > 90
        or min_x > max_x
        or min_y > max_y
    ):
        raise ValueError(f"row {row_number}: geometry is outside WGS84 bounds")
    return geometry
