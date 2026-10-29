"""Conservative polygon partitions used by analysis preparation."""

import numpy as np
import shapely
from pyproj import Transformer

AREA_MAX_SEGMENT_DEGREES = 0.005
_EQUAL_AREA = Transformer.from_crs(4326, "ESRI:54009", always_xy=True)


def equal_area_ha(geometries):
    """Measure curved projected boundaries without unbounded densification RAM."""
    geometries = np.asarray(geometries, dtype=object)
    areas = np.empty(len(geometries))
    for start in range(0, len(geometries), 128):
        dense = shapely.segmentize(
            geometries[start : start + 128], AREA_MAX_SEGMENT_DEGREES
        )
        projected = shapely.transform(
            dense,
            lambda c: np.column_stack(_EQUAL_AREA.transform(c[:, 0], c[:, 1])),
        )
        areas[start : start + 128] = shapely.area(projected) / 10000
    return areas


def polygonal(geometry):
    """Keep polygonal members without discarding valid polygon coverage."""
    if geometry is None or geometry.is_empty:
        raise ValueError("Missing or empty polygon geometry.")
    geometry = shapely.make_valid(geometry)
    if geometry.geom_type == "GeometryCollection":
        parts = []
        pending = list(geometry.geoms)
        while pending:
            part = pending.pop()
            if part.geom_type == "GeometryCollection":
                pending.extend(part.geoms)
            elif part.geom_type in {"Polygon", "MultiPolygon"} and not part.is_empty:
                parts.append(part)
        if not parts:
            raise ValueError("Geometry repair produced no polygonal coverage.")
        geometry = shapely.union_all(parts)
    if geometry.geom_type not in {"Polygon", "MultiPolygon"} or geometry.area <= 0:
        raise ValueError("Geometry has no positive polygonal coverage.")
    return geometry


def partition_by_regions(geometries, region_ids, region_geometries, layer):
    """Return original indices, partition geometries and region IDs.

    Candidate work is bounded to one input piece at a time. Residual coverage
    is retained with a null key; any positive overlapping assignment is an error.
    """
    tree = shapely.STRtree(region_geometries)
    origins, pieces, keys = [], [], []
    for index, source in enumerate(geometries):
        source = polygonal(source)
        candidates = sorted(
            tree.query(source).tolist(), key=lambda i: str(region_ids[i])
        )
        assigned = []
        assigned_indices = set()
        for candidate in candidates:
            part = shapely.intersection(source, region_geometries[candidate])
            if part.is_empty or part.area <= 0:
                continue
            part = polygonal(part)
            for previous in tree.query(part, predicate="intersects"):
                if (
                    previous in assigned_indices
                    and shapely.intersection(part, region_geometries[previous]).area > 0
                ):
                    raise ValueError(
                        f"Overlapping reference coverage in {layer}, "
                        f"region {region_ids[candidate]}, input piece {index + 1}. "
                        "Repair the reference layer before submitting."
                    )
            assigned.append(part)
            assigned_indices.add(candidate)
            origins.append(index)
            pieces.append(part)
            keys.append(str(region_ids[candidate]))
        residual = (
            shapely.difference(source, shapely.union_all(assigned))
            if assigned
            else source
        )
        if not residual.is_empty and residual.area > 0:
            origins.append(index)
            pieces.append(polygonal(residual))
            keys.append(None)
        output = shapely.union_all(assigned + [residual])
        error = shapely.symmetric_difference(source, output).area
        if error > max(1e-14, source.area * 1e-10):
            raise ValueError(
                f"Polygon coverage was not conserved in {layer}, piece {index + 1}."
            )
    return (
        np.asarray(origins, dtype=int),
        np.asarray(pieces, dtype=object),
        np.asarray(keys, dtype=object),
    )
