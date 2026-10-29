"""Read-only polygon inventory, artifact validation, and synthetic benchmark.

Run with PYTHONPATH=webapp:. Database inventory never rewrites historical jobs.
"""

import argparse
import json
import resource
import time
from pathlib import Path

import geopandas as gpd
import shapely
from services.polygon_partition import equal_area_ha, partition_by_regions, polygonal
from shapely.geometry import box


def audit_fragments(path, source_path=None):
    fragments = gpd.read_parquet(path)
    if fragments.crs is None or fragments.crs.to_epsg() != 4326:
        raise ValueError("Fragment artifacts must declare EPSG:4326.")
    if fragments[["site_id", "sub_site_index"]].isna().any().any():
        raise ValueError("Missing immutable fragment keys.")
    if fragments.duplicated(["site_id", "sub_site_index"]).any():
        raise ValueError("Duplicate immutable fragment keys.")
    if not fragments.geom_type.isin(["Polygon", "MultiPolygon"]).all():
        raise ValueError("Nonpolygon fragment geometry.")
    if not fragments.geometry.is_valid.all():
        raise ValueError("Invalid fragment geometry.")
    geometries = [polygonal(g) for g in fragments.geometry]
    measured = equal_area_ha(fragments.geometry)
    if "area_ha" in fragments:
        import numpy as np

        if not np.allclose(fragments.area_ha, measured, rtol=1e-8, atol=1e-6):
            raise ValueError("Declared fragment areas disagree with geometry.")
    sources = gpd.read_parquet(source_path) if source_path else None
    if sources is not None:
        if sources.site_id.duplicated().any() or sources.crs is None:
            raise ValueError("Sources require unique immutable keys and a CRS.")
        sources = sources.to_crs(4326).set_index("site_id")
        if set(sources.index) != set(fragments.site_id):
            raise ValueError("Source and fragment feature identities differ.")
    for site_id, group in fragments.groupby("site_id", sort=False):
        union = shapely.union_all(group.geometry.to_numpy())
        overlap = sum(g.area for g in group.geometry) - union.area
        if overlap > max(1e-14, union.area * 1e-10):
            raise ValueError(f"Overlapping fragments for feature {site_id}.")
        if sources is not None:
            original = polygonal(sources.loc[site_id].geometry)
            if shapely.symmetric_difference(original, union).area > max(
                1e-14, original.area * 1e-10
            ):
                raise ValueError(f"Coverage differs for feature {site_id}.")
    return {
        "features": int(fragments.site_id.nunique()),
        "fragments": len(geometries),
        "area_ha": float(measured.sum()),
        "source_coverage_checked": sources is not None,
    }


def database_inventory():
    from models import get_db
    from services.reference_layers import _EXTENT_TABLE_MAP, _REGION_ID_SQL
    from sqlalchemy import text

    db = get_db()
    try:
        db.execute(text("SET TRANSACTION READ ONLY"))
        references = {}
        for variable, table in _EXTENT_TABLE_MAP.items():
            identity = _REGION_ID_SQL[variable]
            references[variable] = dict(
                db.execute(
                    text(
                        f"""
                        SELECT count(*) AS rows,
                            count(*) FILTER (WHERE geom IS NULL) AS missing,
                            count(*) FILTER (
                                WHERE NOT ST_IsValid(geom)) AS invalid,
                            count(*) - count(DISTINCT {identity}) AS ambiguous_keys
                        FROM {table}
                        """
                    )
                )
                .mappings()
                .one()
            )
        tasks = dict(
            db.execute(
                text(
                    """
                    SELECT count(*) AS total,
                        count(*) FILTER (WHERE
                            metadata->'preparation'->>'version'
                            IS DISTINCT FROM 'authoritative-conservative-v1'
                        ) AS requires_historical_review
                    FROM analysis_tasks
                    """
                )
            )
            .mappings()
            .one()
        )
        return {"references": references, "tasks": tasks, "read_only": True}
    finally:
        db.rollback()
        db.close()


def benchmark(count):
    if count < 1:
        raise ValueError("Benchmark site count must be positive.")
    sites = [box(i * 2, 0, i * 2 + 1, 1) for i in range(count)]
    regions = [
        box(i * 2 + j / 12, 0, i * 2 + (j + 1) / 12, 1)
        for i in range(count)
        for j in range(12)
    ]
    start = time.perf_counter()
    _, pieces, _ = partition_by_regions(
        sites, list(range(len(regions))), regions, "benchmark"
    )
    elapsed = time.perf_counter() - start
    # VmHWM excludes a shell wrapper's memory peak before exec.
    status_path = Path("/proc/self/status")
    if status_path.is_file():
        peak_rss = next(
            int(line.split()[1])
            for line in status_path.read_text().splitlines()
            if line.startswith("VmHWM:")
        )
    else:
        peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return {
        "sites": count,
        "fragments": len(pieces),
        "seconds": elapsed,
        "peak_process_rss_kib": peak_rss,
        "fixture": "twelve equal strata per site; no slivers discarded",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", action="store_true")
    parser.add_argument("--fragments")
    parser.add_argument("--sources")
    parser.add_argument("--benchmark-sites", type=int)
    args = parser.parse_args()
    if not (args.database or args.fragments or args.benchmark_sites is not None):
        parser.error("Choose --database, --fragments, or --benchmark-sites.")
    if args.sources and not args.fragments:
        parser.error("--sources requires --fragments.")
    report = {}
    if args.database:
        report["database"] = database_inventory()
    if args.fragments:
        report["artifact"] = audit_fragments(args.fragments, args.sources)
    if args.benchmark_sites is not None:
        report["benchmark"] = benchmark(args.benchmark_sites)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
