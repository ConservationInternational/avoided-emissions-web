"""Import vector reference data into PostGIS tables.

Downloads geoBoundaries (CGAZ ADM0/1/2), RESOLVE ecoregions, and WDPA
protected areas data and loads them into the database.  Each table is
only populated when it is empty, making repeated runs idempotent.

Usage:
    python import_vector_data.py          # import all datasets
    python import_vector_data.py --check  # only report which tables need data
"""

import logging
import os
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path
from urllib.request import urlretrieve

import geopandas as gpd
from sqlalchemy import create_engine, text

from config import Config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Download URLs
# ---------------------------------------------------------------------------

GEOBOUNDARIES_CGAZ_BASE = (
    "https://github.com/wmgeolab/geoBoundaries/raw/main/releaseData/CGAZ"
)

DOWNLOAD_URLS = {
    "geoboundaries_adm0": f"{GEOBOUNDARIES_CGAZ_BASE}/geoBoundariesCGAZ_ADM0.gpkg",
    "geoboundaries_adm1": f"{GEOBOUNDARIES_CGAZ_BASE}/geoBoundariesCGAZ_ADM1.gpkg",
    "geoboundaries_adm2": f"{GEOBOUNDARIES_CGAZ_BASE}/geoBoundariesCGAZ_ADM2.gpkg",
    "ecoregions": (
        "https://ci-apps.s3.dualstack.us-east-1.amazonaws.com"
        "/avoided-emissions/vector_data"
        "/Resolve_Ecoregions_-6779945127424040112.gpkg"
    ),
    "wdpa": (
        "https://ci-apps.s3.dualstack.us-east-1.amazonaws.com"
        "/avoided-emissions/vector_data"
        "/WDPA_Feb2026_Public.zip"
    ),
}

# ---------------------------------------------------------------------------
# Column mappings (source column name -> DB column name)
# ---------------------------------------------------------------------------

# CGAZ releases never include shapeISO; shapeID is only in ADM1/ADM2.
GEOBOUNDARIES_COL_MAP_ADM0 = {
    "shapeGroup": "shape_group",
    "shapeName": "shape_name",
    "shapeType": "shape_type",
}

GEOBOUNDARIES_COL_MAP = {
    "shapeGroup": "shape_group",
    "shapeName": "shape_name",
    "shapeID": "shape_id",
    "shapeType": "shape_type",
}

ECOREGION_COL_MAP = {
    "ECO_ID": "eco_id",
    "ECO_NAME": "eco_name",
    "BIOME_NUM": "biome_num",
    "BIOME_NAME": "biome_name",
    "REALM": "realm",
    "NNH": "nnh",
    "COLOR": "color",
    "COLOR_BIO": "color_bio",
    "COLOR_NNH": "color_nnh",
}

WDPA_COL_MAP = {
    "WDPAID": "wdpaid",
    "NAME": "name",
    "ORIG_NAME": "orig_name",
    "DESIG": "desig",
    "DESIG_TYPE": "desig_type",
    "IUCN_CAT": "iucn_cat",
    "INT_CRIT": "int_crit",
    "MARINE": "marine",
    "REP_M_AREA": "rep_m_area",
    "GIS_M_AREA": "gis_m_area",
    "REP_AREA": "rep_area",
    "GIS_AREA": "gis_area",
    "NO_TAKE": "no_take",
    "NO_TK_AREA": "no_tk_area",
    "STATUS": "status",
    "STATUS_YR": "status_yr",
    "GOV_TYPE": "gov_type",
    "OWN_TYPE": "own_type",
    "MANG_AUTH": "mang_auth",
    "MANG_PLAN": "mang_plan",
    "VERIF": "verif",
    "ISO3": "iso3",
    "PARENT_ISO3": "parent_iso3",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _table_is_empty(engine, table_name: str) -> bool:
    """Return True if the table exists and has zero rows."""
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT EXISTS (SELECT 1 FROM {table_name} LIMIT 1)")
        )
        return not result.scalar()


def _table_exists(engine, table_name: str) -> bool:
    """Return True if the table exists in the database."""
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT EXISTS ("
                "  SELECT 1 FROM information_schema.tables"
                "  WHERE table_name = :tbl"
                ")"
            ),
            {"tbl": table_name},
        )
        return result.scalar()


def _download(url: str, dest: Path) -> Path:
    """Download a file with progress logging.  Returns the local path."""
    log.info("Downloading %s → %s", url, dest)
    urlretrieve(url, dest)
    size_mb = dest.stat().st_size / (1024 * 1024)
    log.info("Downloaded %.1f MB", size_mb)
    return dest


def _load_geopackage(path: Path, layer: str | None = None) -> gpd.GeoDataFrame:
    """Read a GeoPackage (or shapefile) into a GeoDataFrame."""
    log.info("Reading %s (layer=%s)", path, layer)
    gdf = gpd.read_file(path, layer=layer)
    log.info("Loaded %d features", len(gdf))
    return gdf


def _ensure_multipolygon(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Promote any Polygon geometries to MultiPolygon for consistency."""
    from shapely.geometry import MultiPolygon

    def _to_multi(geom):
        if geom is None:
            return None
        if geom.geom_type == "Polygon":
            return MultiPolygon([geom])
        return geom

    gdf = gdf.copy()
    gdf["geometry"] = gdf["geometry"].apply(_to_multi)
    return gdf


def _select_and_rename(
    gdf: gpd.GeoDataFrame, col_map: dict[str, str]
) -> gpd.GeoDataFrame:
    """Select columns present in the source, rename them, keep geometry."""
    available = {c for c in col_map if c in gdf.columns}
    missing = set(col_map) - available
    if missing:
        log.warning("Source is missing columns: %s", missing)

    # Keep only mapped columns + geometry
    keep = list(available) + ["geometry"]
    gdf = gdf[keep].copy()
    rename_map = {src: dst for src, dst in col_map.items() if src in available}
    gdf = gdf.rename(columns=rename_map)
    return gdf


def _write_to_postgis(
    gdf: gpd.GeoDataFrame,
    table_name: str,
    engine,
    chunksize: int = 5000,
    geom_col: str = "geom",
) -> None:
    """Write a GeoDataFrame to a PostGIS table using append mode.

    The migration creates geometry columns named ``geom``, so we rename the
    GeoDataFrame's active geometry column to match before writing.  This
    ensures ``Find_SRID()`` resolves against the registered column name.
    """
    # Rename the geometry column to match the DB schema
    src_geom = gdf.geometry.name  # usually 'geometry'
    if src_geom != geom_col:
        gdf = gdf.rename_geometry(geom_col)

    log.info("Writing %d rows to %s (chunksize=%d)", len(gdf), table_name, chunksize)
    gdf.to_postgis(
        table_name,
        engine,
        if_exists="append",
        index=False,
        chunksize=chunksize,
    )
    log.info("Finished writing %s", table_name)


# ---------------------------------------------------------------------------
# Per-dataset import functions
# ---------------------------------------------------------------------------


def import_geoboundaries(engine, adm_level: int, tmpdir: Path) -> None:
    """Download and import a single geoBoundaries CGAZ admin level."""
    table = f"geoboundaries_adm{adm_level}"
    url = DOWNLOAD_URLS[table]

    dest = tmpdir / f"geoBoundariesCGAZ_ADM{adm_level}.gpkg"
    _download(url, dest)

    gdf = _load_geopackage(dest)
    col_map = GEOBOUNDARIES_COL_MAP_ADM0 if adm_level == 0 else GEOBOUNDARIES_COL_MAP
    gdf = _select_and_rename(gdf, col_map)
    gdf = _ensure_multipolygon(gdf)
    gdf = gdf.set_crs(epsg=4326, allow_override=True)

    _write_to_postgis(gdf, table, engine)


def import_ecoregions(engine, tmpdir: Path) -> None:
    """Download and import RESOLVE Ecoregions."""
    dest = tmpdir / "resolve_ecoregions.gpkg"
    _download(DOWNLOAD_URLS["ecoregions"], dest)

    gdf = _load_geopackage(dest)
    gdf = _select_and_rename(gdf, ECOREGION_COL_MAP)
    # Source data stores integer fields as floats – cast to int for Postgres
    for col in ("eco_id", "biome_num"):
        if col in gdf.columns:
            gdf[col] = gdf[col].astype("Int64")  # nullable integer
    gdf = _ensure_multipolygon(gdf)
    gdf = gdf.set_crs(epsg=4326, allow_override=True)

    _write_to_postgis(gdf, "ecoregions", engine)


def import_wdpa(engine, tmpdir: Path) -> None:
    """Download and import WDPA protected areas (polygon layer only)."""
    dest = tmpdir / "wdpa.zip"
    _download(DOWNLOAD_URLS["wdpa"], dest)

    # Extract the zip
    extract_dir = tmpdir / "wdpa_extract"
    log.info("Extracting %s", dest)
    with zipfile.ZipFile(dest, "r") as zf:
        zf.extractall(extract_dir)

    # Find the polygon layer – could be a GeoPackage, shapefile, or GDB
    # Try common patterns
    gdf = None

    # Log what was extracted to aid debugging
    extracted_files = list(extract_dir.rglob("*"))
    log.info("Extracted %d items; top-level: %s",
             len(extracted_files),
             [p.name for p in extract_dir.iterdir()])

    # Check for File GeoDatabase (.gdb directory) first
    gdb_dirs = list(extract_dir.rglob("*.gdb"))
    if gdb_dirs:
        gdb_path = gdb_dirs[0]
        log.info("Found GeoDatabase: %s", gdb_path)
        try:
            import fiona

            layers = fiona.listlayers(gdb_path)
            log.info("Available layers: %s", layers)
            poly_layer = None
            for lyr in layers:
                if "poly" in lyr.lower() or "polygon" in lyr.lower():
                    poly_layer = lyr
                    break
            gdf = _load_geopackage(gdb_path, layer=poly_layer)
        except Exception:
            gdf = _load_geopackage(gdb_path)

    # Fall back to GeoPackage / Shapefile patterns
    if gdf is None:
        for pattern in ["**/*Polygons*.gpkg", "**/*polygons*.gpkg",
                        "**/*Polygons*.shp", "**/*polygons*.shp",
                        "**/*.gpkg", "**/*.shp"]:
            matches = list(extract_dir.glob(pattern))
            if matches:
                fpath = matches[0]
                log.info("Found vector file: %s", fpath)
                try:
                    import fiona

                    layers = fiona.listlayers(fpath)
                    log.info("Available layers: %s", layers)
                    poly_layer = None
                    for lyr in layers:
                        if "poly" in lyr.lower() or "polygon" in lyr.lower():
                            poly_layer = lyr
                            break
                    gdf = _load_geopackage(fpath, layer=poly_layer)
                except Exception:
                    gdf = _load_geopackage(fpath)
                break

    if gdf is None:
        raise RuntimeError(
            f"Could not find a supported vector file in {extract_dir}. "
            f"Contents: {[p.name for p in extract_dir.iterdir()]}"
        )

    gdf = _select_and_rename(gdf, WDPA_COL_MAP)
    # Drop rows without geometry (WDPA can include point records)
    gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()]
    gdf = _ensure_multipolygon(gdf)
    gdf = gdf.set_crs(epsg=4326, allow_override=True)

    _write_to_postgis(gdf, "wdpa", engine, chunksize=2000)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


DATASETS = [
    ("geoboundaries_adm0", lambda eng, tmp: import_geoboundaries(eng, 0, tmp)),
    ("geoboundaries_adm1", lambda eng, tmp: import_geoboundaries(eng, 1, tmp)),
    ("geoboundaries_adm2", lambda eng, tmp: import_geoboundaries(eng, 2, tmp)),
    ("ecoregions", import_ecoregions),
    ("wdpa", import_wdpa),
]


def run_import(check_only: bool = False) -> None:
    """Check each table and import data where missing."""
    engine = create_engine(Config.DATABASE_URL)

    needed = []
    for table_name, _ in DATASETS:
        if not _table_exists(engine, table_name):
            log.warning("Table %s does not exist – run migrations first", table_name)
            continue
        if _table_is_empty(engine, table_name):
            log.info("Table %s is empty – import needed", table_name)
            needed.append((table_name, _))
        else:
            log.info("Table %s already has data – skipping", table_name)

    if check_only:
        if needed:
            log.info(
                "Tables needing import: %s", [t for t, _ in needed]
            )
        else:
            log.info("All tables already populated")
        return

    if not needed:
        log.info("All vector reference tables already populated – nothing to do")
        return

    tmpdir = Path(tempfile.mkdtemp(prefix="vector_import_"))
    try:
        for table_name, importer in needed:
            log.info("=" * 60)
            log.info("Importing %s", table_name)
            log.info("=" * 60)
            try:
                importer(engine, tmpdir)
            except Exception:
                log.exception("Failed to import %s", table_name)
                # Continue with remaining datasets
    finally:
        log.info("Cleaning up temp directory %s", tmpdir)
        shutil.rmtree(tmpdir, ignore_errors=True)

    log.info("Vector data import complete")


if __name__ == "__main__":
    check_flag = "--check" in sys.argv
    run_import(check_only=check_flag)
