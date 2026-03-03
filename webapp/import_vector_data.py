"""Import vector reference data into PostGIS tables.

Downloads geoBoundaries (CGAZ ADM0/1/2), RESOLVE ecoregions, and WDPA
protected areas data and loads them into the database.  Each table is
only populated when it is empty, making repeated runs idempotent.

Usage:
    python import_vector_data.py          # import all datasets
    python import_vector_data.py --check  # only report which tables need data
"""

import logging
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
    "SITE_ID": "site_id",
    "SITE_PID": "site_pid",
    "SITE_TYPE": "site_type",
    "NAME_ENG": "name_eng",
    "NAME": "name",
    "DESIG": "desig",
    "DESIG_ENG": "desig_eng",
    "DESIG_TYPE": "desig_type",
    "IUCN_CAT": "iucn_cat",
    "INT_CRIT": "int_crit",
    "REALM": "realm",
    "REP_M_AREA": "rep_m_area",
    "GIS_M_AREA": "gis_m_area",
    "REP_AREA": "rep_area",
    "GIS_AREA": "gis_area",
    "NO_TAKE": "no_take",
    "NO_TK_AREA": "no_tk_area",
    "STATUS": "status",
    "STATUS_YR": "status_yr",
    "GOV_TYPE": "gov_type",
    "GOVSUBTYPE": "govsubtype",
    "OWN_TYPE": "own_type",
    "OWNSUBTYPE": "ownsubtype",
    "MANG_AUTH": "mang_auth",
    "MANG_PLAN": "mang_plan",
    "VERIF": "verif",
    "METADATAID": "metadataid",
    "PRNT_ISO3": "prnt_iso3",
    "ISO3": "iso3",
    "SUPP_INFO": "supp_info",
    "CONS_OBJ": "cons_obj",
    "INLND_WTRS": "inlnd_wtrs",
    "OECM_ASMT": "oecm_asmt",
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
    """Select columns present in the source, rename them, keep geometry.

    Column matching is case-insensitive: the col_map keys are compared
    against the source column names ignoring case.  This handles
    variations across GDB releases (e.g. ``SITE_ID`` vs ``site_id``).
    """
    # Build a lookup from lower-cased source column name → actual name
    src_lower = {c.lower(): c for c in gdf.columns if c != "geometry"}

    # Match col_map keys (case-insensitive) to actual source columns
    matched: dict[str, str] = {}  # actual_src_col → target_col
    for map_key, target in col_map.items():
        actual = src_lower.get(map_key.lower())
        if actual is not None:
            matched[actual] = target

    missing = {k for k in col_map if k.lower() not in src_lower}
    if missing:
        log.warning(
            "Source is missing columns: %s (available: %s)",
            missing,
            sorted(src_lower.keys()),
        )

    # Keep only matched columns + geometry
    keep = list(matched.keys()) + ["geometry"]
    gdf = gdf[keep].copy()
    gdf = gdf.rename(columns=matched)
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
    """Download and import a single geoBoundaries CGAZ admin level.

    Country-level polygons have highly detailed geometries (the ADM0
    file is ~155 MB) which can easily exhaust worker memory when loaded
    all at once.  We therefore read and write in batches using pyogrio.
    """
    import pyogrio

    table = f"geoboundaries_adm{adm_level}"
    url = DOWNLOAD_URLS[table]

    dest = tmpdir / f"geoBoundariesCGAZ_ADM{adm_level}.gpkg"
    _download(url, dest)

    col_map = GEOBOUNDARIES_COL_MAP_ADM0 if adm_level == 0 else GEOBOUNDARIES_COL_MAP

    info = pyogrio.read_info(str(dest))
    total_features = info["features"]
    if total_features < 0:
        info = pyogrio.read_info(str(dest), force_feature_count=True)
        total_features = info["features"]

    # ADM0 countries have extremely detailed coastline geometries (~155 MB
    # for 218 features) — use small batches.  ADM1/ADM2 polygons are much
    # simpler so larger batches are fine.
    batch_size = {0: 50, 1: 500, 2: 5000}.get(adm_level, 500)
    write_chunk = {0: 50, 1: 500, 2: 2000}.get(adm_level, 500)
    log.info(
        "geoBoundaries ADM%d has %d features — reading in batches of %d",
        adm_level,
        total_features,
        batch_size,
    )

    total_written = 0
    for start in range(0, total_features, batch_size):
        n = min(batch_size, total_features - start)
        log.info(
            "Reading ADM%d batch %d – %d of %d",
            adm_level,
            start + 1,
            start + n,
            total_features,
        )
        gdf = pyogrio.read_dataframe(
            str(dest),
            skip_features=start,
            max_features=n,
        )
        gdf = _select_and_rename(gdf, col_map)
        gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()]
        gdf = _ensure_multipolygon(gdf)
        gdf = gdf.set_crs(epsg=4326, allow_override=True)

        _write_to_postgis(gdf, table, engine, chunksize=write_chunk)
        total_written += len(gdf)
        del gdf

    log.info(
        "geoBoundaries ADM%d import complete: %d features written",
        adm_level,
        total_written,
    )


def import_ecoregions(engine, tmpdir: Path) -> None:
    """Download and import RESOLVE Ecoregions.

    Uses chunked reading via pyogrio to limit memory usage, consistent
    with the geoBoundaries and WDPA importers.
    """
    import pyogrio

    dest = tmpdir / "resolve_ecoregions.gpkg"
    _download(DOWNLOAD_URLS["ecoregions"], dest)

    info = pyogrio.read_info(str(dest))
    total_features = info["features"]
    if total_features < 0:
        info = pyogrio.read_info(str(dest), force_feature_count=True)
        total_features = info["features"]

    batch_size = 200
    log.info(
        "Ecoregions has %d features — reading in batches of %d",
        total_features,
        batch_size,
    )

    total_written = 0
    for start in range(0, total_features, batch_size):
        n = min(batch_size, total_features - start)
        log.info(
            "Reading ecoregions batch %d – %d of %d",
            start + 1,
            start + n,
            total_features,
        )
        gdf = pyogrio.read_dataframe(
            str(dest),
            skip_features=start,
            max_features=n,
        )
        gdf = _select_and_rename(gdf, ECOREGION_COL_MAP)
        for col in ("eco_id", "biome_num"):
            if col in gdf.columns:
                gdf[col] = gdf[col].astype("Int64")
        gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()]
        gdf = _ensure_multipolygon(gdf)
        gdf = gdf.set_crs(epsg=4326, allow_override=True)

        _write_to_postgis(gdf, "ecoregions", engine, chunksize=200)
        total_written += len(gdf)
        del gdf

    log.info("Ecoregions import complete: %d features written", total_written)


def import_wdpa(engine, tmpdir: Path) -> None:
    """Download and import WDPA protected areas (polygon layer only).

    The WDPA polygon layer contains ~300 k features with complex
    geometries.  Loading it all at once via ``gpd.read_file()`` easily
    exhausts worker memory and triggers an OOM kill.  To avoid this,
    we first *locate* the data source, then read it in batches using
    pyogrio's ``skip_features`` / ``max_features`` parameters.
    """
    import pyogrio

    dest = tmpdir / "wdpa.zip"
    _download(DOWNLOAD_URLS["wdpa"], dest)

    # Extract the zip
    extract_dir = tmpdir / "wdpa_extract"
    log.info("Extracting %s", dest)
    with zipfile.ZipFile(dest, "r") as zf:
        zf.extractall(extract_dir)

    # Log what was extracted to aid debugging
    extracted_files = list(extract_dir.rglob("*"))
    log.info(
        "Extracted %d items; top-level: %s",
        len(extracted_files),
        [p.name for p in extract_dir.iterdir()],
    )

    # ------------------------------------------------------------------
    # Phase 1: locate the polygon data source (path + layer name)
    # ------------------------------------------------------------------
    src_path: Path | None = None
    poly_layer: str | None = None

    # Check for File GeoDatabase (.gdb directory) first
    gdb_dirs = list(extract_dir.rglob("*.gdb"))
    if gdb_dirs:
        src_path = gdb_dirs[0]
        log.info("Found GeoDatabase: %s", src_path)
        try:
            import fiona

            layers = fiona.listlayers(src_path)
            log.info("Available layers: %s", layers)
            for lyr in layers:
                if "poly" in lyr.lower() or "polygon" in lyr.lower():
                    poly_layer = lyr
                    break
        except Exception:
            pass  # will read the default layer

    # Fall back to GeoPackage / Shapefile patterns
    if src_path is None:
        for pattern in [
            "**/*Polygons*.gpkg",
            "**/*polygons*.gpkg",
            "**/*Polygons*.shp",
            "**/*polygons*.shp",
            "**/*.gpkg",
            "**/*.shp",
        ]:
            matches = list(extract_dir.glob(pattern))
            if matches:
                src_path = matches[0]
                log.info("Found vector file: %s", src_path)
                try:
                    import fiona

                    layers = fiona.listlayers(src_path)
                    log.info("Available layers: %s", layers)
                    for lyr in layers:
                        if "poly" in lyr.lower() or "polygon" in lyr.lower():
                            poly_layer = lyr
                            break
                except Exception:
                    pass
                break

    if src_path is None:
        raise RuntimeError(
            f"Could not find a supported vector file in {extract_dir}. "
            f"Contents: {[p.name for p in extract_dir.iterdir()]}"
        )

    log.info("Using layer %r from %s", poly_layer, src_path.name)

    # ------------------------------------------------------------------
    # Phase 2: chunked read and import
    # ------------------------------------------------------------------
    info = pyogrio.read_info(str(src_path), layer=poly_layer)
    total_features = info["features"]
    if total_features < 0:
        # Some drivers don't report count; force a scan.
        info = pyogrio.read_info(
            str(src_path), layer=poly_layer, force_feature_count=True
        )
        total_features = info["features"]

    batch_size = 10_000
    log.info(
        "WDPA source has %d features — reading in batches of %d",
        total_features,
        batch_size,
    )

    total_written = 0
    for start in range(0, total_features, batch_size):
        n = min(batch_size, total_features - start)
        log.info(
            "Reading WDPA batch %d – %d of %d",
            start + 1,
            start + n,
            total_features,
        )
        gdf = pyogrio.read_dataframe(
            str(src_path),
            layer=poly_layer,
            skip_features=start,
            max_features=n,
        )

        gdf = _select_and_rename(gdf, WDPA_COL_MAP)
        # Drop rows without geometry (WDPA can include point records)
        gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()]
        gdf = _ensure_multipolygon(gdf)
        gdf = gdf.set_crs(epsg=4326, allow_override=True)

        _write_to_postgis(gdf, "wdpa", engine, chunksize=2000)
        total_written += len(gdf)
        del gdf

    log.info("WDPA import complete: %d features written", total_written)


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
            log.info("Tables needing import: %s", [t for t, _ in needed])
        else:
            log.info("All tables already populated")
        return

    if not needed:
        log.info("All vector reference tables already populated – nothing to do")
        return

    tmpdir = Path(tempfile.mkdtemp(prefix="vector_import_"))
    failed: list[str] = []
    try:
        for table_name, importer in needed:
            log.info("=" * 60)
            log.info("Importing %s", table_name)
            log.info("=" * 60)
            try:
                importer(engine, tmpdir)
            except Exception:
                log.exception("Failed to import %s", table_name)
                failed.append(table_name)
                # Continue with remaining datasets
    finally:
        log.info("Cleaning up temp directory %s", tmpdir)
        shutil.rmtree(tmpdir, ignore_errors=True)

    if failed:
        raise RuntimeError(f"Vector data import failed for: {', '.join(failed)}")

    log.info("Vector data import complete")


if __name__ == "__main__":
    check_flag = "--check" in sys.argv
    run_import(check_only=check_flag)
