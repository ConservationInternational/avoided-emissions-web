"""S3 cache of per-site exact-match partitions.

Splitting sites by reference regions is the slowest part of submission. A
site's pieces depend only on its geometry, the reference layers and the
partition algorithm, so the result for a whole site set is stored once in S3
under a content-derived key and reused by later submissions (including ones
with a different minimum site area, which only drops whole sites).
"""

import hashlib
import io
import json
import logging
import os
import tempfile
import time

import geopandas as gpd
import numpy as np
import shapely
from botocore.exceptions import ClientError
from config import Config

from services.polygon_partition import PARTITION_ALGO_VERSION
from services.s3 import S3_COST_TAGGING, get_s3_client

logger = logging.getLogger(__name__)

_REFERENCE_VERSIONS_METADATA = "reference-versions"


def sites_fingerprint(sites: gpd.GeoDataFrame) -> str:
    """Hash site identities and geometries independently of row order."""
    ids = sites["site_id"].astype(str).to_numpy()
    wkbs = shapely.to_wkb(np.asarray(sites.geometry, dtype=object))
    digest = hashlib.sha256()
    for i in np.argsort(ids, kind="stable"):
        site_id = ids[i].encode()
        digest.update(len(site_id).to_bytes(8, "big"))
        digest.update(site_id)
        digest.update(len(wkbs[i]).to_bytes(8, "big"))
        digest.update(wkbs[i])
    return digest.hexdigest()


def partition_cache_key(sites, polygon_vars, reference_layer_uris, piece_uris):
    """Return the cache key for a site partition."""
    from services.reference_layers import REGION_PIECE_MAX_VERTICES

    polygon_vars = sorted(polygon_vars)
    payload = {
        "algorithm": PARTITION_ALGO_VERSION,
        "sites": sites_fingerprint(sites),
        "variables": polygon_vars,
        "references": {v: reference_layer_uris[v] for v in polygon_vars},
        "region_source": {
            v: "pieces" if v in piece_uris else "full" for v in polygon_vars
        },
        "piece_max_vertices": REGION_PIECE_MAX_VERTICES,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def _s3_key(cache_key):
    return f"{Config.S3_PREFIX}/partition-cache/{cache_key}.parquet"


def read_cached_pieces(cache_key, polygon_vars, site_ids):
    """Return cached pieces, or ``None`` on a miss or unusable entry."""
    s3 = get_s3_client()
    try:
        response = s3.get_object(Bucket=Config.S3_BUCKET, Key=_s3_key(cache_key))
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
            return None
        logger.warning("[SPLIT-CACHE] Read failed for %s: %s", cache_key, exc)
        return None
    try:
        pieces = gpd.read_parquet(io.BytesIO(response["Body"].read()))
        missing_columns = {"site_id", *polygon_vars}.difference(pieces.columns)
        if missing_columns or not set(site_ids).issubset(pieces["site_id"]):
            logger.warning("[SPLIT-CACHE] Ignoring incomplete entry %s", cache_key)
            return None
        metadata = response.get("Metadata", {})
        pieces.attrs["reference_versions"] = json.loads(
            metadata.get(_REFERENCE_VERSIONS_METADATA, "{}")
        )
        return pieces
    except Exception as exc:  # noqa: BLE001 -- a bad entry must not fail submission
        logger.warning("[SPLIT-CACHE] Ignoring unreadable entry %s: %s", cache_key, exc)
        return None


def write_cached_pieces(cache_key, pieces):
    """Upload pieces to the cache; failures are logged and ignored."""
    fd, path = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    try:
        pieces.to_parquet(path)
        get_s3_client().upload_file(
            path,
            Config.S3_BUCKET,
            _s3_key(cache_key),
            ExtraArgs={
                "Tagging": S3_COST_TAGGING,
                "Metadata": {
                    _REFERENCE_VERSIONS_METADATA: json.dumps(
                        pieces.attrs.get("reference_versions", {}), sort_keys=True
                    )
                },
            },
        )
        return True
    except Exception as exc:  # noqa: BLE001 -- caching is best effort
        logger.warning("[SPLIT-CACHE] Write failed for %s: %s", cache_key, exc)
        return False
    finally:
        os.unlink(path)


def get_or_compute_site_pieces(sites, polygon_vars, reference_layer_uris=None):
    """Return per-site partition pieces, using the S3 cache when possible.

    The cache is used only when current reference exports exist for every
    variable, since their content-addressed URIs version the regions. A
    result is stored only if those exports are still current afterwards.
    """
    from services.reference_layers import (
        get_reference_layer_uris,
        region_piece_uris,
        split_sites_into_pieces,
    )

    polygon_vars = sorted(polygon_vars)
    reference_layer_uris = reference_layer_uris or {}
    references = {
        v: reference_layer_uris[v] for v in polygon_vars if v in reference_layer_uris
    }
    piece_uris = region_piece_uris(polygon_vars, references) if references else {}
    use_cache = Config.PARTITION_CACHE_ENABLED and len(references) == len(polygon_vars)

    cache_key = None
    if use_cache:
        t0 = time.perf_counter()
        cache_key = partition_cache_key(sites, polygon_vars, references, piece_uris)
        cached = read_cached_pieces(cache_key, polygon_vars, sites["site_id"])
        if cached is not None:
            logger.info(
                "[SPLIT-CACHE] Hit %s: %d pieces for %d sites in %.2fs",
                cache_key,
                len(cached),
                len(sites),
                time.perf_counter() - t0,
            )
            return cached
        logger.info("[SPLIT-CACHE] Miss %s for %d sites", cache_key, len(sites))

    pieces = split_sites_into_pieces(sites, polygon_vars, piece_uris)

    if cache_key is not None:
        if get_reference_layer_uris(polygon_vars) != references:
            logger.warning(
                "[SPLIT-CACHE] Reference exports changed while splitting; "
                "not caching %s",
                cache_key,
            )
        elif write_cached_pieces(cache_key, pieces):
            logger.info("[SPLIT-CACHE] Stored %s", cache_key)
    return pieces


# Keep aligned with layouts.common.DEFAULT_EXACT_MATCH (not imported here to
# keep the UI stack out of Celery workers).
WARMUP_DEFAULT_EXACT_MATCH = ("admin1", "ecoregion", "pa")


def warmup_variable_sets(user_id=None) -> list[list[str]]:
    """Return polygon-variable sets to precompute: the default plus presets."""
    from models import CovariatePreset, get_db

    from services.reference_layers import split_polygon_vars

    candidates = [WARMUP_DEFAULT_EXACT_MATCH]
    if user_id is not None:
        db = get_db()
        try:
            candidates.extend(
                row.exact_match_vars
                for row in db.query(CovariatePreset.exact_match_vars)
                .filter(CovariatePreset.user_id == user_id)
                .all()
                if row.exact_match_vars
            )
        finally:
            db.close()
    unique = []
    for candidate in candidates:
        polygon_vars = split_polygon_vars(candidate)
        if polygon_vars and polygon_vars not in unique:
            unique.append(polygon_vars)
    return unique


def warm_site_set_partition_cache(site_set_id, user_id=None) -> dict:
    """Precompute cached partitions for a site set.

    Sites are loaded and repaired exactly as the submission worker does so
    the cache keys match.
    """
    from models import get_db

    from services.polygon_partition import polygonal
    from services.reference_layers import get_reference_layer_uris
    from services.site_set import _stream_site_set_to_parquet_buf

    if not Config.PARTITION_CACHE_ENABLED:
        return {"status": "disabled"}
    variable_sets = warmup_variable_sets(user_id)
    db = get_db()
    try:
        buf, n_sites = _stream_site_set_to_parquet_buf(site_set_id, db)
    finally:
        db.close()
    try:
        sites = gpd.read_parquet(buf)
    finally:
        buf.close()
    sites = sites.copy()
    sites["geometry"] = [polygonal(geom) for geom in sites.geometry]
    sites = sites[["site_id", "geometry"]]

    results = {}
    for polygon_vars in variable_sets:
        label = ",".join(polygon_vars)
        references = get_reference_layer_uris(polygon_vars)
        if not references:
            results[label] = "reference exports not ready"
            continue
        t0 = time.perf_counter()
        get_or_compute_site_pieces(sites, polygon_vars, references)
        results[label] = f"ready in {time.perf_counter() - t0:.1f}s"
        logger.info(
            "[SPLIT-CACHE] Warmed site set %s for %s (%d sites) in %.2fs",
            site_set_id,
            label,
            n_sites,
            time.perf_counter() - t0,
        )
    return {"status": "complete", "variable_sets": results}
