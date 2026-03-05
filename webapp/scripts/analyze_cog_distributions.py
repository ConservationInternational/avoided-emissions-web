#!/usr/bin/env python3
"""Analyze pixel value distributions for all merged COG covariates.

Runs inside the webapp container.  For each COG it:
  1. Opens the lowest-resolution overview via /vsicurl/ + presigned URL
  2. Reads a random sample of pixels (~100k per layer)
  3. Computes percentiles (1, 5, 10, 25, 50, 75, 90, 95, 99) and basic stats

Usage (from repo root):
    docker compose -f deploy/docker-compose.develop.yml exec webapp \
        python scripts/analyze_cog_distributions.py

Output is printed as formatted text and also saved to
``/tmp/cog_distributions.json`` inside the container.
"""

import json
import subprocess
import sys
import tempfile

import boto3
import numpy as np

# -- App imports (flat PYTHONPATH=/app) ------------------------------------
sys.path.insert(0, "/app")
from config import Config  # noqa: E402
from models import Covariate, get_db  # noqa: E402


def get_merged_covariates():
    """Return dict of {name: Covariate} for all merged layers."""
    db = get_db()
    try:
        latest = {}
        for rec in db.query(Covariate).filter(Covariate.status == "merged").all():
            existing = latest.get(rec.covariate_name)
            if existing is None or (
                rec.started_at
                and (
                    existing.started_at is None or rec.started_at > existing.started_at
                )
            ):
                latest[rec.covariate_name] = rec
        return latest
    finally:
        db.close()


def presign(s3, name):
    """Generate a presigned URL for a COG."""
    key = f"{Config.S3_PREFIX}/cog/{name}.tif"
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": Config.S3_BUCKET, "Key": key},
        ExpiresIn=3600,
    )


def gdalinfo_json(vsicurl_path):
    """Run gdalinfo -json -stats and return parsed JSON."""
    result = subprocess.run(
        ["gdalinfo", "-json", "-stats", vsicurl_path],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        return None
    return json.loads(result.stdout)


def sample_overview(url, max_pixels=200000):
    """Read pixel values from the smallest overview of a COG via gdal_translate.

    Downloads the smallest overview into a tiny temporary GeoTIFF, then reads
    the raw pixel data with numpy.  This avoids fetching the full-resolution
    layer (which can be 600+ MB).
    """
    # First get info to find overview sizes
    info = gdalinfo_json("/vsicurl/" + url)
    if not info:
        return None, None

    band = info.get("bands", [{}])[0]
    dtype = band.get("type", "").lower()
    nodata = band.get("noDataValue")
    stats = band.get("computedStatistics") or band.get("statistics") or {}

    # Find smallest overview
    full_x = info.get("size", [0, 0])[0]
    full_y = info.get("size", [0, 0])[1]

    overviews = band.get("overviews", [])
    if overviews:
        smallest = overviews[-1]
        ov_x = smallest.get("size", [full_x, full_y])[0]
        ov_y = smallest.get("size", [full_x, full_y])[1]
    else:
        ov_x, ov_y = full_x, full_y

    # Limit to manageable size
    target_x = min(ov_x, 2000)
    target_y = min(ov_y, 1000)

    with tempfile.NamedTemporaryFile(suffix=".tif", delete=True) as tmp:
        tmp_path = tmp.name

    # Use gdal_translate to fetch the smallest overview
    cmd = [
        "gdal_translate",
        "-of",
        "GTiff",
        "-outsize",
        str(target_x),
        str(target_y),
        "-r",
        "nearest",
        "/vsicurl/" + url,
        tmp_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        print(f"  gdal_translate failed: {result.stderr[:200]}", flush=True)
        return stats, None

    # Read raw data with numpy
    try:
        # Use gdal_translate to output raw binary, then read with numpy
        raw_cmd = [
            "gdal_translate",
            "-of",
            "EHdr",  # flat binary
            "-outsize",
            str(target_x),
            str(target_y),
            "-r",
            "nearest",
            tmp_path,
            tmp_path + ".bil",
        ]
        subprocess.run(raw_cmd, capture_output=True, text=True, timeout=120)

        # Determine numpy dtype from GDAL type
        dtype_map = {
            "byte": np.uint8,
            "uint8": np.uint8,
            "int16": np.int16,
            "uint16": np.uint16,
            "int32": np.int32,
            "uint32": np.uint32,
            "float32": np.float32,
            "float64": np.float64,
        }
        np_dtype = dtype_map.get(dtype, np.float32)

        data = np.fromfile(tmp_path + ".bil", dtype=np_dtype)

        # Clean up temp files
        import glob
        import os

        for f in glob.glob(tmp_path + "*"):
            os.unlink(f)

        return stats, data, nodata
    except Exception as e:
        print(f"  numpy read failed: {e}", flush=True)
        return stats, None, nodata


def analyze_layer(name, url):
    """Compute distribution statistics for a single COG layer."""
    print(f"  Analyzing {name}...", flush=True)
    result = sample_overview(url)
    if result is None:
        return None

    stats, data, nodata = result

    info = {
        "name": name,
        "gdal_stats": stats,
    }

    if data is not None and len(data) > 0:
        # Filter out nodata
        if nodata is not None:
            valid = data[data != nodata]
        else:
            valid = (
                data[np.isfinite(data)]
                if np.issubdtype(data.dtype, np.floating)
                else data
            )

        if len(valid) == 0:
            info["error"] = "all pixels are nodata"
            return info

        info["total_pixels"] = int(len(data))
        info["valid_pixels"] = int(len(valid))
        info["nodata_value"] = float(nodata) if nodata is not None else None
        info["nodata_fraction"] = round(1.0 - len(valid) / len(data), 4)
        info["dtype"] = str(data.dtype)

        # Basic stats
        info["min"] = float(np.min(valid))
        info["max"] = float(np.max(valid))
        info["mean"] = float(np.mean(valid))
        info["std"] = float(np.std(valid))
        info["median"] = float(np.median(valid))

        # Percentiles
        pcts = [1, 2, 5, 10, 25, 50, 75, 90, 95, 98, 99]
        info["percentiles"] = {str(p): float(np.percentile(valid, p)) for p in pcts}

        # Zero fraction (important for nodata-as-zero layers)
        info["zero_fraction"] = round(float(np.sum(valid == 0)) / len(valid), 4)

        # Histogram (20 bins between p1 and p99 to ignore outliers)
        p1 = np.percentile(valid, 1)
        p99 = np.percentile(valid, 99)
        if p1 != p99:
            hist_counts, hist_edges = np.histogram(valid, bins=20, range=(p1, p99))
            info["histogram"] = {
                "counts": [int(c) for c in hist_counts],
                "edges": [round(float(e), 4) for e in hist_edges],
            }

        # Unique values (for categorical layers)
        unique = np.unique(valid)
        if len(unique) <= 50:
            info["unique_values"] = [float(v) for v in unique]
            info["is_categorical"] = True
        else:
            info["is_categorical"] = False
            info["unique_count"] = int(len(unique))

    return info


def main():
    print("=" * 70)
    print("COG Covariate Distribution Analysis")
    print("=" * 70)

    covariates = get_merged_covariates()
    print(f"\nFound {len(covariates)} merged covariates\n")

    # Collapse near-identical time-series layers: analyse one representative
    # per group and copy results to the rest.  Patterns: fc_YYYY, pop_YYYY,
    # lc_2015_*.
    import re

    series_patterns = [
        (re.compile(r"^fc_\d{4}$"), "fc_2020"),
        (re.compile(r"^pop_\d{4}$"), "pop_2020"),
        (re.compile(r"^lc_2015_"), None),  # each lc subtype is different
    ]

    def pick_representative(name):
        """Return the representative name if *name* is redundant, else None."""
        for pat, rep in series_patterns:
            if rep and pat.match(name) and name != rep:
                return rep
        return None

    s3 = boto3.client("s3", region_name=Config.AWS_REGION)
    results = {}
    skipped = {}  # name ??? representative it copies from

    to_analyze = []
    for name in sorted(covariates.keys()):
        rec = covariates[name]
        if not rec.merged_url:
            continue
        rep = pick_representative(name)
        if rep:
            skipped[name] = rep
        else:
            to_analyze.append(name)

    print(
        f"Will analyze {len(to_analyze)} unique layers "
        f"(skipping {len(skipped)} near-identical time-series members)\n"
    )

    for name in to_analyze:
        try:
            url = presign(s3, name)
            info = analyze_layer(name, url)
            if info:
                results[name] = info

                # Print summary
                if "percentiles" in info:
                    p = info["percentiles"]
                    print(
                        f"    {name}: "
                        f"dtype={info.get('dtype', '?')}, "
                        f"range=[{info['min']:.2f}, {info['max']:.2f}], "
                        f"p1={p['1']:.2f}, p50={p['50']:.2f}, p99={p['99']:.2f}, "
                        f"zeros={info['zero_fraction']:.1%}, "
                        f"nodata_frac={info.get('nodata_fraction', 0):.1%}"
                        f"{', CATEGORICAL (' + str(len(info.get('unique_values', []))) + 'v)' if info.get('is_categorical') else ''}"
                    )
                else:
                    print(f"    {name}: stats only - {info.get('gdal_stats', {})}")
        except Exception as e:
            print(f"    {name}: ERROR - {e}")

    # Copy results from representatives to skipped series members
    for name, rep in sorted(skipped.items()):
        if rep in results:
            results[name] = dict(results[rep])
            results[name]["name"] = name
            results[name]["copied_from"] = rep
            print(f"    {name}: (same distribution as {rep})")

    # Save full results
    output_path = "/tmp/cog_distributions.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nFull results saved to {output_path}")
    print(f"Analyzed {len(results)} layers")

    # Print style recommendations
    print("\n" + "=" * 70)
    print("STYLE RECOMMENDATIONS")
    print("=" * 70)
    for name, info in sorted(results.items()):
        if "percentiles" not in info:
            continue
        p = info["percentiles"]
        cat = info.get("is_categorical", False)
        if cat:
            print(
                f"\n{name}: CATEGORICAL ??? {len(info.get('unique_values', []))} unique values"
            )
        else:
            # Recommend using p2-p98 as min/max for continuous
            print(
                f"\n{name}: CONTINUOUS ??? "
                f"recommend min_value={p['2']:.2f}, max_value={p['98']:.2f} "
                f"(captures 96% of data)"
            )
            # Check skewness
            if info["mean"] < info["median"] * 0.5:
                print(
                    f"  ??? Highly right-skewed (mean={info['mean']:.2f} << median={info['median']:.2f})"
                )
                print(
                    "  ??? Consider LOG or SQRT transform, or quantile-based color stops"
                )
            elif info["mean"] > info["median"] * 2:
                print(
                    f"  ??? Highly left-skewed (mean={info['mean']:.2f} >> median={info['median']:.2f})"
                )


if __name__ == "__main__":
    main()
