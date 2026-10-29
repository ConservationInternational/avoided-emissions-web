#!/usr/bin/env python3
"""Audit stale site-upload jobs and local stages without cloud access."""

import argparse
import json

from tasks.site_upload import audit_site_upload_stages


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Mark stale jobs failed and delete expired local stage files.",
    )
    parser.add_argument(
        "--older-than-hours",
        type=int,
        default=24,
        help="Stale job age threshold (default: 24 hours).",
    )
    args = parser.parse_args()
    print(
        json.dumps(
            audit_site_upload_stages.run(
                apply=args.apply,
                older_than_hours=args.older_than_hours,
            ),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
