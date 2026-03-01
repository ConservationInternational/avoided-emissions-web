#!/usr/bin/env python3
"""Create an OAuth2 service client on the trends.earth API.

This script authenticates with an admin account (email/password), then
creates a new service client via the ``POST /api/v1/oauth/clients``
endpoint.  The resulting ``client_id`` and ``client_secret`` are printed
once — the secret cannot be retrieved again.

Usage
-----
::

    # Interactive (prompts for password)
    python create_service_client.py

    # Non-interactive
    python create_service_client.py \\
        --api-url https://api.trends.earth/api/v1 \\
        --email admin@example.com \\
        --password s3cret \\
        --name "CI publish bot" \\
        --expires-in-days 365

    # Store output for GitHub Actions secrets
    python create_service_client.py --json | jq .

Environment variables
---------------------
All flags can also be set via environment variables:

    TRENDSEARTH_API_URL     (default: https://api.trends.earth/api/v1)
    TRENDSEARTH_API_EMAIL
    TRENDSEARTH_API_PASSWORD
"""

import argparse
import getpass
import json
import os
import sys

import requests

DEFAULT_API_URL = "https://api.trends.earth/api/v1"
TIMEOUT = 30


def login(api_url: str, email: str, password: str) -> str:
    """Authenticate and return a JWT access token."""
    # The auth endpoint is at the API root, not under /api/v1
    base_url = api_url.rstrip("/")
    if base_url.endswith("/api/v1"):
        auth_url = base_url[: -len("/api/v1")] + "/auth"
    else:
        auth_url = base_url + "/auth"

    resp = requests.post(
        auth_url,
        json={"email": email, "password": password},
        timeout=TIMEOUT,
    )
    if resp.status_code != 200:
        print(f"Login failed (HTTP {resp.status_code}): {resp.text}", file=sys.stderr)
        sys.exit(1)

    token = resp.json().get("access_token")
    if not token:
        print("Login succeeded but no access_token in response.", file=sys.stderr)
        sys.exit(1)

    return token


def create_client(
    api_url: str,
    token: str,
    name: str,
    scopes: str = "",
    expires_in_days: int | None = None,
) -> dict:
    """Create an OAuth2 service client and return the response data."""
    url = f"{api_url.rstrip('/')}/oauth/clients"

    body: dict = {"name": name}
    if scopes:
        body["scopes"] = scopes
    if expires_in_days is not None:
        body["expires_in_days"] = expires_in_days

    resp = requests.post(
        url,
        json=body,
        headers={"Authorization": f"Bearer {token}"},
        timeout=TIMEOUT,
    )
    if resp.status_code != 201:
        print(
            f"Client creation failed (HTTP {resp.status_code}): {resp.text}",
            file=sys.stderr,
        )
        sys.exit(1)

    return resp.json()["data"]


def main():
    parser = argparse.ArgumentParser(
        description="Create an OAuth2 service client for the trends.earth API."
    )
    parser.add_argument(
        "--api-url",
        default=os.environ.get("TRENDSEARTH_API_URL", DEFAULT_API_URL),
        help=f"API base URL (default: {DEFAULT_API_URL})",
    )
    parser.add_argument(
        "--email",
        default=os.environ.get("TRENDSEARTH_API_EMAIL", ""),
        help="Admin account email (or set TRENDSEARTH_API_EMAIL)",
    )
    parser.add_argument(
        "--password",
        default=os.environ.get("TRENDSEARTH_API_PASSWORD", ""),
        help="Admin account password (or set TRENDSEARTH_API_PASSWORD)",
    )
    parser.add_argument(
        "--name",
        default="avoided-emissions-ci",
        help="Human-readable label for the service client (default: avoided-emissions-ci)",
    )
    parser.add_argument(
        "--scopes",
        default="",
        help='Space-delimited OAuth2 scopes (default: "" = full user access)',
    )
    parser.add_argument(
        "--expires-in-days",
        type=int,
        default=None,
        help="Optional lifetime in days (default: no expiry)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Output raw JSON instead of formatted text",
    )
    args = parser.parse_args()

    # Prompt for missing credentials
    email = args.email
    while not email:
        email = input("Admin email: ").strip()

    password = args.password
    if not password:
        password = getpass.getpass("Admin password: ")

    # Step 1: Login
    print(f"Logging in to {args.api_url} ...", file=sys.stderr)
    token = login(args.api_url, email, password)
    print("Login successful.", file=sys.stderr)

    # Step 2: Create service client
    print(f'Creating service client "{args.name}" ...', file=sys.stderr)
    data = create_client(
        api_url=args.api_url,
        token=token,
        name=args.name,
        scopes=args.scopes,
        expires_in_days=args.expires_in_days,
    )

    # Step 3: Output credentials
    if args.json_output:
        print(json.dumps(data, indent=2, default=str))
    else:
        print()
        print("=" * 60)
        print("  Service client created successfully")
        print("=" * 60)
        print()
        print(f"  Client ID:     {data['client_id']}")
        print(f"  Client Secret: {data['client_secret']}")
        print()
        print("  ⚠  The secret is shown ONCE. Store it securely now.")
        print()
        print("  Add these as GitHub repository secrets:")
        print(f"    TRENDS_EARTH_CLIENT_ID     = {data['client_id']}")
        print(f"    TRENDS_EARTH_CLIENT_SECRET = {data['client_secret']}")
        print()
        if data.get("expires_at"):
            print(f"  Expires: {data['expires_at']}")
        else:
            print("  Expires: never")
        print(f"  Name:    {data.get('name', args.name)}")
        print(f"  Scopes:  {data.get('scopes') or '(full access)'}")
        print("=" * 60)


if __name__ == "__main__":
    main()
