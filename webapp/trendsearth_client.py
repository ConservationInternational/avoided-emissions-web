"""Client for the trends.earth REST API.

This module replaces the direct AWS Batch submission model.  Instead of
submitting Batch jobs directly, the webapp creates *Executions* on the
trends.earth API which dispatches and monitors the R pipeline.

Usage
-----
::

    from trendsearth_client import TrendsEarthClient

    client = TrendsEarthClient(
        api_url="https://api.trends.earth/api/v1",
        api_key="te_abc123...",
    )
    execution = client.create_execution(script_id, params)
    status = client.get_execution(execution["id"])
"""

import logging
import os

import requests

logger = logging.getLogger(__name__)

# Default timeout for API calls (seconds)
_TIMEOUT = 30


class TrendsEarthClient:
    """Lightweight client for the trends.earth API."""

    def __init__(self, api_url=None, api_key=None, email=None, password=None):
        self.api_url = (
            api_url
            or os.environ.get("TRENDSEARTH_API_URL", "")
        ).rstrip("/")
        self._api_key = api_key or os.environ.get("TRENDSEARTH_API_KEY", "")
        self._email = email or os.environ.get("TRENDSEARTH_API_EMAIL", "")
        self._password = password or os.environ.get(
            "TRENDSEARTH_API_PASSWORD", ""
        )
        self._token = None

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def _headers(self):
        """Return auth headers, preferring API key."""
        if self._api_key:
            return {"X-API-Key": self._api_key}
        if self._token:
            return {"Authorization": f"Bearer {self._token}"}
        # Log in
        self._login()
        return {"Authorization": f"Bearer {self._token}"}

    def _login(self):
        resp = requests.post(
            f"{self.api_url}/auth",
            json={"email": self._email, "password": self._password},
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        self._token = resp.json().get("access_token")

    # ------------------------------------------------------------------
    # API key management
    # ------------------------------------------------------------------

    def create_api_key(self, name="avoided-emissions-web"):
        """Create a new API key (requires JWT auth)."""
        resp = requests.post(
            f"{self.api_url}/api/v1/api-key",
            json={"name": name},
            headers=self._headers(),
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def list_api_keys(self):
        resp = requests.get(
            f"{self.api_url}/api/v1/api-key",
            headers=self._headers(),
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def revoke_api_key(self, key_id):
        resp = requests.delete(
            f"{self.api_url}/api/v1/api-key/{key_id}",
            headers=self._headers(),
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # OAuth2 client management (Client Credentials grant)
    # ------------------------------------------------------------------

    def create_oauth2_client(self, name="avoided-emissions-web",
                             scopes="", expires_in_days=None):
        """Register a new OAuth2 service client on the API.

        Requires JWT authentication (email/password login).  The response
        includes the one-time ``client_secret`` that must be stored
        securely — it cannot be retrieved again.

        Parameters
        ----------
        name : str
            Human-readable label for the client.
        scopes : str
            Space-delimited scope list (empty = full user access).
        expires_in_days : int | None
            Optional lifetime in days.  ``None`` means no expiry.

        Returns
        -------
        dict
            ``{"data": {..., "client_id": "...", "client_secret": "..."}}``
        """
        body = {"name": name}
        if scopes:
            body["scopes"] = scopes
        if expires_in_days is not None:
            body["expires_in_days"] = expires_in_days

        resp = requests.post(
            f"{self.api_url}/api/v1/oauth/clients",
            json=body,
            headers=self._headers(),
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def list_oauth2_clients(self):
        """List the caller's active OAuth2 service clients."""
        resp = requests.get(
            f"{self.api_url}/api/v1/oauth/clients",
            headers=self._headers(),
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def revoke_oauth2_client(self, client_db_id):
        """Revoke an OAuth2 service client by its database UUID."""
        resp = requests.delete(
            f"{self.api_url}/api/v1/oauth/clients/{client_db_id}",
            headers=self._headers(),
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def oauth2_token(self, client_id, client_secret):
        """Exchange OAuth2 client credentials for a short-lived JWT.

        Uses the Client Credentials grant (``grant_type=client_credentials``).

        Parameters
        ----------
        client_id : str
        client_secret : str

        Returns
        -------
        dict
            ``{"access_token": "...", "token_type": "bearer", "expires_in": ...}``
        """
        resp = requests.post(
            f"{self.api_url}/api/v1/oauth/token",
            json={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    @classmethod
    def from_oauth2_credentials(cls, api_url, client_id, client_secret):
        """Create a client authenticated via OAuth2 client credentials.

        Immediately obtains an access token and uses it for subsequent
        requests.

        Parameters
        ----------
        api_url : str
        client_id : str
        client_secret : str

        Returns
        -------
        TrendsEarthClient
        """
        instance = cls(api_url=api_url)
        token_data = instance.oauth2_token(client_id, client_secret)
        instance._token = token_data["access_token"]
        return instance

    # ------------------------------------------------------------------
    # Execution management
    # ------------------------------------------------------------------

    def create_execution(self, script_id, params):
        """Create a new execution on the API.

        The API handles dispatching to the appropriate compute backend
        (Docker or AWS Batch) based on the script's ``environment`` field.

        Parameters
        ----------
        script_id : str
            UUID of the registered avoided-emissions script.
        params : dict
            Execution parameters (AvoidedEmissionsParams schema).

        Returns
        -------
        dict
            Execution record including ``id``, ``status``.
        """
        resp = requests.post(
            f"{self.api_url}/api/v1/script/{script_id}/run",
            json={"params": params},
            headers=self._headers(),
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def get_execution(self, execution_id):
        """Fetch an execution's current state."""
        resp = requests.get(
            f"{self.api_url}/api/v1/execution/{execution_id}",
            headers=self._headers(),
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def get_execution_results(self, execution_id):
        """Convenience: fetch execution and return its results payload."""
        data = self.get_execution(execution_id)
        return data.get("data", {}).get("attributes", {}).get("results")

    def list_executions(self, script_id=None, status=None, per_page=50):
        """List executions, optionally filtered."""
        params = {"per_page": per_page}
        if script_id:
            params["script_id"] = script_id
        if status:
            params["status"] = status
        resp = requests.get(
            f"{self.api_url}/api/v1/execution",
            params=params,
            headers=self._headers(),
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Script management
    # ------------------------------------------------------------------

    def get_script(self, script_id):
        resp = requests.get(
            f"{self.api_url}/api/v1/script/{script_id}",
            headers=self._headers(),
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def find_script_by_slug(self, slug):
        """Find a script by its slug name."""
        resp = requests.get(
            f"{self.api_url}/api/v1/script",
            params={"slug": slug},
            headers=self._headers(),
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        scripts = data.get("data", [])
        for s in scripts:
            attrs = s.get("attributes", {})
            if attrs.get("slug") == slug:
                return s
        return None
