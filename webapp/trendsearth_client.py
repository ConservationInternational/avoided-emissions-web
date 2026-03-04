"""Client for the trends.earth REST API.

This module replaces the direct AWS Batch submission model.  Instead of
submitting Batch jobs directly, the webapp creates *Executions* on the
trends.earth API which dispatches and monitors the R pipeline.

Usage
-----
::

    from trendsearth_client import TrendsEarthClient

    # Using OAuth2 client credentials (preferred for long-lived services)
    client = TrendsEarthClient(
        api_url="https://api.trends.earth/api/v1",
        client_id="your-client-id",
        client_secret="your-client-secret",
    )

    # Using email/password (for interactive flows, e.g. linking accounts)
    client = TrendsEarthClient(
        api_url="https://api.trends.earth/api/v1",
        email="user@example.com",
        password="secret",
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

    def __init__(
        self,
        api_url=None,
        client_id=None,
        client_secret=None,
        email=None,
        password=None,
    ):
        self.api_url = (api_url or os.environ.get("TRENDSEARTH_API_URL", "")).rstrip(
            "/"
        )
        self._client_id = client_id or ""
        self._client_secret = client_secret or ""
        self._email = email or ""
        self._password = password or ""
        self._token = None

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def _headers(self):
        """Return auth headers with a Bearer token."""
        if self._token:
            return {"Authorization": f"Bearer {self._token}"}
        # Obtain a token via OAuth2 client credentials
        self._authenticate()
        return {"Authorization": f"Bearer {self._token}"}

    def _authenticate(self):
        """Authenticate using OAuth2 client credentials grant."""
        if not self._client_id or not self._client_secret:
            raise ValueError(
                "Cannot authenticate: client_id and client_secret are "
                "required. Set TRENDSEARTH_CLIENT_ID and "
                "TRENDSEARTH_CLIENT_SECRET environment variables or pass "
                "them to the constructor."
            )
        token_data = self.oauth2_token(self._client_id, self._client_secret)
        self._token = token_data["access_token"]

    def _login(self):
        """Authenticate with email/password and store the JWT.

        The auth endpoint lives at ``{base}/auth`` (outside ``/api/v1``).
        """
        base_url = self.api_url.rstrip("/")
        if base_url.endswith("/api/v1"):
            auth_url = base_url[: -len("/api/v1")] + "/auth"
        else:
            auth_url = base_url + "/auth"

        resp = requests.post(
            auth_url,
            json={"email": self._email, "password": self._password},
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        token = data.get("access_token")
        if not token:
            raise ValueError(
                "Login succeeded but the response did not contain an access_token."
            )
        self._token = token

    # ------------------------------------------------------------------
    # OAuth2 client management (Client Credentials grant)
    # ------------------------------------------------------------------

    def create_oauth2_client(
        self, name="avoided-emissions-web", scopes="", expires_in_days=None
    ):
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
            f"{self.api_url}/oauth/clients",
            json=body,
            headers=self._headers(),
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def list_oauth2_clients(self):
        """List the caller's active OAuth2 service clients."""
        resp = requests.get(
            f"{self.api_url}/oauth/clients",
            headers=self._headers(),
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def revoke_oauth2_client(self, client_db_id):
        """Revoke an OAuth2 service client by its database UUID."""
        resp = requests.delete(
            f"{self.api_url}/oauth/clients/{client_db_id}",
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
            f"{self.api_url}/oauth/token",
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
        url = f"{self.api_url}/script/{script_id}/run"
        logger.info(
            "[TE-API] POST %s (task_id=%s)",
            url,
            params.get("task_id", "?"),
        )
        resp = requests.post(
            url,
            json=params,
            headers=self._headers(),
            timeout=_TIMEOUT,
        )
        logger.info(
            "[TE-API] POST %s → %d %s",
            url,
            resp.status_code,
            resp.reason,
        )
        if not resp.ok:
            # Log the response body so the actual error detail is visible
            # in the webapp logs, not just the HTTP status code.
            try:
                body = resp.json()
            except Exception:
                body = resp.text[:500]
            logger.error("[TE-API] Error response from %s: %s", url, body)
        resp.raise_for_status()
        return resp.json()

    def get_execution(self, execution_id):
        """Fetch an execution's current state."""
        url = f"{self.api_url}/execution/{execution_id}"
        resp = requests.get(
            url,
            headers=self._headers(),
            timeout=_TIMEOUT,
        )
        if resp.status_code != 200:
            logger.warning(
                "[TE-API] GET %s → %d %s",
                url,
                resp.status_code,
                resp.reason,
            )
        resp.raise_for_status()
        return resp.json()

    def get_execution_results(self, execution_id):
        """Convenience: fetch execution and return its results payload."""
        data = self.get_execution(execution_id)
        return data.get("data", {}).get("results")

    def get_execution_logs(self, execution_id, last_id=None):
        """Fetch execution logs from the API.

        These are user-visible ``ExecutionLog`` entries created by the
        API's batch dispatch and monitoring tasks.

        Parameters
        ----------
        execution_id : str
            UUID of the execution.
        last_id : int | None
            Only return logs with an id greater than *last_id*.
            Useful for incremental polling.

        Returns
        -------
        list[dict]
            List of log entries, each with ``id``, ``text``, ``level``,
            ``register_date``, and ``execution_id``.
        """
        url = f"{self.api_url}/execution/{execution_id}/log"
        params = {}
        if last_id is not None:
            params["last-id"] = last_id
        try:
            resp = requests.get(
                url,
                params=params,
                headers=self._headers(),
                timeout=_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json().get("data", [])
        except Exception as exc:
            logger.warning(
                "[TE-API] Failed to fetch logs for execution %s: %s",
                execution_id,
                exc,
            )
            return []

    def list_executions(self, script_id=None, status=None, per_page=50):
        """List executions, optionally filtered."""
        params = {"per_page": per_page}
        if script_id:
            params["script_id"] = script_id
        if status:
            params["status"] = status
        resp = requests.get(
            f"{self.api_url}/execution",
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
            f"{self.api_url}/script/{script_id}",
            headers=self._headers(),
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def find_script_by_slug(self, slug):
        """Find a script by its slug name."""
        resp = requests.get(
            f"{self.api_url}/script",
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
