"""Trends.Earth OpenID Connect client.

Authlib performs the OAuth protocol work against the Trends.Earth API (the
issuer): the authorization request with state, nonce and PKCE (S256), the code
and refresh-token grants, and token revocation, all authenticated with
``client_secret_post``. ID and access tokens are verified here with PyJWT
against the published JWKS, together with the Trends.Earth-specific claims.
"""

import hmac
import html
import logging
import threading
import time
from urllib.parse import urlparse

import jwt
import requests
from authlib.common.security import generate_token
from authlib.integrations.requests_client import OAuth2Session, OAuthError
from config import Config

logger = logging.getLogger(__name__)

_HTTP_TIMEOUT = 15
_DISCOVERY_TTL_SECONDS = 3600
_ALGORITHMS = ("RS256",)
# Trends.Earth issues access tokens for 5 minutes; reject anything claiming
# to live longer than that (plus clock skew).
_MAX_ACCESS_TOKEN_LIFETIME = 300
_LEEWAY_SECONDS = 30
# The OIDC transaction (state, nonce, PKCE verifier) must complete within
# this many seconds of starting the login.
_LOGIN_TRANSACTION_MAX_AGE = 600
_PENDING_KEY = "oidc_pending"

_discovery_cache = {"doc": None, "fetched_at": 0.0}
_jwk_client = None
_lock = threading.Lock()


class OIDCError(Exception):
    """Base class for OIDC failures."""


class OIDCProviderUnavailable(OIDCError):
    """Trends.Earth or its key set could not be reached."""


class OIDCTokenRejected(OIDCError):
    """Trends.Earth refused a grant, or a token failed verification."""

    def __init__(self, message, error=None):
        super().__init__(message)
        self.error = error


class OIDCAccessDenied(OIDCTokenRejected):
    """The user is not (or no longer) allowed to use this app."""

    def __init__(self, message, status="revoked"):
        super().__init__(message, error="access_denied")
        self.status = status


# ---------------------------------------------------------------------------
# Provider metadata and keys
# ---------------------------------------------------------------------------


def discovery():
    """Return the (cached) OpenID provider configuration document."""
    now = time.time()
    doc = _discovery_cache["doc"]
    if doc and now - _discovery_cache["fetched_at"] < _DISCOVERY_TTL_SECONDS:
        return doc
    with _lock:
        doc = _discovery_cache["doc"]
        if doc and now - _discovery_cache["fetched_at"] < _DISCOVERY_TTL_SECONDS:
            return doc
        url = f"{Config.OIDC_ISSUER}/.well-known/openid-configuration"
        try:
            resp = requests.get(url, timeout=_HTTP_TIMEOUT)
            resp.raise_for_status()
            doc = resp.json()
        except (requests.RequestException, ValueError) as exc:
            if _discovery_cache["doc"]:
                logger.warning("OIDC discovery refresh failed; using cached copy")
                return _discovery_cache["doc"]
            raise OIDCProviderUnavailable(
                "Could not reach the Trends.Earth sign-in service"
            ) from exc
        if doc.get("issuer", "").rstrip("/") != Config.OIDC_ISSUER:
            raise OIDCProviderUnavailable(
                "Trends.Earth discovery document names an unexpected issuer"
            )
        _discovery_cache.update(doc=doc, fetched_at=now)
        return doc


def _endpoint(name):
    url = discovery().get(name)
    if not url:
        raise OIDCProviderUnavailable(f"Trends.Earth does not advertise {name}")
    return url


def _get_jwk_client():
    global _jwk_client
    if _jwk_client is None:
        _jwk_client = jwt.PyJWKClient(
            _endpoint("jwks_uri"), lifespan=300, cache_keys=False, timeout=10
        )
    return _jwk_client


def reset_caches():
    """Forget cached discovery metadata and keys (used by tests)."""
    global _jwk_client
    _discovery_cache.update(doc=None, fetched_at=0.0)
    _jwk_client = None


# ---------------------------------------------------------------------------
# Token verification
# ---------------------------------------------------------------------------


def _decode(token, *, audience, token_use):
    if not token or token.count(".") != 2:
        raise OIDCTokenRejected("Malformed token")
    try:
        header = jwt.get_unverified_header(token)
        if header.get("alg") not in _ALGORITHMS:
            raise OIDCTokenRejected("Unsupported token signing algorithm")
        key = _get_jwk_client().get_signing_key_from_jwt(token).key
        claims = jwt.decode(
            token,
            key,
            algorithms=list(_ALGORITHMS),
            audience=audience,
            issuer=Config.OIDC_ISSUER,
            leeway=_LEEWAY_SECONDS,
            options={"require": ["exp", "iat", "sub", "token_use"]},
        )
    except OIDCError:
        raise
    except jwt.PyJWKClientConnectionError as exc:
        raise OIDCProviderUnavailable("Could not fetch Trends.Earth keys") from exc
    except jwt.PyJWKClientError as exc:
        raise OIDCTokenRejected("Token signing key is not trusted") from exc
    except jwt.InvalidTokenError as exc:
        raise OIDCTokenRejected(f"Token could not be verified: {exc}") from exc
    except (jwt.PyJWKError, ValueError, OSError) as exc:
        raise OIDCProviderUnavailable("Trends.Earth key set is unusable") from exc

    if claims.get("token_use") != token_use:
        raise OIDCTokenRejected(f"Token is not an {token_use}")
    if not isinstance(claims.get("sub"), str) or not claims["sub"]:
        raise OIDCTokenRejected("Token has no subject")
    return claims


def verify_access_token(token):
    """Verify a Trends.Earth access token issued to this app.

    Raises :class:`OIDCAccessDenied` if the token does not carry access to
    ``Config.OIDC_APP_KEY``.
    """
    claims = _decode(
        token, audience=Config.OIDC_ACCESS_AUDIENCE, token_use="access_token"
    )
    if Config.OIDC_CLIENT_ID not in (claims.get("client_id"), claims.get("azp")):
        raise OIDCTokenRejected("Access token was issued to a different client")
    if claims["exp"] - claims["iat"] > _MAX_ACCESS_TOKEN_LIFETIME + _LEEWAY_SECONDS:
        raise OIDCTokenRejected("Access token lifetime is too long")
    if app_role(claims) is None:
        raise OIDCAccessDenied(
            "Your Trends.Earth account does not have access to this app"
        )
    return claims


def verify_id_token(token, *, nonce):
    claims = _decode(token, audience=Config.OIDC_CLIENT_ID, token_use="id_token")
    if claims.get("azp") not in (None, Config.OIDC_CLIENT_ID):
        raise OIDCTokenRejected("ID token was issued to a different client")
    token_nonce = claims.get("nonce")
    if not isinstance(token_nonce, str) or not hmac.compare_digest(token_nonce, nonce):
        raise OIDCTokenRejected("ID token nonce does not match")
    return claims


def app_role(claims):
    """Return the user's role in this app, or ``None`` if they lack access."""
    key = Config.OIDC_APP_KEY
    access = claims.get("app_access") or []
    roles = claims.get("app_roles") or {}
    if key not in access:
        return None
    role = roles.get(key) if isinstance(roles, dict) else None
    return role if role in ("member", "admin") else "member"


def session_fields_from_access(tokens, claims):
    """Session-record fields derived from a token response + access claims."""
    expires_in = int(tokens.get("expires_in") or 0)
    expires_at = min(
        int(claims["exp"]), int(time.time()) + expires_in if expires_in else 1 << 62
    )
    fields = {
        "sub": claims["sub"],
        "access_token": tokens["access_token"],
        "access_expires_at": expires_at,
        "app_role": app_role(claims),
        "te_role": claims.get("role"),
    }
    if tokens.get("refresh_token"):
        fields["refresh_token"] = tokens["refresh_token"]
    return fields


# ---------------------------------------------------------------------------
# Authorization-code flow
# ---------------------------------------------------------------------------


def _oauth_client():
    """A fresh Authlib client (``requests`` sessions are not thread-safe)."""
    return OAuth2Session(
        client_id=Config.OIDC_CLIENT_ID,
        client_secret=Config.OIDC_CLIENT_SECRET,
        # Trends.Earth supports only ``none`` and ``client_secret_post``.
        token_endpoint_auth_method="client_secret_post",
        revocation_endpoint_auth_method="client_secret_post",
        redirect_uri=Config.OIDC_REDIRECT_URI,
        code_challenge_method="S256",
    )


def safe_next_url(value):
    """Return *value* if it is a same-origin relative path, else ``/``."""
    if not value or not isinstance(value, str):
        return "/"
    parsed = urlparse(value)
    if (
        parsed.scheme
        or parsed.netloc
        or not value.startswith("/")
        or value.startswith("//")
        or "\\" in value
    ):
        return "/"
    if value.startswith("/auth/"):
        return "/"
    return value


def begin_login(session, next_url="/"):
    """Start a login: stash state in *session* and return the authorize URL."""
    nonce = generate_token(43)
    verifier = generate_token(64)
    with _oauth_client() as client:
        url, state = client.create_authorization_url(
            _endpoint("authorization_endpoint"),
            state=generate_token(43),
            code_verifier=verifier,
            nonce=nonce,
            scope=Config.OIDC_SCOPES,
        )
    session[_PENDING_KEY] = {
        "state": state,
        "nonce": nonce,
        "verifier": verifier,
        "next": safe_next_url(next_url),
        "started_at": int(time.time()),
    }
    return url


def _token_request(grant_type, **params):
    """Call the token endpoint through Authlib and return the token response."""
    url = _endpoint("token_endpoint")
    try:
        with _oauth_client() as client:
            tokens = client.fetch_token(
                url, grant_type=grant_type, timeout=_HTTP_TIMEOUT, **params
            )
    except OAuthError as exc:
        description = exc.description or exc.error or "invalid_grant"
        if exc.error == "access_denied":
            raise OIDCAccessDenied(description) from exc
        raise OIDCTokenRejected(description, error=exc.error) from exc
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else 0
        if status >= 500:
            raise OIDCProviderUnavailable(
                f"Trends.Earth sign-in service error ({status})"
            ) from exc
        raise OIDCTokenRejected(
            "Trends.Earth rejected the token request", error="invalid_grant"
        ) from exc
    except requests.RequestException as exc:
        raise OIDCProviderUnavailable(
            "Could not reach the Trends.Earth sign-in service"
        ) from exc
    except ValueError as exc:
        raise OIDCTokenRejected(
            "Trends.Earth returned an invalid token response", error="invalid_grant"
        ) from exc
    if not isinstance(tokens.get("access_token"), str) or not tokens["access_token"]:
        raise OIDCTokenRejected(
            "Trends.Earth returned no access token", error="invalid_grant"
        )
    return dict(tokens)


def handle_callback(session, args):
    """Complete a login from the callback query *args*.

    Returns ``(record, next_url)`` where *record* is the new session record.
    Raises :class:`OIDCAccessDenied` (with ``status`` pending, revoked or
    unavailable) when Trends.Earth refused access to the app.
    """
    pending = session.pop(_PENDING_KEY, None)
    if not pending:
        raise OIDCTokenRejected("No sign-in is in progress")
    if time.time() - pending.get("started_at", 0) > _LOGIN_TRANSACTION_MAX_AGE:
        raise OIDCTokenRejected("Sign-in took too long; please try again")
    state = args.get("state") or ""
    if not hmac.compare_digest(state, pending["state"]):
        raise OIDCTokenRejected("Sign-in state does not match")

    error = args.get("error")
    if error:
        if error == "access_denied":
            status = args.get("te_app_access") or "denied"
            raise OIDCAccessDenied(
                args.get("error_description") or "Access was denied", status=status
            )
        raise OIDCTokenRejected(args.get("error_description") or error, error=error)

    code = args.get("code")
    if not code:
        raise OIDCTokenRejected("Sign-in response did not include a code")

    tokens = _token_request(
        "authorization_code", code=code, code_verifier=pending["verifier"]
    )
    id_claims = verify_id_token(tokens.get("id_token"), nonce=pending["nonce"])
    access_claims = verify_access_token(tokens["access_token"])
    if id_claims["sub"] != access_claims["sub"]:
        raise OIDCTokenRejected("ID and access tokens name different users")
    if not tokens.get("refresh_token"):
        raise OIDCTokenRejected("Trends.Earth did not issue a refresh token")

    record = {
        "email": id_claims.get("email") or "",
        "name": id_claims.get("name") or id_claims.get("email") or "",
        "id_token": tokens["id_token"],
        **session_fields_from_access(tokens, access_claims),
    }
    return record, pending.get("next") or "/"


def refresh(refresh_token):
    """Exchange a refresh token; returns the token response."""
    if not refresh_token:
        raise OIDCTokenRejected("No refresh token")
    # fetch_token (not Authlib's refresh_token helper) so a response without a
    # rotated refresh token is not silently patched with the spent one.
    return _token_request("refresh_token", refresh_token=refresh_token)


def revoke(token):
    """Best-effort revocation of an access or refresh token."""
    if not token:
        return
    try:
        url = discovery().get("revocation_endpoint")
        if not url:
            return
        with _oauth_client() as client:
            client.revoke_token(url, token=token, timeout=_HTTP_TIMEOUT)
    except (requests.RequestException, OIDCError):
        logger.warning("Token revocation failed", exc_info=True)


def logout_page(id_token_hint=None):
    """HTML that POSTs the browser to the Trends.Earth logout endpoint.

    Ending the Trends.Earth browser session as well prevents the next
    "Sign in" from silently re-authenticating the same person on a shared
    computer.
    """
    try:
        action = _endpoint("end_session_endpoint")
    except OIDCError:
        action = f"{Config.OIDC_ISSUER}/oauth/logout"
    fields = {
        "client_id": Config.OIDC_CLIENT_ID,
        "post_logout_redirect_uri": f"{Config.APP_URL}/",
    }
    if id_token_hint:
        fields["id_token_hint"] = id_token_hint
    inputs = "".join(
        f'<input type="hidden" name="{html.escape(k)}" value="{html.escape(v)}">'
        for k, v in fields.items()
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>Signing out…</title></head>
<body>
<form id="te-logout" method="post" action="{html.escape(action)}">{inputs}
<noscript><button type="submit">Continue signing out</button></noscript>
</form>
<script>document.getElementById("te-logout").submit();</script>
</body></html>"""
