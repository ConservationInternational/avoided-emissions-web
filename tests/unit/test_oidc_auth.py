"""Unit tests for Trends.Earth OIDC sign-in (oidc.py, session_store.py, auth.py).

Tokens are signed with a throwaway RSA key and verified through a stubbed
JWK client; Redis is replaced by a small in-memory stand-in.  No network
or Redis connection is made.
"""

import base64
import hashlib
import json
import time
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import jwt
import oidc
import pytest
import requests
import session_store
from config import Config
from cryptography.hazmat.primitives.asymmetric import rsa

_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _sign(claims, key=_KEY):
    return jwt.encode(claims, key, algorithm="RS256", headers={"kid": "test"})


def _access_claims(**overrides):
    now = int(time.time())
    claims = {
        "iss": Config.OIDC_ISSUER,
        "aud": Config.OIDC_ACCESS_AUDIENCE,
        "sub": "user-sub",
        "client_id": Config.OIDC_CLIENT_ID,
        "token_use": "access_token",
        "iat": now,
        "exp": now + 300,
        "role": "USER",
        "app_access": [Config.OIDC_APP_KEY],
        "app_roles": {Config.OIDC_APP_KEY: "member"},
    }
    claims.update(overrides)
    return claims


def _id_claims(nonce, **overrides):
    now = int(time.time())
    claims = {
        "iss": Config.OIDC_ISSUER,
        "aud": Config.OIDC_CLIENT_ID,
        "sub": "user-sub",
        "token_use": "id_token",
        "iat": now,
        "exp": now + 3600,
        "nonce": nonce,
        "email": "user@example.org",
        "name": "Test User",
    }
    claims.update(overrides)
    return claims


@pytest.fixture(autouse=True)
def _stub_provider(mocker):
    oidc.reset_caches()
    jwk = SimpleNamespace(
        get_signing_key_from_jwt=lambda token: SimpleNamespace(key=_KEY.public_key())
    )
    mocker.patch("oidc._get_jwk_client", return_value=jwk)
    mocker.patch(
        "oidc.discovery",
        return_value={
            "issuer": Config.OIDC_ISSUER,
            "authorization_endpoint": f"{Config.OIDC_ISSUER}/oauth/authorize",
            "token_endpoint": f"{Config.OIDC_ISSUER}/oauth/token",
            "revocation_endpoint": f"{Config.OIDC_ISSUER}/oauth/revoke",
            "jwks_uri": f"{Config.OIDC_ISSUER}/.well-known/jwks.json",
        },
    )
    yield
    oidc.reset_caches()


# ---------------------------------------------------------------------------
# Token verification
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestVerifyAccessToken:
    def test_accepts_valid_token(self):
        claims = oidc.verify_access_token(_sign(_access_claims()))
        assert claims["sub"] == "user-sub"
        assert oidc.app_role(claims) == "member"

    def test_reads_admin_role(self):
        token = _sign(_access_claims(app_roles={Config.OIDC_APP_KEY: "admin"}))
        assert oidc.app_role(oidc.verify_access_token(token)) == "admin"

    def test_role_defaults_to_member(self):
        token = _sign(_access_claims(app_roles={}))
        assert oidc.app_role(oidc.verify_access_token(token)) == "member"

    def test_rejects_missing_app_access(self):
        token = _sign(_access_claims(app_access=["rio_coherence"]))
        with pytest.raises(oidc.OIDCAccessDenied):
            oidc.verify_access_token(token)

    def test_rejects_wrong_audience(self):
        with pytest.raises(oidc.OIDCTokenRejected):
            oidc.verify_access_token(_sign(_access_claims(aud="other-app")))

    def test_rejects_wrong_issuer(self):
        with pytest.raises(oidc.OIDCTokenRejected):
            oidc.verify_access_token(_sign(_access_claims(iss="https://evil.test")))

    def test_rejects_other_client(self):
        with pytest.raises(oidc.OIDCTokenRejected, match="different client"):
            oidc.verify_access_token(_sign(_access_claims(client_id="rio")))

    def test_rejects_id_token_used_as_access_token(self):
        with pytest.raises(oidc.OIDCTokenRejected):
            oidc.verify_access_token(_sign(_access_claims(token_use="id_token")))

    def test_rejects_long_lived_token(self):
        now = int(time.time())
        token = _sign(_access_claims(iat=now, exp=now + 86400))
        with pytest.raises(oidc.OIDCTokenRejected, match="lifetime"):
            oidc.verify_access_token(token)

    def test_rejects_expired_token(self):
        now = int(time.time())
        token = _sign(_access_claims(iat=now - 900, exp=now - 600))
        with pytest.raises(oidc.OIDCTokenRejected):
            oidc.verify_access_token(token)

    def test_rejects_untrusted_signature(self):
        other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        with pytest.raises(oidc.OIDCTokenRejected):
            oidc.verify_access_token(_sign(_access_claims(), key=other))

    def test_rejects_unsigned_token(self):
        token = jwt.encode(_access_claims(), key=None, algorithm="none")
        with pytest.raises(oidc.OIDCTokenRejected):
            oidc.verify_access_token(token)


@pytest.mark.unit
class TestVerifyIdToken:
    def test_accepts_matching_nonce(self):
        claims = oidc.verify_id_token(_sign(_id_claims("n1")), nonce="n1")
        assert claims["email"] == "user@example.org"

    def test_rejects_nonce_mismatch(self):
        with pytest.raises(oidc.OIDCTokenRejected, match="nonce"):
            oidc.verify_id_token(_sign(_id_claims("n1")), nonce="n2")


# ---------------------------------------------------------------------------
# Authorization-code flow
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSafeNextUrl:
    @pytest.mark.parametrize(
        "value", ["/", "/task/abc", "/admin?status=pending", "/settings"]
    )
    def test_keeps_local_paths(self, value):
        assert oidc.safe_next_url(value) == value

    @pytest.mark.parametrize(
        "value",
        [
            None,
            "",
            "https://evil.test/",
            "//evil.test/",
            "/\\evil.test",
            "javascript:alert(1)",
            "task/abc",
            "/auth/logout",
        ],
    )
    def test_rejects_unsafe_values(self, value):
        assert oidc.safe_next_url(value) == "/"


def _start_login(next_url="/task/1"):
    session = {}
    url = oidc.begin_login(session, next_url)
    return session, url


@pytest.mark.unit
class TestLoginFlow:
    def test_begin_login_uses_pkce_and_state(self):
        session, url = _start_login()
        parsed = urlparse(url)
        params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        pending = session["oidc_pending"]
        assert url.startswith(f"{Config.OIDC_ISSUER}/oauth/authorize?")
        assert params["client_id"] == Config.OIDC_CLIENT_ID
        assert params["redirect_uri"] == Config.OIDC_REDIRECT_URI
        assert params["code_challenge_method"] == "S256"
        assert params["state"] == pending["state"]
        assert params["nonce"] == pending["nonce"]
        assert params["code_challenge"] != pending["verifier"]
        assert "openid" in params["scope"].split()
        assert pending["next"] == "/task/1"

    def test_callback_success(self, mocker):
        session, _ = _start_login()
        pending = dict(session["oidc_pending"])
        token_request = mocker.patch(
            "oidc._token_request",
            return_value={
                "access_token": _sign(_access_claims()),
                "id_token": _sign(_id_claims(pending["nonce"])),
                "refresh_token": "rt-1",
                "expires_in": 300,
            },
        )
        record, next_url = oidc.handle_callback(
            session, {"state": pending["state"], "code": "abc"}
        )
        assert next_url == "/task/1"
        assert record["sub"] == "user-sub"
        assert record["email"] == "user@example.org"
        assert record["name"] == "Test User"
        assert record["app_role"] == "member"
        assert record["refresh_token"] == "rt-1"
        assert token_request.call_args.args == ("authorization_code",)
        sent = token_request.call_args.kwargs
        assert sent == {"code": "abc", "code_verifier": pending["verifier"]}
        assert "oidc_pending" not in session

    def test_callback_rejects_state_mismatch(self, mocker):
        session, _ = _start_login()
        token_request = mocker.patch("oidc._token_request")
        with pytest.raises(oidc.OIDCTokenRejected, match="state"):
            oidc.handle_callback(session, {"state": "forged", "code": "abc"})
        token_request.assert_not_called()

    def test_callback_without_login_in_progress(self):
        with pytest.raises(oidc.OIDCTokenRejected):
            oidc.handle_callback({}, {"state": "x", "code": "abc"})

    def test_callback_rejects_stale_login(self):
        session, _ = _start_login()
        session["oidc_pending"]["started_at"] -= 3600
        with pytest.raises(oidc.OIDCTokenRejected, match="too long"):
            oidc.handle_callback(
                session, {"state": session["oidc_pending"]["state"], "code": "c"}
            )

    @pytest.mark.parametrize("status", ["pending", "revoked", "unavailable"])
    def test_callback_reports_app_access_status(self, status):
        session, _ = _start_login()
        state = session["oidc_pending"]["state"]
        with pytest.raises(oidc.OIDCAccessDenied) as excinfo:
            oidc.handle_callback(
                session,
                {"state": state, "error": "access_denied", "te_app_access": status},
            )
        assert excinfo.value.status == status

    def test_callback_rejects_mismatched_subjects(self, mocker):
        session, _ = _start_login()
        pending = dict(session["oidc_pending"])
        mocker.patch(
            "oidc._token_request",
            return_value={
                "access_token": _sign(_access_claims(sub="someone-else")),
                "id_token": _sign(_id_claims(pending["nonce"])),
                "refresh_token": "rt-1",
            },
        )
        with pytest.raises(oidc.OIDCTokenRejected, match="different users"):
            oidc.handle_callback(session, {"state": pending["state"], "code": "c"})

    def test_logout_page_posts_to_trends_earth(self):
        page = oidc.logout_page("id.token.value")
        assert f'action="{Config.OIDC_ISSUER}/oauth/logout"' in page
        assert 'name="id_token_hint" value="id.token.value"' in page
        assert f'value="{Config.APP_URL}/"' in page


# ---------------------------------------------------------------------------
# Session store
# ---------------------------------------------------------------------------


class _FakeLock:
    def acquire(self):
        return True

    def release(self):
        pass


class _FakeRedis:
    def __init__(self):
        self.data = {}

    def set(self, key, value, ex=None):
        self.data[key] = value

    def get(self, key):
        return self.data.get(key)

    def ttl(self, key):
        return 100 if key in self.data else -2

    def delete(self, key):
        self.data.pop(key, None)

    def lock(self, name, timeout=None, blocking_timeout=None):
        return _FakeLock()


@pytest.fixture
def fake_redis():
    client = _FakeRedis()
    session_store.set_redis(client)
    yield client
    session_store.set_redis(None)


def _record(expires_in=300, refresh_token="rt-1"):
    return {
        "sub": "user-sub",
        "email": "user@example.org",
        "name": "Test User",
        "access_token": "old-access",
        "access_expires_at": int(time.time()) + expires_in,
        "refresh_token": refresh_token,
        "app_role": "member",
    }


@pytest.mark.unit
class TestSessionStore:
    def test_create_and_get(self, fake_redis):
        sid = session_store.create(_record())
        assert session_store.get(sid)["email"] == "user@example.org"
        assert session_store.get("missing") is None

    def test_fresh_token_is_not_refreshed(self, fake_redis, mocker):
        refresh = mocker.patch("oidc.refresh")
        sid = session_store.create(_record())
        assert session_store.get_access_token(sid) == "old-access"
        refresh.assert_not_called()

    def test_expiring_token_is_refreshed_and_rotated(self, fake_redis, mocker):
        new_access = _sign(_access_claims(app_roles={Config.OIDC_APP_KEY: "admin"}))
        refresh = mocker.patch(
            "oidc.refresh",
            return_value={
                "access_token": new_access,
                "refresh_token": "rt-2",
                "expires_in": 300,
            },
        )
        sid = session_store.create(_record(expires_in=5))
        assert session_store.get_access_token(sid) == new_access
        refresh.assert_called_once_with("rt-1")
        stored = session_store.get(sid)
        assert stored["refresh_token"] == "rt-2"
        assert stored["app_role"] == "admin"
        assert stored["email"] == "user@example.org"

    def test_rejected_refresh_ends_session(self, fake_redis, mocker):
        mocker.patch(
            "oidc.refresh",
            side_effect=oidc.OIDCTokenRejected("revoked", error="invalid_grant"),
        )
        sid = session_store.create(_record(expires_in=5))
        with pytest.raises(session_store.SessionExpired):
            session_store.ensure_fresh(sid)
        assert session_store.get(sid) is None

    def test_revoked_app_access_ends_session(self, fake_redis, mocker):
        mocker.patch(
            "oidc.refresh",
            return_value={
                "access_token": _sign(_access_claims(app_access=[])),
                "refresh_token": "rt-2",
            },
        )
        sid = session_store.create(_record(expires_in=5))
        with pytest.raises(session_store.SessionExpired):
            session_store.ensure_fresh(sid)
        assert session_store.get(sid) is None

    def test_provider_outage_keeps_session(self, fake_redis, mocker):
        mocker.patch("oidc.refresh", side_effect=oidc.OIDCProviderUnavailable("down"))
        sid = session_store.create(_record(expires_in=5))
        with pytest.raises(session_store.SessionUnavailable):
            session_store.ensure_fresh(sid)
        assert session_store.get(sid) is not None

    def test_missing_session_is_expired(self, fake_redis):
        with pytest.raises(session_store.SessionExpired):
            session_store.ensure_fresh("missing")


# ---------------------------------------------------------------------------
# SessionUser
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSessionUser:
    def test_member(self):
        from auth import SessionUser

        user = SessionUser(_record(), session_id="sid")
        assert user.id == "user-sub"
        assert user.get_id() == "user-sub"
        assert user.email == "user@example.org"
        assert user.session_id == "sid"
        assert not user.is_admin

    def test_admin(self):
        from auth import SessionUser

        record = _record()
        record["app_role"] = "admin"
        assert SessionUser(record).is_admin


# ---------------------------------------------------------------------------
# Token endpoint requests made through Authlib
# ---------------------------------------------------------------------------


class _FakeProvider:
    """Answers requests made by Authlib's ``requests`` session."""

    def __init__(self, status=200, body=None, text=None, error=None):
        self.status, self.body, self.text, self.error = status, body, text, error
        self.requests = []

    def send(self, session, request, **kwargs):
        self.requests.append((request, kwargs))
        if self.error:
            raise self.error
        resp = requests.Response()
        resp.status_code = self.status
        resp.url = request.url
        resp.request = request
        if self.text is not None:
            resp._content = self.text.encode()
        else:
            resp._content = json.dumps(self.body).encode()
            resp.headers["Content-Type"] = "application/json"
        return resp

    def form(self, index=-1):
        request, _ = self.requests[index]
        return {k: v[0] for k, v in parse_qs(request.body).items()}


@pytest.fixture
def provider(mocker):
    fake = _FakeProvider(body={"access_token": "at", "refresh_token": "rt-2"})
    mocker.patch.object(requests.Session, "send", autospec=True, side_effect=fake.send)
    return fake


@pytest.mark.unit
class TestAuthlibTokenRequests:
    def test_code_exchange_sends_pkce_verifier_and_client_secret_post(self, provider):
        session, url = _start_login()
        pending = session["oidc_pending"]
        challenge = parse_qs(urlparse(url).query)["code_challenge"][0]
        expected = (
            base64.urlsafe_b64encode(
                hashlib.sha256(pending["verifier"].encode()).digest()
            )
            .rstrip(b"=")
            .decode()
        )
        assert challenge == expected

        tokens = oidc._token_request(
            "authorization_code", code="abc", code_verifier=pending["verifier"]
        )

        assert tokens["access_token"] == "at"
        request, kwargs = provider.requests[0]
        assert request.url == f"{Config.OIDC_ISSUER}/oauth/token"
        assert "Authorization" not in request.headers
        assert kwargs["timeout"] == oidc._HTTP_TIMEOUT
        assert provider.form() == {
            "grant_type": "authorization_code",
            "code": "abc",
            "code_verifier": pending["verifier"],
            "redirect_uri": Config.OIDC_REDIRECT_URI,
            "client_id": Config.OIDC_CLIENT_ID,
            "client_secret": Config.OIDC_CLIENT_SECRET,
        }

    def test_refresh_does_not_reuse_spent_refresh_token(self, provider):
        provider.body = {"access_token": "at-2"}
        tokens = oidc.refresh("rt-1")
        assert "refresh_token" not in tokens
        form = provider.form()
        assert form["grant_type"] == "refresh_token"
        assert form["refresh_token"] == "rt-1"
        assert "scope" not in form

    @pytest.mark.parametrize(
        "status,body,text,expected,error",
        [
            (
                400,
                {"error": "invalid_grant"},
                None,
                oidc.OIDCTokenRejected,
                "invalid_grant",
            ),
            (
                400,
                {"error": "access_denied"},
                None,
                oidc.OIDCAccessDenied,
                "access_denied",
            ),
            (400, None, "bad request", oidc.OIDCTokenRejected, "invalid_grant"),
            (200, None, "not json", oidc.OIDCTokenRejected, "invalid_grant"),
            (
                200,
                {"token_type": "Bearer"},
                None,
                oidc.OIDCTokenRejected,
                "invalid_grant",
            ),
        ],
    )
    def test_rejections(self, provider, status, body, text, expected, error):
        provider.status, provider.body, provider.text = status, body, text
        with pytest.raises(expected) as excinfo:
            oidc.refresh("rt-1")
        assert type(excinfo.value) is expected
        assert excinfo.value.error == error

    def test_server_error_is_unavailable(self, provider):
        provider.status, provider.text = 502, "bad gateway"
        with pytest.raises(oidc.OIDCProviderUnavailable):
            oidc.refresh("rt-1")

    def test_network_error_is_unavailable(self, provider):
        provider.error = requests.ConnectionError("down")
        with pytest.raises(oidc.OIDCProviderUnavailable):
            oidc.refresh("rt-1")

    def test_revoke_posts_token_with_client_credentials(self, provider):
        oidc.revoke("rt-1")
        request, _ = provider.requests[0]
        assert request.url == f"{Config.OIDC_ISSUER}/oauth/revoke"
        assert provider.form() == {
            "token": "rt-1",
            "client_id": Config.OIDC_CLIENT_ID,
            "client_secret": Config.OIDC_CLIENT_SECRET,
        }

    def test_revoke_failure_is_swallowed(self, provider):
        provider.error = requests.ConnectionError("down")
        oidc.revoke("rt-1")
