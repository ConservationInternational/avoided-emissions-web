"""Unit tests for Trends.Earth app-user administration and API error mapping."""

from unittest.mock import MagicMock

import pytest
import requests
from config import Config
from services import user_admin
from trendsearth_client import TrendsEarthAPIError, _raise_for_status


def _response(status, body):
    resp = MagicMock()
    resp.status_code = status
    resp.ok = status < 400
    resp.reason = "Error"
    resp.json.return_value = body
    return resp


@pytest.mark.unit
class TestListAppUsers:
    def test_pages_through_results_with_admin_token(self, mocker):
        pages = [
            _response(200, {"data": [{"sub": "a"}, {"sub": "b"}], "total": 3}),
            _response(200, {"data": [{"sub": "c"}], "total": 3}),
        ]
        request = mocker.patch(
            "services.user_admin.requests.request", side_effect=pages
        )

        users = user_admin.list_app_users("admin-token", status="pending")

        assert [u["sub"] for u in users] == ["a", "b", "c"]
        method, url = request.call_args_list[0].args
        kwargs = request.call_args_list[0].kwargs
        assert method == "GET"
        assert url == (f"{Config.TRENDSEARTH_API_URL}/apps/{Config.OIDC_APP_KEY}/users")
        assert kwargs["headers"]["Authorization"] == "Bearer admin-token"
        assert kwargs["params"] == {"page": 1, "per_page": 100, "status": "pending"}
        assert request.call_args_list[1].kwargs["params"]["page"] == 2

    def test_rejects_unknown_status(self):
        with pytest.raises(ValueError):
            user_admin.list_app_users("t", status="deleted")

    def test_maps_error_codes(self, mocker):
        mocker.patch(
            "services.user_admin.requests.request",
            return_value=_response(
                403, {"status": 403, "error": "app_role_required", "detail": "x"}
            ),
        )
        with pytest.raises(user_admin.AppUsersError) as excinfo:
            user_admin.list_app_users("t")
        assert excinfo.value.status == 403
        assert excinfo.value.code == "app_role_required"
        assert "admin role" in str(excinfo.value)

    def test_network_failure(self, mocker):
        mocker.patch(
            "services.user_admin.requests.request",
            side_effect=requests.ConnectionError("down"),
        )
        with pytest.raises(user_admin.AppUsersError, match="reach Trends.Earth"):
            user_admin.list_app_users("t")


@pytest.mark.unit
class TestSetAppUserAccess:
    def test_approve_with_role(self, mocker):
        request = mocker.patch(
            "services.user_admin.requests.request",
            return_value=_response(
                200, {"data": {"sub": "u1", "app_access": "active"}}
            ),
        )
        profile = user_admin.set_app_user_access("t", "u1", "active", role="admin")
        assert profile["app_access"] == "active"
        method, url = request.call_args.args
        assert method == "PUT"
        assert url.endswith(f"/apps/{Config.OIDC_APP_KEY}/users/u1/access")
        assert request.call_args.kwargs["json"] == {"status": "active", "role": "admin"}

    def test_revoke_sends_no_role(self, mocker):
        request = mocker.patch(
            "services.user_admin.requests.request",
            return_value=_response(200, {"data": {}}),
        )
        user_admin.set_app_user_access("t", "u1", "revoked")
        assert request.call_args.kwargs["json"] == {"status": "revoked"}

    @pytest.mark.parametrize(
        "code,status,text",
        [
            ("superadmin_protected", 403, "superadmins"),
            ("self_demotion_forbidden", 409, "your own access"),
        ],
    )
    def test_protected_users(self, mocker, code, status, text):
        mocker.patch(
            "services.user_admin.requests.request",
            return_value=_response(status, {"error": code}),
        )
        with pytest.raises(user_admin.AppUsersError, match=text):
            user_admin.set_app_user_access("t", "u1", "revoked")

    def test_rejects_unknown_role(self):
        with pytest.raises(ValueError):
            user_admin.set_app_user_access("t", "u1", "active", role="owner")


@pytest.mark.unit
class TestTrendsEarthErrors:
    def test_gee_terms_detail_is_shown(self):
        resp = _response(403, {"status": 403, "detail": "This script requires GEE"})
        with pytest.raises(TrendsEarthAPIError, match="requires GEE"):
            _raise_for_status(resp)

    def test_insufficient_scope(self):
        resp = _response(403, {"error": "insufficient_scope"})
        with pytest.raises(TrendsEarthAPIError, match="sign in again"):
            _raise_for_status(resp)

    def test_expired_token(self):
        resp = _response(401, None)
        with pytest.raises(TrendsEarthAPIError, match="session has expired"):
            _raise_for_status(resp)

    def test_is_http_error(self):
        resp = _response(500, {})
        with pytest.raises(requests.HTTPError):
            _raise_for_status(resp)

    def test_success_passes(self):
        _raise_for_status(_response(200, {}))
