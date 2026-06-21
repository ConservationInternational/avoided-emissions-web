"""Unit tests for authentication helpers (auth.py).

These tests exercise hash_password, verify_password, validate_password, and
authenticate without touching a real database — auth.get_db is mocked for
all authenticate tests.
"""

import uuid
from unittest.mock import MagicMock

import pytest

from auth import authenticate, hash_password, validate_password, verify_password


# ---------------------------------------------------------------------------
# hash_password
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestHashPassword:
    def test_produces_bcrypt_hash(self):
        h = hash_password("SecurePass1!abcdef")
        assert h.startswith("$2b$") or h.startswith("$2a$")

    def test_returns_string(self):
        assert isinstance(hash_password("SecurePass1!abcdef"), str)

    def test_different_calls_produce_different_hashes(self):
        """bcrypt uses a random salt, so two hashes of the same password differ."""
        pw = "SecurePass1!abcdef"
        assert hash_password(pw) != hash_password(pw)


# ---------------------------------------------------------------------------
# verify_password
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestVerifyPassword:
    def test_correct_password_returns_true(self):
        pw = "SecurePass1!abcdef"
        h = hash_password(pw)
        assert verify_password(pw, h) is True

    def test_wrong_password_returns_false(self):
        h = hash_password("CorrectPass1!abcdef")
        assert verify_password("WrongPass1!abcdef", h) is False

    def test_empty_string_does_not_match(self):
        h = hash_password("SecurePass1!abcdef")
        assert verify_password("", h) is False


# ---------------------------------------------------------------------------
# validate_password
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestValidatePassword:
    def test_valid_password_returns_empty_list(self):
        assert validate_password("StrongPass1!abcdef") == []

    def test_too_short_returns_error(self):
        errors = validate_password("Short1!")
        assert any("12 characters" in e for e in errors)

    def test_too_long_returns_error(self):
        errors = validate_password("A1!" + "a" * 130)
        assert any("128 characters" in e for e in errors)

    def test_missing_uppercase_returns_error(self):
        errors = validate_password("lowercase1!abcdefgh")
        assert any("uppercase" in e.lower() for e in errors)

    def test_missing_lowercase_returns_error(self):
        errors = validate_password("UPPERCASE1!ABCDEFGH")
        assert any("lowercase" in e.lower() for e in errors)

    def test_missing_digit_returns_error(self):
        errors = validate_password("NoDigitsHere!abcdef")
        assert any("digit" in e.lower() for e in errors)

    def test_missing_special_character_returns_error(self):
        errors = validate_password("NoSpecialChar1abcdef")
        assert any("special" in e.lower() for e in errors)

    def test_exactly_12_chars_is_valid(self):
        errors = validate_password("Passw0rd!xyz")  # 12 chars
        assert errors == []

    def test_exactly_128_chars_is_valid(self):
        # 123 lowercase + 1 upper + 1 digit + 1 special = 126… pad to 128
        pw = "Aa1!" + "a" * 124  # 128 chars
        assert validate_password(pw) == []


# ---------------------------------------------------------------------------
# authenticate
# ---------------------------------------------------------------------------


def _make_user(password, is_approved=True, is_active=True):
    """Create a User instance with a real bcrypt hash (no DB required)."""
    from models import User

    return User(
        id=uuid.uuid4(),
        email="test@example.com",
        password_hash=hash_password(password),
        name="Test User",
        role="user",
        is_approved=is_approved,
        is_active=is_active,
    )


def _patch_db(mocker, user):
    """Mock auth.get_db to return a session whose query returns *user*."""
    mock_db = MagicMock()
    mock_db.query.return_value.filter.return_value.first.return_value = user
    mocker.patch("auth.get_db", return_value=mock_db)
    return mock_db


@pytest.mark.unit
class TestAuthenticate:
    def test_correct_credentials_returns_session_user(self, mocker):
        from auth import SessionUser

        pw = "SecurePass1!xyz01"
        user = _make_user(pw)
        _patch_db(mocker, user)

        result = authenticate("test@example.com", pw)

        assert isinstance(result, SessionUser)
        assert result.email == "test@example.com"

    def test_wrong_password_returns_none(self, mocker):
        user = _make_user("CorrectPass1!xyz01")
        _patch_db(mocker, user)

        result = authenticate("test@example.com", "WrongPass1!xyz01")

        assert result is None

    def test_unapproved_user_returns_pending_approval(self, mocker):
        pw = "SecurePass1!xyz01"
        user = _make_user(pw, is_approved=False)
        _patch_db(mocker, user)

        result = authenticate("test@example.com", pw)

        assert result == "pending_approval"

    def test_inactive_user_returns_none(self, mocker):
        pw = "SecurePass1!xyz01"
        user = _make_user(pw, is_active=False)
        _patch_db(mocker, user)

        result = authenticate("test@example.com", pw)

        assert result is None

    def test_updates_last_login_on_successful_auth(self, mocker):
        pw = "SecurePass1!xyz01"
        user = _make_user(pw)
        mock_db = _patch_db(mocker, user)

        authenticate("test@example.com", pw)

        assert user.last_login is not None
        mock_db.commit.assert_called_once()

    def test_user_not_found_returns_none(self, mocker):
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = None
        mocker.patch("auth.get_db", return_value=mock_db)

        result = authenticate("nobody@example.com", "SomePass1!xyz01")

        assert result is None
