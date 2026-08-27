"""Email verification -> automatic authenticated entry (Task 2)."""
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import jwt
import pytest
from fastapi import HTTPException, Response

from app.api import auth as auth_api
from app.core.config import Settings
from app.core.security import ALGORITHM, SECRET_KEY
from app.schemas.user import ForgotPasswordRequest
from app.services.email import send_verification_email


def _token(email, token_type="email_verification", expires_in=timedelta(hours=24)):
    payload = {"sub": email, "type": token_type, "exp": datetime.now(UTC) + expires_in}
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def _cookie_names(response: Response) -> set[str]:
    return {
        header[1].decode().split("=", 1)[0]
        for header in response.raw_headers
        if header[0] == b"set-cookie"
    }


def test_successful_verification_marks_user_verified_and_authenticates(monkeypatch):
    user = SimpleNamespace(email="new@example.com", is_verified=False)
    db = Mock()
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=user))
    response = Response()

    result = auth_api.verify_email(token=_token(user.email), response=response, db=db)

    assert result == {"message": "Email verified successfully"}
    assert user.is_verified is True
    db.commit.assert_called_once()
    # Same cookie mechanism /auth/login uses - proves the SAME authenticated
    # state is established, not a second auth system.
    assert _cookie_names(response) == {"access_token", "refresh_token"}


def test_already_verified_user_revisiting_a_valid_link_still_authenticates(monkeypatch):
    """A double-click / re-open of a still-valid link must still log the
    user in, not just silently no-op."""
    user = SimpleNamespace(email="already@example.com", is_verified=True)
    db = Mock()
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=user))
    response = Response()

    auth_api.verify_email(token=_token(user.email), response=response, db=db)

    db.commit.assert_not_called()  # nothing new to persist
    assert _cookie_names(response) == {"access_token", "refresh_token"}


def test_expired_token_is_rejected_safely_with_no_cookies_set(monkeypatch):
    user = SimpleNamespace(email="x@example.com", is_verified=False)
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=user))
    response = Response()
    expired = _token(user.email, expires_in=timedelta(hours=-1))

    with pytest.raises(HTTPException) as error:
        auth_api.verify_email(token=expired, response=response, db=Mock())

    assert error.value.status_code == 400
    assert _cookie_names(response) == set()


def test_malformed_token_is_rejected_safely():
    with pytest.raises(HTTPException) as error:
        auth_api.verify_email(token="not-a-real-token", response=Response(), db=Mock())

    assert error.value.status_code == 400


def test_wrong_token_type_is_rejected_safely():
    """A password-reset or access token must not double as a verification
    token even though it is a validly-signed JWT."""
    wrong_type_token = _token("x@example.com", token_type="access")

    with pytest.raises(HTTPException) as error:
        auth_api.verify_email(token=wrong_type_token, response=Response(), db=Mock())

    assert error.value.status_code == 400


def test_unknown_user_returns_404_not_500(monkeypatch):
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=None))

    with pytest.raises(HTTPException) as error:
        auth_api.verify_email(token=_token("ghost@example.com"), response=Response(), db=Mock())

    assert error.value.status_code == 404


def test_verification_email_links_to_the_frontend_verify_page(monkeypatch):
    """The link must be environment-specific (existing FRONTEND_URL setting)
    and point at the frontend's own /verify-email page - not a hardcoded
    host, and not directly at the backend API."""
    sent = {}
    monkeypatch.setattr(
        "app.services.email.send_verification_email_message",
        lambda to_email, verification_url: sent.update(url=verification_url),
    )

    send_verification_email(email="user@example.com", token="abc123")

    assert sent["url"] == f"{Settings.FRONTEND_URL}/verify-email?token=abc123"
    assert "localhost:8000" not in sent["url"]


# --- forgot-password: undefined send_password_reset_email regression ------


def test_forgot_password_does_not_raise_nameerror_for_verified_user(monkeypatch):
    """Regression for the previous bug: forgot_password() called
    send_password_reset_email(...) without it being imported/defined
    anywhere, so a real request would fail at runtime with a NameError.
    This exercises the REAL call chain (auth.py -> app.services.email ->
    app.services.email_service) down to the Resend boundary - only
    resend.Emails.send itself is mocked, proving every name in between
    actually resolves."""
    user = SimpleNamespace(email="verified@example.com", is_verified=True)
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=user))
    sent = {}
    monkeypatch.setattr(
        "app.services.email_service.resend.Emails.send",
        lambda params: sent.update(params) or {"id": "fake"},
    )

    result = auth_api.forgot_password(
        request=ForgotPasswordRequest(email=user.email), db=Mock()
    )

    assert result == {
        "message": (
            "If an account exists for this email, "
            "a password reset link has been sent."
        )
    }
    assert sent["to"] == [user.email]
    assert sent["subject"] == "Reset your Devflo password"
    assert sent["subject"] != "Verify your Devflo account"
    reset_body = sent["text"]
    assert f"{Settings.FRONTEND_URL}/reset-password?token=" in reset_body
    assert "localhost:3000" not in reset_body


def test_forgot_password_reset_token_is_a_real_password_reset_token(monkeypatch):
    user = SimpleNamespace(email="verified@example.com", is_verified=True)
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=user))
    captured = {}
    monkeypatch.setattr(
        auth_api,
        "send_password_reset_email",
        lambda email, token: captured.update(email=email, token=token),
    )

    auth_api.forgot_password(request=ForgotPasswordRequest(email=user.email), db=Mock())

    payload = jwt.decode(captured["token"], SECRET_KEY, algorithms=[ALGORITHM])
    assert payload["sub"] == user.email
    assert payload["type"] == "password_reset"


def test_forgot_password_unknown_email_sends_nothing_but_same_message(monkeypatch):
    """Anti-user-enumeration: an unknown email must get the identical
    response and must not trigger any outbound email."""
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=None))
    send_mock = Mock()
    monkeypatch.setattr(auth_api, "send_password_reset_email", send_mock)

    result = auth_api.forgot_password(
        request=ForgotPasswordRequest(email="ghost@example.com"), db=Mock()
    )

    assert result == {
        "message": (
            "If an account exists for this email, "
            "a password reset link has been sent."
        )
    }
    send_mock.assert_not_called()


def test_forgot_password_unverified_user_sends_nothing_but_same_message(monkeypatch):
    """An existing-but-unverified account must not leak its existence
    either, and must not receive a reset link before it is even verified."""
    user = SimpleNamespace(email="unverified@example.com", is_verified=False)
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=user))
    send_mock = Mock()
    monkeypatch.setattr(auth_api, "send_password_reset_email", send_mock)

    result = auth_api.forgot_password(
        request=ForgotPasswordRequest(email=user.email), db=Mock()
    )

    assert result == {
        "message": (
            "If an account exists for this email, "
            "a password reset link has been sent."
        )
    }
    send_mock.assert_not_called()


def test_password_reset_email_links_to_frontend_reset_page_not_localhost(monkeypatch):
    """Mirrors test_verification_email_links_to_the_frontend_verify_page:
    the reset link must use Settings.FRONTEND_URL, never the previous
    hardcoded http://localhost:3000."""
    from app.services.email import send_password_reset_email

    sent = {}
    monkeypatch.setattr(
        "app.services.email.send_password_reset_email_message",
        lambda to_email, reset_url: sent.update(url=reset_url),
    )

    send_password_reset_email(email="user@example.com", token="reset-abc123")

    assert sent["url"] == f"{Settings.FRONTEND_URL}/reset-password?token=reset-abc123"
    assert "localhost:3000" not in sent["url"]
