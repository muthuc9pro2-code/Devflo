"""Email verification and original-browser session handoff."""
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import jwt
import pytest
from fastapi import HTTPException, Response

from app.api import auth as auth_api
from app.api import dependencies as auth_dependencies
from app.core.config import Settings
from app.core.security import (
    ALGORITHM,
    SECRET_KEY,
    create_password_reset_token,
    create_verification_handoff_token,
    decode_verification_handoff_token,
)
from app.schemas.user import ForgotPasswordRequest, ResetPasswordRequest, UserRegister
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


def _cookie_headers(response: Response) -> list[str]:
    return [
        header[1].decode()
        for header in response.raw_headers
        if header[0] == b"set-cookie"
    ]


def _cookie_header(response: Response, name: str) -> str:
    return next(
        header for header in _cookie_headers(response) if header.startswith(f"{name}=")
    )


@pytest.mark.parametrize("cookie_secure", [False, True])
def test_registration_sets_secure_scoped_http_only_handoff_cookie(
    monkeypatch, cookie_secure
):
    request = UserRegister(
        username="new_user",
        email="new@example.com",
        password="password",
    )
    created_user = SimpleNamespace(email=str(request.email))
    monkeypatch.setattr(Settings, "COOKIE_SECURE", cookie_secure)
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=None))
    monkeypatch.setattr(auth_api, "get_user_by_username", Mock(return_value=None))
    monkeypatch.setattr(auth_api, "send_verification_email", Mock())
    monkeypatch.setattr(auth_api, "create_user", Mock(return_value=created_user))
    response = Response()

    result = auth_api.register(user=request, response=response, db=Mock())

    assert result["email"] == created_user.email
    cookie = _cookie_header(response, "verification_handoff")
    cookie_lower = cookie.lower()
    assert "httponly" in cookie_lower
    assert "samesite=lax" in cookie_lower
    assert "max-age=1800" in cookie_lower
    assert "path=/auth/verification-session" in cookie_lower
    assert ("secure" in cookie_lower) is cookie_secure

    encoded_token = cookie.split(";", 1)[0].split("=", 1)[1]
    payload = decode_verification_handoff_token(encoded_token)
    assert payload["sub"] == created_user.email
    assert payload["type"] == "verification_handoff"
    remaining_lifetime = payload["exp"] - datetime.now(UTC).timestamp()
    assert 29 * 60 <= remaining_lifetime <= 30 * 60


def test_existing_unverified_registration_resend_replaces_handoff_cookie(monkeypatch):
    existing_user = SimpleNamespace(email="existing@example.com", is_verified=False)
    send_mock = Mock()
    monkeypatch.setattr(
        auth_api, "get_user_by_email", Mock(return_value=existing_user)
    )
    monkeypatch.setattr(auth_api, "send_verification_email", send_mock)
    response = Response()

    result = auth_api.register(
        user=UserRegister(
            username="ignored_user",
            email=existing_user.email,
            password="password",
        ),
        response=response,
        db=Mock(),
    )

    assert result == {
        "message": "Verification email resent. Please verify email.",
        "email": existing_user.email,
    }
    send_mock.assert_called_once()
    payload = decode_verification_handoff_token(
        _cookie_header(response, "verification_handoff")
        .split(";", 1)[0]
        .split("=", 1)[1]
    )
    assert payload["sub"] == existing_user.email


def test_successful_verification_marks_user_verified_without_authenticating(monkeypatch):
    user = SimpleNamespace(email="new@example.com", is_verified=False)
    db = Mock()
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=user))
    response = Response()

    result = auth_api.verify_email(token=_token(user.email), response=response, db=db)

    assert result == {"message": "Email verified successfully"}
    assert user.is_verified is True
    db.commit.assert_called_once()
    assert _cookie_names(response) == set()


def test_already_verified_user_revisiting_valid_link_is_idempotent(monkeypatch):
    user = SimpleNamespace(email="already@example.com", is_verified=True)
    db = Mock()
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=user))
    response = Response()

    auth_api.verify_email(token=_token(user.email), response=response, db=db)

    db.commit.assert_not_called()  # nothing new to persist
    assert _cookie_names(response) == set()


def test_verification_session_pending_does_not_set_authentication_cookies(monkeypatch):
    user = SimpleNamespace(email="pending@example.com", is_verified=False)
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=user))
    response = Response()

    result = auth_api.complete_verification_session(
        request=SimpleNamespace(
            cookies={
                "verification_handoff": create_verification_handoff_token(user.email)
            }
        ),
        response=response,
        db=Mock(),
    )

    assert result == {"status": "pending"}
    assert _cookie_names(response) == set()


def test_verified_handoff_authenticates_original_browser_and_deletes_cookie(
    monkeypatch,
):
    user = SimpleNamespace(email="verified@example.com", is_verified=True)
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=user))
    response = Response()

    result = auth_api.complete_verification_session(
        request=SimpleNamespace(
            cookies={
                "verification_handoff": create_verification_handoff_token(user.email)
            }
        ),
        response=response,
        db=Mock(),
    )

    assert result == {"status": "authenticated"}
    assert _cookie_names(response) == {
        "access_token",
        "refresh_token",
        "verification_handoff",
    }
    access_token = _cookie_header(response, "access_token").split(";", 1)[0].split("=", 1)[1]
    refresh_token = _cookie_header(response, "refresh_token").split(";", 1)[0].split("=", 1)[1]
    assert jwt.decode(access_token, SECRET_KEY, algorithms=[ALGORITHM])["type"] == "access"
    assert jwt.decode(refresh_token, SECRET_KEY, algorithms=[ALGORITHM])["type"] == "refresh"

    deleted_handoff = _cookie_header(response, "verification_handoff").lower()
    assert "max-age=0" in deleted_handoff
    assert "path=/auth/verification-session" in deleted_handoff


@pytest.mark.parametrize(
    "handoff_token",
    [
        None,
        "not-a-real-token",
        _token("expired@example.com", "verification_handoff", timedelta(seconds=-1)),
        _token("wrong-type@example.com", "email_verification"),
        jwt.encode(
            {
                "type": "verification_handoff",
                "exp": datetime.now(UTC) + timedelta(minutes=5),
            },
            SECRET_KEY,
            algorithm=ALGORITHM,
        ),
        jwt.encode(
            {"sub": "missing-exp@example.com", "type": "verification_handoff"},
            SECRET_KEY,
            algorithm=ALGORITHM,
        ),
    ],
)
def test_missing_invalid_expired_or_wrong_type_handoff_cannot_authenticate(
    handoff_token, monkeypatch
):
    get_user_mock = Mock()
    monkeypatch.setattr(auth_api, "get_user_by_email", get_user_mock)
    cookies = {} if handoff_token is None else {"verification_handoff": handoff_token}
    response = Response()

    with pytest.raises(HTTPException) as error:
        auth_api.complete_verification_session(
            request=SimpleNamespace(cookies=cookies),
            response=response,
            db=Mock(),
        )

    assert error.value.status_code == 401
    assert error.value.detail == "Verification handoff unavailable or expired"
    assert _cookie_names(response) == set()
    get_user_mock.assert_not_called()


def test_handoff_token_cannot_be_used_as_an_access_token(monkeypatch):
    get_user_mock = Mock()
    monkeypatch.setattr(auth_dependencies, "get_user_by_email", get_user_mock)

    with pytest.raises(HTTPException) as error:
        auth_dependencies.get_current_user(
            request=SimpleNamespace(
                cookies={
                    "access_token": create_verification_handoff_token(
                        "handoff@example.com"
                    )
                }
            ),
            db=Mock(),
        )

    assert error.value.status_code == 401
    assert error.value.detail == "Invalid token type"
    get_user_mock.assert_not_called()


def test_handoff_for_unknown_user_cannot_authenticate(monkeypatch):
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=None))
    response = Response()

    with pytest.raises(HTTPException) as error:
        auth_api.complete_verification_session(
            request=SimpleNamespace(
                cookies={
                    "verification_handoff": create_verification_handoff_token(
                        "ghost@example.com"
                    )
                }
            ),
            response=response,
            db=Mock(),
        )

    assert error.value.status_code == 401
    assert _cookie_names(response) == set()


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


def test_password_reset_backend_behavior_remains_unchanged(monkeypatch):
    user = SimpleNamespace(
        email="verified@example.com",
        hashed_password="old-password-hash",
    )
    hash_mock = Mock(return_value="new-password-hash")
    db = Mock()
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=user))
    monkeypatch.setattr(auth_api, "hash_password", hash_mock)

    result = auth_api.reset_password(
        request=ResetPasswordRequest(
            token=create_password_reset_token(user.email),
            new_password="new-password",
        ),
        db=db,
    )

    assert result == {"message": "Password reset successfully"}
    hash_mock.assert_called_once_with("new-password")
    assert user.hashed_password == "new-password-hash"
    db.commit.assert_called_once()


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
