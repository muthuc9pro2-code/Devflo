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
        "app.services.email.send_ses_email",
        lambda to_email, verification_url: sent.update(url=verification_url),
    )

    send_verification_email(email="user@example.com", token="abc123")

    assert sent["url"] == f"{Settings.FRONTEND_URL}/verify-email?token=abc123"
    assert "localhost:8000" not in sent["url"]
