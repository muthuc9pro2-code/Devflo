"""Per-user token-version invalidation and single-use password resets."""

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import jwt
import pytest
from fastapi import HTTPException, Response

from app.api import auth as auth_api
from app.api import dependencies as auth_dependencies
from app.core.security import (
    ALGORITHM,
    SECRET_KEY,
    create_access_token,
    create_password_reset_token,
    create_refresh_token,
    create_verification_handoff_token,
    hash_password,
    verify_password,
)
from app.crud.user import authenticate_user
from app.models.user import User
from app.schemas.user import ResetPasswordRequest, UserLogin


def _decode(token: str) -> dict:
    return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])


def _token_without_version(email: str, token_type: str) -> str:
    return jwt.encode(
        {
            "sub": email,
            "type": token_type,
            "exp": datetime.now(UTC) + timedelta(minutes=15),
        },
        SECRET_KEY,
        algorithm=ALGORITHM,
    )


def _token_with_invalid_version(email: str, token_type: str) -> str:
    return jwt.encode(
        {
            "sub": email,
            "type": token_type,
            "ver": "4",
            "exp": datetime.now(UTC) + timedelta(minutes=15),
        },
        SECRET_KEY,
        algorithm=ALGORITHM,
    )


def _cookie_value(response: Response, name: str) -> str:
    header = next(
        value.decode()
        for key, value in response.raw_headers
        if key == b"set-cookie" and value.decode().startswith(f"{name}=")
    )
    return header.split(";", 1)[0].split("=", 1)[1]


def _cookie_names(response: Response) -> set[str]:
    return {
        value.decode().split("=", 1)[0]
        for key, value in response.raw_headers
        if key == b"set-cookie"
    }


@pytest.mark.parametrize(
    ("creator", "token_type"),
    [
        (create_access_token, "access"),
        (create_refresh_token, "refresh"),
        (create_password_reset_token, "password_reset"),
    ],
)
def test_session_and_reset_token_creators_include_version(creator, token_type):
    payload = _decode(creator("user@example.com", 9))

    assert payload["type"] == token_type
    assert payload["ver"] == 9


def test_access_token_with_current_version_authenticates(monkeypatch):
    user = SimpleNamespace(email="user@example.com", token_version=4)
    monkeypatch.setattr(
        auth_dependencies,
        "get_user_by_email",
        Mock(return_value=user),
    )

    current_user = auth_dependencies.get_current_user(
        request=SimpleNamespace(
            cookies={"access_token": create_access_token(user.email, 4)}
        ),
        db=Mock(),
    )

    assert current_user is user


@pytest.mark.parametrize(
    "access_token",
    [
        create_access_token("user@example.com", 3),
        _token_without_version("user@example.com", "access"),
        _token_with_invalid_version("user@example.com", "access"),
    ],
    ids=["old-version", "missing-version", "invalid-version"],
)
def test_access_token_with_old_or_missing_version_is_rejected(
    access_token, monkeypatch
):
    user = SimpleNamespace(email="user@example.com", token_version=4)
    monkeypatch.setattr(
        auth_dependencies,
        "get_user_by_email",
        Mock(return_value=user),
    )

    with pytest.raises(HTTPException) as error:
        auth_dependencies.get_current_user(
            request=SimpleNamespace(cookies={"access_token": access_token}),
            db=Mock(),
        )

    assert error.value.status_code == 401
    assert error.value.detail == "Invalid access token"


@pytest.mark.parametrize(
    "access_token",
    [
        "not-a-jwt",
        jwt.encode(
            {
                "sub": "user@example.com",
                "type": "access",
                "ver": 4,
                "exp": datetime.now(UTC) - timedelta(seconds=1),
            },
            SECRET_KEY,
            algorithm=ALGORITHM,
        ),
    ],
    ids=["malformed", "expired"],
)
def test_malformed_or_expired_access_token_is_rejected_before_user_lookup(
    access_token,
    monkeypatch,
):
    get_user_mock = Mock()
    monkeypatch.setattr(auth_dependencies, "get_user_by_email", get_user_mock)

    with pytest.raises(HTTPException) as error:
        auth_dependencies.get_current_user(
            request=SimpleNamespace(cookies={"access_token": access_token}),
            db=Mock(),
        )

    assert error.value.status_code == 401
    get_user_mock.assert_not_called()


def test_refresh_token_with_current_version_rotates_versioned_tokens(monkeypatch):
    user = SimpleNamespace(email="user@example.com", token_version=6)
    user.is_verified = True
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=user))
    response = Response()

    result = auth_api.refresh_token(
        request=SimpleNamespace(
            cookies={"refresh_token": create_refresh_token(user.email, 6)}
        ),
        response=response,
        db=Mock(),
    )

    assert result == {"message": "Tokens refreshed successfully"}
    assert _decode(_cookie_value(response, "access_token"))["ver"] == 6
    assert _decode(_cookie_value(response, "refresh_token"))["ver"] == 6


@pytest.mark.parametrize(
    "refresh_token",
    [
        create_refresh_token("user@example.com", 5),
        _token_without_version("user@example.com", "refresh"),
        _token_with_invalid_version("user@example.com", "refresh"),
    ],
    ids=["old-version", "missing-version", "invalid-version"],
)
def test_refresh_token_with_old_or_missing_version_is_rejected_without_rotation(
    refresh_token, monkeypatch
):
    user = SimpleNamespace(email="user@example.com", token_version=6)
    user.is_verified = True
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=user))
    response = Response()

    rejection = auth_api.refresh_token(
        request=SimpleNamespace(cookies={"refresh_token": refresh_token}),
        response=response,
        db=Mock(),
    )

    assert rejection.status_code == 401
    assert json.loads(rejection.body) == {"detail": "Invalid refresh token"}
    assert _cookie_names(rejection) == {"access_token", "refresh_token"}


@pytest.mark.parametrize(
    "refresh_token",
    [
        "not-a-jwt",
        jwt.encode(
            {
                "sub": "user@example.com",
                "type": "refresh",
                "ver": 6,
                "exp": datetime.now(UTC) - timedelta(seconds=1),
            },
            SECRET_KEY,
            algorithm=ALGORITHM,
        ),
    ],
    ids=["malformed", "expired"],
)
def test_malformed_or_expired_refresh_token_is_rejected_and_cookies_expire(
    refresh_token,
    monkeypatch,
):
    get_user_mock = Mock()
    monkeypatch.setattr(auth_api, "get_user_by_email", get_user_mock)
    response = Response()

    rejection = auth_api.refresh_token(
        request=SimpleNamespace(cookies={"refresh_token": refresh_token}),
        response=response,
        db=Mock(),
    )

    assert rejection.status_code == 401
    assert _cookie_names(rejection) == {"access_token", "refresh_token"}
    get_user_mock.assert_not_called()


def test_login_uses_authenticated_users_current_token_version(monkeypatch):
    user = SimpleNamespace(email="user@example.com", token_version=12)
    monkeypatch.setattr(auth_api, "authenticate_user", Mock(return_value=user))
    response = Response()

    auth_api.login(
        response=response,
        user=UserLogin(email=user.email, password="current-password"),
        db=Mock(),
    )

    assert _decode(_cookie_value(response, "access_token"))["ver"] == 12
    assert _decode(_cookie_value(response, "refresh_token"))["ver"] == 12


def test_reset_token_is_single_use_and_only_new_password_authenticates(monkeypatch):
    old_password = "old-password"
    new_password = "new-password"
    user = SimpleNamespace(
        email="user@example.com",
        is_verified=True,
        hashed_password=hash_password(old_password),
        token_version=8,
    )
    reset_token = create_password_reset_token(user.email, user.token_version)
    db = Mock()
    locked_query = db.query.return_value.filter.return_value.with_for_update.return_value
    locked_query.first.return_value = user
    original_hash = user.hashed_password
    db.commit.side_effect = lambda: (
        user.hashed_password != original_hash and user.token_version == 9
    ) or pytest.fail("reset fields were not both updated before commit")

    result = auth_api.reset_password(
        request=ResetPasswordRequest(token=reset_token, new_password=new_password),
        db=db,
    )

    assert result == {"message": "Password reset successfully"}
    assert user.token_version == 9
    assert user.hashed_password != original_hash
    assert not verify_password(old_password, user.hashed_password)
    assert verify_password(new_password, user.hashed_password)
    db.commit.assert_called_once()

    with pytest.raises(HTTPException) as replay_error:
        auth_api.reset_password(
            request=ResetPasswordRequest(token=reset_token, new_password="attacker-pass"),
            db=db,
        )

    assert replay_error.value.status_code == 400
    assert replay_error.value.detail == "Invalid or expired password reset token"
    assert user.token_version == 9
    assert verify_password(new_password, user.hashed_password)
    assert db.query.return_value.filter.return_value.with_for_update.call_count == 2
    db.commit.assert_called_once()

    monkeypatch.setattr("app.crud.user.get_user_by_email", Mock(return_value=user))
    with pytest.raises(HTTPException) as old_password_error:
        authenticate_user(db=Mock(), email=user.email, password=old_password)
    assert old_password_error.value.status_code == 401
    assert authenticate_user(db=Mock(), email=user.email, password=new_password) is user


def test_stale_reset_token_is_rejected_without_password_change_or_commit():
    original_hash = hash_password("current-password")
    user = SimpleNamespace(
        email="user@example.com",
        hashed_password=original_hash,
        token_version=5,
    )
    db = Mock()
    db.query.return_value.filter.return_value.with_for_update.return_value.first.return_value = user

    with pytest.raises(HTTPException) as error:
        auth_api.reset_password(
            request=ResetPasswordRequest(
                token=create_password_reset_token(user.email, 4),
                new_password="new-password",
            ),
            db=db,
        )

    assert error.value.status_code == 400
    assert error.value.detail == "Invalid or expired password reset token"
    assert user.hashed_password == original_hash
    assert user.token_version == 5
    db.commit.assert_not_called()


@pytest.mark.parametrize(
    "reset_token",
    [
        "not-a-jwt",
        jwt.encode(
            {
                "sub": "user@example.com",
                "type": "password_reset",
                "ver": 2,
                "exp": datetime.now(UTC) - timedelta(seconds=1),
            },
            SECRET_KEY,
            algorithm=ALGORITHM,
        ),
    ],
    ids=["malformed", "expired"],
)
def test_malformed_or_expired_reset_token_returns_controlled_400(reset_token):
    db = Mock()

    with pytest.raises(HTTPException) as error:
        auth_api.reset_password(
            request=ResetPasswordRequest(
                token=reset_token,
                new_password="new-password",
            ),
            db=db,
        )

    assert error.value.status_code == 400
    assert error.value.detail == "Invalid or expired password reset token"
    db.query.assert_not_called()
    db.commit.assert_not_called()


def test_reset_invalidates_old_sessions_and_new_login_uses_incremented_version(
    monkeypatch,
):
    user = SimpleNamespace(
        email="user@example.com",
        is_verified=True,
        hashed_password=hash_password("old-password"),
        token_version=2,
    )
    old_access = create_access_token(user.email, user.token_version)
    old_refresh = create_refresh_token(user.email, user.token_version)
    reset_token = create_password_reset_token(user.email, user.token_version)
    db = Mock()
    db.query.return_value.filter.return_value.with_for_update.return_value.first.return_value = user

    auth_api.reset_password(
        request=ResetPasswordRequest(token=reset_token, new_password="new-password"),
        db=db,
    )

    assert user.token_version == 3
    monkeypatch.setattr(
        auth_dependencies,
        "get_user_by_email",
        Mock(return_value=user),
    )
    with pytest.raises(HTTPException) as access_error:
        auth_dependencies.get_current_user(
            request=SimpleNamespace(cookies={"access_token": old_access}),
            db=Mock(),
        )
    assert access_error.value.status_code == 401

    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=user))
    refresh_rejection = auth_api.refresh_token(
        request=SimpleNamespace(cookies={"refresh_token": old_refresh}),
        response=Response(),
        db=Mock(),
    )
    assert refresh_rejection.status_code == 401
    assert _cookie_names(refresh_rejection) == {"access_token", "refresh_token"}

    monkeypatch.setattr(auth_api, "authenticate_user", Mock(return_value=user))
    login_response = Response()
    auth_api.login(
        response=login_response,
        user=UserLogin(email=user.email, password="new-password"),
        db=Mock(),
    )
    assert _decode(_cookie_value(login_response, "access_token"))["ver"] == 3
    assert _decode(_cookie_value(login_response, "refresh_token"))["ver"] == 3


def test_password_reset_invalidates_previously_issued_verification_handoff(
    monkeypatch,
):
    user = SimpleNamespace(
        email="user@example.com",
        is_verified=True,
        hashed_password=hash_password("old-password"),
        token_version=4,
    )
    old_handoff = create_verification_handoff_token(
        user.email,
        user.token_version,
    )
    reset_token = create_password_reset_token(user.email, user.token_version)
    db = Mock()
    db.query.return_value.filter.return_value.with_for_update.return_value.first.return_value = user

    auth_api.reset_password(
        request=ResetPasswordRequest(token=reset_token, new_password="new-password"),
        db=db,
    )

    assert user.token_version == 5
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=user))
    response = Response()
    with pytest.raises(HTTPException) as error:
        auth_api.complete_verification_session(
            request=SimpleNamespace(cookies={"verification_handoff": old_handoff}),
            response=response,
            db=Mock(),
        )

    assert error.value.status_code == 401
    assert _cookie_names(response) == set()


def test_unverified_user_cannot_refresh_even_with_current_version(monkeypatch):
    user = SimpleNamespace(
        email="user@example.com",
        is_verified=False,
        token_version=3,
    )
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=user))
    response = Response()

    rejection = auth_api.refresh_token(
        request=SimpleNamespace(
            cookies={"refresh_token": create_refresh_token(user.email, 3)}
        ),
        response=response,
        db=Mock(),
    )

    assert rejection.status_code == 401
    assert _cookie_names(rejection) == {"access_token", "refresh_token"}


def test_user_token_version_column_has_safe_zero_defaults():
    column = User.__table__.c.token_version

    assert column.nullable is False
    assert column.default.arg == 0
    assert str(column.server_default.arg) == "0"
