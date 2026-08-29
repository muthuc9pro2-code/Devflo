"""Focused account-lifecycle and authentication boundary hardening."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import HTTPException, Response
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from resend.exceptions import ResendError

from app.api import auth as auth_api
from app.api import image as image_api
from app.api.dependencies import get_current_verified_user
from app.crud import user as user_crud
from app.db.database import Base
from app.main import app
from app.models.user import User
from app.schemas.user import ForgotPasswordRequest, UserLogin, UserRegister
from app.tasks import user_cleanup


def _provider_error() -> ResendError:
    return ResendError(
        code=503,
        error_type="service_unavailable",
        message="provider unavailable",
        suggested_action="retry later",
    )


def _cookie_headers(response: Response) -> list[str]:
    return [
        value.decode()
        for key, value in response.raw_headers
        if key == b"set-cookie"
    ]


def _deleted_cookie(response: Response, name: str) -> str:
    return next(
        header
        for header in _cookie_headers(response)
        if header.startswith(f"{name}=") and "Max-Age=0" in header
    )


def test_unknown_login_executes_precomputed_dummy_password_verification(monkeypatch):
    verify_mock = Mock(return_value=False)
    monkeypatch.setattr(user_crud, "get_user_by_email", Mock(return_value=None))
    monkeypatch.setattr(user_crud, "verify_password", verify_mock)

    with pytest.raises(HTTPException) as error:
        user_crud.authenticate_user(
            db=Mock(),
            email="missing@example.com",
            password="candidate-password",
        )

    assert error.value.status_code == 401
    assert error.value.detail == "Invalid email or password"
    verify_mock.assert_called_once_with(
        "candidate-password",
        user_crud._DUMMY_PASSWORD_HASH,
    )


def test_precomputed_dummy_password_hash_is_valid_and_never_matches_candidate():
    assert user_crud.verify_password(
        "candidate-password",
        user_crud._DUMMY_PASSWORD_HASH,
    ) is False


def test_known_email_wrong_password_uses_real_hash_with_same_public_failure(monkeypatch):
    user = SimpleNamespace(
        email="known@example.com",
        hashed_password="stored-password-hash",
        is_verified=True,
    )
    verify_mock = Mock(return_value=False)
    monkeypatch.setattr(user_crud, "get_user_by_email", Mock(return_value=user))
    monkeypatch.setattr(user_crud, "verify_password", verify_mock)

    with pytest.raises(HTTPException) as error:
        user_crud.authenticate_user(
            db=Mock(),
            email=user.email,
            password="candidate-password",
        )

    assert error.value.status_code == 401
    assert error.value.detail == "Invalid email or password"
    verify_mock.assert_called_once_with("candidate-password", user.hashed_password)


def test_new_registration_creates_durable_user_before_sending_email(monkeypatch):
    events = []
    created_user = SimpleNamespace(email="new@example.com", token_version=0)
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=None))
    monkeypatch.setattr(auth_api, "get_user_by_username", Mock(return_value=None))
    monkeypatch.setattr(
        auth_api,
        "create_user",
        Mock(side_effect=lambda db, request: events.append("durable-user") or created_user),
    )
    monkeypatch.setattr(
        auth_api,
        "send_verification_email",
        Mock(side_effect=lambda **kwargs: events.append("email")),
    )

    auth_api.register(
        user=UserRegister(
            username="new_user",
            email=created_user.email,
            password="new-password",
        ),
        response=Response(),
        db=Mock(),
    )

    assert events == ["durable-user", "email"]


def test_registration_email_failure_keeps_created_unverified_account_and_no_handoff(
    monkeypatch,
):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    db = sessionmaker(bind=engine)()
    monkeypatch.setattr(
        auth_api,
        "send_verification_email",
        Mock(side_effect=_provider_error()),
    )
    response = Response()
    registration = UserRegister(
        username="new_user",
        email="new@example.com",
        password="new-password",
    )

    with pytest.raises(HTTPException) as error:
        auth_api.register(
            user=registration,
            response=response,
            db=db,
        )

    assert error.value.status_code == 503
    assert error.value.detail == "Unable to send verification email. Please try again."
    created_user = db.query(User).filter(User.email == registration.email).one()
    assert created_user.is_verified is False
    assert created_user.unverified_activity_at is not None
    assert _cookie_headers(response) == []

    send_retry = Mock()
    monkeypatch.setattr(auth_api, "send_verification_email", send_retry)
    retry_response = Response()
    retry_result = auth_api.register(
        user=registration,
        response=retry_response,
        db=db,
    )

    assert retry_result["message"] == "Verification email resent. Please verify email."
    send_retry.assert_called_once()
    assert any(
        header.startswith("verification_handoff=")
        for header in _cookie_headers(retry_response)
    )
    db.close()


@pytest.mark.parametrize(
    ("first", "second"),
    [
        (
            UserRegister(
                username="first_user",
                email="same@example.com",
                password="new-password",
            ),
            UserRegister(
                username="second_user",
                email="same@example.com",
                password="new-password",
            ),
        ),
        (
            UserRegister(
                username="same_user",
                email="first@example.com",
                password="new-password",
            ),
            UserRegister(
                username="same_user",
                email="second@example.com",
                password="new-password",
            ),
        ),
    ],
    ids=["email", "username"],
)
def test_registration_uniqueness_race_rolls_back_and_returns_controlled_409(
    first,
    second,
    monkeypatch,
):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    first_db = session_factory()
    second_db = session_factory()
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=None))
    monkeypatch.setattr(auth_api, "get_user_by_username", Mock(return_value=None))
    send_mock = Mock()
    monkeypatch.setattr(auth_api, "send_verification_email", send_mock)

    auth_api.register(user=first, response=Response(), db=first_db)
    losing_response = Response()

    with pytest.raises(HTTPException) as error:
        auth_api.register(user=second, response=losing_response, db=second_db)

    assert error.value.status_code == 409
    assert error.value.detail == "Registration conflict"
    assert "integrity" not in error.value.detail.lower()
    assert second_db.query(User).count() == 1
    assert send_mock.call_count == 1
    assert _cookie_headers(losing_response) == []
    first_db.close()
    second_db.close()


def test_forgot_password_provider_failure_preserves_neutral_public_response(
    monkeypatch,
    caplog,
):
    user = SimpleNamespace(
        email="verified@example.com",
        is_verified=True,
        token_version=2,
    )
    monkeypatch.setattr(auth_api, "get_user_by_email", Mock(return_value=user))
    monkeypatch.setattr(
        auth_api,
        "send_password_reset_email",
        Mock(side_effect=_provider_error()),
    )

    result = auth_api.forgot_password(
        request=ForgotPasswordRequest(email=user.email),
        db=Mock(),
    )

    assert result == {
        "message": (
            "If an account exists for this email, "
            "a password reset link has been sent."
        )
    }
    assert "Password reset email delivery failed" in caplog.text
    assert "provider unavailable" not in caplog.text
    assert user.email not in caplog.text


def test_successful_login_deletes_custom_path_verification_handoff(monkeypatch):
    user = SimpleNamespace(email="verified@example.com", token_version=2)
    monkeypatch.setattr(auth_api, "authenticate_user", Mock(return_value=user))
    response = Response()

    auth_api.login(
        response=response,
        user=UserLogin(email=user.email, password="current-password"),
        db=Mock(),
    )

    handoff = _deleted_cookie(response, "verification_handoff")
    assert "Path=/auth/verification-session" in handoff


def test_logout_deletes_access_refresh_and_custom_path_handoff():
    response = Response()

    result = auth_api.logout(response=response)

    assert result == {"message": "Logged out successfully"}
    assert "Path=/" in _deleted_cookie(response, "access_token")
    assert "Path=/" in _deleted_cookie(response, "refresh_token")
    assert "Path=/auth/verification-session" in _deleted_cookie(
        response,
        "verification_handoff",
    )


def test_stale_unverified_cleanup_uses_latest_verification_activity(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    now = datetime.now(UTC)
    old = now - timedelta(hours=25)
    recent = now - timedelta(minutes=5)

    db = session_factory()
    db.add_all(
        [
            User(
                username="recent_activity",
                email="recent@example.com",
                hashed_password="unused-hash",
                is_verified=False,
                created_at=old,
                unverified_activity_at=recent,
            ),
            User(
                username="stale_unverified",
                email="stale@example.com",
                hashed_password="unused-hash",
                is_verified=False,
                created_at=old,
                unverified_activity_at=None,
            ),
            User(
                username="verified_user",
                email="verified@example.com",
                hashed_password="unused-hash",
                is_verified=True,
                created_at=old,
                unverified_activity_at=None,
            ),
        ]
    )
    db.commit()
    db.close()
    monkeypatch.setattr(user_cleanup, "sessionLocal", session_factory)

    assert user_cleanup.delete_stale_unverified_users() == 1

    check_db = session_factory()
    remaining = {user.email for user in check_db.query(User).all()}
    check_db.close()
    assert remaining == {"recent@example.com", "verified@example.com"}


@pytest.fixture
def image_client():
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


def test_standalone_ocr_rejects_anonymous_request(image_client):
    response = image_client.post(
        "/image/extract-text",
        files={"image": ("tiny.png", b"image", "image/png")},
    )

    assert response.status_code == 401


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("post", "/analysis/upload"),
        ("get", "/analysis/history"),
        ("get", "/analysis/123"),
        ("post", "/analysis/123/cancel"),
        ("get", "/analyses/123/events"),
    ],
)
def test_analysis_routes_reject_anonymous_direct_backend_requests(
    image_client,
    method,
    path,
):
    response = getattr(image_client, method)(path)

    assert response.status_code == 401


def test_standalone_ocr_rejects_unverified_user(image_client):
    def reject_unverified():
        raise HTTPException(status_code=403, detail="Email not verified")

    app.dependency_overrides[get_current_verified_user] = reject_unverified
    response = image_client.post(
        "/image/extract-text",
        files={"image": ("tiny.png", b"image", "image/png")},
    )

    assert response.status_code == 403


def test_standalone_ocr_allows_verified_user_through_auth_layer(
    image_client,
    monkeypatch,
):
    app.dependency_overrides[get_current_verified_user] = lambda: SimpleNamespace(
        id=1,
        is_verified=True,
    )
    monkeypatch.setattr(image_api, "validate_ocr_image", Mock())
    monkeypatch.setattr(image_api, "extract_text_from_image", Mock(return_value="text"))

    response = image_client.post(
        "/image/extract-text",
        files={"image": ("tiny.png", b"image", "image/png")},
    )

    assert response.status_code == 200
    assert response.json() == {"extracted_text": "text"}


def test_verify_email_is_post_only_and_excessive_token_is_rejected(image_client):
    verification_operations = app.openapi()["paths"]["/auth/verify-email"]

    assert set(verification_operations) == {"post"}
    response = image_client.post(
        "/auth/verify-email",
        json={"token": "x" * 4097},
    )
    assert response.status_code == 422

    query_only = image_client.post("/auth/verify-email?token=legacy-query-token")
    assert query_only.status_code == 422


def test_auth_me_is_not_cacheable(image_client):
    app.dependency_overrides[get_current_verified_user] = lambda: SimpleNamespace(
        id=1,
        username="verified_user",
        email="verified@example.com",
        is_verified=True,
    )

    response = image_client.get("/auth/me")

    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "no-store"


def test_refresh_rejection_http_response_expires_stale_auth_cookies(image_client):
    image_client.cookies.set("access_token", "stale-access", path="/")
    image_client.cookies.set("refresh_token", "malformed-refresh", path="/")

    response = image_client.post("/auth/refresh")

    assert response.status_code == 401
    set_cookie_headers = response.headers.get_list("set-cookie")
    assert any(
        header.startswith("access_token=") and "Max-Age=0" in header
        for header in set_cookie_headers
    )
    assert any(
        header.startswith("refresh_token=") and "Max-Age=0" in header
        for header in set_cookie_headers
    )
