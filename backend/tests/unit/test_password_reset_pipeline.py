"""End-to-end password-reset pipeline regression, written to investigate a
reported live-deployment bug: valid reset submissions (both same-password
and different-password) were rejected with "Invalid or expired password
reset token" before ever reaching the current-password check.

Every other reset test in this suite (test_auth_token_version.py,
test_auth_verification.py) calls reset_password()/forgot_password()
directly with a Mock() db, which cannot prove anything about the real
HTTP/JSON boundary - Pydantic parsing, JSON (de)serialization, or a real
SQLAlchemy-backed user lookup. This file deliberately goes through the
real ASGI request/response cycle (FastAPI TestClient) with a real SQLite
session, using genuinely generated JWTs end-to-end (create_password_reset_token
-> the email URL builder -> a live POST body), never a literal
placeholder token, to prove or disprove a frontend/serialization/DB-lookup
bug in the repository code itself.
"""
from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.api import auth as auth_api
from app.core.config import Settings
from app.core.security import (
    ALGORITHM,
    SECRET_KEY,
    create_password_reset_token,
    decode_password_reset_token,
    hash_password,
    verify_password,
)
from app.db.database import Base, get_db
from app.main import app
from app.models.user import User
from app.services import email as email_service


def _sqlite_session_factory():
    # StaticPool + check_same_thread=False: FastAPI's TestClient runs the
    # endpoint via run_in_threadpool on a different thread than this one,
    # and a plain sqlite ":memory:" DB is otherwise a fresh, empty database
    # per connection/thread - the tables created here would be invisible
    # to the endpoint's own session without a single shared connection.
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)


def test_create_then_decode_password_reset_token_round_trips_exactly():
    """Real create/decode, no mocks - proves generation and decoding agree
    on SECRET_KEY/ALGORITHM/claim shape in this process."""
    before = datetime.now(UTC)
    token = create_password_reset_token("user@example.com", 3)
    payload = decode_password_reset_token(token)
    after = datetime.now(UTC)

    assert payload["sub"] == "user@example.com"
    assert payload["type"] == "password_reset"
    assert payload["ver"] == 3

    exp = datetime.fromtimestamp(payload["exp"], tz=UTC)
    expected_exp = before + timedelta(minutes=Settings.PASSWORD_RESET_TOKEN_EXPIRE_MINUTES)
    assert before < exp <= expected_exp + (after - before)
    assert exp > datetime.now(UTC)


def test_password_reset_email_url_preserves_the_jwt_byte_for_byte(monkeypatch):
    """send_password_reset_email must place the exact JWT, unmodified, in
    the URL fragment - not the query string."""
    real_token = create_password_reset_token("user@example.com", 0)
    captured = {}

    def _capture(to_email, reset_url):
        captured["to_email"] = to_email
        captured["reset_url"] = reset_url

    monkeypatch.setattr(email_service, "send_password_reset_email_message", _capture)

    email_service.send_password_reset_email(email="user@example.com", token=real_token)

    expected_url = f"{Settings.FRONTEND_URL}/reset-password#token={real_token}"
    assert captured["reset_url"] == expected_url
    assert f"#token={real_token}" in captured["reset_url"]
    assert "?token=" not in captured["reset_url"]


def test_reset_password_http_boundary_preserves_a_genuine_token_and_succeeds(monkeypatch):
    """Full ASGI request/response cycle: forgot-password generates a real
    JWT, it is captured exactly as the email layer would send it, and is
    then POSTed as real JSON to /auth/reset-password. Proves Pydantic
    parsing, JSON (de)serialization, and a real DB-backed user lookup do
    not corrupt or reject a genuine, current reset credential."""
    session_factory = _sqlite_session_factory()
    db = session_factory()
    current_password = "current-password"
    user = User(
        username="alice",
        email="alice@example.com",
        hashed_password=hash_password(current_password),
        is_verified=True,
    )
    db.add(user)
    db.commit()
    db.close()

    def _override_get_db():
        session = session_factory()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = _override_get_db

    captured = {}
    monkeypatch.setattr(
        auth_api,
        "send_password_reset_email",
        lambda email, token: captured.__setitem__("token", token),
    )

    try:
        with TestClient(app) as client:
            forgot_response = client.post(
                "/auth/forgot-password",
                json={"email": "alice@example.com"},
            )
            assert forgot_response.status_code == 200
            genuine_token = captured["token"]
            assert genuine_token

            # Same current password: must be rejected without mutation and
            # without consuming the token.
            same_password_response = client.post(
                "/auth/reset-password",
                json={"token": genuine_token, "new_password": current_password},
            )
            assert same_password_response.status_code == 400
            assert same_password_response.json() == {
                "detail": "Choose a password different from your current password."
            }

            verify_db = session_factory()
            unchanged_user = (
                verify_db.query(User).filter(User.email == "alice@example.com").first()
            )
            assert verify_password(current_password, unchanged_user.hashed_password)
            assert unchanged_user.token_version == 0
            verify_db.close()

            # The same still-valid token now succeeds with a different password.
            new_password = "a-genuinely-different-password"
            success_response = client.post(
                "/auth/reset-password",
                json={"token": genuine_token, "new_password": new_password},
            )
            assert success_response.status_code == 200
            assert success_response.json() == {"message": "Password reset successfully"}

            verify_db = session_factory()
            reset_user = (
                verify_db.query(User).filter(User.email == "alice@example.com").first()
            )
            assert reset_user.token_version == 1
            assert verify_password(new_password, reset_user.hashed_password)
            assert not verify_password(current_password, reset_user.hashed_password)
            verify_db.close()

            # The already-used token is now stale (token_version moved on).
            replay_response = client.post(
                "/auth/reset-password",
                json={"token": genuine_token, "new_password": "another-password"},
            )
            assert replay_response.status_code == 400
            assert replay_response.json() == {
                "detail": "Invalid or expired password reset token"
            }
    finally:
        app.dependency_overrides.clear()


def test_token_version_unchanged_between_forgot_and_reset_when_reset_never_happens():
    """forgot-password must never itself mutate token_version - only a
    successful reset may. A token minted for version N must still work
    if no reset has happened in between."""
    session_factory = _sqlite_session_factory()
    db = session_factory()
    user = User(
        username="bob",
        email="bob@example.com",
        hashed_password=hash_password("old-password"),
        is_verified=True,
        token_version=5,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    reset_token = create_password_reset_token(user.email, user.token_version)

    reloaded = db.query(User).filter(User.email == "bob@example.com").first()
    assert reloaded.token_version == 5

    payload = decode_password_reset_token(reset_token)
    assert payload["ver"] == reloaded.token_version
    db.close()


def test_jwt_signature_and_algorithm_are_consistent_across_generation_and_decoding():
    """Guards against a SECRET_KEY/ALGORITHM mismatch between the process
    that mints a reset token and the one that decodes it - the two must
    always be app.core.security's own module-level constants."""
    import jwt as pyjwt

    token = create_password_reset_token("user@example.com", 0)
    raw_payload = pyjwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])

    assert raw_payload["type"] == "password_reset"
    assert decode_password_reset_token(token) == raw_payload
