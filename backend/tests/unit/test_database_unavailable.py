"""Section 7A/7B: the global sqlalchemy.exc.OperationalError -> sanitized
HTTP 503 exception boundary in app/main.py.

Deliberately the one place in this suite that uses FastAPI's TestClient
rather than calling endpoint functions directly (see
test_analysis_cancellation.py's module docstring for the usual
convention here) - a registered @app.exception_handler only ever runs
inside the real ASGI request/response cycle. Calling an endpoint function
directly (as every other test file does) bypasses it entirely, so there
is no honest way to prove the handler works without going through the
real app.

A fake DB session (never a real MySQL/PyMySQL dependency) is injected via
FastAPI's dependency_overrides, raising the exact exception classes
SQLAlchemy uses for a genuine connection failure vs. an unrelated
application-level DB error - the same distinction app/main.py's handler
is required to make.
"""
from fastapi.testclient import TestClient
from sqlalchemy.exc import IntegrityError, OperationalError

from app.api.dependencies import get_current_verified_user
from app.db.database import get_db
from app.main import _SERVICE_UNAVAILABLE_DETAIL, app
from app.models.user import User


def _fake_user() -> User:
    return User(
        id=1, username="alice", email="alice@example.com",
        hashed_password="x", is_verified=True,
    )


class _RaisingSession:
    """Stands in for a Session whose first query cannot reach the
    database at all - never a real socket/connection, just the exact
    exception SQLAlchemy/PyMySQL raise when that happens."""

    def __init__(self, exc):
        self._exc = exc

    def query(self, *args, **kwargs):
        raise self._exc

    def close(self):
        pass


def _override(exc):
    def _raising_get_db():
        yield _RaisingSession(exc)

    app.dependency_overrides[get_db] = _raising_get_db
    app.dependency_overrides[get_current_verified_user] = _fake_user


def test_operational_error_maps_to_a_sanitized_503():
    # The real shape PyMySQL/SQLAlchemy raise for "server unreachable".
    _override(
        OperationalError(
            "SELECT 1", {}, Exception("(2003, \"Can't connect to MySQL server on 'db:3306' (111)\")")
        )
    )
    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.get("/analysis/history")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json() == {"detail": _SERVICE_UNAVAILABLE_DETAIL}

    leaked_terms = ("pymysql", "3306", "traceback", "connect to mysql", "operationalerror")
    body_lower = response.text.lower()
    for term in leaked_terms:
        assert term not in body_lower, f"leaked internal detail: {term!r}"


def test_non_connectivity_sqlalchemy_error_is_not_converted_to_503():
    """A real application bug (bad SQL, a constraint violation) must keep
    surfacing as its normal (existing) error handling, not be blindly
    reinterpreted as a service outage."""
    _override(IntegrityError("INSERT INTO x", {}, Exception("UNIQUE constraint failed")))
    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.get("/analysis/history")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code != 503


def test_ordinary_application_exception_is_not_converted_to_503():
    """A plain application bug unrelated to the DB (e.g. a bug inside
    endpoint logic) must also never be reinterpreted as a DB outage."""
    _override(RuntimeError("some unrelated bug"))
    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.get("/analysis/history")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code != 503
