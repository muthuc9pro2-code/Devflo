from contextlib import nullcontext

import pytest
from fastapi import HTTPException
from sqlalchemy.exc import OperationalError

from app.api.v1 import health


class _Connection:
    def __init__(self):
        self.statements = []

    def execute(self, statement):
        self.statements.append(str(statement))


def test_health_is_liveness_only_and_never_touches_database(monkeypatch):
    monkeypatch.setattr(
        health.engine,
        "connect",
        lambda: (_ for _ in ()).throw(AssertionError("DB must not be touched")),
    )

    assert health.health_check() == {"status": "healthy"}


def test_readiness_executes_a_real_database_probe(monkeypatch):
    connection = _Connection()
    monkeypatch.setattr(health.engine, "connect", lambda: nullcontext(connection))

    assert health.readiness_check() == {"status": "ready"}
    assert connection.statements == ["SELECT 1"]


def test_readiness_returns_503_when_database_is_unavailable(monkeypatch):
    error = OperationalError("SELECT 1", {}, Exception("database unavailable"))
    monkeypatch.setattr(health.engine, "connect", lambda: (_ for _ in ()).throw(error))

    with pytest.raises(HTTPException) as exc_info:
        health.readiness_check()

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "Database unavailable"
