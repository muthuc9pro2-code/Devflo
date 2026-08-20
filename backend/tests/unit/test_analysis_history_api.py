"""Section L: History API tests (GET /analysis/history, GET /analysis/{id}).

Scenarios 1-14 and 17 from the History spec - scenarios 15 (persist-before-
publish ordering) and 16 (legacy analyses without result_snapshot) are
covered directly against reconstruct_current_investigation_result /
_finalize_analysis_task in test_result_snapshot.py, not duplicated here.

Endpoint functions are called directly (db/current_user/response passed in
explicitly) rather than through a FastAPI TestClient - the same pattern the
rest of this suite already uses for SSE/task-layer testing, and it keeps
these tests fast and independent of auth-dependency wiring.
"""
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException, Response
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.api import analysis as analysis_api
from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, User


def _session():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _user(db, name="alice") -> User:
    user = User(username=name, email=f"{name}@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()
    return user


def _analysis(db, user, *, created_at, status="completed", **kwargs) -> Analysis:
    defaults = dict(
        user_id=user.id, original_filename="a.log", saved_file_path="/uploads/a.log",
        status=status, created_at=created_at, updated_at=created_at,
    )
    defaults.update(kwargs)
    analysis = Analysis(**defaults)
    db.add(analysis)
    db.commit()
    return analysis


def _artifact(db, analysis, position=0) -> AnalysisArtifact:
    artifact = AnalysisArtifact(
        analysis_id=analysis.id, position=position, original_filename=f"f{position}.log",
        saved_file_path=f"/uploads/f{position}.log", size_bytes=10, status="completed",
        last_processed_line=1, processed_bytes=10,
    )
    db.add(artifact)
    db.commit()
    return artifact


# --- Scenarios 1-2: ownership scoping --------------------------------------


def test_history_only_returns_the_current_users_own_analyses():
    db = _session()
    alice = _user(db, "alice")
    bob = _user(db, "bob")
    base = datetime.now(timezone.utc)
    alice_analysis = _analysis(db, alice, created_at=base)
    _analysis(db, bob, created_at=base + timedelta(seconds=1))

    page = analysis_api.get_analysis_history(db=db, current_user=alice, response=Response())

    assert len(page.items) == 1
    assert page.items[0].analysis_id == alice_analysis.id


def test_user_cannot_fetch_another_users_analysis_by_id():
    db = _session()
    alice = _user(db, "alice")
    bob = _user(db, "bob")
    alice_analysis = _analysis(db, alice, created_at=datetime.now(timezone.utc))

    with pytest.raises(HTTPException) as error:
        analysis_api.get_analysis_detail(
            analysis_id=alice_analysis.id, db=db, current_user=bob
        )

    assert error.value.status_code == 404


def test_fetching_a_nonexistent_analysis_id_also_404s():
    """A missing id and another user's real id must be indistinguishable -
    both plain 404, never a different error that would leak existence."""
    db = _session()
    alice = _user(db, "alice")

    with pytest.raises(HTTPException) as error:
        analysis_api.get_analysis_detail(analysis_id=999999, db=db, current_user=alice)

    assert error.value.status_code == 404


# --- Scenario 3: newest-first ------------------------------------------


def test_history_is_ordered_newest_first():
    db = _session()
    alice = _user(db)
    base = datetime.now(timezone.utc)
    oldest = _analysis(db, alice, created_at=base)
    middle = _analysis(db, alice, created_at=base + timedelta(seconds=1))
    newest = _analysis(db, alice, created_at=base + timedelta(seconds=2))

    page = analysis_api.get_analysis_history(db=db, current_user=alice, response=Response())

    assert [item.analysis_id for item in page.items] == [newest.id, middle.id, oldest.id]


# --- Scenario 4: pagination is deterministic and bounded -------------------


def test_history_pagination_is_deterministic_and_bounded():
    db = _session()
    alice = _user(db)
    base = datetime.now(timezone.utc)
    analyses = [
        _analysis(db, alice, created_at=base + timedelta(seconds=i)) for i in range(5)
    ]
    expected_order = [a.id for a in reversed(analyses)]  # newest first

    seen: list[int] = []
    cursor = None
    for _ in range(10):  # generous upper bound on pages, loop exits via break
        page = analysis_api.get_analysis_history(
            db=db, current_user=alice, response=Response(), limit=2, cursor=cursor
        )
        assert len(page.items) <= 2
        seen.extend(item.analysis_id for item in page.items)
        if page.next_cursor is None:
            break
        cursor = page.next_cursor

    assert seen == expected_order  # every analysis exactly once, in order


def test_history_limit_is_clamped_to_the_configured_maximum():
    db = _session()
    alice = _user(db)
    base = datetime.now(timezone.utc)
    for i in range(3):
        _analysis(db, alice, created_at=base + timedelta(seconds=i))

    page = analysis_api.get_analysis_history(
        db=db, current_user=alice, response=Response(), limit=10_000
    )

    assert len(page.items) == 3  # not an error, just bounded by what exists


def test_invalid_history_cursor_is_rejected():
    db = _session()
    alice = _user(db)

    with pytest.raises(HTTPException) as error:
        analysis_api.get_analysis_history(
            db=db, current_user=alice, response=Response(), cursor="not-a-real-cursor!!"
        )

    assert error.value.status_code == 400


# --- Scenarios 5-6: history list is cheap -----------------------------------


def test_history_list_never_touches_correlation_or_gemini(monkeypatch):
    def _must_not_run_correlation(**kwargs):
        raise AssertionError("history list must not reconstruct/run correlation")

    def _must_not_call_gemini(context):
        raise AssertionError("history list must not call Gemini")

    from app.tasks import analysis as analysis_task
    monkeypatch.setattr(analysis_task, "run_correlation", _must_not_run_correlation)
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _must_not_call_gemini)

    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, created_at=datetime.now(timezone.utc),
        result_snapshot={"investigation_path": "correlated", "components": [{"nodes": []}]},
    )
    _artifact(db, analysis)

    page = analysis_api.get_analysis_history(db=db, current_user=alice, response=Response())

    assert len(page.items) == 1  # returned successfully without touching either


# --- Scenarios 7-9: completed detail uses the persisted snapshot -----------


def test_completed_detail_returns_the_persisted_result_snapshot():
    db = _session()
    alice = _user(db)
    snapshot = {
        "investigation_path": "simple",
        "evidence": [{"service": "worker"}],
        "ai_analysis": {"title": "stored title"},
    }
    analysis = _analysis(
        db, alice, created_at=datetime.now(timezone.utc), result_snapshot=snapshot
    )
    _artifact(db, analysis)

    response = analysis_api.get_analysis_detail(
        analysis_id=analysis.id, db=db, current_user=alice
    )

    import json
    body = json.loads(response.body)
    assert body["status"] == "completed"
    assert body["investigation_result"] == snapshot


def test_completed_detail_never_calls_gemini_or_reruns_correlation_when_snapshot_exists(monkeypatch):
    from app.tasks import analysis as analysis_task

    def _must_not_run_correlation(**kwargs):
        raise AssertionError("must not rerun correlation when a snapshot exists")

    def _must_not_call_gemini(context):
        raise AssertionError("must not call Gemini when a snapshot exists")

    monkeypatch.setattr(analysis_task, "run_correlation", _must_not_run_correlation)
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _must_not_call_gemini)

    db = _session()
    alice = _user(db)
    snapshot = {"investigation_path": "zero_evidence", "evidence_count": 0}
    analysis = _analysis(
        db, alice, created_at=datetime.now(timezone.utc), result_snapshot=snapshot
    )
    _artifact(db, analysis)

    response = analysis_api.get_analysis_detail(
        analysis_id=analysis.id, db=db, current_user=alice
    )

    assert response.status_code == 200


# --- Scenarios 10-12: in-progress / completed progress contract ------------


def test_in_progress_detail_reports_persisted_byte_progress():
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, created_at=datetime.now(timezone.utc), status="processing")
    db.add(
        AnalysisArtifact(
            analysis_id=analysis.id, position=0, original_filename="a.log",
            saved_file_path="/uploads/a.log", size_bytes=1000, status="processing",
            last_processed_line=1, processed_bytes=530,
        )
    )
    db.commit()

    response = analysis_api.get_analysis_detail(
        analysis_id=analysis.id, db=db, current_user=alice
    )

    import json
    body = json.loads(response.body)
    assert body["status"] == "processing"
    assert body["progress"] == 53  # scenario 11: a 53% persisted analysis returns 53%


def test_completed_detail_reports_progress_99_never_100():
    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, created_at=datetime.now(timezone.utc),
        result_snapshot={"investigation_path": "zero_evidence", "evidence_count": 0},
    )
    _artifact(db, analysis)

    response = analysis_api.get_analysis_detail(
        analysis_id=analysis.id, db=db, current_user=alice
    )

    import json
    body = json.loads(response.body)
    assert body["progress"] == 99
    assert body["progress"] != 100


# --- Scenarios 13-14: SIMPLE/CORRELATED history restoration ----------------


def test_simple_history_restores_the_exact_stored_response():
    db = _session()
    alice = _user(db)
    snapshot = {
        "investigation_path": "simple",
        "evidence": [{"service": "worker", "severity": "ERROR"}],
        "ai_analysis": {
            "title": "Worker crash", "summary": "s",
            "recommended_actions": ["restart the worker"],
            "uncertainties": [],
        },
    }
    analysis = _analysis(
        db, alice, created_at=datetime.now(timezone.utc), result_snapshot=snapshot
    )
    _artifact(db, analysis)

    response = analysis_api.get_analysis_detail(
        analysis_id=analysis.id, db=db, current_user=alice
    )

    import json
    body = json.loads(response.body)["investigation_result"]
    assert body == snapshot


def test_correlated_history_restores_the_exact_stored_graph_and_timeline():
    db = _session()
    alice = _user(db)
    snapshot = {
        "investigation_path": "correlated",
        "components": [
            {
                "nodes": [{"id": "n1", "role": "root"}, {"id": "n2", "role": "victim"}],
                "edges": [{"source": "n1", "target": "n2", "relationship_type": "causal", "delta_ms": 12.5}],
                "associations": [],
                "timeline": [{"node_id": "n1", "relative_ms": 0}],
            }
        ],
        "ai_analysis": {"title": "Cascading failure", "summary": "s", "recommended_actions": [], "uncertainties": []},
    }
    analysis = _analysis(
        db, alice, created_at=datetime.now(timezone.utc), result_snapshot=snapshot
    )
    _artifact(db, analysis)

    response = analysis_api.get_analysis_detail(
        analysis_id=analysis.id, db=db, current_user=alice
    )

    import json
    body = json.loads(response.body)["investigation_result"]
    assert body == snapshot


# --- Scenario 17: no internal fields leaked ---------------------------------


def test_history_list_does_not_expose_saved_file_path_or_source_reference():
    db = _session()
    alice = _user(db)
    _analysis(
        db, alice, created_at=datetime.now(timezone.utc),
        saved_file_path="/uploads/super-secret-internal-path.log",
        source_kind="github", source_reference="git@internal:private/repo.git",
    )

    page = analysis_api.get_analysis_history(db=db, current_user=alice, response=Response())
    dumped = page.model_dump_json()

    assert "super-secret-internal-path" not in dumped
    assert "private/repo.git" not in dumped


def test_detail_response_does_not_expose_saved_file_path_or_source_reference():
    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, created_at=datetime.now(timezone.utc),
        saved_file_path="/uploads/super-secret-internal-path.log",
        source_kind="github", source_reference="git@internal:private/repo.git",
        result_snapshot={"investigation_path": "zero_evidence", "evidence_count": 0},
    )
    _artifact(db, analysis)

    response = analysis_api.get_analysis_detail(
        analysis_id=analysis.id, db=db, current_user=alice
    )

    body_text = response.body.decode("utf-8")
    assert "super-secret-internal-path" not in body_text
    assert "private/repo.git" not in body_text


# --- Cache-Control -----------------------------------------------------


def test_history_and_detail_responses_disable_caching():
    db = _session()
    alice = _user(db)
    analysis = _analysis(
        db, alice, created_at=datetime.now(timezone.utc),
        result_snapshot={"investigation_path": "zero_evidence", "evidence_count": 0},
    )
    _artifact(db, analysis)

    history_response = Response()
    analysis_api.get_analysis_history(db=db, current_user=alice, response=history_response)
    detail_response = analysis_api.get_analysis_detail(
        analysis_id=analysis.id, db=db, current_user=alice
    )

    assert history_response.headers["Cache-Control"] == "no-store"
    assert detail_response.headers["Cache-Control"] == "no-store"
