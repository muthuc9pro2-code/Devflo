import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.core.processing_config import BOUNDED_SELECTION_MAX_AGGREGATE_GROUPS
from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.schemas.gemini import GeminiInvestigationResponse
from app.services.investigation_context import (
    _bounded_evidence_priority,
    select_bounded_evidence_from_db,
    select_evidence_counts_by_artifact,
)
from app.tasks import analysis as analysis_task

_FAKE_GEMINI_RESULT = GeminiInvestigationResponse(
    title="t", summary="s", probable_root_causes=[], what_happened=[],
    source_code_findings=[], recommended_actions=[], uncertainties=[],
)

def _db_with_schema():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    return session_factory

def _seed(session_factory, artifacts_evidence: dict[str, list[dict]], *, fallback_context_artifacts=()):
    db = session_factory()
    user = User(username="t", email="t@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()
    analysis = Analysis(user_id=user.id, original_filename="a", saved_file_path="a", status="processing")
    db.add(analysis)
    db.commit()

    artifact_ids: dict[str, int] = {}
    for filename in artifacts_evidence:
        artifact = AnalysisArtifact(
            analysis_id=analysis.id, position=len(artifact_ids), original_filename=filename,
            saved_file_path=filename, size_bytes=10, status="completed",
            last_processed_line=1, processed_bytes=10,
            fallback_context={"text": "diagnostic text", "kind": "text"} if filename in fallback_context_artifacts else None,
        )
        db.add(artifact)
        db.commit()
        artifact_ids[filename] = artifact.id

    base = datetime.now(timezone.utc)
    counter = 0
    for filename, rows in artifacts_evidence.items():
        for kwargs in rows:
            counter += 1
            defaults = dict(
                analysis_id=analysis.id, artifact_id=artifact_ids[filename],
                correlation_key=f"ck-{counter}", fingerprint=f"fp-{counter}",
                source_format="generic", first_line_number=1, last_line_number=1,
                first_seen=base + timedelta(milliseconds=counter), severity="ERROR",
            )
            defaults.update(kwargs)
            db.add(Evidence(**defaults))
    db.commit()

    analysis_id = analysis.id
    db.close()
    return analysis_id, artifact_ids

def _mock_gemini(monkeypatch):
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", lambda ctx: _FAKE_GEMINI_RESULT)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

def test_finalize_selects_the_bounded_working_set_exactly_once(monkeypatch):
    monkeypatch.setattr(analysis_task, "CORRELATED_MAX_EVIDENCE_RECORDS", 2)
    session_factory = _db_with_schema()
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    analysis_id, _ids = _seed(
        session_factory,
        {"a.log": [{"trace_id": "trace-1"} for _ in range(5)]},
    )
    _mock_gemini(monkeypatch)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)

    real_select = analysis_task.select_bounded_evidence_from_db
    spy = Mock(side_effect=real_select)
    monkeypatch.setattr(analysis_task, "select_bounded_evidence_from_db", spy)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    spy.assert_called_once()

def test_simple_path_evidence_is_genuinely_bounded_not_unbounded_then_truncated(monkeypatch):
    monkeypatch.setattr(analysis_task, "CORRELATED_MAX_EVIDENCE_RECORDS", 2)
    session_factory = _db_with_schema()
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    analysis_id, _ids = _seed(
        session_factory,
        {"a.log": [{"fingerprint": f"fp-unique-{i}", "service": f"svc-{i}"} for i in range(5)]},
    )
    _mock_gemini(monkeypatch)
    published = []
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda aid, p: published.append(p))

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert len(published) == 1
    payload = published[0]
    assert payload["investigation_path"] == "simple"
    assert payload["evidence_count"] == 5
    assert len(payload["evidence"]) == 2
    assert payload["evidence_count_returned"] == 2
    assert payload["evidence_truncated"] is True

def test_route_and_correlation_receive_the_exact_same_evidence_ids(monkeypatch):
    monkeypatch.setattr(analysis_task, "CORRELATED_MAX_EVIDENCE_RECORDS", 3)
    session_factory = _db_with_schema()
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    analysis_id, _ids = _seed(
        session_factory,
        {"a.log": [{"trace_id": "trace-1"} for _ in range(6)]},
    )
    _mock_gemini(monkeypatch)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)

    captured = {}
    real_route = analysis_task.choose_investigation_path

    def spy_route(evidence_rows, **kwargs):
        captured["route_ids"] = {e.id for e in evidence_rows}
        captured["preparation"] = kwargs.get("preparation")
        return real_route(evidence_rows, **kwargs)

    real_correlate = analysis_task.run_correlation

    def spy_correlate(*, analysis_id, evidence_rows, **kwargs):
        captured["correlate_ids"] = {e.id for e in evidence_rows}
        captured["correlate_preparation"] = kwargs.get("preparation")
        return real_correlate(analysis_id=analysis_id, evidence_rows=evidence_rows, **kwargs)

    monkeypatch.setattr(analysis_task, "choose_investigation_path", spy_route)
    monkeypatch.setattr(analysis_task, "run_correlation", spy_correlate)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert captured["route_ids"]
    assert captured["route_ids"] == captured["correlate_ids"]
    assert captured["preparation"] is captured["correlate_preparation"]

def test_correlated_real_total_evidence_count_survives_truncation(monkeypatch):
    monkeypatch.setattr(analysis_task, "CORRELATED_MAX_EVIDENCE_RECORDS", 2)
    session_factory = _db_with_schema()
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    analysis_id, _ids = _seed(
        session_factory,
        {"a.log": [{"trace_id": "trace-1"} for _ in range(5)]},
    )
    _mock_gemini(monkeypatch)
    published = []
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda aid, p: published.append(p))

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    payload = published[0]
    assert payload["investigation_path"] == "correlated"
    assert payload["evidence_count"] == 5
    assert payload["evidence_count_returned"] == 2
    assert payload["evidence_truncated"] is True

def test_real_per_artifact_evidence_counts_survive_truncation(monkeypatch):
    monkeypatch.setattr(analysis_task, "CORRELATED_MAX_EVIDENCE_RECORDS", 2)
    session_factory = _db_with_schema()
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    analysis_id, ids = _seed(
        session_factory,
        {
            "a.log": [{"trace_id": "trace-1"} for _ in range(4)],
            "b.log": [{"trace_id": "trace-1"} for _ in range(3)],
        },
    )
    _mock_gemini(monkeypatch)
    published = []
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda aid, p: published.append(p))

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    payload = published[0]
    assert payload["evidence_artifact_count"] == 2
    artifacts_by_id = {a["artifact_id"]: a for a in payload["artifacts"]}
    assert artifacts_by_id[ids["a.log"]]["evidence_count"] == 4
    assert artifacts_by_id[ids["b.log"]]["evidence_count"] == 3

def test_select_evidence_counts_by_artifact_reports_real_membership_regardless_of_any_subset():
    session_factory = _db_with_schema()
    analysis_id, ids = _seed(
        session_factory,
        {"a.log": [{} for _ in range(3)], "b.log": [{}]},
        fallback_context_artifacts=("a.log",),
    )
    db = session_factory()

    real_map = select_evidence_counts_by_artifact(db, analysis_id=analysis_id)

    assert ids["a.log"] in real_map
    assert real_map[ids["a.log"]] == 3
    assert real_map[ids["b.log"]] == 1
    db.close()

def test_finalize_does_not_misclassify_an_artifact_whose_evidence_was_bounded_out(monkeypatch):
    monkeypatch.setattr(analysis_task, "CORRELATED_MAX_EVIDENCE_RECORDS", 1)
    session_factory = _db_with_schema()
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    analysis_id, ids = _seed(
        session_factory,
        {
            "a.log": [{"first_seen": None, "trace_id": "trace-1"} for _ in range(3)],
            "b.log": [{"trace_id": "trace-1"}],
        },
        fallback_context_artifacts=("a.log",),
    )
    _mock_gemini(monkeypatch)
    published = []
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda aid, p: published.append(p))

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    payload = published[0]
    assert payload["evidence_count_returned"] == 1
    supplemental_files = {
        entry["source_file"] for entry in payload.get("supplemental_low_structure_context", [])
    }
    assert "a.log" not in supplemental_files

def test_unresolved_identity_receives_no_bounded_selection_identity_bonus():
    base = datetime.now(timezone.utc)
    common_kwargs = dict(
        id=1, analysis_id=1, artifact_id=1, fingerprint="fp-unique-1",
        occurrence_count=1, first_seen=base, severity=None, source_matches=None,
    )
    unresolved_row = Evidence(
        **common_kwargs,
        resolved_identity="unresolved:1", identity_match_type="unresolved", identity_strength=0.0,
    )
    no_identity_row = Evidence(**{**common_kwargs, "id": 2, "fingerprint": "fp-unique-2"})

    unresolved_score = _bounded_evidence_priority(
        unresolved_row, fingerprint_counts={}, bridging_trace_ids=set(), bridging_request_ids=set(),
    )
    no_identity_score = _bounded_evidence_priority(
        no_identity_row, fingerprint_counts={}, bridging_trace_ids=set(), bridging_request_ids=set(),
    )

    assert unresolved_score == no_identity_score

def test_real_resolved_identity_still_receives_the_identity_bonus():
    base = datetime.now(timezone.utc)
    row = Evidence(
        id=1, analysis_id=1, artifact_id=1, fingerprint="fp-x", occurrence_count=1,
        first_seen=base, severity=None, source_matches=None,
        resolved_identity="trace:t1", identity_match_type="trace_id", identity_strength=1.0,
        trace_id=None, request_id=None,
    )

    score = _bounded_evidence_priority(
        row, fingerprint_counts={}, bridging_trace_ids=set(), bridging_request_ids=set(),
    )

    assert score >= 3.0

def test_request_id_only_cross_artifact_bridge_is_retained_under_the_bound():
    session_factory = _db_with_schema()
    analysis_id, _ids = _seed(
        session_factory,
        {
            "a.log": [{"request_id": "req-bridge", "severity": None}],
            "b.log": [{"request_id": "req-bridge", "severity": None}],
            "c.log": [{"severity": None} for _ in range(20)],
        },
    )
    db = session_factory()

    selected, total_count = select_bounded_evidence_from_db(
        db, analysis_id=analysis_id, max_records=5, max_context_bytes=10_000_000,
    )

    assert total_count == 22
    selected_request_ids = {e.request_id for e in selected}
    assert "req-bridge" in selected_request_ids
    bridge_rows = [e for e in selected if e.request_id == "req-bridge"]
    assert len(bridge_rows) == 2
    db.close()

def test_fingerprint_aggregate_query_is_capped_to_the_configured_limit(monkeypatch):
    monkeypatch.setattr(
        "app.services.investigation_context.BOUNDED_SELECTION_MAX_AGGREGATE_GROUPS", 2
    )
    session_factory = _db_with_schema()
    rows = []
    for fp_index in range(5):
        rows.append({"fingerprint": f"fp-repeat-{fp_index}"})
        rows.append({"fingerprint": f"fp-repeat-{fp_index}"})
    analysis_id, _ids = _seed(session_factory, {"a.log": rows})
    db = session_factory()

    selected, total_count = select_bounded_evidence_from_db(
        db, analysis_id=analysis_id, max_records=3, max_context_bytes=10_000_000,
    )

    assert total_count == 10
    assert len(selected) == 3
    db.close()

def test_bounded_selection_max_aggregate_groups_is_derived_from_correlated_max_evidence_records():
    from app.core.processing_config import CORRELATED_MAX_EVIDENCE_RECORDS

    assert BOUNDED_SELECTION_MAX_AGGREGATE_GROUPS == CORRELATED_MAX_EVIDENCE_RECORDS * 4

def test_small_investigation_below_the_bound_keeps_the_same_route_and_payload_semantics(monkeypatch):
    session_factory = _db_with_schema()
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    analysis_id, _ids = _seed(
        session_factory,
        {"a.log": [{"trace_id": "trace-1"}, {"trace_id": "trace-1"}]},
    )
    _mock_gemini(monkeypatch)
    published = []
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda aid, p: published.append(p))

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    payload = published[0]
    assert payload["investigation_path"] == "correlated"
    assert payload["evidence_count"] == 2
    assert "evidence_truncated" not in payload
    assert len(payload["components"][0]["nodes"]) == 2

def test_legacy_reconstruction_is_bounded(monkeypatch):
    monkeypatch.setattr(analysis_task, "CORRELATED_MAX_EVIDENCE_RECORDS", 2)
    session_factory = _db_with_schema()
    analysis_id, _ids = _seed(
        session_factory,
        {"a.log": [{"trace_id": "trace-1"} for _ in range(5)]},
    )
    db = session_factory()

    payload = analysis_task.reconstruct_current_investigation_result(
        db, analysis_id, ai_analysis=None, result_snapshot=None,
    )

    assert payload["investigation_path"] == "correlated"
    assert payload["evidence_count"] == 5
    assert payload["evidence_count_returned"] == 2
    assert payload["evidence_truncated"] is True
    db.close()

def test_legacy_reconstruction_simple_path_is_bounded_too(monkeypatch):
    monkeypatch.setattr(analysis_task, "CORRELATED_MAX_EVIDENCE_RECORDS", 2)
    session_factory = _db_with_schema()
    analysis_id, _ids = _seed(
        session_factory,
        {"a.log": [{"fingerprint": f"fp-unique-{i}", "service": f"svc-{i}"} for i in range(5)]},
    )
    db = session_factory()

    payload = analysis_task.reconstruct_current_investigation_result(
        db, analysis_id, ai_analysis=None, result_snapshot=None,
    )

    assert payload["investigation_path"] == "simple"
    assert payload["evidence_count"] == 5
    assert len(payload["evidence"]) == 2
    assert payload["evidence_truncated"] is True
    db.close()

def _seed_reshuffled_evidence(
    session_factory,
    insertion_order,
    *,
    tie_first_line=False,
):
    stride = 10**9
    db = session_factory()
    unique = uuid.uuid4().hex[:8]
    user = User(
        username=f"t-{unique}", email=f"t-{unique}@example.com",
        hashed_password="x", is_verified=True,
    )
    db.add(user)
    db.commit()
    analysis = Analysis(
        user_id=user.id, original_filename="a", saved_file_path="a", status="processing",
    )
    db.add(analysis)
    db.commit()

    artifact_ids = {}
    for position in (0, 1, 2):
        artifact = AnalysisArtifact(
            analysis_id=analysis.id, position=position, original_filename=f"artifact-{position}.log",
            saved_file_path=f"artifact-{position}.log", size_bytes=10, status="completed",
            last_processed_line=8, processed_bytes=10,
        )
        db.add(artifact)
        db.commit()
        artifact_ids[position] = artifact.id

    base = datetime.now(timezone.utc)
    for position, local_line in insertion_order:
        first_line_number = (
            position * stride
            + (1 if tie_first_line else local_line)
        )
        db.add(
            Evidence(
                analysis_id=analysis.id,
                artifact_id=artifact_ids[position],
                correlation_key=f"ck-{position}-{local_line}",
                fingerprint=f"fp-{position}-{local_line}",
                source_format="generic",
                first_line_number=first_line_number,
                last_line_number=first_line_number,
                first_seen=base,
                severity="ERROR",
            )
        )
        db.commit()

    analysis_id = analysis.id
    db.close()
    return analysis_id

def test_bounded_selection_over_5000_is_invariant_to_evidence_id_and_commit_order():
    session_factory = _db_with_schema()

    all_keys = [(position, local_line) for position in (0, 1, 2) for local_line in range(1, 9)]

    import random

    forward_order = list(all_keys)
    shuffled_order = list(all_keys)
    random.Random(1234).shuffle(shuffled_order)
    assert forward_order != shuffled_order

    analysis_id_a = _seed_reshuffled_evidence(session_factory, forward_order)
    analysis_id_b = _seed_reshuffled_evidence(session_factory, shuffled_order)

    db = session_factory()
    try:
        rows_a, total_a = select_bounded_evidence_from_db(
            db, analysis_id=analysis_id_a, max_records=10, max_context_bytes=10_000_000,
        )
        rows_b, total_b = select_bounded_evidence_from_db(
            db, analysis_id=analysis_id_b, max_records=10, max_context_bytes=10_000_000,
        )
    finally:
        db.close()

    assert total_a == total_b == 24
    assert len(rows_a) == len(rows_b) == 10

    keys_a = sorted(row.first_line_number for row in rows_a)
    keys_b = sorted(row.first_line_number for row in rows_b)
    assert keys_a == keys_b

def test_bounded_selection_tied_first_lines_is_invariant_to_evidence_id_and_commit_order():
    session_factory = _db_with_schema()

    all_keys = [
        (position, local_line)
        for position in (0, 1, 2)
        for local_line in range(1, 9)
    ]

    import random

    forward_order = list(all_keys)
    shuffled_order = list(all_keys)

    random.Random(4321).shuffle(
        shuffled_order
    )

    assert forward_order != shuffled_order

    analysis_id_a = _seed_reshuffled_evidence(
        session_factory,
        forward_order,
        tie_first_line=True,
    )

    analysis_id_b = _seed_reshuffled_evidence(
        session_factory,
        shuffled_order,
        tie_first_line=True,
    )

    db = session_factory()

    try:
        rows_a, total_a = (
            select_bounded_evidence_from_db(
                db,
                analysis_id=analysis_id_a,
                max_records=10,
                max_context_bytes=10_000_000,
                batch_size=3,
            )
        )

        rows_b, total_b = (
            select_bounded_evidence_from_db(
                db,
                analysis_id=analysis_id_b,
                max_records=10,
                max_context_bytes=10_000_000,
                batch_size=3,
            )
        )
    finally:
        db.close()

    assert total_a == total_b == 24
    assert len(rows_a) == len(rows_b) == 10

    logical_a = sorted(
        (
            row.first_line_number,
            row.fingerprint,
            row.correlation_key,
        )
        for row in rows_a
    )

    logical_b = sorted(
        (
            row.first_line_number,
            row.fingerprint,
            row.correlation_key,
        )
        for row in rows_b
    )

    assert logical_a == logical_b
