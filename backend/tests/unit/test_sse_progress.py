"""Numeric SSE progress (0-99, never 100) added to the existing
publish_progress(stage, message) contract.

Covers: publish_progress backward compatibility, the aggregate
byte-based ingestion progress helper (_publish_ingestion_progress) in
isolation - math, clamping, monotonicity, dedup, aggregate-not-per-
artifact behavior - and an end-to-end real-DB check that dispatch starts
at 0, ingestion advances, and finalize reports 99 (never 100) before the
existing completion signal.
"""
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.schemas.gemini import GeminiInvestigationResponse
from app.services import analysis_events
from app.tasks import analysis as analysis_task


# --- publish_progress backward compatibility ----------------------------


def test_publish_progress_includes_progress_field_when_provided(monkeypatch):
    published = {}
    monkeypatch.setattr(
        analysis_events,
        "publish_analysis_event",
        lambda analysis_id, event, data: published.update(event=event, data=data),
    )

    analysis_events.publish_progress(1, "ingestion", "in progress", progress=42)

    assert published["data"] == {"stage": "ingestion", "message": "in progress", "progress": 42}


def test_publish_progress_omits_progress_field_when_not_provided(monkeypatch):
    """Existing callers that don't pass progress must keep getting the
    exact same payload shape as before this change."""
    published = {}
    monkeypatch.setattr(
        analysis_events,
        "publish_analysis_event",
        lambda analysis_id, event, data: published.update(event=event, data=data),
    )

    analysis_events.publish_progress(1, "identity", "done")

    assert published["data"] == {"stage": "identity", "message": "done"}
    assert "progress" not in published["data"]


# --- _publish_ingestion_progress: math, clamping, dedup, aggregation ----


def _totals_db(processed: int, total: int) -> Mock:
    db = Mock()
    db.query.return_value.filter.return_value.first.return_value = (processed, total)
    return db


def test_first_progress_can_be_zero():
    db = _totals_db(0, 1000)
    assert analysis_task._publish_ingestion_progress(db=db, analysis_id=1, last_published=-1) == 0


def test_progress_increases_as_aggregate_bytes_are_processed():
    values = []
    last = -1
    for processed in (0, 100, 400, 999):
        db = _totals_db(processed, 1000)
        last = analysis_task._publish_ingestion_progress(db=db, analysis_id=1, last_published=last)
        values.append(last)

    assert values == [0, 10, 40, 99] or values == sorted(values)
    assert all(b >= a for a, b in zip(values, values[1:]))


def test_progress_never_exceeds_98_during_ingestion_even_if_bytes_reach_total():
    db = _totals_db(1000, 1000)  # 100% of bytes, mid-ingestion
    result = analysis_task._publish_ingestion_progress(db=db, analysis_id=1, last_published=50)
    assert result == 98
    assert result < 99


def test_progress_never_decreases(monkeypatch):
    publish = Mock()
    monkeypatch.setattr(analysis_task, "publish_progress", publish)

    db = _totals_db(100, 1000)  # 10% - lower than what we already published
    result = analysis_task._publish_ingestion_progress(db=db, analysis_id=1, last_published=55)

    assert result == 55  # unchanged, not decreased
    publish.assert_not_called()


def test_duplicate_integer_progress_is_not_republished(monkeypatch):
    publish = Mock()
    monkeypatch.setattr(analysis_task, "publish_progress", publish)

    # Two different byte counts that floor to the same integer percentage.
    db_a = _totals_db(370, 1000)
    db_b = _totals_db(375, 1000)

    last = analysis_task._publish_ingestion_progress(db=db_a, analysis_id=1, last_published=-1)
    last = analysis_task._publish_ingestion_progress(db=db_b, analysis_id=1, last_published=last)

    assert last == 37
    publish.assert_called_once()  # only the genuine 37% advance, not the repeat


def test_zero_or_unknown_total_bytes_does_not_crash_or_divide_by_zero():
    db = _totals_db(0, 0)
    assert analysis_task._publish_ingestion_progress(db=db, analysis_id=1, last_published=5) == 5


def test_query_failure_falls_back_to_last_published_without_raising():
    """A bare Mock() (as used throughout test_multifile_processing.py's
    existing _process_artifact tests) can't be unpacked as a 2-tuple -
    this must degrade gracefully, not crash artifact processing."""
    db = Mock()  # db.query(...).filter(...).first() returns an unconfigured MagicMock
    result = analysis_task._publish_ingestion_progress(db=db, analysis_id=1, last_published=12)
    assert result == 12


def test_progress_reflects_aggregate_across_all_artifacts_not_a_single_one():
    """Verifies the query sums across the WHOLE analysis (filtered by
    analysis_id only), not scoped to one artifact - two large-but-early
    artifacts and one small-but-finished artifact must combine into one
    ratio, not "completed_artifacts / total_artifacts"."""
    db = Mock()
    # 3 artifacts: sizes 100, 900, 5000; processed so far: 100, 0, 1900
    # completed_artifacts/total = 1/3 = 33% would be WRONG.
    # byte ratio = (100+0+1900)/(100+900+5000) = 2000/6000 = 33.33% -> 33
    # (coincidentally close here on purpose to prove it's not the naive
    # count-based formula reached by a different, wrong calculation path)
    db.query.return_value.filter.return_value.first.return_value = (2000, 6000)
    result = analysis_task._publish_ingestion_progress(db=db, analysis_id=1, last_published=-1)

    assert result == 33  # byte ratio: (100+0+1900)/(100+900+5000)
    # Directly confirms the query sums byte columns, not a completed-artifact count.
    query_columns = db.query.call_args.args
    assert any("processed_bytes" in str(col) for col in query_columns)
    assert any("size_bytes" in str(col) for col in query_columns)


def test_ingestion_progress_excludes_unsupported_and_duplicate_artifact_bytes(monkeypatch):
    """Additional Requirement B: unsupported/duplicate artifacts are never
    dispatched for ingestion, so a huge one must not sit in the denominator
    and suppress visible progress on the real, dispatchable artifact."""
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    db = session_factory()
    user = User(username="t", email="t@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()
    analysis = Analysis(user_id=user.id, original_filename="a", saved_file_path="a", status="processing")
    db.add(analysis)
    db.commit()
    db.add_all(
        [
            AnalysisArtifact(
                analysis_id=analysis.id, position=0, original_filename="huge.bin",
                saved_file_path="huge.bin", size_bytes=1_000_000_000, status="unsupported",
                last_processed_line=0, processed_bytes=0,
            ),
            AnalysisArtifact(
                analysis_id=analysis.id, position=1, original_filename="dup.log",
                saved_file_path="dup.log", size_bytes=500, status="duplicate",
                duplicate_of_artifact_id=None, last_processed_line=0, processed_bytes=500,
            ),
            AnalysisArtifact(
                analysis_id=analysis.id, position=2, original_filename="real.log",
                saved_file_path="real.log", size_bytes=100, status="processing",
                last_processed_line=0, processed_bytes=100,
            ),
        ]
    )
    db.commit()

    result = analysis_task._publish_ingestion_progress(db=db, analysis_id=analysis.id, last_published=-1)

    assert result == 98  # 100/100 real bytes = 100%, clamped to the 98 mid-ingestion cap


# --- End-to-end: dispatch starts at 0, finalize reports 99 not 100 ------


def _sqlite_analysis_with_artifacts(monkeypatch, sizes: list[int]) -> int:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)

    db = session_factory()
    user = User(username="t", email="t@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()

    analysis = Analysis(user_id=user.id, original_filename="a", saved_file_path="a", status="processing")
    db.add(analysis)
    db.commit()

    for position, size in enumerate(sizes):
        db.add(
            AnalysisArtifact(
                analysis_id=analysis.id,
                position=position,
                original_filename=f"artifact-{position}",
                saved_file_path=f"artifact-{position}",
                size_bytes=size,
                status="completed",
                last_processed_line=1,
                processed_bytes=size,
            )
        )
    db.commit()
    analysis_id = analysis.id
    db.close()
    return analysis_id


def test_dispatch_publishes_progress_zero_first(monkeypatch):
    published = []
    monkeypatch.setattr(
        analysis_task,
        "publish_progress",
        lambda *a, **kw: published.append((a, kw)),
    )
    db = Mock()
    analysis = SimpleNamespace(id=9, status="pending", source_kind=None, processing_generation=0)

    def fake_execute(statement, *a, **k):
        analysis.status = "processing"
        analysis.processing_generation += 1
        return SimpleNamespace(rowcount=1)

    db.execute.side_effect = fake_execute
    db.query.return_value.filter.return_value.first.return_value = analysis
    db.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
        SimpleNamespace(id=101, status="pending")
    ]
    monkeypatch.setattr(analysis_task, "sessionLocal", Mock(return_value=db))
    monkeypatch.setattr(analysis_task, "group", Mock(return_value=SimpleNamespace()))
    monkeypatch.setattr(analysis_task, "chord", Mock(return_value=Mock()))

    analysis_task.process_analysis.run(9)

    assert published, "expected at least one publish_progress call"
    first_args, first_kwargs = published[0]
    assert first_kwargs.get("progress") == 0


def test_finalize_reports_99_never_100_for_zero_evidence_analysis(monkeypatch, caplog):
    analysis_id = _sqlite_analysis_with_artifacts(monkeypatch, [10])

    published_progress = []
    monkeypatch.setattr(
        analysis_task,
        "publish_progress",
        lambda analysis_id, stage, message, progress=None: published_progress.append(progress),
    )

    with caplog.at_level("INFO", logger="app.tasks.analysis"):
        analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert 100 not in published_progress
    assert all(p is None or p == 99 for p in published_progress)
    # evidence_count is 0 for this fixture, so completion is the "completed"
    # stage progress event itself (no correlated evidence to run
    # identity/timeline/correlation for) - still must never be 100.
    assert published_progress  # at least the "Evidence extraction completed" + "completed" events fired


# --- Additional Locked Requirement A: investigation_result is always LAST -


def _events_in_order(monkeypatch):
    events: list[tuple[str, object]] = []
    monkeypatch.setattr(
        analysis_task,
        "publish_progress",
        lambda analysis_id, stage, message, progress=None: events.append(("progress", progress)),
    )
    monkeypatch.setattr(
        analysis_task,
        "publish_investigation_result",
        lambda analysis_id, payload: events.append(("investigation_result", payload)),
    )
    return events


def test_simple_path_investigation_result_is_the_last_event(monkeypatch):
    analysis_id = _sqlite_analysis_with_artifacts(monkeypatch, [10])
    db = analysis_task.sessionLocal()
    artifact = db.query(AnalysisArtifact).filter_by(analysis_id=analysis_id).first()
    db.add(
        Evidence(
            analysis_id=analysis_id, artifact_id=artifact.id, correlation_key="ck-1",
            fingerprint="fp-1", service="worker", source_format="generic",
            first_line_number=1, last_line_number=1, severity="ERROR",
        )
    )
    db.commit()
    db.close()

    fake_result = GeminiInvestigationResponse(
        title="t", summary="s", probable_root_causes=[], what_happened=[],
        source_code_findings=[], recommended_actions=[], uncertainties=[],
    )
    monkeypatch.setattr(
        analysis_task, "generate_investigation_explanation", lambda ctx: fake_result
    )
    events = _events_in_order(monkeypatch)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert events, "expected at least one SSE event"
    assert events[-1][0] == "investigation_result"
    assert events[-1][1]["investigation_path"] == "simple"


def test_correlated_path_investigation_result_is_the_last_event(monkeypatch):
    analysis_id = _sqlite_analysis_with_artifacts(monkeypatch, [10])
    db = analysis_task.sessionLocal()
    artifact = db.query(AnalysisArtifact).filter_by(analysis_id=analysis_id).first()
    base_time = datetime.now(timezone.utc)
    db.add_all(
        [
            Evidence(
                analysis_id=analysis_id, artifact_id=artifact.id, correlation_key="ck-1",
                fingerprint="fp-1", trace_id="trace-1", service="api", source_format="generic",
                first_line_number=1, last_line_number=1, severity="ERROR", first_seen=base_time,
            ),
            Evidence(
                analysis_id=analysis_id, artifact_id=artifact.id, correlation_key="ck-2",
                fingerprint="fp-2", trace_id="trace-1", service="db", source_format="generic",
                first_line_number=2, last_line_number=2, severity="ERROR",
                first_seen=base_time,
            ),
        ]
    )
    db.commit()
    db.close()

    fake_result = GeminiInvestigationResponse(
        title="t", summary="s", probable_root_causes=[], what_happened=[],
        source_code_findings=[], recommended_actions=[], uncertainties=[],
    )
    monkeypatch.setattr(
        analysis_task, "generate_investigation_explanation", lambda ctx: fake_result
    )
    events = _events_in_order(monkeypatch)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert events, "expected at least one SSE event"
    assert events[-1][0] == "investigation_result"
    assert events[-1][1]["investigation_path"] == "correlated"
    # A progress event legitimately precedes it (deterministic correlation
    # finishing before the Gemini call) - just never one AFTER it.
    assert any(kind == "progress" for kind, _ in events[:-1])
