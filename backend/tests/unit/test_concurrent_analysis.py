"""Bounded cross-artifact concurrency (Task 1).

Celery itself isn't exercised here (no broker/worker in unit tests, matching
this repo's existing style - see test_worker_session_keeps_checkpoint_rows_
loaded_across_commits in test_multifile_processing.py, which also calls
task.run() directly). These tests instead prove: the dispatcher builds the
expected bounded group/chord workflow instead of looping; the global
concurrency bound is an explicit, discoverable setting; downstream
identity/timeline/correlation only run after every artifact has actually
completed; a failing artifact marks the analysis failed instead of silently
proceeding; and sequential vs. per-task processing produce equivalent
evidence for the same fixtures.
"""
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.celery_app import celery_app
from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, User
from app.services.log_praser import ParsedEvent
from app.tasks import analysis as analysis_task
from app.tasks.analysis import _process_artifact, _process_artifact_task

FIXTURES = Path(__file__).parents[1] / "fixtures" / "diagnostics"


def _artifact(identifier: int, position: int, fixture_name: str, status: str = "pending"):
    path = FIXTURES / fixture_name
    return SimpleNamespace(
        id=identifier,
        analysis_id=9,
        position=position,
        original_filename=fixture_name,
        saved_file_path=str(path),
        content_type=None,
        size_bytes=path.stat().st_size,
        detected_format=None,
        status=status,
        last_processed_line=0,
        processed_bytes=0,
    )


def test_dispatch_builds_a_bounded_group_and_chord_instead_of_looping(monkeypatch):
    """process_analysis must fan out into one task per pending artifact
    (group), joined by a chord callback, and must not block waiting on
    them - it dispatches and returns.
    """
    db = Mock()
    analysis = SimpleNamespace(id=9, status="pending", source_kind=None)
    db.query.return_value.filter.return_value.first.return_value = analysis
    artifacts = [SimpleNamespace(id=101), SimpleNamespace(id=102), SimpleNamespace(id=103)]
    db.query.return_value.filter.return_value.order_by.return_value.all.return_value = artifacts
    monkeypatch.setattr(analysis_task, "sessionLocal", Mock(return_value=db))

    captured = {}

    def fake_group(sigs):
        captured["group_sigs"] = list(sigs)
        return SimpleNamespace(kind="group")

    def fake_chord(group_obj, callback):
        captured["chord_group"] = group_obj
        captured["chord_callback"] = callback
        workflow = Mock()
        captured["workflow"] = workflow
        return workflow

    monkeypatch.setattr(analysis_task, "group", fake_group)
    monkeypatch.setattr(analysis_task, "chord", fake_chord)

    process_artifact_sig = Mock()
    finalize_sig = Mock()
    monkeypatch.setattr(analysis_task._process_artifact_task, "si", Mock(return_value=process_artifact_sig))
    monkeypatch.setattr(analysis_task._finalize_analysis_task, "s", Mock(return_value=finalize_sig))

    analysis_task.process_analysis.run(9)

    assert captured["group_sigs"] == [process_artifact_sig, process_artifact_sig, process_artifact_sig]
    assert analysis_task._process_artifact_task.si.call_count == 3
    for call in analysis_task._process_artifact_task.si.call_args_list:
        assert call.args[0] == 9  # analysis_id
    assert {c.args[1] for c in analysis_task._process_artifact_task.si.call_args_list} == {101, 102, 103}
    assert captured["chord_callback"] is finalize_sig
    captured["workflow"].apply_async.assert_called_once_with()


def test_source_prep_is_chained_before_the_artifact_chord_when_source_present(monkeypatch):
    db = Mock()
    analysis = SimpleNamespace(id=9, status="pending", source_kind="zip")
    db.query.return_value.filter.return_value.first.return_value = analysis
    db.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
        SimpleNamespace(id=101)
    ]
    monkeypatch.setattr(analysis_task, "sessionLocal", Mock(return_value=db))
    monkeypatch.setattr(analysis_task, "group", Mock(return_value=SimpleNamespace()))
    monkeypatch.setattr(analysis_task, "chord", Mock(return_value=Mock()))

    captured = {}

    def fake_chain(*steps):
        captured["steps"] = steps
        workflow = Mock()
        captured["workflow"] = workflow
        return workflow

    monkeypatch.setattr(analysis_task, "chain", fake_chain)
    prep_sig = Mock()
    monkeypatch.setattr(analysis_task._prepare_source_task, "si", Mock(return_value=prep_sig))

    analysis_task.process_analysis.run(9)

    assert captured["steps"][0] is prep_sig
    analysis_task._prepare_source_task.si.assert_called_once_with(9)
    captured["workflow"].apply_async.assert_called_once_with()


def test_dispatch_skips_source_chain_when_no_source_present(monkeypatch):
    db = Mock()
    analysis = SimpleNamespace(id=9, status="pending", source_kind=None)
    db.query.return_value.filter.return_value.first.return_value = analysis
    db.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
        SimpleNamespace(id=101)
    ]
    monkeypatch.setattr(analysis_task, "sessionLocal", Mock(return_value=db))
    monkeypatch.setattr(analysis_task, "group", Mock(return_value=SimpleNamespace()))
    chord_workflow = Mock()
    monkeypatch.setattr(analysis_task, "chord", Mock(return_value=chord_workflow))
    chain_mock = Mock()
    monkeypatch.setattr(analysis_task, "chain", chain_mock)

    analysis_task.process_analysis.run(9)

    chain_mock.assert_not_called()
    chord_workflow.apply_async.assert_called_once_with()


def test_global_concurrency_is_explicitly_bounded_for_2_vcpu_target():
    assert celery_app.conf.worker_concurrency == 2


def test_finalize_skips_downstream_work_when_an_artifact_is_incomplete(monkeypatch):
    db = Mock()
    analysis = SimpleNamespace(id=9, status="processing")
    incomplete_artifact = SimpleNamespace(id=102, status="processing")
    db.query.return_value.filter.return_value.first.side_effect = [analysis, incomplete_artifact]
    monkeypatch.setattr(analysis_task, "sessionLocal", Mock(return_value=db))

    choose_path = Mock()
    persist_identities = Mock()
    run_correlation = Mock()
    monkeypatch.setattr(analysis_task, "choose_investigation_path", choose_path)
    monkeypatch.setattr(analysis_task, "persist_resolved_identities", persist_identities)
    monkeypatch.setattr(analysis_task, "run_correlation", run_correlation)

    analysis_task._finalize_analysis_task.run([1, 2], 9, None)

    choose_path.assert_not_called()
    persist_identities.assert_not_called()
    run_correlation.assert_not_called()
    assert analysis.status == "processing"  # unchanged - never marked completed


def test_finalize_skips_when_analysis_already_marked_failed(monkeypatch):
    db = Mock()
    analysis = SimpleNamespace(id=9, status="failed")
    db.query.return_value.filter.return_value.first.return_value = analysis
    monkeypatch.setattr(analysis_task, "sessionLocal", Mock(return_value=db))
    choose_path = Mock()
    monkeypatch.setattr(analysis_task, "choose_investigation_path", choose_path)

    analysis_task._finalize_analysis_task.run([1], 9, None)

    choose_path.assert_not_called()


def test_artifact_task_failure_marks_analysis_failed_and_reraises(monkeypatch):
    db = Mock()
    analysis = SimpleNamespace(id=9)
    artifact = SimpleNamespace(id=101, position=0, status="pending")
    db.query.return_value.filter.return_value.first.side_effect = [analysis, artifact]
    monkeypatch.setattr(analysis_task, "sessionLocal", Mock(return_value=db))
    monkeypatch.setattr(analysis_task, "_prepare_source_index", Mock(return_value=None))

    def boom(**_kwargs):
        raise RuntimeError("parser exploded")

    monkeypatch.setattr(analysis_task, "_process_artifact", boom)
    mark_failed = Mock()
    monkeypatch.setattr(analysis_task, "_mark_analysis_failed", mark_failed)

    try:
        analysis_task._process_artifact_task.run(9, 101)
        raised = False
    except RuntimeError:
        raised = True

    assert raised
    mark_failed.assert_called_once_with(db, 9)
    db.close.assert_called_once()


def test_process_artifact_task_skips_an_already_completed_artifact(monkeypatch):
    db = Mock()
    analysis = SimpleNamespace(id=9)
    artifact = SimpleNamespace(id=101, status="completed")
    db.query.return_value.filter.return_value.first.side_effect = [analysis, artifact]
    monkeypatch.setattr(analysis_task, "sessionLocal", Mock(return_value=db))
    process_artifact = Mock()
    monkeypatch.setattr(analysis_task, "_process_artifact", process_artifact)

    result = analysis_task._process_artifact_task.run(9, 101)

    assert result == 0
    process_artifact.assert_not_called()


def test_sequential_and_per_task_processing_produce_equivalent_evidence(monkeypatch):
    """The only intentional behavior difference between the old sequential
    loop and the new per-artifact-task style is the numeric encoding of
    global_line_number (see _GLOBAL_LINE_NUMBER_STRIDE) - never exposed via
    any schema/API/frontend, and never used by correlation_engine.py
    (confirmed by inspection: it sorts strictly by evidence.first_seen).
    Every actual evidence field must match exactly.
    """
    captured_sequential = []
    captured_per_task = []

    def capture(events_list):
        def fake_persist(*, db, analysis_id, events, artifact_id):
            events_list.extend(events)
        return fake_persist

    fixtures = [(101, 0, "generic.txt"), (102, 1, "syslog.txt")]

    # OLD style: one shared analysis object, artifacts processed in a
    # sequential loop, last_processed_line accumulating naturally.
    monkeypatch.setattr(analysis_task, "persist_evidence_batch", capture(captured_sequential))
    shared_analysis = SimpleNamespace(id=9, processed_bytes=0, last_processed_line=0)
    for identifier, position, name in fixtures:
        _process_artifact(
            db=Mock(),
            analysis=shared_analysis,
            artifact=_artifact(identifier, position, name),
        )

    # NEW style: a fresh analysis object per artifact, global_line_number
    # seeded from the position-based band exactly like _process_artifact_task.
    monkeypatch.setattr(analysis_task, "persist_evidence_batch", capture(captured_per_task))
    for identifier, position, name in fixtures:
        per_task_analysis = SimpleNamespace(
            id=9,
            processed_bytes=0,
            last_processed_line=position * analysis_task._GLOBAL_LINE_NUMBER_STRIDE,
        )
        _process_artifact(
            db=Mock(),
            analysis=per_task_analysis,
            artifact=_artifact(identifier, position, name),
        )

    assert len(captured_sequential) == len(captured_per_task) > 0

    def content_signature(event: ParsedEvent) -> tuple:
        return (
            event.level,
            event.trace_id,
            event.request_id,
            event.service,
            event.module,
            event.exception_type,
            event.exception_message,
            event.endpoint,
            event.http_status,
            event.span_id,
            event.parent_span_id,
            event.host,
            event.raw_line,
            event.artifact_id,
        )

    assert [content_signature(e) for e in captured_sequential] == [
        content_signature(e) for e in captured_per_task
    ]


def _sqlite_analysis_with_artifacts(monkeypatch, artifact_lines: list[int]) -> int:
    """Real sqlite-backed Analysis + N completed AnalysisArtifact rows (0
    evidence rows), so _finalize_analysis_task takes the early-return
    no-evidence path - the shortest route to both accounting log lines
    without needing to mock the identity/timeline/correlation stack.
    """
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)

    db = session_factory()
    user = User(username="t", email="t@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()

    analysis = Analysis(
        user_id=user.id, original_filename="a", saved_file_path="a", status="processing"
    )
    db.add(analysis)
    db.commit()

    for position, local_line_count in enumerate(artifact_lines):
        db.add(
            AnalysisArtifact(
                analysis_id=analysis.id,
                position=position,
                original_filename=f"artifact-{position}",
                saved_file_path=f"artifact-{position}",
                size_bytes=10,
                status="completed",
                last_processed_line=local_line_count,
                processed_bytes=10,
            )
        )
    db.commit()
    analysis_id = analysis.id
    db.close()
    return analysis_id


def test_total_lines_reports_real_per_artifact_sum_not_stride_bands(monkeypatch, caplog):
    """Regression test for the confirmed EC2 bug: total_lines was reported
    as ~12000100572 instead of the real ~3273410 because the stride-banded
    Analysis.last_processed_line (position * 10**9 + local_offset) was
    logged directly instead of the actual per-artifact line counts.
    """
    # Artifact 1 sits at position 1: under the bug, Analysis.last_processed_line
    # would be 1 * 10**9 + 2_273_410 - unmistakably different from the real sum.
    analysis_id = _sqlite_analysis_with_artifacts(monkeypatch, [1_000_000, 2_273_410])

    with caplog.at_level("INFO", logger="app.tasks.analysis"):
        analysis_task._finalize_analysis_task.run([], analysis_id, None)

    total_lines_records = [
        record for record in caplog.records if "total_lines=" in record.getMessage()
    ]
    assert len(total_lines_records) == 1
    message = total_lines_records[0].getMessage()
    assert "total_lines=3273410" in message
    assert "total_lines=1000000002273410" not in message
    assert "12000" not in message


def test_total_processing_time_reflects_full_dispatch_to_finalize_span(monkeypatch, caplog):
    """Regression test for the confirmed EC2 issue: 'TOTAL processing time'
    was measured from a perf_counter started at the top of
    _finalize_analysis_task, so it only ever showed that task's own
    (correctly fast) work - not the real end-to-end wall time including
    source prep and concurrent artifact processing.
    """
    analysis_id = _sqlite_analysis_with_artifacts(monkeypatch, [42])
    dispatch_start = time.time() - 5.0  # simulate 5s already spent dispatching/ingesting

    with caplog.at_level("INFO", logger="app.tasks.analysis"):
        analysis_task._finalize_analysis_task.run([], analysis_id, dispatch_start)

    total_time_records = [
        record
        for record in caplog.records
        if "TOTAL processing time" in record.getMessage()
    ]
    assert len(total_time_records) == 1
    reported_seconds = float(total_time_records[0].getMessage().rsplit(" ", 1)[-1].rstrip("s"))
    assert reported_seconds >= 4.9  # reflects dispatch_start, not a ~0s finalize-local timer


def test_total_processing_time_falls_back_to_local_timing_without_dispatch_start(monkeypatch, caplog):
    """If ever invoked without dispatch_start (e.g. a direct/legacy call),
    must not crash and must still report a sane (small, non-negative) time
    rather than blowing up on a None subtraction.
    """
    analysis_id = _sqlite_analysis_with_artifacts(monkeypatch, [1])

    with caplog.at_level("INFO", logger="app.tasks.analysis"):
        analysis_task._finalize_analysis_task.run([], analysis_id, None)

    total_time_records = [
        record
        for record in caplog.records
        if "TOTAL processing time" in record.getMessage()
    ]
    assert len(total_time_records) == 1
    reported_seconds = float(total_time_records[0].getMessage().rsplit(" ", 1)[-1].rstrip("s"))
    assert 0.0 <= reported_seconds < 2.0
