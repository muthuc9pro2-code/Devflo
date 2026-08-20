from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from app.services.artifact_detector import ArtifactFormat, detect_artifact
from app.services.diagnostic_adapters import stream_artifact_events
from app.services.log_praser import ParsedEvent
from app.tasks import analysis as analysis_task
from app.tasks.analysis import _process_artifact

FIXTURES = Path(__file__).parents[1] / "fixtures" / "diagnostics"


def test_two_artifacts_process_independently_with_position_based_line_bands():
    """Section 5: each artifact computes its own starting global_line_number
    from its own position band (exactly like _process_artifact_task does),
    independent of any other artifact - no shared, accumulating checkpoint
    on the Analysis row anymore."""
    db = Mock()
    analysis = SimpleNamespace(id=9, processed_bytes=0, last_processed_line=0)
    first = _artifact(101, 0, "generic.txt")
    second = _artifact(102, 1, "docker.jsonl")

    first_count = _process_artifact(
        db=db, analysis=analysis, artifact=first, global_line_number=0
    )
    second_count = _process_artifact(
        db=db,
        analysis=analysis,
        artifact=second,
        global_line_number=1 * analysis_task._GLOBAL_LINE_NUMBER_STRIDE,
    )

    assert (first_count, second_count) == (1, 1)
    assert first.status == second.status == "completed"
    assert first.processed_bytes == first.size_bytes
    assert second.processed_bytes == second.size_bytes
    assert db.execute.call_count == 2


def test_processing_never_mutates_the_shared_analysis_row():
    """Section 5: batch/artifact processing must not repeatedly dirty the
    shared Analysis.processed_bytes/last_processed_line row - only this
    artifact's own row is ever written. (Recomputing the legacy aggregate
    columns once, at finalize time, is covered separately.)"""
    db = Mock()
    analysis = SimpleNamespace(id=9, processed_bytes=0, last_processed_line=0)
    artifact = _artifact(101, 0, "generic.txt")

    _process_artifact(db=db, analysis=analysis, artifact=artifact, global_line_number=0)

    assert analysis.processed_bytes == 0
    assert analysis.last_processed_line == 0


def test_artifact_batch_fingerprints_only_retained_events(monkeypatch):
    db = Mock()
    analysis = SimpleNamespace(id=9, processed_bytes=0, last_processed_line=0)
    artifact = SimpleNamespace(id=101, processed_bytes=0, last_processed_line=0)
    discarded = ParsedEvent(line_number=1, raw_line="INFO routine", level="INFO")
    retained = ParsedEvent(line_number=2, raw_line="ERROR failure", level="ERROR")
    batch = [
        SimpleNamespace(event=discarded),
        SimpleNamespace(
            event=retained,
            end_offset=20,
            artifact_line_number=2,
            global_end_line_number=2,
        ),
    ]
    assign_fingerprints = Mock()
    persist_batch = Mock()
    monkeypatch.setattr(
        analysis_task,
        "_assign_batch_fingerprints",
        assign_fingerprints,
    )
    monkeypatch.setattr(analysis_task, "persist_evidence_batch", persist_batch)

    analysis_task._persist_artifact_batch(
        db=db,
        analysis=analysis,
        artifact=artifact,
        batch=batch,
    )

    assign_fingerprints.assert_called_once_with([retained])
    persist_batch.assert_called_once_with(
        db=db,
        analysis_id=analysis.id,
        events=[retained],
        artifact_id=artifact.id,
    )
    db.commit.assert_called_once()


def test_artifact_batch_correlates_only_retained_events(monkeypatch):
    db = Mock()
    analysis = SimpleNamespace(id=9, processed_bytes=0, last_processed_line=0)
    artifact = SimpleNamespace(id=101, processed_bytes=0, last_processed_line=0)
    discarded = ParsedEvent(line_number=1, raw_line="INFO routine", level="INFO")
    retained = ParsedEvent(line_number=2, raw_line="ERROR failure", level="ERROR")
    retained.stack_frames = [SimpleNamespace(file="app/main.py", line=9)]
    batch = [
        SimpleNamespace(event=discarded),
        SimpleNamespace(
            event=retained,
            end_offset=20,
            artifact_line_number=2,
            global_end_line_number=2,
        ),
    ]
    source_index = Mock()
    correlate = Mock(return_value=[{"relative_path": "app/main.py"}])
    monkeypatch.setattr(analysis_task, "correlate_event", correlate)
    monkeypatch.setattr(analysis_task, "persist_evidence_batch", Mock())

    analysis_task._persist_artifact_batch(
        db=db,
        analysis=analysis,
        artifact=artifact,
        batch=batch,
        source_index=source_index,
    )

    correlate.assert_called_once_with(retained, source_index)
    assert retained.source_matches == [{"relative_path": "app/main.py"}]
    assert discarded.source_matches == []


def test_source_index_is_prepared_once_and_zip_staging_is_removed(monkeypatch):
    # Section 6's process-local cache is module-level and keyed by
    # analysis.id - reset it so an id=9 entry left behind by another test
    # (this fixture id is reused throughout the suite) can never make this
    # test silently skip calling prepare_source.
    monkeypatch.setattr(analysis_task, "_source_index_process_cache", {})
    analysis = SimpleNamespace(
        id=9,
        source_kind="zip",
        source_reference="uploads/source.zip",
    )
    index = object()
    prepare = Mock(return_value=index)
    remove = Mock()
    monkeypatch.setattr(analysis_task, "prepare_source", prepare)
    monkeypatch.setattr(analysis_task, "_remove_staged_source_archive", remove)

    assert analysis_task._prepare_source_index(analysis) is index
    prepare.assert_called_once_with("zip", "uploads/source.zip", 9)
    remove.assert_called_once_with("uploads/source.zip")


def test_log_only_analysis_does_not_prepare_source(monkeypatch):
    prepare = Mock()
    monkeypatch.setattr(analysis_task, "prepare_source", prepare)

    assert (
        analysis_task._prepare_source_index(
            SimpleNamespace(id=9, source_kind=None, source_reference=None)
        )
        is None
    )
    prepare.assert_not_called()


def test_line_stream_resume_starts_after_the_committed_record():
    path = FIXTURES / "syslog.txt"
    artifact_format = detect_artifact(path, filename=path.name)
    first_record = next(
        stream_artifact_events(
            file_path=str(path),
            artifact_format=artifact_format,
            source_file=path.name,
        )
    )

    resumed = list(
        stream_artifact_events(
            file_path=str(path),
            artifact_format=artifact_format,
            source_file=path.name,
            start_offset=first_record.end_offset,
            start_artifact_line=first_record.artifact_line_number,
            global_line_number=first_record.global_end_line_number,
        )
    )

    assert len(resumed) == 1
    assert resumed[0].event.host == "worker-1"
    assert resumed[0].event.line_number == first_record.global_end_line_number + 1


def test_otlp_resume_skips_committed_structured_records():
    path = FIXTURES / "otlp.json"
    artifact_format = detect_artifact(path, filename=path.name)
    first_record = next(
        stream_artifact_events(
            file_path=str(path),
            artifact_format=artifact_format,
            source_file=path.name,
        )
    )

    resumed = list(
        stream_artifact_events(
            file_path=str(path),
            artifact_format=artifact_format,
            source_file=path.name,
            start_artifact_line=first_record.artifact_line_number,
            global_line_number=first_record.global_end_line_number,
        )
    )

    assert len(resumed) == 1
    assert resumed[0].event.span_id == "span-child"
    assert resumed[0].event.line_number == first_record.global_end_line_number + 1


def test_migrated_partial_checkpoint_resumes_with_generic_byte_offsets(tmp_path):
    first_line = b"ERROR first failure\n"
    path = tmp_path / "looks-structured.json"
    path.write_bytes(first_line + b"ERROR second failure\n")
    db = Mock()
    analysis = SimpleNamespace(
        id=9,
        processed_bytes=len(first_line),
        last_processed_line=1,
    )
    artifact = SimpleNamespace(
        id=101,
        position=0,
        original_filename=path.name,
        saved_file_path=str(path),
        content_type="application/json",
        # The migration uses zero as a one-time unknown-size sentinel for
        # unfinished legacy analyses.
        size_bytes=0,
        detected_format=ArtifactFormat.GENERIC.value,
        status="pending",
        last_processed_line=1,
        processed_bytes=len(first_line),
    )

    parsed_count = _process_artifact(db=db, analysis=analysis, artifact=artifact)

    assert parsed_count == 1
    assert artifact.detected_format == ArtifactFormat.GENERIC.value
    assert artifact.size_bytes == path.stat().st_size
    assert artifact.processed_bytes == path.stat().st_size
    # Section 5: the shared Analysis row is no longer written by
    # _process_artifact - only this artifact's own row (asserted above).
    assert analysis.last_processed_line == 1
    assert analysis.processed_bytes == len(first_line)
    assert db.execute.call_count == 1


def test_legacy_single_file_artifact_resolves_unknown_size_and_detects_format():
    path = FIXTURES / "json_in_txt.txt"
    db = Mock()
    analysis = SimpleNamespace(id=9, processed_bytes=0, last_processed_line=0)
    artifact = SimpleNamespace(
        id=101,
        position=0,
        original_filename=path.name,
        saved_file_path=str(path),
        content_type=None,
        size_bytes=0,
        detected_format=None,
        status="pending",
        last_processed_line=0,
        processed_bytes=0,
    )

    parsed_count = _process_artifact(db=db, analysis=analysis, artifact=artifact)

    assert parsed_count == 1
    assert artifact.detected_format == ArtifactFormat.JSON.value
    assert artifact.size_bytes == path.stat().st_size
    assert artifact.processed_bytes == path.stat().st_size


def test_worker_session_keeps_checkpoint_rows_loaded_across_commits(monkeypatch):
    db = Mock()
    db.query.return_value.filter.return_value.first.return_value = SimpleNamespace(
        status="completed"
    )
    session_factory = Mock(return_value=db)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)

    analysis_task.process_analysis.run(7)

    session_factory.assert_called_once_with(expire_on_commit=False)
    db.close.assert_called_once()


def _artifact(identifier: int, position: int, fixture_name: str):
    path = FIXTURES / fixture_name
    return SimpleNamespace(
        id=identifier,
        position=position,
        original_filename=fixture_name,
        saved_file_path=str(path),
        content_type=None,
        size_bytes=path.stat().st_size,
        detected_format=None,
        status="pending",
        last_processed_line=0,
        processed_bytes=0,
    )
