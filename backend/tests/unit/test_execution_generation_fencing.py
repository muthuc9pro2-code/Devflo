from types import SimpleNamespace
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.services.log_praser import ParsedEvent
from app.tasks import analysis as analysis_task
from app.tasks.analysis import (
    _finalize_commit_if_processing,
    _mark_analysis_failed,
    _persist_artifact_batch,
    _record_controlled_artifact_failure,
    cancel_analysis_and_cleanup,
)

def _engine_session_factory():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)

def _session():
    return _engine_session_factory()()

def _user(db, name="alice") -> User:
    user = User(username=name, email=f"{name}@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()
    return user

def _analysis(db, user, *, status="pending", **kwargs) -> Analysis:
    defaults = dict(
        user_id=user.id, original_filename="a.log", saved_file_path="/uploads/a.log",
        status=status,
    )
    defaults.update(kwargs)
    analysis = Analysis(**defaults)
    db.add(analysis)
    db.commit()
    return analysis

def _artifact(db, analysis, position=0, status="pending", **kwargs) -> AnalysisArtifact:
    defaults = dict(
        analysis_id=analysis.id, position=position, original_filename=f"f{position}.log",
        saved_file_path=f"/uploads/f{position}.log", size_bytes=100, status=status,
        last_processed_line=0, processed_bytes=0,
    )
    defaults.update(kwargs)
    artifact = AnalysisArtifact(**defaults)
    db.add(artifact)
    db.commit()
    return artifact

def _retained_batch():
    event = ParsedEvent(line_number=1, raw_line="ERROR failure", level="ERROR")
    return [
        SimpleNamespace(
            event=event, end_offset=20, artifact_line_number=1, global_end_line_number=1,
        )
    ]

def test_duplicate_process_analysis_invocations_yield_exactly_one_claim(monkeypatch):
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="pending", source_kind=None)
    _artifact(db, analysis, status="pending")
    monkeypatch.setattr(analysis_task, "sessionLocal", lambda **k: db)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    dispatch_count = {"n": 0}
    monkeypatch.setattr(
        analysis_task, "group",
        lambda sigs: dispatch_count.__setitem__("n", dispatch_count["n"] + 1) or SimpleNamespace(),
    )
    monkeypatch.setattr(
        analysis_task, "chord",
        lambda group_obj, callback: SimpleNamespace(apply_async=lambda: None),
    )
    monkeypatch.setattr(analysis_task._process_artifact_task, "si", lambda *a: SimpleNamespace())
    monkeypatch.setattr(analysis_task._finalize_analysis_task, "s", lambda *a: SimpleNamespace())

    analysis_task.process_analysis.run(analysis.id)
    analysis_task.process_analysis.run(analysis.id)

    assert dispatch_count["n"] == 1
    db.expire_all()
    assert analysis.status == "processing"
    assert analysis.processing_generation == 1

def test_workflow_publish_failure_returns_analysis_to_recoverable_pending(monkeypatch):
    session_factory = _engine_session_factory()
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="pending", source_kind=None)
    _artifact(db, analysis, status="pending")
    analysis_id = analysis.id
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    monkeypatch.setattr(
        analysis_task, "group",
        lambda sigs: (_ for _ in ()).throw(RuntimeError("broker connection refused")),
    )

    redispatched = []
    monkeypatch.setattr(
        analysis_task.process_analysis, "delay", lambda aid: redispatched.append(aid)
    )

    analysis_task.process_analysis.run(analysis_id)

    assert redispatched == [analysis_id]
    reloaded = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "pending"
    assert reloaded.finalization_generation is None
    reloaded_artifact = (
        db.query(AnalysisArtifact).filter(AnalysisArtifact.analysis_id == analysis_id).first()
    )
    assert reloaded_artifact.status == "pending"

    monkeypatch.setattr(
        analysis_task, "group",
        lambda sigs: [sig for sig in sigs],
    )
    monkeypatch.setattr(
        analysis_task, "chord",
        lambda group_obj, callback: SimpleNamespace(apply_async=lambda: None),
    )
    monkeypatch.setattr(analysis_task._process_artifact_task, "si", lambda *a: SimpleNamespace())
    monkeypatch.setattr(analysis_task._finalize_analysis_task, "s", lambda *a: SimpleNamespace())

    analysis_task.process_analysis.run(analysis_id)

    db.expire_all()
    reloaded = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "processing"
    assert reloaded.processing_generation == 2

def test_workflow_publish_failure_when_redispatch_also_fails_leaves_analysis_pending(monkeypatch):
    session_factory = _engine_session_factory()
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="pending", source_kind=None)
    _artifact(db, analysis, status="pending")
    analysis_id = analysis.id
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)
    monkeypatch.setattr(
        analysis_task, "group",
        lambda sigs: (_ for _ in ()).throw(RuntimeError("broker connection refused")),
    )
    monkeypatch.setattr(
        analysis_task.process_analysis, "delay",
        lambda aid: (_ for _ in ()).throw(RuntimeError("still down")),
    )

    analysis_task.process_analysis.run(analysis_id)

    reloaded = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "pending"
    assert reloaded.finalization_generation is None

def test_duplicate_process_artifact_task_invocations_parse_exactly_once(monkeypatch, tmp_path):
    session_factory = _engine_session_factory()
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_generation=1, source_kind=None)
    path = tmp_path / "a.log"
    path.write_text("2026-01-01T00:00:00Z ERROR service=a boom\n")
    artifact = _artifact(
        db, analysis, status="pending", saved_file_path=str(path),
        size_bytes=path.stat().st_size, original_filename="a.log",
    )
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)

    persisted = []

    def fake_persist(*, db, analysis_id, events, artifact_id=None):
        for event in events:
            persisted.append(event)
            db.add(Evidence(
                analysis_id=analysis_id,
                artifact_id=artifact_id if artifact_id is not None else event.artifact_id,
                correlation_key=f"ck-{len(persisted)}",
                fingerprint=event.fingerprint or f"fp-{len(persisted)}",
                first_line_number=event.line_number or 1,
                last_line_number=event.line_number or 1,
                severity=event.level,
            ))
        db.commit()

    monkeypatch.setattr(analysis_task, "persist_evidence_batch", fake_persist)

    first_result = analysis_task._process_artifact_task.run(analysis.id, artifact.id, 1)
    second_result = analysis_task._process_artifact_task.run(analysis.id, artifact.id, 1)

    assert first_result == 1
    assert second_result == 0
    db.expire_all()
    evidence_count = db.query(Evidence).filter(Evidence.artifact_id == artifact.id).count()
    assert evidence_count == 1

def test_old_generation_worker_cannot_persist_evidence_or_advance_checkpoint(monkeypatch):
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_generation=1)
    artifact = _artifact(db, analysis, status="processing")
    monkeypatch.setattr(analysis_task, "persist_evidence_batch", lambda **kwargs: None)

    analysis.processing_generation = 2
    db.commit()

    result = _persist_artifact_batch(
        db=db, analysis=analysis, artifact=artifact, generation=1, batch=_retained_batch(),
    )

    assert result is None
    db.expire_all()
    assert db.query(Evidence).filter(Evidence.artifact_id == artifact.id).count() == 0
    assert artifact.processed_bytes == 0
    assert artifact.last_processed_line == 0

def test_old_generation_worker_cannot_record_a_controlled_artifact_failure():
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_generation=2)
    artifact = _artifact(db, analysis, status="processing")

    _record_controlled_artifact_failure(
        db=db, analysis_id=analysis.id, artifact_id=artifact.id, generation=1,
        status="processing_error", reason="stale worker",
    )

    db.expire_all()
    assert artifact.status == "processing"
    assert artifact.failure_reason is None

def test_controlled_failure_commit_loses_to_a_cancel_that_lands_first(monkeypatch):
    session_factory = _engine_session_factory()
    worker_db = session_factory()
    alice = _user(worker_db)
    analysis = _analysis(worker_db, alice, status="processing", processing_generation=1)
    artifact = _artifact(worker_db, analysis, status="processing")
    worker_db.add(Evidence(
        analysis_id=analysis.id, artifact_id=artifact.id, correlation_key="k1",
        fingerprint="fp1", first_line_number=1, last_line_number=1,
    ))
    worker_db.commit()
    analysis_id = analysis.id
    artifact_id = artifact.id

    owned = (
        worker_db.query(Analysis.status, Analysis.processing_generation)
        .filter(Analysis.id == analysis_id)
        .first()
    )
    assert owned == ("processing", 1)

    cancel_db = session_factory()
    result = cancel_analysis_and_cleanup(cancel_db, analysis_id)
    assert result == "processing"
    cancel_db.close()

    _record_controlled_artifact_failure(
        db=worker_db, analysis_id=analysis_id, artifact_id=artifact_id, generation=1,
        status="processing_error", reason="parser exploded",
    )

    verify_db = session_factory()
    reloaded_analysis = verify_db.query(Analysis).filter(Analysis.id == analysis_id).first()
    reloaded_artifact = (
        verify_db.query(AnalysisArtifact).filter(AnalysisArtifact.id == artifact_id).first()
    )
    assert reloaded_analysis.status == "cancelled"
    assert reloaded_artifact.status == "processing"
    assert reloaded_artifact.failure_reason is None
    assert verify_db.query(Evidence).filter(Evidence.artifact_id == artifact_id).count() == 0

def test_old_generation_finalizer_cannot_complete_the_new_generations_work():
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_generation=2)
    analysis.finalization_generation = 1
    db.commit()

    won = _finalize_commit_if_processing(
        db, analysis, generation=1, result_snapshot={"investigation_path": "simple"}, ai_analysis=None, processed_bytes=0, last_processed_line=0, stage="test",
    )

    assert won is False
    db.expire_all()
    assert analysis.status == "processing"
    assert analysis.result_snapshot is None

def test_stale_finalizer_never_calls_gemini_once_a_concurrent_cancel_lands_first(monkeypatch):
    session_factory = _engine_session_factory()
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)

    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_generation=1, source_kind=None)
    artifact = _artifact(db, analysis, status="completed", last_processed_line=1, processed_bytes=10)
    db.add(Evidence(
        analysis_id=analysis.id, artifact_id=artifact.id, correlation_key="ck-1",
        fingerprint="fp-1", first_line_number=1, last_line_number=1, severity="ERROR",
    ))
    db.commit()
    analysis_id = analysis.id
    db.close()

    gemini_calls = []
    monkeypatch.setattr(
        analysis_task, "generate_investigation_explanation",
        lambda ctx: gemini_calls.append(ctx) or (_ for _ in ()).throw(
            AssertionError("a stale finalizer must never reach Gemini")
        ),
    )

    real_persist_resolved_identities = analysis_task.persist_resolved_identities

    def persist_identities_then_cancel(*args, **kwargs):
        result = real_persist_resolved_identities(*args, **kwargs)
        cancel_db = session_factory()
        cancel_analysis_and_cleanup(cancel_db, analysis_id)
        cancel_db.close()
        return result

    monkeypatch.setattr(
        analysis_task, "persist_resolved_identities", persist_identities_then_cancel
    )

    analysis_task._finalize_analysis_task.run([1], analysis_id, 1, None)

    assert gemini_calls == []
    verify_db = session_factory()
    reloaded = verify_db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "cancelled"
    assert reloaded.result_snapshot is None
    assert reloaded.ai_analysis is None
    assert verify_db.query(Evidence).filter(Evidence.analysis_id == analysis_id).count() == 0

def test_cancel_wins_race_against_finalize_completion(monkeypatch):
    session_factory = _engine_session_factory()
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_generation=1)
    analysis.finalization_generation = 1
    db.commit()
    analysis_id = analysis.id

    cancel_db = session_factory()
    cancel_analysis_and_cleanup(cancel_db, analysis_id)
    cancel_db.close()

    won = _finalize_commit_if_processing(
        db, analysis, generation=1, result_snapshot={"investigation_path": "simple"}, ai_analysis=None, processed_bytes=0, last_processed_line=0, stage="test",
    )

    assert won is False
    verify_db = session_factory()
    reloaded = verify_db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "cancelled"
    assert reloaded.result_snapshot is None
    verify_db.close()

def test_complete_wins_race_against_a_later_cancel_request():
    session_factory = _engine_session_factory()
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_generation=1)
    analysis.finalization_generation = 1
    db.commit()
    analysis_id = analysis.id

    won = _finalize_commit_if_processing(
        db, analysis, generation=1, result_snapshot={"investigation_path": "simple"}, ai_analysis=None, processed_bytes=0, last_processed_line=0, stage="test",
    )
    assert won is True
    db.close()

    cancel_db = session_factory()
    previous_status = cancel_analysis_and_cleanup(cancel_db, analysis_id)
    cancel_db.close()

    assert previous_status is None
    verify_db = session_factory()
    reloaded = verify_db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "completed"
    assert reloaded.result_snapshot == {"investigation_path": "simple"}
    verify_db.close()

def test_fail_then_cancel_terminal_winner_is_immutable():
    session_factory = _engine_session_factory()
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")
    analysis_id = analysis.id

    won = _mark_analysis_failed(db, analysis_id)
    assert won is True
    db.close()

    cancel_db = session_factory()
    previous_status = cancel_analysis_and_cleanup(cancel_db, analysis_id)
    cancel_db.close()

    assert previous_status is None
    verify_db = session_factory()
    assert verify_db.query(Analysis).filter(Analysis.id == analysis_id).first().status == "failed"
    verify_db.close()

def test_cancel_then_fail_terminal_winner_is_immutable():
    session_factory = _engine_session_factory()
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")
    analysis_id = analysis.id

    cancel_analysis_and_cleanup(db, analysis_id)
    db.close()

    fail_db = session_factory()
    won = _mark_analysis_failed(fail_db, analysis_id)
    fail_db.close()

    assert won is False
    verify_db = session_factory()
    assert verify_db.query(Analysis).filter(Analysis.id == analysis_id).first().status == "cancelled"
    verify_db.close()

def test_complete_then_fail_terminal_winner_is_immutable():
    session_factory = _engine_session_factory()
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_generation=1)
    analysis.finalization_generation = 1
    db.commit()
    analysis_id = analysis.id

    won = _finalize_commit_if_processing(
        db, analysis, generation=1, result_snapshot={"investigation_path": "simple"}, ai_analysis=None, processed_bytes=0, last_processed_line=0, stage="test",
    )
    assert won is True
    db.close()

    fail_db = session_factory()
    fail_won = _mark_analysis_failed(fail_db, analysis_id)
    fail_db.close()

    assert fail_won is False
    verify_db = session_factory()
    reloaded = verify_db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "completed"
    assert reloaded.result_snapshot == {"investigation_path": "simple"}
    verify_db.close()

def test_fail_then_complete_terminal_winner_is_immutable():
    session_factory = _engine_session_factory()
    db = session_factory()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", processing_generation=1)
    analysis.finalization_generation = 1
    db.commit()
    analysis_id = analysis.id

    fail_won = _mark_analysis_failed(db, analysis_id)
    assert fail_won is True
    db.close()

    finalize_db = session_factory()
    reloaded_for_finalize = finalize_db.query(Analysis).filter(Analysis.id == analysis_id).first()
    won = _finalize_commit_if_processing(
        finalize_db, reloaded_for_finalize, generation=1,
        result_snapshot={"investigation_path": "simple"}, ai_analysis=None, processed_bytes=0, last_processed_line=0, stage="test",
    )
    finalize_db.close()

    assert won is False
    verify_db = session_factory()
    assert verify_db.query(Analysis).filter(Analysis.id == analysis_id).first().status == "failed"
    verify_db.close()

def test_cancellation_db_failure_leaves_no_partial_state():
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing")
    artifact = _artifact(db, analysis, status="processing")
    db.add(Evidence(
        analysis_id=analysis.id, artifact_id=artifact.id, correlation_key="k1",
        fingerprint="fp1", first_line_number=1, last_line_number=1,
    ))
    db.commit()
    analysis_id = analysis.id

    real_commit = db.commit
    db.commit = lambda: (_ for _ in ()).throw(RuntimeError("db gone"))

    with pytest.raises(RuntimeError):
        cancel_analysis_and_cleanup(db, analysis_id)

    db.commit = real_commit
    db.rollback()
    reloaded = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert reloaded.status == "processing"
    assert db.query(Evidence).filter(Evidence.analysis_id == analysis_id).count() == 1

def test_cancellation_filesystem_cleanup_error_never_changes_the_cancelled_status(monkeypatch):
    db = _session()
    alice = _user(db)
    analysis = _analysis(db, alice, status="processing", source_kind="zip", source_reference="uploads/x.zip")
    analysis_id = analysis.id
    monkeypatch.setattr(
        analysis_task, "cleanup_prepared_source",
        lambda aid: (_ for _ in ()).throw(OSError("disk error")),
    )

    result = cancel_analysis_and_cleanup(db, analysis_id)

    assert result == "processing"
    db.expire_all()
    assert db.query(Analysis).filter(Analysis.id == analysis_id).first().status == "cancelled"

def _threaded_file_session_factory(tmp_path):
    database_path = tmp_path / "lifecycle-interleavings.sqlite3"
    engine = create_engine(
        f"sqlite+pysqlite:///{database_path}",
        connect_args={"check_same_thread": False, "timeout": 10},
    )
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)

def test_source_generation_lost_during_private_preparation_cannot_publish(
    tmp_path, monkeypatch,
):
    import threading
    from app.services import source_archive

    session_factory = _threaded_file_session_factory(tmp_path)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    seed_db = session_factory()
    alice = _user(seed_db)
    analysis = _analysis(
        seed_db, alice, status="processing", processing_generation=1,
        source_kind="github", source_reference="https://github.com/acme/project",
        source_status="preparing",
    )
    analysis_id = analysis.id
    seed_db.close()

    clone_started = threading.Event()
    allow_clone_finish = threading.Event()
    worker_errors = []
    worker_results = []

    def fake_clone(_url, destination):
        destination.mkdir(parents=True, exist_ok=True)
        (destination / "g1.py").write_text("print('g1')\n", encoding="utf-8")
        clone_started.set()
        if not allow_clone_finish.wait(timeout=5):
            raise AssertionError("test barrier timed out waiting to resume G1")

    monkeypatch.setattr(source_archive, "_clone_github", fake_clone)

    def run_generation_1():
        try:
            result = source_archive.prepare_source(
                "github",
                "https://github.com/acme/project",
                analysis_id,
                1,
                publish_callback=lambda publisher: (
                    analysis_task._publish_source_for_current_generation(
                        analysis_id, 1, publisher,
                    )
                ),
            )
            worker_results.append(result)
        except BaseException as error:
            worker_errors.append(error)

    worker = threading.Thread(target=run_generation_1)
    worker.start()

    assert clone_started.wait(timeout=5), (
        "G1 never reached private preparation barrier"
    )

    generation_2_db = session_factory()
    generation_2_db.query(Analysis).filter(Analysis.id == analysis_id).update(
        {"processing_generation": 2, "source_status": "preparing"},
        synchronize_session=False,
    )
    generation_2_db.commit()
    generation_2_db.close()

    allow_clone_finish.set()
    worker.join(timeout=5)

    assert not worker.is_alive(), "G1 worker did not finish"
    assert worker_errors == []
    assert worker_results == [None]

    canonical = tmp_path / "sources" / str(analysis_id)
    marker = source_archive._ready_marker(canonical)
    assert not canonical.exists()
    assert not marker.exists()

    verify_db = session_factory()
    current = verify_db.query(Analysis).filter(Analysis.id == analysis_id).one()
    assert current.status == "processing"
    assert current.processing_generation == 2
    assert current.source_status == "preparing"
    verify_db.close()

def test_g2_published_without_ready_marker_cannot_be_touched_by_stale_g1(
    tmp_path, monkeypatch,
):
    import threading
    from app.services import source_archive

    session_factory = _threaded_file_session_factory(tmp_path)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    seed_db = session_factory()
    alice = _user(seed_db)
    analysis = _analysis(
        seed_db, alice, status="processing", processing_generation=2,
        source_kind="github", source_reference="https://github.com/acme/project",
        source_status="preparing",
    )
    analysis_id = analysis.id
    seed_db.close()

    canonical = tmp_path / "sources" / str(analysis_id)
    marker = source_archive._ready_marker(canonical)

    g2_canonical_without_ready = threading.Event()
    allow_g2_finish = threading.Event()
    thread_errors = []
    g2_results = []
    g1_results = []

    def fake_clone(_url, destination):
        destination.mkdir(parents=True, exist_ok=True)
        generation_label = "g2" if ".tmp-2-" in destination.name else "g1"
        (destination / f"{generation_label}.py").write_text(
            f"print('{generation_label}')\n", encoding="utf-8",
        )

    monkeypatch.setattr(source_archive, "_clone_github", fake_clone)

    real_replace = source_archive.os.replace

    def gated_replace(source, destination):
        result = real_replace(source, destination)
        source_path = source_archive.Path(source)
        destination_path = source_archive.Path(destination)
        if destination_path == canonical and ".tmp-2-" in source_path.name:
            assert canonical.exists()
            assert (canonical / "g2.py").exists()
            assert not marker.exists()
            g2_canonical_without_ready.set()
            if not allow_g2_finish.wait(timeout=5):
                raise AssertionError("test barrier timed out waiting to resume G2")
        return result

    monkeypatch.setattr(source_archive.os, "replace", gated_replace)

    def run_generation_2():
        try:
            result = source_archive.prepare_source(
                "github",
                "https://github.com/acme/project",
                analysis_id,
                2,
                publish_callback=lambda publisher: (
                    analysis_task._publish_source_for_current_generation(
                        analysis_id, 2, publisher,
                    )
                ),
            )
            g2_results.append(result)
        except BaseException as error:
            thread_errors.append(error)

    def run_stale_generation_1():
        try:
            result = source_archive.prepare_source(
                "github",
                "https://github.com/acme/project",
                analysis_id,
                1,
                publish_callback=lambda publisher: (
                    analysis_task._publish_source_for_current_generation(
                        analysis_id, 1, publisher,
                    )
                ),
            )
            g1_results.append(result)
        except BaseException as error:
            thread_errors.append(error)

    g2_thread = threading.Thread(target=run_generation_2)
    g2_thread.start()

    assert g2_canonical_without_ready.wait(timeout=5), (
        "G2 never reached canonical-without-ready publication barrier"
    )

    g1_thread = threading.Thread(target=run_stale_generation_1)
    g1_thread.start()
    g1_thread.join(timeout=5)

    assert not g1_thread.is_alive(), "stale G1 did not finish"
    assert thread_errors == []
    assert g1_results == [None]

    assert canonical.exists()
    assert (canonical / "g2.py").exists()
    assert not (canonical / "g1.py").exists()
    assert not marker.exists()

    allow_g2_finish.set()
    g2_thread.join(timeout=5)

    assert not g2_thread.is_alive(), "G2 did not finish"
    assert thread_errors == []
    assert len(g2_results) == 1
    assert g2_results[0] is not None
    assert marker.exists()
    assert (canonical / "g2.py").exists()
    assert not (canonical / "g1.py").exists()

    verify_db = session_factory()
    current = verify_db.query(Analysis).filter(Analysis.id == analysis_id).one()
    assert current.status == "processing"
    assert current.processing_generation == 2
    assert current.source_status == "ready"
    verify_db.close()

    assert list((tmp_path / "sources").glob(f"{analysis_id}.tmp-1-*")) == []
    assert list((tmp_path / "sources").glob(f"{analysis_id}.tmp-2-*")) == []
