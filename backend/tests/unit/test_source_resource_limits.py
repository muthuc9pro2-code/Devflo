"""Source-code input limits & cleanup (Item 9):

- GitHub clone behavior (timeout, flags, env, .git removal) and the new
  post-clone _validate_cloned_source_tree() resource/symlink contract,
  which applies the same MAX_SOURCE_FILES / MAX_SOURCE_TOTAL_BYTES bound
  ZIP source already gets per-entry (via _safe_members) to a cloned
  working tree instead.
- cleanup_prepared_source(): idempotent, root-scoped deletion of every
  on-disk artifact prepare_source() may have produced, used both by
  prepare_source()'s own failure path (never leave a partial prepared
  tree behind) and by _finalize_analysis_task's best-effort post-
  completion cleanup (the physical tree is no longer needed once every
  artifact task has persisted its source_matches into Evidence).

Uses only temporary directories and monkeypatched subprocess.run/
constants - never hits real GitHub, never allocates a genuinely large
(500 MiB) fixture.
"""
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core import processing_config
from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.services import source_archive
from app.services.gemini_service import GeminiUnavailableError
from app.services.source_archive import (
    SourceInputError,
    _analysis_source_dir,
    _validate_cloned_source_tree,
    cleanup_prepared_source,
    prepare_source,
)
from app.tasks import analysis as analysis_task


# --- 1: ZIP/total/file-count constants are unchanged ----------------------


def test_source_zip_resource_constants_are_unchanged():
    assert processing_config.MAX_SOURCE_ARCHIVE_BYTES == 200 * processing_config.MEBIBYTE
    assert processing_config.MAX_SOURCE_TOTAL_BYTES == 500 * processing_config.MEBIBYTE
    assert processing_config.MAX_SOURCE_FILES == 20_000


# --- 2/3/4: clone timeout, flags, and environment --------------------------


def test_github_clone_timeout_is_60_seconds():
    assert processing_config.GITHUB_CLONE_TIMEOUT_SECONDS == 60


def _fake_git_clone(monkeypatch, *, extra_files: dict[str, str] | None = None):
    """Replaces subprocess.run with a fake that records the exact command/
    kwargs `_clone_github` invoked it with, and materializes a small
    working tree (including a `.git` directory, as a real `git clone`
    would) at the destination `git clone` was told to use."""
    calls: dict = {}

    def fake_run(cmd, **kwargs):
        calls["cmd"] = cmd
        calls["kwargs"] = kwargs
        dest = Path(cmd[-1])
        dest.mkdir(parents=True, exist_ok=True)
        git_dir = dest / ".git"
        git_dir.mkdir(exist_ok=True)
        (git_dir / "HEAD").write_text("ref: refs/heads/main\n")
        (dest / "app").mkdir(exist_ok=True)
        (dest / "app" / "main.py").write_text("print('hi')\n")
        for relative, content in (extra_files or {}).items():
            path = dest / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
        return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    monkeypatch.setattr(source_archive.subprocess, "run", fake_run)
    return calls


def test_clone_command_still_uses_depth_single_branch_no_tags(tmp_path, monkeypatch):
    calls = _fake_git_clone(monkeypatch)
    dest = tmp_path / "dest"

    source_archive._clone_github("https://github.com/acme/project", dest)

    cmd = calls["cmd"]
    assert isinstance(cmd, list)  # argv list, never a shell string
    assert cmd[:2] == ["git", "clone"]
    assert "--depth" in cmd and cmd[cmd.index("--depth") + 1] == "1"
    assert "--single-branch" in cmd
    assert "--no-tags" in cmd
    # subprocess.run defaults to shell=False unless explicitly overridden -
    # this codebase never passes shell=True for this call, so repository
    # content is never handed to a shell for interpretation.
    assert calls["kwargs"].get("shell", False) is False
    assert calls["kwargs"]["timeout"] == processing_config.GITHUB_CLONE_TIMEOUT_SECONDS


def test_clone_environment_disables_terminal_prompt_and_lfs_smudge(tmp_path, monkeypatch):
    calls = _fake_git_clone(monkeypatch)
    dest = tmp_path / "dest"

    source_archive._clone_github("https://github.com/acme/project", dest)

    env = calls["kwargs"]["env"]
    assert env["GIT_TERMINAL_PROMPT"] == "0"
    assert env["GIT_LFS_SKIP_SMUDGE"] == "1"


# --- 5: .git is removed before source indexing -----------------------------


def test_git_directory_is_removed_after_a_successful_clone(tmp_path, monkeypatch):
    _fake_git_clone(monkeypatch)
    dest = tmp_path / "dest"

    source_archive._clone_github("https://github.com/acme/project", dest)

    assert not (dest / ".git").exists()
    assert (dest / "app" / "main.py").exists()  # the real tree survives


def test_prepare_source_never_indexes_git_metadata(tmp_path, monkeypatch):
    """End to end through prepare_source(): the .git directory removed by
    _clone_github must never show up in the resulting SourceIndex."""
    _fake_git_clone(monkeypatch)
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    index = prepare_source("github", "https://github.com/acme/project", 501, 0)

    assert set(index.by_path) == {"app/main.py"}
    assert not any(path.startswith(".git/") for path in index.by_path)


# --- 6/7/8: _validate_cloned_source_tree resource bounds --------------------


def _tree(tmp_path, count: int, size_each: int) -> Path:
    root = tmp_path / "tree"
    root.mkdir()
    for i in range(count):
        (root / f"file{i}.py").write_bytes(b"x" * size_each)
    return root


def test_cloned_source_over_max_files_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(source_archive, "MAX_SOURCE_FILES", 3)
    root = _tree(tmp_path, count=4, size_each=1)

    with pytest.raises(SourceInputError, match="too many files"):
        _validate_cloned_source_tree(root)


def test_cloned_source_over_max_total_bytes_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(source_archive, "MAX_SOURCE_TOTAL_BYTES", 10)
    root = _tree(tmp_path, count=2, size_each=6)  # 12 bytes total > 10

    with pytest.raises(SourceInputError, match="extracted size limit"):
        _validate_cloned_source_tree(root)


def test_cloned_source_exactly_at_file_and_byte_limits_is_accepted(tmp_path, monkeypatch):
    monkeypatch.setattr(source_archive, "MAX_SOURCE_FILES", 4)
    monkeypatch.setattr(source_archive, "MAX_SOURCE_TOTAL_BYTES", 40)
    root = _tree(tmp_path, count=4, size_each=10)  # exactly 4 files, exactly 40 bytes

    _validate_cloned_source_tree(root)  # must not raise


# --- 9/10: symlinks are rejected -------------------------------------------


def test_cloned_file_symlink_is_rejected(tmp_path):
    root = tmp_path / "tree"
    root.mkdir()
    real = root / "real.py"
    real.write_text("print(1)\n")
    (root / "link.py").symlink_to(real)

    with pytest.raises(SourceInputError, match="Symlink"):
        _validate_cloned_source_tree(root)


def test_cloned_directory_symlink_is_rejected(tmp_path):
    root = tmp_path / "tree"
    root.mkdir()
    real_dir = tmp_path / "outside"
    real_dir.mkdir()
    (real_dir / "evil.py").write_text("print(1)\n")
    (root / "linked_dir").symlink_to(real_dir, target_is_directory=True)

    with pytest.raises(SourceInputError, match="Symlink"):
        _validate_cloned_source_tree(root)


# --- 11: a normal small tree passes validation and indexes -----------------


def test_normal_small_cloned_tree_passes_validation_and_indexes(tmp_path, monkeypatch):
    _fake_git_clone(monkeypatch, extra_files={"app/utils.py": "def helper(): pass\n"})
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    index = prepare_source("github", "https://github.com/acme/project", 502, 0)

    assert set(index.by_path) == {"app/main.py", "app/utils.py"}


# --- 12: failed preparation removes every prepared artifact -----------------


def test_failed_source_preparation_removes_prepared_dir_marker_and_manifest(tmp_path, monkeypatch):
    _fake_git_clone(monkeypatch)
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    def failing_build_index(root):
        raise RuntimeError("simulated index-build failure")

    monkeypatch.setattr(source_archive, "build_index", failing_build_index)

    with pytest.raises(RuntimeError, match="simulated index-build failure"):
        prepare_source("github", "https://github.com/acme/project", 503, 0)

    dest = tmp_path / "sources" / "503"
    marker = tmp_path / "sources" / "503.ready"
    manifest = tmp_path / "sources" / "503.index.json"
    manifest_tmp = tmp_path / "sources" / "503.index.json.tmp"

    assert not dest.exists()
    assert not marker.exists()
    assert not manifest.exists()
    assert not manifest_tmp.exists()


def test_failed_zip_preparation_does_not_delete_the_staged_zip(tmp_path, monkeypatch):
    """The staged uploaded ZIP is useful for a legitimate retry/resume -
    only the PREPARED (extracted) tree and its markers are cleaned up on
    failure, never the caller's original staged upload."""
    import zipfile

    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))
    archive = tmp_path / "upload.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("app/main.py", "print('hi')\n")

    def failing_build_index(root):
        raise RuntimeError("simulated index-build failure")

    monkeypatch.setattr(source_archive, "build_index", failing_build_index)

    with pytest.raises(RuntimeError):
        prepare_source("zip", str(archive), 504, 0)

    assert archive.exists()


# --- 13: cleanup_prepared_source() is idempotent ----------------------------


def test_cleanup_prepared_source_is_a_no_op_when_nothing_exists(tmp_path, monkeypatch):
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    cleanup_prepared_source(999)  # must not raise
    cleanup_prepared_source(999)  # calling twice must also not raise


def test_cleanup_prepared_source_removes_everything_then_stays_a_no_op(tmp_path, monkeypatch):
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))
    dest = tmp_path / "sources" / "77"
    dest.mkdir(parents=True)
    (dest / "app.py").write_text("x")
    marker = tmp_path / "sources" / "77.ready"
    marker.touch()
    manifest = tmp_path / "sources" / "77.index.json"
    manifest.write_text("{}")
    manifest_tmp = tmp_path / "sources" / "77.index.json.tmp"
    manifest_tmp.write_text("{}")

    cleanup_prepared_source(77)

    assert not dest.exists()
    assert not marker.exists()
    assert not manifest.exists()
    assert not manifest_tmp.exists()

    cleanup_prepared_source(77)  # idempotent: calling again on an already-clean state


# --- 14: cleanup cannot escape SOURCE_STORAGE_ROOT --------------------------


def test_analysis_source_dir_refuses_to_escape_storage_root(tmp_path, monkeypatch):
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    with pytest.raises(ValueError):
        _analysis_source_dir("../escape")


def test_cleanup_prepared_source_refuses_to_escape_storage_root(tmp_path, monkeypatch):
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))
    outside = tmp_path / "escape"
    outside.mkdir()
    (outside / "do_not_delete.txt").write_text("precious")

    with pytest.raises(ValueError):
        cleanup_prepared_source("../escape")

    assert (outside / "do_not_delete.txt").exists()


# --- 15: successful source preparation still allows correlation ------------


def test_prepared_source_still_correlates_stack_frames(tmp_path, monkeypatch):
    from app.services.log_praser import ParsedEvent, StackFrame
    from app.services.source_index import correlate_event

    _fake_git_clone(monkeypatch)
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    index = prepare_source("github", "https://github.com/acme/project", 505, 0)

    event = ParsedEvent(line_number=1, raw_line="ERROR failure", module=None)
    event.stack_frames = [StackFrame(file="app/main.py", line=1, function="run")]

    matches = correlate_event(event, index)
    assert len(matches) == 1
    assert matches[0]["relative_path"] == "app/main.py"


def test_source_context_read_failure_keeps_match_without_snippet(tmp_path, monkeypatch):
    from app.services.log_praser import ParsedEvent, StackFrame
    from app.services.source_index import correlate_event

    _fake_git_clone(monkeypatch)
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))
    index = prepare_source("github", "https://github.com/acme/project", 506, 0)

    # The file was indexed successfully but became unreadable/unavailable
    # before optional snippet enrichment.  The diagnostic match itself and
    # investigation must survive without context text.
    (index.root / "app" / "main.py").unlink()
    event = ParsedEvent(line_number=1, raw_line="ERROR failure", module=None)
    event.stack_frames = [StackFrame(file="app/main.py", line=1, function="run")]

    matches = correlate_event(event, index)

    assert len(matches) == 1
    assert matches[0]["relative_path"] == "app/main.py"
    assert matches[0]["snippet"] is None


# --- 16/17/18: finalize-time cleanup ----------------------------------------


def _db_with_schema(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    monkeypatch.setattr(analysis_task, "sessionLocal", session_factory)
    return session_factory


def _seed_source_analysis(session_factory, *, source_kind, source_reference, evidence_kwargs):
    db = session_factory()
    user = User(username="t", email="t@example.com", hashed_password="x", is_verified=True)
    db.add(user)
    db.commit()

    analysis = Analysis(
        user_id=user.id,
        original_filename="a",
        saved_file_path="a",
        status="processing",
        source_kind=source_kind,
        source_reference=source_reference,
    )
    db.add(analysis)
    db.commit()

    artifact = AnalysisArtifact(
        analysis_id=analysis.id,
        position=0,
        original_filename="artifact-0",
        saved_file_path="artifact-0",
        size_bytes=10,
        status="completed",
        last_processed_line=1,
        processed_bytes=10,
    )
    db.add(artifact)
    db.commit()

    base = datetime.now(timezone.utc)
    for i, kwargs in enumerate(evidence_kwargs):
        defaults = dict(
            analysis_id=analysis.id,
            artifact_id=artifact.id,
            correlation_key=f"ck-{i}",
            fingerprint=f"fp-{i}",
            first_line_number=1,
            last_line_number=1,
            first_seen=base,
            severity="ERROR",
        )
        defaults.update(kwargs)
        db.add(Evidence(**defaults))
    db.commit()

    analysis_id = analysis.id
    db.close()
    return analysis_id


def _stage_prepared_source(tmp_path, analysis_id: int):
    dest = tmp_path / "sources" / str(analysis_id)
    dest.mkdir(parents=True)
    (dest / "app").mkdir()
    (dest / "app" / "main.py").write_text("print('hi')\n")
    marker = tmp_path / "sources" / f"{analysis_id}.ready"
    marker.touch()
    manifest = tmp_path / "sources" / f"{analysis_id}.index.json"
    manifest.write_text("{}")
    return dest, marker, manifest


def _raise_gemini_unavailable(context):
    raise GeminiUnavailableError("unavailable")


def test_finalize_removes_prepared_source_after_all_artifacts_complete(tmp_path, monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))
    analysis_id = _seed_source_analysis(
        session_factory,
        source_kind="zip",
        source_reference=str(tmp_path / "src.zip"),
        evidence_kwargs=[{"service": "worker"}],
    )
    dest, marker, manifest = _stage_prepared_source(tmp_path, analysis_id)

    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _raise_gemini_unavailable)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert not dest.exists()
    assert not marker.exists()
    assert not manifest.exists()

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    db.close()


def test_finalize_drops_process_local_source_index_cache_entry(tmp_path, monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))
    analysis_id = _seed_source_analysis(
        session_factory,
        source_kind="zip",
        source_reference=str(tmp_path / "src.zip"),
        evidence_kwargs=[{"service": "worker"}],
    )
    _stage_prepared_source(tmp_path, analysis_id)
    monkeypatch.setattr(
        analysis_task,
        "_source_index_process_cache",
        {(analysis_id, 0): object(), (12345, 0): object()},
    )

    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _raise_gemini_unavailable)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert (analysis_id, 0) not in analysis_task._source_index_process_cache
    assert (12345, 0) in analysis_task._source_index_process_cache  # unrelated entries untouched


def test_persisted_source_matches_remain_usable_after_physical_source_cleanup(tmp_path, monkeypatch):
    session_factory = _db_with_schema(monkeypatch)
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))
    source_matches = [
        {
            "relative_path": "app/main.py",
            "requested_path": "app/main.py",
            "line_number": 5,
            "function": "run",
            "snippet": "print('hi')",
            "match_method": "exact",
            "confidence": "high",
        }
    ]
    analysis_id = _seed_source_analysis(
        session_factory,
        source_kind="zip",
        source_reference=str(tmp_path / "src.zip"),
        evidence_kwargs=[{"service": "worker", "source_matches": source_matches}],
    )
    dest, _marker, _manifest = _stage_prepared_source(tmp_path, analysis_id)

    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _raise_gemini_unavailable)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert not dest.exists()  # physical source is gone

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    evidence_payload = analysis.result_snapshot["evidence"][0]
    assert evidence_payload["source_matches"] == source_matches
    db.close()


def test_cleanup_oserror_does_not_fail_an_otherwise_valid_analysis(tmp_path, monkeypatch, caplog):
    session_factory = _db_with_schema(monkeypatch)
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))
    analysis_id = _seed_source_analysis(
        session_factory,
        source_kind="zip",
        source_reference=str(tmp_path / "src.zip"),
        evidence_kwargs=[{"service": "worker"}],
    )
    _stage_prepared_source(tmp_path, analysis_id)

    def failing_cleanup(aid):
        raise OSError("permission denied")

    monkeypatch.setattr(analysis_task, "cleanup_prepared_source", failing_cleanup)
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _raise_gemini_unavailable)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    with caplog.at_level("WARNING", logger="app.tasks.analysis"):
        analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    db = session_factory()
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    assert analysis.status == "completed"
    assert analysis.result_snapshot is not None
    db.close()

    assert any(str(analysis_id) in record.getMessage() for record in caplog.records)


def test_cleanup_not_attempted_for_analyses_without_source_input(tmp_path, monkeypatch):
    """No source_kind at all - cleanup_prepared_source must not even be
    called, since there is nothing to clean up and no SOURCE_STORAGE_ROOT
    entry was ever created for this analysis."""
    session_factory = _db_with_schema(monkeypatch)
    analysis_id = _seed_source_analysis(
        session_factory,
        source_kind=None,
        source_reference=None,
        evidence_kwargs=[{"service": "worker"}],
    )

    calls = []
    monkeypatch.setattr(analysis_task, "cleanup_prepared_source", lambda aid: calls.append(aid))
    monkeypatch.setattr(analysis_task, "generate_investigation_explanation", _raise_gemini_unavailable)
    monkeypatch.setattr(analysis_task, "publish_investigation_result", lambda *a, **k: None)
    monkeypatch.setattr(analysis_task, "publish_progress", lambda *a, **k: None)

    analysis_task._finalize_analysis_task.run([], analysis_id, 0, None)

    assert calls == []


# --- 19: existing ZIP resume/idempotence behavior is unaffected ------------


def test_zip_resume_after_staged_upload_removed_still_works(tmp_path, monkeypatch):
    """Same scenario as test_source_correlation.py's existing resume test -
    reasserted here to prove the new try/except cleanup-on-failure wrapper
    in prepare_source() did not disturb the successful path's existing
    idempotent-resume contract."""
    import zipfile

    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))
    archive = tmp_path / "upload.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("app/main.py", "print('hi')\n")

    first = prepare_source("zip", str(archive), 506, 0)
    archive.unlink()  # simulates _remove_staged_source_archive already having run

    second = prepare_source("zip", str(archive), 506, 0)

    assert set(first.by_path) == set(second.by_path) == {"app/main.py"}
