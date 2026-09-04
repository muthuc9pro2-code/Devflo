from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.core import processing_config
from app.db.database import Base
from app.models import Analysis, AnalysisArtifact, Evidence, User
from app.services import source_archive, source_index
from app.services.gemini_service import GeminiUnavailableError
from app.services.source_archive import (
    SourceInputError,
    _analysis_source_dir,
    _validate_cloned_source_tree,
    cleanup_prepared_source,
    prepare_source,
)
from app.services.source_index import (
    SourceIndexLimitError,
    build_index,
    load_index_manifest,
    save_index_manifest,
)
from app.tasks import analysis as analysis_task

def test_source_zip_resource_constants_are_unchanged():
    assert processing_config.MAX_SOURCE_ARCHIVE_BYTES == 200 * processing_config.MEBIBYTE
    assert processing_config.MAX_SOURCE_TOTAL_BYTES == 500 * processing_config.MEBIBYTE
    assert processing_config.MAX_SOURCE_FILES == 20_000
    assert processing_config.MAX_SOURCE_RELATIVE_PATH_BYTES == 1024
    assert processing_config.MAX_SOURCE_PATH_DEPTH == 32
    assert processing_config.MAX_SOURCE_INDEX_LOOKUP_KEYS_PER_FILE == 1
    assert (
        processing_config.MAX_SOURCE_INDEX_MANIFEST_BYTES
        == 64 * processing_config.MEBIBYTE
    )

def test_github_clone_timeout_is_60_seconds():
    assert processing_config.GITHUB_CLONE_TIMEOUT_SECONDS == 60

def _fake_git_clone(monkeypatch, *, extra_files: dict[str, str] | None = None):
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
    assert isinstance(cmd, list)
    assert cmd[:2] == ["git", "clone"]
    assert "--depth" in cmd and cmd[cmd.index("--depth") + 1] == "1"
    assert "--single-branch" in cmd
    assert "--no-tags" in cmd
    assert calls["kwargs"].get("shell", False) is False
    assert calls["kwargs"]["timeout"] == processing_config.GITHUB_CLONE_TIMEOUT_SECONDS

def test_clone_environment_disables_terminal_prompt_and_lfs_smudge(tmp_path, monkeypatch):
    calls = _fake_git_clone(monkeypatch)
    dest = tmp_path / "dest"

    source_archive._clone_github("https://github.com/acme/project", dest)

    env = calls["kwargs"]["env"]
    assert env["GIT_TERMINAL_PROMPT"] == "0"
    assert env["GIT_LFS_SKIP_SMUDGE"] == "1"

def test_git_directory_is_removed_after_a_successful_clone(tmp_path, monkeypatch):
    _fake_git_clone(monkeypatch)
    dest = tmp_path / "dest"

    source_archive._clone_github("https://github.com/acme/project", dest)

    assert not (dest / ".git").exists()
    assert (dest / "app" / "main.py").exists()

def test_prepare_source_never_indexes_git_metadata(tmp_path, monkeypatch):
    _fake_git_clone(monkeypatch)
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    index = prepare_source("github", "https://github.com/acme/project", 501, 0)

    assert set(index.by_path) == {"app/main.py"}
    assert not any(path.startswith(".git/") for path in index.by_path)

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
    root = _tree(tmp_path, count=2, size_each=6)

    with pytest.raises(SourceInputError, match="extracted size limit"):
        _validate_cloned_source_tree(root)

def test_cloned_source_rejects_excessive_path_depth_before_indexing(tmp_path, monkeypatch):
    monkeypatch.setattr(source_archive, "MAX_SOURCE_PATH_DEPTH", 3)
    root = tmp_path / "tree"
    path = root / "a" / "b" / "c" / "main.py"
    path.parent.mkdir(parents=True)
    path.write_text("print('hi')\n")

    with pytest.raises(SourceInputError, match="path depth"):
        _validate_cloned_source_tree(root)

def test_source_zip_rejects_excessive_path_depth_before_extraction(tmp_path, monkeypatch):
    import zipfile

    monkeypatch.setattr(source_archive, "MAX_SOURCE_PATH_DEPTH", 3)
    archive = tmp_path / "deep.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("a/b/c/main.py", "print('hi')\n")

    with pytest.raises(SourceInputError, match="path depth"):
        source_archive.validate_source_zip(archive)

def test_cloned_source_exactly_at_file_and_byte_limits_is_accepted(tmp_path, monkeypatch):
    monkeypatch.setattr(source_archive, "MAX_SOURCE_FILES", 4)
    monkeypatch.setattr(source_archive, "MAX_SOURCE_TOTAL_BYTES", 40)
    root = _tree(tmp_path, count=4, size_each=10)

    _validate_cloned_source_tree(root)

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

def test_normal_small_cloned_tree_passes_validation_and_indexes(tmp_path, monkeypatch):
    _fake_git_clone(monkeypatch, extra_files={"app/utils.py": "def helper(): pass\n"})
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    index = prepare_source("github", "https://github.com/acme/project", 502, 0)

    assert set(index.by_path) == {"app/main.py", "app/utils.py"}

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

def test_source_index_rejects_path_depth_above_bound(tmp_path, monkeypatch):
    monkeypatch.setattr(source_index, "MAX_SOURCE_PATH_DEPTH", 3)
    path = tmp_path / "a" / "b" / "c" / "main.py"
    path.parent.mkdir(parents=True)
    path.write_text("print('hi')\n")

    with pytest.raises(SourceIndexLimitError, match="path depth"):
        build_index(tmp_path)

def test_source_index_rejects_relative_path_above_byte_bound(tmp_path, monkeypatch):
    monkeypatch.setattr(source_index, "MAX_SOURCE_RELATIVE_PATH_BYTES", 16)
    path = tmp_path / "directory" / "very_long_name.py"
    path.parent.mkdir(parents=True)
    path.write_text("print('hi')\n")

    with pytest.raises(SourceIndexLimitError, match="path length"):
        build_index(tmp_path)

def test_source_index_builds_one_reversed_lookup_key_per_file(tmp_path):
    path = tmp_path / "a" / "b" / "c" / "main.py"
    path.parent.mkdir(parents=True)
    path.write_text("print('hi')\n")

    index = build_index(tmp_path)

    assert set(index.by_path) == {"a/b/c/main.py"}
    assert len(index._suffix_keys) == 1
    assert len(index._suffix_paths) == 1
    assert index._suffix_paths[0] == "a/b/c/main.py"

def test_bounded_lookup_still_resolves_deep_stack_frame_paths(tmp_path):
    from app.services.log_praser import ParsedEvent, StackFrame
    from app.services.source_index import correlate_event

    path = tmp_path / "a" / "b" / "c" / "d" / "main.py"
    path.parent.mkdir(parents=True)
    path.write_text("print('hi')\n")

    index = build_index(tmp_path)

    event = ParsedEvent(line_number=1, raw_line="ERROR failure")
    event.stack_frames = [
        StackFrame(file="/srv/worktree/a/b/c/d/main.py", line=1, function="run")
    ]

    matches = correlate_event(event, index)

    assert len(matches) == 1
    assert matches[0]["relative_path"] == "a/b/c/d/main.py"
    assert matches[0]["match_method"] == "suffix"

def test_bounded_lookup_disambiguates_shared_short_tails(tmp_path):
    from app.services.log_praser import ParsedEvent, StackFrame
    from app.services.source_index import correlate_event

    first = tmp_path / "x" / "one" / "a" / "b" / "c" / "d" / "main.py"
    second = tmp_path / "y" / "two" / "a" / "b" / "c" / "d" / "main.py"
    for path in (first, second):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("print('hi')\n")

    index = build_index(tmp_path)

    event = ParsedEvent(line_number=1, raw_line="ERROR failure")
    event.stack_frames = [
        StackFrame(file="/srv/one/a/b/c/d/main.py", line=1, function="run")
    ]

    matches = correlate_event(event, index)

    assert len(matches) == 1
    assert matches[0]["relative_path"] == "x/one/a/b/c/d/main.py"

def test_bounded_lookup_preserves_middle_suffix_semantics(tmp_path):
    from app.services.log_praser import ParsedEvent, StackFrame
    from app.services.source_index import correlate_event

    first = tmp_path / "x" / "a" / "b" / "c" / "d" / "E" / "f" / "g" / "h" / "i" / "j" / "k" / "l" / "main.py"
    second = tmp_path / "y" / "a2" / "b2" / "c2" / "d2" / "Q" / "f" / "g" / "h" / "i" / "j" / "k" / "l" / "main.py"
    for path in (first, second):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("print('hi')\n")

    index = build_index(tmp_path)

    event = ParsedEvent(line_number=1, raw_line="ERROR failure")
    event.stack_frames = [
        StackFrame(file="/srv/E/f/g/h/i/j/k/l/main.py", line=1, function="run")
    ]

    matches = correlate_event(event, index)

    assert len(matches) == 1
    assert matches[0]["relative_path"] == "x/a/b/c/d/E/f/g/h/i/j/k/l/main.py"

def test_bounded_lookup_preserves_old_root_vs_nested_behavior(tmp_path):
    from app.services.log_praser import ParsedEvent, StackFrame
    from app.services.source_index import correlate_event

    (tmp_path / "d.py").write_text("root\n")
    nested = tmp_path / "j" / "d.py"
    nested.parent.mkdir()
    nested.write_text("nested\n")

    index = build_index(tmp_path)

    event = ParsedEvent(line_number=1, raw_line="ERROR failure")
    event.stack_frames = [StackFrame(file="/build/d.py", line=1, function="run")]

    matches = correlate_event(event, index)

    assert matches[0]["relative_path"] == "j/d.py"
    assert matches[0]["match_method"] == "basename"

    exact_event = ParsedEvent(line_number=1, raw_line="ERROR failure")
    exact_event.stack_frames = [StackFrame(file="d.py", line=1, function="run")]

    exact_matches = correlate_event(exact_event, index)

    assert exact_matches[0]["relative_path"] == "d.py"
    assert exact_matches[0]["match_method"] == "exact"

def test_source_index_enforces_file_count_itself(tmp_path, monkeypatch):
    monkeypatch.setattr(source_index, "MAX_SOURCE_FILES", 2)
    for filename in ("a.py", "b.py", "c.py"):
        (tmp_path / filename).write_text("x\n")

    with pytest.raises(SourceIndexLimitError, match="file count"):
        build_index(tmp_path)

def test_source_index_manifest_write_is_bounded_before_publication(tmp_path, monkeypatch):
    path = tmp_path / "app.py"
    path.write_text("print('hi')\n")
    index = build_index(tmp_path)
    manifest = tmp_path / "index.json"
    monkeypatch.setattr(source_index, "MAX_SOURCE_INDEX_MANIFEST_BYTES", 8)

    with pytest.raises(SourceIndexLimitError, match="manifest"):
        save_index_manifest(index, manifest)

    assert not manifest.exists()
    assert list(tmp_path.glob("index.json.tmp-*")) == []

def test_oversized_source_index_manifest_is_not_loaded(tmp_path, monkeypatch):
    manifest = tmp_path / "index.json"
    manifest.write_text("x" * 100, encoding="utf-8")
    monkeypatch.setattr(source_index, "MAX_SOURCE_INDEX_MANIFEST_BYTES", 10)

    assert load_index_manifest(manifest, tmp_path) is None

@pytest.mark.parametrize("legacy_version", [None, 1])
def test_legacy_manifest_ignores_derived_maps_and_rebuilds_from_by_path(
    tmp_path, legacy_version
):
    import json

    payload = {
        "by_path": {
            "a/main.py": ["main.py", ".py", 1],
            "b/main.py": ["main.py", ".py", 1],
        },
        "by_suffix": {"wrong.py": ["missing.py"]},
        "by_stem": {"wrong": ["missing.py"]},
    }
    if legacy_version is not None:
        payload["version"] = legacy_version

    manifest = tmp_path / "index.json"
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    loaded = load_index_manifest(manifest, tmp_path)

    assert loaded is not None
    assert set(loaded.by_path) == {"a/main.py", "b/main.py"}
    assert len(loaded._suffix_keys) == 2
    assert loaded.by_stem == {"main": ["a/main.py", "b/main.py"]}

def test_v2_manifest_persists_only_canonical_by_path(tmp_path):
    import json

    path = tmp_path / "app" / "main.py"
    path.parent.mkdir()
    path.write_text("print('hi')\n")
    index = build_index(tmp_path)
    manifest = tmp_path / "index.json"

    save_index_manifest(index, manifest)

    payload = json.loads(manifest.read_text(encoding="utf-8"))
    assert set(payload) == {"version", "by_path"}
    assert payload["version"] == source_index._SOURCE_INDEX_MANIFEST_VERSION

    loaded = load_index_manifest(manifest, tmp_path)
    assert loaded is not None
    assert set(loaded.by_path) == {"app/main.py"}
    assert len(loaded._suffix_keys) == 1

def test_future_manifest_version_is_cache_miss(tmp_path):
    import json

    manifest = tmp_path / "index.json"
    manifest.write_text(
        json.dumps(
            {
                "version": source_index._SOURCE_INDEX_MANIFEST_VERSION + 100,
                "by_path": {"app.py": ["app.py", ".py", 1]},
            }
        ),
        encoding="utf-8",
    )

    assert load_index_manifest(manifest, tmp_path) is None

@pytest.mark.parametrize(
    "metadata",
    [
        ["wrong.py", ".py", 1],
        ["app.py", ".txt", 1],
        ["app.py", ".py", -1],
        ["app.py", ".py", True],
    ],
)
def test_manifest_rejects_inconsistent_file_metadata(tmp_path, metadata):
    import json

    manifest = tmp_path / "index.json"
    manifest.write_text(
        json.dumps(
            {
                "version": source_index._SOURCE_INDEX_MANIFEST_VERSION,
                "by_path": {"app.py": metadata},
            }
        ),
        encoding="utf-8",
    )

    assert load_index_manifest(manifest, tmp_path) is None

@pytest.mark.parametrize(
    "relative_path",
    ["../outside.py", "/absolute.py", "a/../b.py"],
)
def test_manifest_rejects_paths_that_escape_or_alias_source_root(
    tmp_path, relative_path
):
    import json

    basename = Path(relative_path).name
    manifest = tmp_path / "index.json"
    manifest.write_text(
        json.dumps(
            {
                "version": source_index._SOURCE_INDEX_MANIFEST_VERSION,
                "by_path": {
                    relative_path: [
                        basename,
                        Path(basename).suffix.lower(),
                        1,
                    ]
                },
            }
        ),
        encoding="utf-8",
    )

    assert load_index_manifest(manifest, tmp_path) is None

def test_concurrent_manifest_writers_use_distinct_temp_files(tmp_path, monkeypatch):
    import threading
    from concurrent.futures import ThreadPoolExecutor

    path = tmp_path / "app" / "main.py"
    path.parent.mkdir()
    path.write_text("print('hi')\n")
    index = build_index(tmp_path)
    manifest = tmp_path / "index.json"

    real_replace = source_index.os.replace
    barrier = threading.Barrier(2)
    replace_sources = []
    capture_lock = threading.Lock()

    def synchronized_replace(source, destination):
        if Path(destination) == manifest:
            with capture_lock:
                replace_sources.append(Path(source))
            barrier.wait(timeout=5)
        return real_replace(source, destination)

    monkeypatch.setattr(source_index.os, "replace", synchronized_replace)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(save_index_manifest, index, manifest) for _ in range(2)
        ]
        for future in futures:
            future.result(timeout=5)

    assert len(replace_sources) == 2
    assert len(set(replace_sources)) == 2
    assert all(path.suffix == ".pyc" for path in replace_sources)
    assert list(tmp_path.glob("index.json.tmp-*")) == []
    assert list(tmp_path.glob(".devflo-index-manifest-*.pyc")) == []
    assert load_index_manifest(manifest, tmp_path) is not None

def test_terminal_cleanup_cannot_be_undone_by_late_manifest_writer(tmp_path, monkeypatch):
    import threading
    from concurrent.futures import ThreadPoolExecutor

    source_root = tmp_path / "sources"
    source_root.mkdir()
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(source_root))

    analysis_id = 77
    dest = source_root / str(analysis_id)
    dest.mkdir()
    source_file = dest / "app.py"
    source_file.write_text("print('hi')\n")
    marker = source_archive._ready_marker(dest)
    marker.touch()
    manifest = source_index.index_manifest_path(dest)
    index = build_index(dest)

    writer_reached_temp_write = threading.Event()
    allow_writer_to_continue = threading.Event()
    real_write_bytes = Path.write_bytes

    def blocked_write_bytes(path, data):
        if path.name.startswith(".devflo-index-manifest-"):
            writer_reached_temp_write.set()
            assert allow_writer_to_continue.wait(timeout=5)
        return real_write_bytes(path, data)

    monkeypatch.setattr(Path, "write_bytes", blocked_write_bytes)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(save_index_manifest, index, manifest)
        assert writer_reached_temp_write.wait(timeout=5)

        cleanup_prepared_source(analysis_id)

        allow_writer_to_continue.set()
        with pytest.raises(OSError):
            future.result(timeout=5)

    assert not dest.exists()
    assert not marker.exists()
    assert not manifest.exists()
    assert list(source_root.glob(f"{analysis_id}.index.json.tmp-*")) == []
    assert list(source_root.rglob(".devflo-index-manifest-*.pyc")) == []

def test_cleanup_prepared_source_is_a_no_op_when_nothing_exists(tmp_path, monkeypatch):
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    cleanup_prepared_source(999)
    cleanup_prepared_source(999)

def test_cleanup_prepared_source_removes_everything_then_stays_a_no_op(tmp_path, monkeypatch):
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))
    dest = tmp_path / "sources" / "77"
    dest.mkdir(parents=True)
    (dest / "app.py").write_text("x")
    marker = tmp_path / "sources" / "77.ready"
    marker.touch()
    manifest = tmp_path / "sources" / "77.index.json"
    manifest.write_text("{}")
    legacy_manifest_tmp = tmp_path / "sources" / "77.index.json.tmp"
    legacy_manifest_tmp.write_text("{}")
    writer_tmp_a = tmp_path / "sources" / "77.index.json.tmp-100-a"
    writer_tmp_b = tmp_path / "sources" / "77.index.json.tmp-200-b"
    writer_tmp_a.write_text("{}")
    writer_tmp_b.write_text("{}")

    cleanup_prepared_source(77)

    assert not dest.exists()
    assert not marker.exists()
    assert not manifest.exists()
    assert not legacy_manifest_tmp.exists()
    assert not writer_tmp_a.exists()
    assert not writer_tmp_b.exists()

    cleanup_prepared_source(77)

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

    (index.root / "app" / "main.py").unlink()
    event = ParsedEvent(line_number=1, raw_line="ERROR failure", module=None)
    event.stack_frames = [StackFrame(file="app/main.py", line=1, function="run")]

    matches = correlate_event(event, index)

    assert len(matches) == 1
    assert matches[0]["relative_path"] == "app/main.py"
    assert matches[0]["snippet"] is None

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
    assert (12345, 0) in analysis_task._source_index_process_cache

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

    assert not dest.exists()

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

def test_zip_resume_after_staged_upload_removed_still_works(tmp_path, monkeypatch):
    import zipfile

    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))
    archive = tmp_path / "upload.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("app/main.py", "print('hi')\n")

    first = prepare_source("zip", str(archive), 506, 0)
    archive.unlink()

    second = prepare_source("zip", str(archive), 506, 0)

    assert set(first.by_path) == set(second.by_path) == {"app/main.py"}
