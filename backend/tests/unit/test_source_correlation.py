import zipfile

import pytest

from app.services import source_archive
from app.services.log_praser import ParsedEvent, StackFrame
from app.services.source_archive import (
    SourceInputError,
    prepare_source,
    validate_source_zip,
)
from app.services.source_index import build_index, correlate_event


def _repo(tmp_path, files: dict[str, str]):
    for relative_path, content in files.items():
        path = tmp_path / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    return build_index(tmp_path)


def _event(file: str, line: int, module: str | None = None) -> ParsedEvent:
    event = ParsedEvent(line_number=1, raw_line="ERROR failure", module=module)
    event.stack_frames = [StackFrame(file=file, line=line, function="run")]
    return event


def test_exact_relative_path_match(tmp_path):
    index = _repo(tmp_path, {"app/main.py": "\n".join(f"line{i}" for i in range(1, 30))})

    matches = correlate_event(_event("app/main.py", 10), index)

    assert len(matches) == 1
    assert matches[0]["match_method"] == "exact"
    assert matches[0]["relative_path"] == "app/main.py"
    assert "line10" in matches[0]["snippet"]


def test_suffix_path_match(tmp_path):
    index = _repo(tmp_path, {"backend/app/main.py": "\n".join(f"line{i}" for i in range(1, 10))})

    matches = correlate_event(_event("/ci/workspace/app/main.py", 3), index)

    assert len(matches) == 1
    assert matches[0]["match_method"] == "suffix"
    assert matches[0]["relative_path"] == "backend/app/main.py"


def test_unique_basename_fallback(tmp_path):
    index = _repo(tmp_path, {"lib/utils/helper.py": "\n".join(f"line{i}" for i in range(1, 5))})

    matches = correlate_event(_event("totally/different/path/helper.py", 2), index)

    assert len(matches) == 1
    assert matches[0]["match_method"] == "basename"
    assert matches[0]["relative_path"] == "lib/utils/helper.py"


def test_ambiguous_basename_does_not_fabricate_a_match(tmp_path):
    index = _repo(
        tmp_path,
        {"service_a/handler.py": "a", "service_b/handler.py": "b"},
    )

    assert correlate_event(_event("somewhere/handler.py", 1), index) == []


def test_missing_source_returns_no_match():
    assert correlate_event(_event("app/main.py", 1), None) == []


@pytest.mark.parametrize("member_name", ["../evil.txt", "/etc/passwd", "a/../../evil.txt"])
def test_zip_traversal_is_rejected(tmp_path, member_name):
    archive = tmp_path / "source.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr(member_name, "payload")

    with pytest.raises(SourceInputError):
        validate_source_zip(archive)


def test_extract_zip_rejects_escape_via_preexisting_symlink(tmp_path):
    """_safe_members() rejects '..'/absolute paths and symlink *entries* in
    the zip itself, but _extract_zip() has its own realpath-based check for
    a subtler case: an already-safe-looking relative path (no '..', no
    leading '/') that walks through a symlink already sitting inside the
    destination directory and escapes outside it. This is the exact
    boundary the os.path.realpath() optimization in _extract_zip touches,
    so it needs its own direct coverage rather than relying on the
    unrelated validate_source_zip() tests above.
    """
    outside = tmp_path / "outside"
    outside.mkdir()
    dest = tmp_path / "dest"
    dest.mkdir()
    (dest / "escape_link").symlink_to(outside, target_is_directory=True)

    archive = tmp_path / "source.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("escape_link/evil.txt", "payload")

    with pytest.raises(SourceInputError, match="Unsafe path"):
        source_archive._extract_zip(archive, dest)
    assert not (outside / "evil.txt").exists()


def test_prepare_source_skips_reclone_on_resume_for_github(tmp_path, monkeypatch):
    """process_analysis can be re-invoked (e.g. after a Celery retry) for an
    analysis that's still 'processing'. A naive prepare_source would call
    git clone again on the second invocation, and git clone always refuses
    a non-empty destination directory - so without idempotency, resuming a
    GitHub-sourced analysis would fail every time. Simulate that: a clone
    stub that raises if invoked more than once.
    """
    clone_calls = []

    def fake_clone(url, dest):
        if clone_calls:
            raise AssertionError("git clone should not be re-invoked on resume")
        clone_calls.append(url)
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "app").mkdir()
        (dest / "app" / "main.py").write_text("print('hi')\n")

    monkeypatch.setattr(source_archive, "_clone_github", fake_clone)
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    first = prepare_source("github", "https://github.com/acme/project", 42)
    second = prepare_source("github", "https://github.com/acme/project", 42)

    assert len(clone_calls) == 1
    assert set(first.by_path) == set(second.by_path) == {"app/main.py"}


def test_prepare_source_skips_reextract_on_resume_for_zip_after_upload_deleted(tmp_path, monkeypatch):
    """The staged upload ZIP is deleted after prepare_source's first
    successful use (see _remove_staged_source_archive in app.tasks.analysis).
    A resumed process_analysis run must not need that file to still exist.
    """
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    archive = tmp_path / "upload.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("app/main.py", "print('hi')\n")

    first = prepare_source("zip", str(archive), 7)
    archive.unlink()  # simulates _remove_staged_source_archive already having run

    second = prepare_source("zip", str(archive), 7)

    assert set(first.by_path) == set(second.by_path) == {"app/main.py"}


def test_prepare_source_discards_partial_dest_from_a_crashed_prior_attempt(tmp_path, monkeypatch):
    """If a prior process_analysis run crashed mid-clone/mid-extract, dest
    can be left populated but incomplete, with no ready marker written (the
    marker is only written after a full success). That must be treated as
    not-prepared and redone from scratch, not silently reused as if it were
    the real, complete source tree.
    """
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    dest = tmp_path / "sources" / "99"
    dest.mkdir(parents=True)
    (dest / "partial_garbage.py").write_text("this should not survive")

    def fake_clone(url, dest):
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "real.py").write_text("print(1)\n")

    monkeypatch.setattr(source_archive, "_clone_github", fake_clone)

    index = prepare_source("github", "https://github.com/acme/project", 99)

    assert set(index.by_path) == {"real.py"}


def test_source_zip_and_github_source_share_one_index_path(tmp_path, monkeypatch):
    archive = tmp_path / "source.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("app/main.py", "print('hi')\n")

    def fake_clone(url, dest):
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "app").mkdir()
        (dest / "app" / "main.py").write_text("print('hi')\n")

    monkeypatch.setattr(source_archive, "_clone_github", fake_clone)
    monkeypatch.setattr(
        source_archive,
        "SOURCE_STORAGE_ROOT",
        str(tmp_path / "sources"),
    )

    zip_index = prepare_source("zip", str(archive), 1)
    github_index = prepare_source("github", "https://github.com/acme/project", 2)

    assert set(zip_index.by_path) == set(github_index.by_path) == {"app/main.py"}


# --- Crash safety: the .ready marker is published LAST ---------------------


def test_ready_marker_is_not_written_if_index_build_crashes(tmp_path, monkeypatch):
    """A reader must be able to trust that observing the .ready marker
    means the WHOLE prepared state (tree + index + manifest) is loadable -
    so the marker can only ever be published after build_index() and
    save_index_manifest() both succeed. If index building crashes, no
    marker may exist yet, even though the tree itself was fully cloned."""
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    def fake_clone(url, dest):
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "app.py").write_text("print(1)\n")

    monkeypatch.setattr(source_archive, "_clone_github", fake_clone)
    monkeypatch.setattr(
        source_archive, "build_index",
        lambda dest: (_ for _ in ()).throw(RuntimeError("index build crashed")),
    )

    with pytest.raises(RuntimeError, match="index build crashed"):
        prepare_source("github", "https://github.com/acme/project", 55)

    dest = tmp_path / "sources" / "55"
    marker = source_archive._ready_marker(dest)
    assert not marker.exists()
    # The crash's own cleanup (prepare_source's except-block) also removes
    # the partial tree itself, matching the existing "partial dest must
    # never look prepared" contract proven above.
    assert not dest.exists()


def test_ready_marker_is_only_written_after_the_manifest_is_saved(tmp_path, monkeypatch):
    """Same crash window, one step later: the tree is cloned AND the index
    is built in memory, but persisting the manifest itself fails. The
    marker still must not exist - a reader must never see "ready" while the
    on-disk manifest a fast resume depends on is missing."""
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    def fake_clone(url, dest):
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "app.py").write_text("print(1)\n")

    monkeypatch.setattr(source_archive, "_clone_github", fake_clone)
    monkeypatch.setattr(
        source_archive, "save_index_manifest",
        lambda index, manifest_path: (_ for _ in ()).throw(OSError("disk full")),
    )

    with pytest.raises(OSError, match="disk full"):
        prepare_source("github", "https://github.com/acme/project", 56)

    dest = tmp_path / "sources" / "56"
    marker = source_archive._ready_marker(dest)
    assert not marker.exists()


def test_prepare_source_adopts_an_already_ready_source_without_recloning(tmp_path, monkeypatch):
    """The other side of the crash window: the marker (now only ever
    written last) IS present, meaning the tree, index, and manifest were
    all genuinely completed by a prior attempt - even if that prior
    process then crashed before the caller got to persist
    Analysis.source_status="ready" in the database. A resumed
    prepare_source() call must adopt that already-complete state as-is,
    never re-clone or re-extract."""
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    clone_calls = []

    def fake_clone(url, dest):
        clone_calls.append(url)
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "app.py").write_text("print(1)\n")

    monkeypatch.setattr(source_archive, "_clone_github", fake_clone)

    first = prepare_source("github", "https://github.com/acme/project", 57)
    assert len(clone_calls) == 1

    # Simulate the exact crash window: everything prepare_source() itself
    # does (tree, index, manifest, marker) already fully happened - only
    # the CALLER's later "source_status = ready" DB commit never ran.
    second = prepare_source("github", "https://github.com/acme/project", 57)

    assert len(clone_calls) == 1  # never re-cloned
    assert set(first.by_path) == set(second.by_path) == {"app.py"}
