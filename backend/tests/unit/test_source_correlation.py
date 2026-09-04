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

    first = prepare_source("github", "https://github.com/acme/project", 42, 0)
    second = prepare_source("github", "https://github.com/acme/project", 42, 0)

    assert len(clone_calls) == 1
    assert set(first.by_path) == set(second.by_path) == {"app/main.py"}

def test_prepare_source_skips_reextract_on_resume_for_zip_after_upload_deleted(tmp_path, monkeypatch):
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    archive = tmp_path / "upload.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("app/main.py", "print('hi')\n")

    first = prepare_source("zip", str(archive), 7, 0)
    archive.unlink()

    second = prepare_source("zip", str(archive), 7, 0)

    assert set(first.by_path) == set(second.by_path) == {"app/main.py"}

def test_prepare_source_discards_partial_dest_from_a_crashed_prior_attempt(tmp_path, monkeypatch):
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    dest = tmp_path / "sources" / "99"
    dest.mkdir(parents=True)
    (dest / "partial_garbage.py").write_text("this should not survive")

    def fake_clone(url, dest):
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "real.py").write_text("print(1)\n")

    monkeypatch.setattr(source_archive, "_clone_github", fake_clone)

    index = prepare_source("github", "https://github.com/acme/project", 99, 0)

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

    zip_index = prepare_source("zip", str(archive), 1, 0)
    github_index = prepare_source("github", "https://github.com/acme/project", 2, 0)

    assert set(zip_index.by_path) == set(github_index.by_path) == {"app/main.py"}

def test_ready_marker_is_not_written_if_index_build_crashes(tmp_path, monkeypatch):
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
        prepare_source("github", "https://github.com/acme/project", 55, 0)

    dest = tmp_path / "sources" / "55"
    marker = source_archive._ready_marker(dest)
    assert not marker.exists()
    assert not dest.exists()

def test_ready_marker_is_only_written_after_the_manifest_is_saved(tmp_path, monkeypatch):
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
        prepare_source("github", "https://github.com/acme/project", 56, 0)

    dest = tmp_path / "sources" / "56"
    marker = source_archive._ready_marker(dest)
    assert not marker.exists()

def test_prepare_source_adopts_an_already_ready_source_without_recloning(tmp_path, monkeypatch):
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    clone_calls = []

    def fake_clone(url, dest):
        clone_calls.append(url)
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "app.py").write_text("print(1)\n")

    monkeypatch.setattr(source_archive, "_clone_github", fake_clone)

    first = prepare_source("github", "https://github.com/acme/project", 57, 0)
    assert len(clone_calls) == 1

    second = prepare_source("github", "https://github.com/acme/project", 57, 0)

    assert len(clone_calls) == 1
    assert set(first.by_path) == set(second.by_path) == {"app.py"}

def test_old_generation_cannot_delete_or_replace_a_newer_generations_published_source(
    tmp_path, monkeypatch,
):
    monkeypatch.setattr(source_archive, "SOURCE_STORAGE_ROOT", str(tmp_path / "sources"))

    generation_2_started = []

    def fake_clone(url, dest):
        if "gen1" in dest.name and not generation_2_started:
            generation_2_started.append(True)
            def fake_clone_gen2(url2, dest2):
                dest2.mkdir(parents=True, exist_ok=True)
                (dest2 / "gen2_real.py").write_text("print('gen2')\n")
            monkeypatch.setattr(source_archive, "_clone_github", fake_clone_gen2)
            prepare_source("github", "https://github.com/acme/project", 99, 2)
            monkeypatch.setattr(source_archive, "_clone_github", fake_clone)

        dest.mkdir(parents=True, exist_ok=True)
        (dest / "gen1_stale.py").write_text("print('gen1')\n")

    monkeypatch.setattr(source_archive, "_clone_github", fake_clone)

    gen1_index = prepare_source("github", "https://github.com/acme/project", 99, "gen1")

    dest = tmp_path / "sources" / "99"
    marker = source_archive._ready_marker(dest)

    assert set(gen1_index.by_path) == {"gen2_real.py"}
    assert marker.exists()
    assert (dest / "gen2_real.py").exists()
    assert not (dest / "gen1_stale.py").exists()
    stray_temps = list((tmp_path / "sources").glob("99.tmp-*"))
    assert stray_temps == []

def test_stale_generation_denied_publication_cannot_touch_unready_canonical_tree(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        source_archive,
        "SOURCE_STORAGE_ROOT",
        str(tmp_path / "sources"),
    )

    root = tmp_path / "sources"
    root.mkdir(parents=True)

    dest = root / "99"
    dest.mkdir()

    (dest / "gen2_real.py").write_text(
        "print('gen2')\n"
    )

    def fake_clone(_url, private_dest):
        private_dest.mkdir(
            parents=True,
            exist_ok=True,
        )
        (private_dest / "gen1_stale.py").write_text(
            "print('gen1')\n"
        )

    monkeypatch.setattr(
        source_archive,
        "_clone_github",
        fake_clone,
    )

    result = prepare_source(
        "github",
        "https://github.com/acme/project",
        99,
        1,
        publish_callback=lambda _publisher: None,
    )

    assert result is None
    assert (dest / "gen2_real.py").exists()
    assert not (dest / "gen1_stale.py").exists()
    assert list(root.glob("99.tmp-1-*")) == []
