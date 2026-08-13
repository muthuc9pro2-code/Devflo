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
