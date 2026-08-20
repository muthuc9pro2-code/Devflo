"""Sections 22-23: language-agnostic path:line[:column] source-location
fallback, on top of the existing explicit Python/JVM/Node frame parsers.

Explicit parsers still take precedence (never touched here); the generic
fallback only ever engages when none of them found anything, and never
claims a real source match on its own - source_index.py's resolution step
still has to find the path unambiguously in the real indexed source tree.
"""
from app.services.diagnostic_parser import _parse_stack_frames, normalize_text_event
from app.services.log_praser import ParsedEvent, StackFrame
from app.services.source_index import build_index, correlate_event


# --- extraction: representative shapes per language ------------------------


def test_go_panic_extracts_generic_frame():
    raw = "panic: runtime error: invalid memory address\n\t/home/dev/app/main.go:42 +0x1d"
    frames = _parse_stack_frames(raw)
    assert any(f.file == "/home/dev/app/main.go" and f.line == 42 for f in frames)
    assert all(f.function is None for f in frames if f.file.endswith(".go"))


def test_rust_panic_extracts_generic_frame():
    raw = "thread 'main' panicked at src/main.rs:10:5:\nindex out of bounds"
    frames = _parse_stack_frames(raw)
    assert any(f.file == "src/main.rs" and f.line == 10 for f in frames)


def test_ruby_backtrace_extracts_generic_frame():
    raw = "app/models/user.rb:23:in `save': undefined method 'foo'"
    frames = _parse_stack_frames(raw)
    assert any(f.file == "app/models/user.rb" and f.line == 23 for f in frames)


def test_dotnet_stack_trace_extracts_generic_frame():
    raw = "System.NullReferenceException: Object reference not set\n   at Program.Main() in /src/Program.cs:line 42"
    frames = _parse_stack_frames(raw)
    assert any(f.file == "/src/Program.cs" and f.line == 42 for f in frames)


def test_c_cpp_assertion_extracts_generic_frame():
    raw = "worker.c:88: main: Assertion `ptr != NULL' failed."
    frames = _parse_stack_frames(raw)
    assert any(f.file == "worker.c" and f.line == 88 for f in frames)


def test_cpp_compiler_style_extracts_generic_frame():
    raw = "src/engine.cpp:120:9: error: use of undeclared identifier 'foo'"
    frames = _parse_stack_frames(raw)
    assert any(f.file == "src/engine.cpp" and f.line == 120 for f in frames)


def test_explicit_python_parser_still_takes_precedence():
    """When a Python-shaped frame is present, the generic fallback must not
    also fire and duplicate/conflict with it."""
    raw = 'Traceback (most recent call last):\n  File "app/worker.py", line 42, in run\n    raise RuntimeError("boom")\nRuntimeError: boom'
    frames = _parse_stack_frames(raw)
    assert len(frames) == 1
    assert frames[0].file == "app/worker.py"
    assert frames[0].function == "run"


def test_generic_pattern_does_not_fire_on_ordinary_prose():
    assert _parse_stack_frames("everything is running smoothly today") == []


def test_single_line_generic_frame_is_detected_by_normalize_text_event():
    """The single-line _STACK feature gate must recognize a bare
    path:line shape even with no newline and no 'at '/'File ' prefix -
    otherwise _parse_stack_frames would never even be invoked for it."""
    event = normalize_text_event("ERROR panic at /srv/app/main.go:42 nil pointer", 1)
    assert any(f.file == "/srv/app/main.go" and f.line == 42 for f in event.stack_frames)


# --- resolution: only a real, unambiguous source-index match counts -------


def _repo(tmp_path, files):
    for relative_path, content in files.items():
        path = tmp_path / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    return build_index(tmp_path)


def _event_with_generic_frame(file: str, line: int) -> ParsedEvent:
    event = ParsedEvent(line_number=1, raw_line="panic: nil pointer")
    event.stack_frames = [StackFrame(file=file, line=line, function=None)]
    return event


def test_generic_go_frame_resolves_through_the_real_source_index(tmp_path):
    index = _repo(tmp_path, {"cmd/server/main.go": "\n".join(f"line{i}" for i in range(1, 50))})

    matches = correlate_event(_event_with_generic_frame("cmd/server/main.go", 42), index)

    assert len(matches) == 1
    assert matches[0]["relative_path"] == "cmd/server/main.go"
    assert matches[0]["function"] is None


def test_generic_frame_with_no_matching_source_file_produces_no_match(tmp_path):
    index = _repo(tmp_path, {"cmd/server/main.go": "line1\n"})

    matches = correlate_event(_event_with_generic_frame("totally/different/unknown.go", 1), index)

    assert matches == []


def test_generic_frame_never_fabricates_a_match_without_a_source_index():
    assert correlate_event(_event_with_generic_frame("main.go", 1), None) == []
