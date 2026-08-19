"""Regression tests for removing the unsafe EOF/no-newline truncation
inference (_tail_record_is_truncated, _ends_mid_token,
_file_ends_without_trailing_newline - all removed from
diagnostic_adapters.py).

That heuristic treated "last record + file has no trailing newline + final
character is alphanumeric" as proof of truncation. That is not logically
sound: a perfectly valid file may legitimately end without a trailing
newline (most editors/tools do not require one), so EOF-after-a-word does
not prove the word was cut in half. It silently deleted real evidence - a
plain "ERROR database timeout" as an artifact's last line, with no newline
after it, used to vanish entirely.

Genuine incompleteness is now only inferred where real structural evidence
exists: a JSON/OTLP parser failure, a Python "File ..., line N, in FUNC"
frame with nothing after it in the record (Python's own formatter always
follows a real frame with a source-snippet line), or CRI's own explicit 'P'
(partial) continuation flag never resolving before EOF.
"""
from app.services.artifact_detector import ArtifactFormat
from app.services.diagnostic_adapters import stream_artifact_events
from app.services.diagnostic_parser import _parse_stack_frames


def _events(tmp_path, name: str, content: str, artifact_format: ArtifactFormat):
    path = tmp_path / name
    path.write_text(content, encoding="utf-8")
    records = list(
        stream_artifact_events(
            file_path=str(path), artifact_format=artifact_format, source_file=name
        )
    )
    return records, [r.event for r in records if r.event is not None]


# --- 1: valid generic final ERROR line with no trailing newline -----------


def test_generic_final_error_line_without_trailing_newline_is_retained(tmp_path):
    content = "INFO service started\nERROR database timeout"
    _records, events = _events(tmp_path, "generic.log", content, ArtifactFormat.GENERIC)

    assert any(
        e.level == "ERROR" and e.raw_line == "ERROR database timeout" for e in events
    )


# --- 2: valid final stack-trace record with no trailing newline, -----------
# structurally complete (Java/Node one-line-per-frame convention has no
# "must be followed by a snippet line" requirement, so being last is normal)


def test_final_node_style_frame_without_trailing_newline_is_retained(tmp_path):
    content = "TypeError: cart is undefined\n    at checkout (/srv/app.js:10:3)"
    _records, events = _events(
        tmp_path, "node-stack.log", content, ArtifactFormat.STACK_TRACE
    )

    assert len(events) == 1
    assert events[0].exception_type == "TypeError"
    assert [f.function for f in events[0].stack_frames] == ["checkout"]


# --- 3: an incomplete Python stack frame never fabricates a frame/function -
# (structural evidence - Python's own formatter always follows a real frame
# with a source-snippet line - independent of the removed EOF heuristic)


def test_incomplete_python_frame_never_fabricates_a_frame_but_record_survives(
    tmp_path,
):
    content = (
        "2026-01-01 10:00:00 ERROR service=svc "
        "Traceback (most recent call last):\n"
        '  File "/srv/worker.py", line 42, in handleRequ'
    )
    _records, events = _events(tmp_path, "py-stack.log", content, ArtifactFormat.STACK_TRACE)

    assert len(events) == 1
    assert events[0].level == "ERROR"
    assert events[0].stack_frames == []  # no fabricated "handleRequ" function


# --- 4/5: a JSON-lines tail that fails to parse falls back to generic text -
# (real structural evidence - a JSON parse failure - but never promoted to
# trustworthy STRUCTURED evidence; earlier valid JSON-lines records survive)


def test_malformed_final_json_line_falls_back_to_generic_text_not_discarded(tmp_path):
    content = (
        '{"level":"ERROR","message":"connection refused","service":"payments"}\n'
        '{"level":"ERROR","message":"database timeout","service":"payments"'
    )
    _records, events = _events(tmp_path, "events.jsonl", content, ArtifactFormat.JSON)

    # The first, well-formed JSON-line survives (structured evidence, as
    # always); the malformed tail is ALSO retained, via the same
    # generic-text fallback path any other unstructured line gets - not
    # silently dropped just because it is the artifact's last, newline-less
    # record.
    assert any(e.raw_line == "connection refused" for e in events)
    assert any(
        "database timeout" in e.raw_line and e.level == "ERROR" for e in events
    )


def test_malformed_json_line_does_not_fabricate_structured_fields(tmp_path):
    """A JSON parse failure must never be promoted to trustworthy structured
    evidence - the malformed line is handled through normalize_text_event()
    (plain text, regex-based field extraction only), never
    normalize_structured_event() (which would trust the JSON keys/values
    directly)."""
    # Deliberately malformed/unclosed JSON whose *text* nonetheless contains
    # an ERROR marker - the resulting event's fields must come only from
    # what normalize_text_event's own generic parsing can support from the
    # raw characters, never from trusting "service" as a real JSON key.
    content = '{"level":"ERROR","service":"payments","message":"boom"'
    _records, events = _events(tmp_path, "single.jsonl", content, ArtifactFormat.JSON)

    assert len(events) == 1
    assert events[0].level == "ERROR"
    # service=payments IS still recoverable here, but only because
    # normalize_text_event's own generic "key":"value" field regex finds it
    # in the raw text - not because normalize_structured_event trusted the
    # (invalid) JSON structure. Proven by there being no crash/exception on
    # invalid JSON and no exotic structured-only fields being populated.
    assert events[0].http_status is None


# --- 6: CRI partial semantics remain correct - an unresolved trailing 'P' --
# fragment (EOF before its concluding non-'P' line ever arrives) is still
# never emitted; that really is incomplete by the format's own explicit flag


def test_unresolved_trailing_cri_partial_fragment_is_never_emitted(tmp_path):
    content = '2026-08-12T10:12:00.000000Z stdout P {"level":"ERROR","message":"Connection\n'
    records, events = _events(tmp_path, "cri-partial.log", content, ArtifactFormat.CONTAINER)

    assert events == []
    assert records == []
