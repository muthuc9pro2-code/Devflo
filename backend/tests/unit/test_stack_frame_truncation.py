"""Regression tests for the Analysis 280 "ru" vs "run" source-function
inconsistency.

Root cause (confirmed by reading the actual persisted representative_line
and the raw uploaded file's final bytes for Analysis 280 directly): the
uploaded artifact's own last physical line was genuinely truncated mid-word
at the true end of the file ("...in ru", no trailing newline, nothing
after it). Devflo's streaming line reader correctly emitted whatever bytes
were actually present at end-of-stream (that is its documented, unchanged,
correct behavior for a legitimately unterminated final line) - the defect
was downstream: PYTHON_FRAME_PATTERN's "in (.+)$" function-name capture
had no way to tell a genuinely short function name apart from one merely
cut off by the record ending early, so it captured "ru" as if it were
complete and persisted it into source_matches.

Fix: _parse_stack_frames() now only accepts a Python-style "File ..., line
N, in FUNC" frame when at least one more character of real content follows
it in the record - every well-formed Python traceback (Python's own
formatter, always) follows that line with the actual source snippet line,
so "nothing at all follows" only happens when the record was cut short
before that snippet ever arrived. This is generic (works for any function
name, not just "run") and scoped to the Python two-line-per-frame
convention only - Java/Node's one-line "at foo (file:N)" frames are
naturally often the last line of a well-formed block and are unaffected.
"""
from app.services.artifact_detector import ArtifactFormat
from app.services.diagnostic_adapters import stream_artifact_events
from app.services.diagnostic_parser import _parse_stack_frames, normalize_text_event


def test_truncated_final_traceback_line_extracts_no_frame():
    """The exact Analysis 280 shape: a Traceback header immediately
    followed by a File line that never got its source-snippet line
    because the record ended right there."""
    raw_text = (
        "2026-08-14 15:30:40 ERROR service=payment-api "
        "Traceback (most recent call last):\n"
        '  File "/srv/worker.py", line 42, in ru'
    )

    assert _parse_stack_frames(raw_text) == []


def test_complete_traceback_line_extracts_the_full_function_name():
    raw_text = (
        "Traceback (most recent call last):\n"
        '  File "/srv/worker.py", line 42, in run\n'
        '    raise RuntimeError("database timeout")\n'
        "RuntimeError: database timeout"
    )

    frames = _parse_stack_frames(raw_text)

    assert len(frames) == 1
    assert frames[0].file == "/srv/worker.py"
    assert frames[0].line == 42
    assert frames[0].function == "run"  # not "ru" - the full name, byte-for-byte


def test_generic_truncation_not_special_cased_to_run():
    """Proves the fix is generic: an entirely different function name,
    truncated the exact same way, is rejected exactly the same way -
    nothing here is keyed on "run" or this specific fixture."""
    raw_text = (
        "Traceback (most recent call last):\n"
        '  File "/srv/checkout.py", line 88, in handleSubm'
    )

    assert _parse_stack_frames(raw_text) == []

    complete = raw_text + (
        "it\n    raise ValueError(\"bad cart\")\nValueError: bad cart"
    )
    frames = _parse_stack_frames(complete)
    assert len(frames) == 1
    assert frames[0].function == "handleSubmit"


def test_multi_frame_traceback_only_drops_the_truncated_trailing_frame():
    """An earlier, complete frame in the same record must still be
    extracted even when a LATER frame in the same record is truncated."""
    raw_text = (
        "Traceback (most recent call last):\n"
        '  File "/srv/a.py", line 1, in foo\n'
        "    bar()\n"
        '  File "/srv/b.py", line 2, in b'
    )

    frames = _parse_stack_frames(raw_text)

    assert len(frames) == 1
    assert frames[0].file == "/srv/a.py"
    assert frames[0].function == "foo"


def test_node_java_single_line_frames_are_unaffected_by_the_fix():
    """Java/Node's "at func (file:line)" convention is naturally one line
    per frame - being the record's last line is normal there, not a sign
    of truncation, and must not be rejected."""
    raw_text = (
        "TypeError: cart is undefined\n"
        "    at checkout (/srv/app.js:10:3)\n"
        "    at handler (/srv/router.js:22:7)"
    )

    frames = _parse_stack_frames(raw_text)

    assert [f.function for f in frames] == ["checkout", "handler"]
    assert [f.line for f in frames] == [10, 22]


def test_end_to_end_normalize_text_event_never_persists_a_truncated_function_name():
    truncated = (
        "2026-08-14 15:30:40 ERROR service=payment-api "
        "Traceback (most recent call last):\n"
        '  File "/srv/worker.py", line 42, in ru'
    )

    event = normalize_text_event(
        truncated, 1, source_file="03_stack_trace.log", source_format="stack_trace"
    )

    assert event.stack_frames == []
    # The event itself is still retained (real ERROR-level record) -
    # only the unreliable stack-frame/source-match candidate is dropped.
    assert event.level == "ERROR"


def test_truncated_frame_does_not_discard_the_rest_of_the_artifact(tmp_path):
    """Confirms the fix is a targeted per-record decision, not an
    artifact-level one: a valid, complete record BEFORE the truncated tail
    is still parsed and retained normally, and the artifact is not marked
    unsupported/zero-evidence merely because its last record is incomplete.
    Exercises the real stream_artifact_events() entry point end to end, not
    just _parse_stack_frames() in isolation.

    The file's last line here is never terminated (no trailing newline) and
    ends mid-token ("incomple") - the same generic, format-agnostic
    truncated-tail signature as real cut-off uploads - so it is now isolated
    entirely (see _tail_record_is_truncated in diagnostic_adapters.py)
    rather than surviving as a misleading bare ERROR record with no stack
    frames. Everything before it is untouched.
    """
    content = (
        "2026-01-01 10:00:00 ERROR service=svc-a "
        "Traceback (most recent call last):\n"
        '  File "/srv/a.py", line 1, in foo\n'
        "    bar()\n"
        "RuntimeError: first failure\n"
        "2026-01-01 10:00:05 INFO service=svc-a startup checkpoint reached\n"
        "2026-01-01 10:00:10 ERROR service=svc-b "
        "Traceback (most recent call last):\n"
        '  File "/srv/b.py", line 2, in incomple'
    )
    path = tmp_path / "mixed.log"
    path.write_text(content, encoding="utf-8")

    records = list(
        stream_artifact_events(
            file_path=str(path),
            artifact_format=ArtifactFormat.STACK_TRACE,
            source_file="mixed.log",
        )
    )
    events = [record.event for record in records if record.event is not None]

    # The complete traceback and the plain INFO line (dropped as unimportant
    # by existing retention rules - unrelated to this fix) are streamed as
    # records; the truncated tail is isolated entirely rather than streamed
    # as a misleading bare ERROR record.
    assert len(records) == 2

    complete = next(e for e in events if e.service == "svc-a" and e.level == "ERROR")
    assert len(complete.stack_frames) == 1
    assert complete.stack_frames[0].function == "foo"
    assert complete.exception_type == "RuntimeError"

    # The truncated svc-b tail never became evidence at all - not even a
    # bare ERROR with empty stack_frames - since it is indistinguishable
    # from a genuinely incomplete cut-off write.
    assert not any(e.service == "svc-b" for e in events)

    # Artifact-level outcome is unaffected: not marked unsupported/
    # zero-evidence merely because its last record was incomplete - the
    # earlier, complete traceback is still real, retained evidence.
    assert len(events) == 1
