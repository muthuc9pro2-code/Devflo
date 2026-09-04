from app.services.artifact_detector import ArtifactFormat
from app.services.diagnostic_adapters import stream_artifact_events
from app.services.diagnostic_parser import _parse_stack_frames, normalize_text_event

def test_truncated_final_traceback_line_extracts_no_frame():
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
    assert frames[0].function == "run"

def test_generic_truncation_not_special_cased_to_run():
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
    assert event.level == "ERROR"

def test_truncated_frame_does_not_discard_the_rest_of_the_artifact(tmp_path):
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

    assert len(records) == 3

    complete = next(e for e in events if e.service == "svc-a" and e.level == "ERROR")
    assert len(complete.stack_frames) == 1
    assert complete.stack_frames[0].function == "foo"
    assert complete.exception_type == "RuntimeError"

    tail = next(e for e in events if e.service == "svc-b")
    assert tail.level == "ERROR"
    assert tail.stack_frames == []

    assert len(events) == 2
