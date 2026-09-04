from app.services.artifact_detector import ArtifactFormat
from app.services.diagnostic_adapters import stream_artifact_events

def _events(tmp_path, name: str, content: str, artifact_format: ArtifactFormat):
    path = tmp_path / name
    path.write_text(content, encoding="utf-8")
    records = list(
        stream_artifact_events(
            file_path=str(path), artifact_format=artifact_format, source_file=name
        )
    )
    return records, [r.event for r in records if r.event is not None]

def test_generic_final_error_line_without_trailing_newline_is_retained(tmp_path):
    content = "INFO service started\nERROR database timeout"
    _records, events = _events(tmp_path, "generic.log", content, ArtifactFormat.GENERIC)

    assert any(
        e.level == "ERROR" and e.raw_line == "ERROR database timeout" for e in events
    )

def test_final_node_style_frame_without_trailing_newline_is_retained(tmp_path):
    content = "TypeError: cart is undefined\n    at checkout (/srv/app.js:10:3)"
    _records, events = _events(
        tmp_path, "node-stack.log", content, ArtifactFormat.STACK_TRACE
    )

    assert len(events) == 1
    assert events[0].exception_type == "TypeError"
    assert [f.function for f in events[0].stack_frames] == ["checkout"]

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
    assert events[0].stack_frames == []

def test_malformed_final_json_line_falls_back_to_generic_text_not_discarded(tmp_path):
    content = (
        '{"level":"ERROR","message":"connection refused","service":"payments"}\n'
        '{"level":"ERROR","message":"database timeout","service":"payments"'
    )
    _records, events = _events(tmp_path, "events.jsonl", content, ArtifactFormat.JSON)

    assert any(e.raw_line == "connection refused" for e in events)
    assert any(
        "database timeout" in e.raw_line and e.level == "ERROR" for e in events
    )

def test_malformed_json_line_does_not_fabricate_structured_fields(tmp_path):
    content = '{"level":"ERROR","service":"payments","message":"boom"'
    _records, events = _events(tmp_path, "single.jsonl", content, ArtifactFormat.JSON)

    assert len(events) == 1
    assert events[0].level == "ERROR"
    assert events[0].http_status is None

def test_unresolved_trailing_cri_partial_fragment_is_never_emitted(tmp_path):
    content = '2026-08-12T10:12:00.000000Z stdout P {"level":"ERROR","message":"Connection\n'
    records, events = _events(tmp_path, "cri-partial.log", content, ArtifactFormat.CONTAINER)

    assert events == []
    assert records == []
