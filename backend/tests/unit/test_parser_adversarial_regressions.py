from app.services.artifact_detector import ArtifactFormat
from app.services.diagnostic_adapters import stream_artifact_events
from app.services.diagnostic_parser import normalize_structured_event, normalize_text_event
from app.services.exception_fingerprint import build_exception_fingerprint
from app.services.log_praser import ParsedEvent

def test_generic_field_extraction_tolerates_irregular_whitespace():
    tight = normalize_text_event(
        "2026-01-01 10:00:00 ERROR service=orders-api trace_id=abc123 database timeout",
        1,
    )
    spaced = normalize_text_event(
        "2026-01-01 10:00:00   ERROR   service=orders-api   trace_id=abc123   database timeout",
        1,
    )
    tabbed = normalize_text_event(
        "2026-01-01 10:00:00\tERROR\tservice=orders-api\ttrace_id=abc123\tdatabase timeout",
        1,
    )

    for event in (tight, spaced, tabbed):
        assert event.level == "ERROR"
        assert event.service == "orders-api"
        assert event.trace_id == "abc123"

def test_generic_field_extraction_tolerates_spaced_key_value_form():
    event = normalize_text_event(
        "2026-01-01 10:00:00 ERROR service  orders-api   trace_id  abc123 timeout",
        1,
    )
    assert event.service == "orders-api"
    assert event.trace_id == "abc123"

def test_generic_field_extraction_is_order_independent():
    forward = normalize_text_event(
        "2026-01-01 10:00:00 ERROR service=orders-api trace_id=abc123 request_id=req-1 timeout",
        1,
    )
    reversed_fields = normalize_text_event(
        "2026-01-01 10:00:00 ERROR request_id=req-1 trace_id=abc123 service=orders-api timeout",
        1,
    )

    assert forward.service == reversed_fields.service == "orders-api"
    assert forward.trace_id == reversed_fields.trace_id == "abc123"
    assert forward.request_id == reversed_fields.request_id == "req-1"

def test_structured_field_order_does_not_affect_extraction():
    forward = normalize_structured_event(
        {"level": "ERROR", "trace_id": "t-1", "service": "checkout", "message": "boom"},
        1,
    )
    reordered = normalize_structured_event(
        {"message": "boom", "service": "checkout", "trace_id": "t-1", "level": "ERROR"},
        1,
    )

    assert forward.level == reordered.level == "ERROR"
    assert forward.trace_id == reordered.trace_id == "t-1"
    assert forward.service == reordered.service == "checkout"

def test_structured_event_extra_unknown_fields_are_ignored_not_fatal():
    data = {
        "level": "ERROR",
        "trace_id": "t-1",
        "message": "boom",
        "some_vendor_specific_blob": {"nested": {"deeply": [1, 2, 3]}},
        "unrecognized_flag_xyz": True,
        "another_custom_field": "irrelevant-value",
    }

    event = normalize_structured_event(data, 1)

    assert event.level == "ERROR"
    assert event.trace_id == "t-1"
    assert event.service is None
    assert event.module is None
    assert event.host is None

def test_generic_field_extraction_ignores_unrecognized_key_value_pairs():
    event = normalize_text_event(
        "2026-01-01 10:00:00 ERROR service=orders-api "
        "some_custom_field=whatever another_one=123 trace_id=abc123 timeout",
        1,
    )

    assert event.service == "orders-api"
    assert event.trace_id == "abc123"

def test_mixed_good_and_bad_records_in_one_artifact_retains_only_the_real_ones(tmp_path):
    content = (
        "2026-01-01 10:00:00 INFO service=svc-a heartbeat ok\n"
        "2026-01-01 10:00:01 ERROR service=svc-a trace_id=t-1 RuntimeError: boom\n"
        "complete garbage line !!! ### not a log at all\n"
        "\n"
        "2026-01-01 10:00:03 WARNING service=svc-a slow query detected\n"
        "!@#$%^&*()_+ {}[];':\",./<>?\n"
        "2026-01-01 10:00:05 ERROR service=svc-a trace_id=t-2 ConnectionError: refused\n"
    )
    path = tmp_path / "mixed.log"
    path.write_text(content, encoding="utf-8")

    records = list(
        stream_artifact_events(
            file_path=str(path), artifact_format=ArtifactFormat.GENERIC, source_file="mixed.log"
        )
    )
    events = [r.event for r in records if r.event is not None]

    assert len(events) == 3
    assert {e.trace_id for e in events if e.trace_id} == {"t-1", "t-2"}
    assert all(e.level in {"ERROR", "WARNING"} for e in events)

def test_duplicated_observations_produce_the_same_fingerprint():
    first = ParsedEvent(
        line_number=1, raw_line="RuntimeError: database timeout",
        exception_type="RuntimeError", exception_message="database timeout",
    )
    second = ParsedEvent(
        line_number=99, raw_line="RuntimeError: database timeout",
        exception_type="RuntimeError", exception_message="database timeout",
    )

    assert build_exception_fingerprint(first) == build_exception_fingerprint(second)

def test_duplicated_observations_with_varying_numbers_still_share_a_fingerprint():
    first = ParsedEvent(
        line_number=1, raw_line="x",
        exception_type="RuntimeError", exception_message="timeout for request 44",
    )
    second = ParsedEvent(
        line_number=2, raw_line="x",
        exception_type="RuntimeError", exception_message="timeout for request 91",
    )

    assert build_exception_fingerprint(first) == build_exception_fingerprint(second)
