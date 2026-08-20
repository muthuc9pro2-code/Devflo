"""Section 8: one canonical is_evidence_worthy() predicate, reused as the
actual final persistence gate (app/tasks/analysis.py._persist_artifact_batch)
instead of an inlined, independently-drifting event.level check.

Root bug this guards against: a producer's own severity label is not
trustworthy on its own. {"level": "INFO", "exception": {"type":
"ConnectionError", ...}} is real diagnostic evidence, but the previous
final gate checked ONLY event.level, and structured_event_may_be_important()
(the pre-parse gate) short-circuited on any explicit level field before ever
checking for a real exception/status - so this exact shape was discarded
before it even reached the full parser.
"""
from app.services.diagnostic_parser import (
    normalize_structured_event,
    structured_event_may_be_important,
)
from app.services.event_filter import is_evidence_worthy
from app.services.log_praser import ParsedEvent, StackFrame


def _event(**overrides) -> ParsedEvent:
    fields = dict(line_number=1, raw_line="x")
    fields.update(overrides)
    return ParsedEvent(**fields)


# --- is_evidence_worthy(): the canonical final-gate predicate -------------


def test_important_severity_is_evidence_worthy():
    for level in ("WARNING", "WARN", "ERROR", "CRITICAL"):
        assert is_evidence_worthy(_event(level=level))


def test_real_exception_type_is_evidence_worthy_even_at_info_level():
    """Scenario K: explicit INFO with a real exception must be retained."""
    assert is_evidence_worthy(_event(level="INFO", exception_type="ConnectionError"))


def test_real_stack_frames_are_evidence_worthy_even_without_a_level():
    assert is_evidence_worthy(
        _event(level=None, stack_frames=[StackFrame(file="a.py", line=1, function="f")])
    )


def test_http_5xx_is_evidence_worthy_even_at_info_level():
    assert is_evidence_worthy(_event(level="INFO", http_status=503))


def test_http_4xx_is_evidence_worthy():
    assert is_evidence_worthy(_event(level=None, http_status=404))


def test_http_2xx_3xx_alone_is_not_evidence_worthy():
    assert not is_evidence_worthy(_event(level="INFO", http_status=200))
    assert not is_evidence_worthy(_event(level="INFO", http_status=301))


def test_opentelemetry_identity_is_evidence_worthy_even_without_a_level():
    assert is_evidence_worthy(
        _event(level=None, source_format="opentelemetry", trace_id="t-1")
    )
    assert is_evidence_worthy(
        _event(level=None, source_format="opentelemetry", span_id="s-1")
    )


def test_non_opentelemetry_trace_id_alone_is_not_evidence_worthy():
    """The OTel-identity special case is deliberately format-scoped -
    trace_id alone on a non-OTel record is not itself a retention signal."""
    assert not is_evidence_worthy(
        _event(level="INFO", source_format="generic", trace_id="t-1")
    )


def test_ordinary_info_heartbeat_is_not_evidence_worthy():
    """Scenario L: an ordinary INFO heartbeat with no other real signal
    must not be retained."""
    assert not is_evidence_worthy(
        _event(level="INFO", raw_line="health check succeeded")
    )
    assert not is_evidence_worthy(_event(level="DEBUG", raw_line="cache lookup started"))
    assert not is_evidence_worthy(_event(level="TRACE", raw_line="entering function"))


def test_no_level_no_signal_is_not_evidence_worthy():
    assert not is_evidence_worthy(_event(level=None))


# --- structured_event_may_be_important(): the pre-parse gate --------------


def test_structured_gate_retains_info_level_with_real_exception():
    data = {"level": "INFO", "exception": {"type": "ConnectionError", "message": "database unavailable"}}
    assert structured_event_may_be_important(data) is True

    event = normalize_structured_event(data, 1)
    assert is_evidence_worthy(event)
    assert event.exception_type == "ConnectionError"


def test_structured_gate_retains_info_level_with_5xx_status():
    data = {"level": "INFO", "http_status": 503, "message": "upstream unavailable"}
    assert structured_event_may_be_important(data) is True

    event = normalize_structured_event(data, 1)
    assert is_evidence_worthy(event)


def test_structured_gate_still_rejects_plain_info_with_no_real_signal():
    data = {"level": "INFO", "message": "health check succeeded"}
    assert structured_event_may_be_important(data) is False


def test_structured_gate_still_rejects_ordinary_200_status_with_no_level():
    data = {"http_status": 200, "message": "request handled"}
    assert structured_event_may_be_important(data) is False
