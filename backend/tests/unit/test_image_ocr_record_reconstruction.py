"""OCR -> Evidence reconstruction correctness.

Root cause this guards against: the whole image used to become ONE raw_text
blob handed to normalize_text_event(). That function's level detection picks
the *first* level keyword found anywhere in whatever text it is given, so a
benign early line (e.g. an "INFO:" startup message) silently shadowed a
genuine ERROR-level traceback appearing later in the very same image -
producing evidence_count=0 for a real Python ImportError screenshot even
though OCR itself read the text correctly.

The fix reconstructs OCR text into the same per-record units
(_multiline_kind/_is_multiline_continuation) ordinary STACK_TRACE-format
text artifacts already use, so each distinct line/traceback is normalized
and gated for importance independently - not a second/parallel evidence
model, the same one every other format already goes through.
"""
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from app.services import diagnostic_adapters, image_text_extractor
from app.services.artifact_detector import ArtifactFormat
from app.services.diagnostic_adapters import stream_artifact_events
from app.tasks import analysis as analysis_task

FIXTURES = Path(__file__).parents[1] / "fixtures" / "diagnostics"


def _retained_events(records):
    return [r.event for r in records if r.event is not None]


def _stream_ocr_text(monkeypatch, tmp_path, ocr_text: str, *, confidence=0.9):
    monkeypatch.setattr(
        diagnostic_adapters,
        "extract_text_from_image_with_confidence",
        lambda path: (ocr_text, confidence),
    )
    # normalize_ocr_text is exercised for real elsewhere (ocr_normalizer
    # tests); identity here keeps these cases about record reconstruction,
    # not normalization details.
    monkeypatch.setattr(diagnostic_adapters, "normalize_ocr_text", lambda text: text)
    image_path = tmp_path / "shot.png"
    image_path.write_bytes(b"\x89PNG\r\n\x1a\n")
    return list(
        stream_artifact_events(
            file_path=str(image_path),
            artifact_format=ArtifactFormat.IMAGE,
            source_file="shot.png",
        )
    )


# --- 1: a real traceback/error no longer becomes zero evidence ------------


def test_ocr_traceback_with_leading_benign_line_produces_retained_evidence(monkeypatch, tmp_path):
    ocr_text = (
        "INFO:     Uvicorn running on http://127.0.0.1:8000\n"
        "Traceback (most recent call last):\n"
        "ImportError: cannot import name 'send_password_reset_email' from 'app.services.email_service'\n"
    )
    records = _stream_ocr_text(monkeypatch, tmp_path, ocr_text)
    events = _retained_events(records)

    assert len(events) > 0
    assert any(event.level == "ERROR" for event in events)


# --- 2: generic - a different exception type works without any hardcoding -


def test_ocr_different_exception_type_is_extracted_generically(monkeypatch, tmp_path):
    ocr_text = (
        "INFO:     server starting\n"
        "Traceback (most recent call last):\n"
        "ZeroDivisionError: division by zero\n"
    )
    records = _stream_ocr_text(monkeypatch, tmp_path, ocr_text)
    events = _retained_events(records)

    exception_event = next(event for event in events if event.exception_type is not None)
    # Extracted purely from the generic EXCEPTION_PATTERN
    # (`<Name>(Error|Exception|Failure)`), same as every other format - a
    # type Devflo has never seen before still comes through correctly.
    assert exception_event.exception_type == "ZeroDivisionError"
    assert exception_event.exception_message == "division by zero"


# --- 3: a plain warning/error line with no traceback still becomes evidence


def test_ocr_single_line_warning_without_traceback_becomes_evidence(monkeypatch, tmp_path):
    ocr_text = "WARNING: connection pool exhausted, retrying\n"
    records = _stream_ocr_text(monkeypatch, tmp_path, ocr_text)
    events = _retained_events(records)

    assert len(events) == 1
    assert events[0].level == "WARNING"


# --- real-world regression: bare frame lines with no ERROR/Traceback ------
# keyword anywhere in the captured text (a cropped terminal screenshot can
# lose the header/exception line while keeping the frame chain below it),
# and OCR's typical dropped whitespace around "File"/commas.


def test_ocr_bare_python_frames_with_no_error_keyword_still_become_evidence(monkeypatch, tmp_path):
    """Reproduces the real screenshot that motivated this fix: a VS Code
    terminal panel showing only the frame chain (no literal "Traceback"/
    "Error" line survived OCR - it scrolled out of the captured region),
    with OCR's characteristic missing spaces around quotes/commas."""
    ocr_text = (
        'File "<frozen importlib._bootstrap>",line 935,in _load_unlocked\n'
        'File"<frozen importlib._bootstrap_external>",line 995,in exec_module\n'
        'File"/home/dev/app/main.py",line 7,in <module>\n'
        "from app.api import auth\n"
        'File"/home/dev/app/api/auth.py",line 13,in<module>\n'
        "from app.services.email_service import send_password_reset_email\n"
    )
    records = _stream_ocr_text(monkeypatch, tmp_path, ocr_text)
    events = _retained_events(records)

    # The whole frame chain must reconstruct into ONE coherent record, not
    # five unrelated singleton "incidents" (one per bare "File ..." line) -
    # see test_image_ocr_record_reconstruction module docstring's sibling
    # concern, and the real-world regression this guards: a single
    # ImportError screenshot previously became 5 separate uncorrelated
    # components in the correlation graph.
    assert len(events) == 1
    assert events[0].level == "ERROR"
    assert "auth.py" in events[0].raw_line and "13" in events[0].raw_line
    assert "_load_unlocked" in events[0].raw_line
    assert "main.py" in events[0].raw_line


def test_ocr_frame_chain_with_exception_line_extracts_frames_and_exception_together(
    monkeypatch, tmp_path
):
    """Well-formed spacing variant of the bare-frame-chain case above: when
    OCR also captures the trailing exception line, the reconstructed single
    record must let the ordinary parser extract both the real stack frames
    and the real exception type/message - nothing fabricated, everything
    structurally present in the OCR text itself."""
    ocr_text = (
        'File "/home/dev/app/main.py", line 7, in <module>\n'
        'File "/home/dev/app/api/auth.py", line 13, in <module>\n'
        "ImportError: cannot import name 'send_password_reset_email'\n"
    )
    records = _stream_ocr_text(monkeypatch, tmp_path, ocr_text)
    events = _retained_events(records)

    assert len(events) == 1
    event = events[0]
    assert event.level == "ERROR"
    assert event.exception_type == "ImportError"
    assert event.exception_message == "cannot import name 'send_password_reset_email'"
    assert [frame.file for frame in event.stack_frames] == [
        "/home/dev/app/main.py",
        "/home/dev/app/api/auth.py",
    ]
    assert [frame.line for frame in event.stack_frames] == [7, 13]


def test_ocr_frame_chain_record_is_bounded_by_max_diagnostic_record_bytes(
    monkeypatch, tmp_path
):
    """The bare-frame-chain reconstruction must not grow a single record
    without bound - mirrors the same MAX_DIAGNOSTIC_RECORD_BYTES cap
    ordinary multi-line text records (_stream_text_events) already enforce,
    reused here rather than inventing a second limit."""
    monkeypatch.setattr(diagnostic_adapters, "MAX_DIAGNOSTIC_RECORD_BYTES", 200)
    frame_line = 'File "/home/dev/app/mod_{i}.py", line {i}, in handler\n'
    ocr_text = "".join(frame_line.format(i=i) for i in range(50))

    records = _stream_ocr_text(monkeypatch, tmp_path, ocr_text)
    events = _retained_events(records)

    assert len(events) > 1
    for event in events:
        assert len(event.raw_line.encode("utf-8")) <= 200 + len(frame_line)


# --- structured fallback: marker-only OCR text with no explicit level -----


def test_ocr_marker_only_text_without_explicit_level_still_becomes_evidence(
    monkeypatch, tmp_path
):
    """Reproduces the remaining silent-loss gap: text that already tripped
    the importance gate via a plain marker word ("exception") - not a
    standalone LOG_LEVEL_PATTERN token, a capitalized "XxxException:" match,
    or a recognized stack frame - used to come back with level=None and
    then vanish at the Evidence-persistence gate (_IMPORTANT_LEVELS in
    app/tasks/analysis.py checks event.level, never the raw text) even
    though it was already judged genuinely diagnostic. Nothing is
    fabricated: no exception_type/frames/service/trace_id are invented,
    only the level gets a conservative fallback so the record survives."""
    ocr_text = "An exception occurred while saving the file\n"
    records = _stream_ocr_text(monkeypatch, tmp_path, ocr_text)
    events = _retained_events(records)

    assert len(events) == 1
    assert events[0].level == "ERROR"
    assert events[0].raw_line == "An exception occurred while saving the file"
    assert events[0].exception_type is None
    assert events[0].stack_frames == []


def test_ocr_bare_java_frame_with_no_error_keyword_becomes_evidence(monkeypatch, tmp_path):
    """Java-style frame, no accompanying ERROR/Exception keyword line."""
    ocr_text = "at com.example.payments.Worker.run(Worker.java:88)\n"
    records = _stream_ocr_text(monkeypatch, tmp_path, ocr_text)
    events = _retained_events(records)

    assert len(events) == 1
    assert events[0].level == "ERROR"


def test_ocr_bare_node_frame_with_no_error_keyword_becomes_evidence(monkeypatch, tmp_path):
    """Node-style frame, no accompanying ERROR/Exception keyword line."""
    ocr_text = "at Object.<anonymous> (/srv/app/worker.js:42:17)\n"
    records = _stream_ocr_text(monkeypatch, tmp_path, ocr_text)
    events = _retained_events(records)

    assert len(events) == 1
    assert events[0].level == "ERROR"


# --- 4: irrelevant OCR text still yields zero retained evidence -----------


def test_irrelevant_ocr_text_still_produces_zero_retained_evidence(monkeypatch, tmp_path):
    ocr_text = (
        "Welcome to My Cool App\n"
        "Everything is running smoothly today\n"
        "Have a nice day!\n"
    )
    records = _stream_ocr_text(monkeypatch, tmp_path, ocr_text)
    events = _retained_events(records)

    assert events == []


# --- 5: OCR confidence/provenance survives on every reconstructed record --


def test_ocr_confidence_and_provenance_survive_across_multiple_records(monkeypatch, tmp_path):
    ocr_text = "ERROR: first failure\nERROR: second failure\n"
    records = _stream_ocr_text(monkeypatch, tmp_path, ocr_text, confidence=0.65)
    events = _retained_events(records)

    assert len(events) == 2
    for event in events:
        assert event.ocr_confidence == 0.65
        assert event.source_format == "image"
        assert event.source_file == "shot.png"


def test_ocr_none_confidence_is_never_fabricated(monkeypatch, tmp_path):
    ocr_text = "ERROR: unscored failure\n"
    records = _stream_ocr_text(monkeypatch, tmp_path, ocr_text, confidence=None)
    events = _retained_events(records)

    assert len(events) == 1
    assert events[0].ocr_confidence is None


# --- 6/7: every existing non-image ArtifactFormat still retains real ------
# diagnostic failures - untouched code paths, exercised against the same
# real fixtures test_artifact_detection.py uses to prove format detection,
# now proving retention too.


REPRESENTATIVE_FIXTURES = (
    ("generic.txt", ArtifactFormat.GENERIC),
    ("json_in_txt.txt", ArtifactFormat.JSON),
    ("stack_trace.txt", ArtifactFormat.STACK_TRACE),
    ("nginx.txt", ArtifactFormat.WEB_SERVER),
    ("docker.jsonl", ArtifactFormat.CONTAINER),
    ("ci.txt", ArtifactFormat.CI_CD),
    ("syslog.txt", ArtifactFormat.SYSLOG),
    ("otlp.json", ArtifactFormat.OPENTELEMETRY),
    ("browser.har", ArtifactFormat.BROWSER),
    ("cloud_gateway.txt", ArtifactFormat.CLOUD_GATEWAY),
    ("cloudfront.tsv", ArtifactFormat.CLOUD_GATEWAY),
    ("cloudwatch.json", ArtifactFormat.SERVERLESS),
    ("database.txt", ArtifactFormat.DATABASE),
    ("message_broker.txt", ArtifactFormat.MESSAGE_BROKER),
)


def test_every_non_image_format_family_still_retains_a_real_failure():
    formats_covered = set()
    for fixture_name, artifact_format in REPRESENTATIVE_FIXTURES:
        records = list(
            stream_artifact_events(
                file_path=str(FIXTURES / fixture_name),
                artifact_format=artifact_format,
                source_file=fixture_name,
            )
        )
        events = _retained_events(records)
        assert events, f"{fixture_name} ({artifact_format.value}) retained no evidence"
        formats_covered.add(artifact_format)

    # All 13 non-image ArtifactFormat values are represented (CLOUD_GATEWAY
    # and SERVERLESS/CONTAINER each appear via more than one fixture above).
    all_formats = set(ArtifactFormat) - {ArtifactFormat.IMAGE, ArtifactFormat.UNSUPPORTED}
    assert formats_covered == all_formats


# --- 8: no raw image is ever sent to Gemini --------------------------------


def test_gemini_context_never_contains_raw_image_bytes():
    from datetime import datetime, timezone

    from app.models.evidence import Evidence
    from app.services.investigation_context import build_simple_llm_context

    raw_image_marker = b"\x89PNG\r\n\x1a\n" + bytes(range(256)) * 4
    evidence = Evidence(
        id=1,
        analysis_id=1,
        artifact_id=1,
        fingerprint="fp-1",
        severity="ERROR",
        occurrence_count=1,
        first_seen=datetime.now(timezone.utc),
        last_seen=datetime.now(timezone.utc),
        source_format="image",
        source_file="shot.png",
        first_line_number=1,
        last_line_number=1,
        representative_line="ERROR something failed",
        ocr_confidence=0.9,
    )

    context = build_simple_llm_context(1, [evidence])
    serialized = json.dumps(context, default=str)

    assert raw_image_marker.decode("latin-1") not in serialized
    assert "image_bytes" not in serialized
    assert "image_data" not in serialized


def test_generate_investigation_explanation_sends_json_text_not_image_bytes():
    from unittest.mock import MagicMock, patch

    from app.services.gemini_service import generate_investigation_explanation

    mock_response = MagicMock()
    mock_response.text = (
        '{"title": "t", "summary": "s", "probable_root_causes": [], '
        '"what_happened": [], "source_code_findings": [], '
        '"recommended_actions": [], "uncertainties": []}'
    )

    context = {
        "analysis_id": 1,
        "evidence": [{"id": 1, "source_format": "image", "ocr_confidence": 0.9}],
    }

    with patch(
        "app.services.gemini_service._client.models.generate_content",
        return_value=mock_response,
    ) as generate_content:
        generate_investigation_explanation(context)

    _, kwargs = generate_content.call_args
    assert isinstance(kwargs["contents"], str)
    json.loads(kwargs["contents"])  # plain JSON text, not an image Part/bytes payload


# --- 9: OCR runs at most once per uploaded image ---------------------------


def test_ocr_runs_at_most_once_per_uploaded_image(monkeypatch, tmp_path):
    call_count = {"n": 0}
    fake_box = [[0, 0], [10, 0], [10, 10], [0, 10]]

    def _counting_ocr(path):
        call_count["n"] += 1
        return [(fake_box, "ERROR something failed", 0.9)], None

    monkeypatch.setattr(image_text_extractor, "_ocr", _counting_ocr)
    monkeypatch.setattr(analysis_task, "publish_artifact_outcome", lambda *a, **k: None)

    image_path = tmp_path / "shot.png"
    image_path.write_bytes(b"\x89PNG\r\n\x1a\n")

    db = Mock()
    db.query.return_value.filter.return_value.scalar.return_value = 1
    analysis = SimpleNamespace(id=1, processed_bytes=0, last_processed_line=0)
    artifact = SimpleNamespace(
        id=1,
        position=0,
        original_filename="shot.png",
        saved_file_path=str(image_path),
        content_type=None,
        size_bytes=image_path.stat().st_size,
        detected_format="image",
        status="pending",
        last_processed_line=0,
        processed_bytes=0,
        duplicate_of_artifact_id=None,
    )

    analysis_task._process_artifact(db=db, analysis=analysis, artifact=artifact)

    assert call_count["n"] == 1
