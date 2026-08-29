"""RapidOCR confidence propagation: RapidOCR result -> normalized OCR event
-> Evidence -> frontend investigation_result payload -> SIMPLE/CORRELATED
Gemini context.

Reuses the existing OCR -> normalization -> Evidence architecture end to
end; no parallel OCR pipeline, no fabricated confidence for anything
RapidOCR didn't actually score, no fabricated confidence on non-image
evidence.
"""
from datetime import datetime, timezone
from io import BytesIO

from PIL import Image
import pytest

from app.models.evidence import Evidence
from app.services import diagnostic_adapters, evidence_store, image_text_extractor
from app.services.artifact_detector import ArtifactFormat
from app.services.correlation_engine import run_correlation
from app.services.diagnostic_adapters import stream_artifact_events
from app.services.investigation_context import (
    build_correlation_payload,
    build_llm_context,
    build_simple_llm_context,
    build_simple_payload,
)


def _write_tiny_png(path):
    """A real, tiny, decodable image on disk - item 1's shared
    validate_ocr_image() now runs inside _run_ocr() itself (defense in
    depth) before any mocked/real RapidOCR call, so tests exercising
    _run_ocr()/extract_text_from_image[_with_confidence] need a genuine
    file at the given path rather than an arbitrary placeholder string."""
    buffer = BytesIO()
    Image.new("RGB", (4, 4), "white").save(buffer, format="PNG")
    path.write_bytes(buffer.getvalue())
    return str(path)


def _evidence(evidence_id: int, **kwargs) -> Evidence:
    defaults = {
        "id": evidence_id,
        "analysis_id": 1,
        "artifact_id": 1,
        "fingerprint": f"fp-{evidence_id}",
        "severity": "ERROR",
        "occurrence_count": 1,
        "first_seen": datetime.now(timezone.utc),
        "last_seen": datetime.now(timezone.utc),
        "source_format": "generic",
        "first_line_number": 1,
        "last_line_number": 1,
        "representative_line": "ERROR something failed",
    }
    defaults.update(kwargs)
    return Evidence(**defaults)


# --- image_text_extractor: real RapidOCR confidence, mean-of-lines, no fabrication --


def test_extract_with_confidence_reuses_the_same_ocr_call_and_returns_mean_confidence(monkeypatch, tmp_path):
    fake_results = [
        (None, "ERROR something failed", 0.95),
        (None, "  at handler (App.tsx:42)", 0.61),
        (None, "   ", 0.99),  # blank text after strip -> excluded, like extract_text_from_image already does
    ]
    monkeypatch.setattr(image_text_extractor, "_ocr", lambda path: (fake_results, None))
    image_path = _write_tiny_png(tmp_path / "shot.png")

    text, confidence = image_text_extractor.extract_text_from_image_with_confidence(image_path)

    assert text == "ERROR something failed\nat handler (App.tsx:42)"
    # mean of the two retained lines' confidences (0.95, 0.61), not min/max/fabricated
    assert confidence == (0.95 + 0.61) / 2


def test_extract_text_from_image_output_is_unchanged_by_the_new_function(monkeypatch, tmp_path):
    """Backward compatibility: the pre-existing plain-text function must
    return byte-identical output to before this change."""
    fake_results = [(None, "line one", 0.9), (None, "line two", 0.5)]
    monkeypatch.setattr(image_text_extractor, "_ocr", lambda path: (fake_results, None))
    image_path = _write_tiny_png(tmp_path / "shot.png")

    assert image_text_extractor.extract_text_from_image(image_path) == "line one\nline two"


def test_no_usable_confidence_returns_none_not_fabricated(monkeypatch, tmp_path):
    # RapidOCR result tuples without a third (confidence) element.
    fake_results = [(None, "some text")]
    monkeypatch.setattr(image_text_extractor, "_ocr", lambda path: (fake_results, None))
    image_path = _write_tiny_png(tmp_path / "shot.png")

    text, confidence = image_text_extractor.extract_text_from_image_with_confidence(image_path)

    assert text == "some text"
    assert confidence is None


def test_no_ocr_results_at_all_returns_empty_text_and_none_confidence(monkeypatch, tmp_path):
    monkeypatch.setattr(image_text_extractor, "_ocr", lambda path: ([], None))
    image_path = _write_tiny_png(tmp_path / "shot.png")

    text, confidence = image_text_extractor.extract_text_from_image_with_confidence(image_path)

    assert text == ""
    assert confidence is None


def test_rapidocr_initialization_failure_is_converted_after_validation(monkeypatch, tmp_path):
    image_path = _write_tiny_png(tmp_path / "shot.png")
    monkeypatch.setattr(image_text_extractor, "_ocr", None)

    def fail_initialization():
        raise RuntimeError("native model initialization failed")

    monkeypatch.setattr(
        "rapidocr_onnxruntime.RapidOCR",
        fail_initialization,
    )

    with pytest.raises(image_text_extractor.OcrProcessingError):
        image_text_extractor.extract_text_from_image(image_path)


def test_rapidocr_inference_failure_is_converted(monkeypatch, tmp_path):
    image_path = _write_tiny_png(tmp_path / "shot.png")

    def fail_inference(_path):
        raise RuntimeError("onnx inference failed")

    monkeypatch.setattr(image_text_extractor, "_ocr", fail_inference)

    with pytest.raises(image_text_extractor.OcrProcessingError):
        image_text_extractor.extract_text_from_image(image_path)


@pytest.mark.parametrize(
    "malformed_result",
    [
        None,
        ([None], None),
        ([(None, None, 0.8)], None),
        ([(None, "ERROR boom", "not-a-number")], None),
    ],
)
def test_malformed_rapidocr_results_are_converted(
    monkeypatch, tmp_path, malformed_result
):
    image_path = _write_tiny_png(tmp_path / "shot.png")
    monkeypatch.setattr(
        image_text_extractor,
        "_ocr",
        lambda _path: malformed_result,
    )

    with pytest.raises(image_text_extractor.OcrProcessingError):
        image_text_extractor.extract_text_from_image(image_path)


# --- diagnostic_adapters: confidence lands on the ParsedEvent -------------


def test_stream_image_events_attaches_real_confidence_to_the_event(monkeypatch, tmp_path):
    monkeypatch.setattr(
        diagnostic_adapters,
        "extract_text_from_image_with_confidence",
        lambda path: ("ERROR boom", 0.73),
    )
    monkeypatch.setattr(diagnostic_adapters, "normalize_ocr_text", lambda text: text)
    image_path = tmp_path / "shot.png"
    image_path.write_bytes(b"\x89PNG\r\n\x1a\n")

    records = list(
        stream_artifact_events(
            file_path=str(image_path), artifact_format=ArtifactFormat.IMAGE, source_file="shot.png"
        )
    )

    assert records[0].event.ocr_confidence == 0.73


def test_stream_image_events_preserves_none_confidence(monkeypatch, tmp_path):
    monkeypatch.setattr(
        diagnostic_adapters,
        "extract_text_from_image_with_confidence",
        lambda path: ("ERROR boom", None),
    )
    monkeypatch.setattr(diagnostic_adapters, "normalize_ocr_text", lambda text: text)
    image_path = tmp_path / "shot.png"
    image_path.write_bytes(b"\x89PNG\r\n\x1a\n")

    records = list(
        stream_artifact_events(
            file_path=str(image_path), artifact_format=ArtifactFormat.IMAGE, source_file="shot.png"
        )
    )

    assert records[0].event.ocr_confidence is None


def test_non_image_text_events_never_carry_a_confidence_value(tmp_path):
    """Requirement: non-image evidence must remain unchanged / no fake
    OCR confidence. Runs the real GENERIC text path (not image) end to end."""
    log_path = tmp_path / "app.log"
    log_path.write_text("ERROR something failed\n")

    records = list(
        stream_artifact_events(
            file_path=str(log_path), artifact_format=ArtifactFormat.GENERIC, source_file="app.log"
        )
    )

    assert records[0].event.ocr_confidence is None


# --- evidence_store: persistence keeps confidence tied to the right row ---


def test_persist_evidence_batch_writes_ocr_confidence_only_for_the_image_event():
    captured_rows = {}

    class _FakeResult:
        def scalar(self):
            return None

    class _FakeDB:
        def execute(self, statement, rows):
            captured_rows["rows"] = rows
            return _FakeResult()

    from app.services.log_praser import ParsedEvent

    image_event = ParsedEvent(
        line_number=1, raw_line="ERROR boom", level="ERROR", source_format="image",
        source_file="shot.png", artifact_id=1, fingerprint="fp-image", ocr_confidence=0.42,
    )
    text_event = ParsedEvent(
        line_number=2, raw_line="ERROR text failure", level="ERROR", source_format="generic",
        source_file="app.log", artifact_id=1, fingerprint="fp-text",
    )

    evidence_store.persist_evidence_batch(
        db=_FakeDB(), analysis_id=1, events=[image_event, text_event], artifact_id=1
    )

    rows_by_fingerprint = {row["fingerprint"]: row for row in captured_rows["rows"]}
    assert rows_by_fingerprint["fp-image"]["ocr_confidence"] == 0.42
    assert rows_by_fingerprint["fp-text"]["ocr_confidence"] is None


# --- 5: keep confidence associated with the correct evidence item ---------


def test_confidence_stays_attached_to_its_own_evidence_item_not_others():
    rows = [
        _evidence(1, artifact_id=101, source_format="image", source_file="a.png", ocr_confidence=0.9),
        _evidence(2, artifact_id=102, source_format="image", source_file="b.png", ocr_confidence=0.2),
        _evidence(3, artifact_id=103, source_format="generic", source_file="c.log"),
    ]

    payload = build_simple_payload(1, rows)

    by_id = {item["id"]: item for item in payload["evidence"]}
    assert by_id[1]["ocr_confidence"] == 0.9
    assert by_id[2]["ocr_confidence"] == 0.2
    assert by_id[3]["ocr_confidence"] is None


# --- frontend investigation_result payload ---------------------------------


def test_frontend_payload_exposes_ocr_confidence_for_image_evidence():
    rows = [_evidence(1, artifact_id=1, source_format="image", source_file="shot.png", ocr_confidence=0.81)]

    payload = build_simple_payload(1, rows)

    assert payload["evidence"][0]["ocr_confidence"] == 0.81


def test_frontend_payload_leaves_non_image_evidence_confidence_null():
    rows = [_evidence(1, artifact_id=1, source_format="web_server", service="checkout")]

    payload = build_simple_payload(1, rows)

    assert "ocr_confidence" in payload["evidence"][0]
    assert payload["evidence"][0]["ocr_confidence"] is None


def test_correlated_frontend_payload_exposes_ocr_confidence_on_node_evidence():
    from datetime import timedelta

    base = datetime.now(timezone.utc)
    web = _evidence(1, artifact_id=101, source_format="web_server", trace_id="trace-1", service="api", first_seen=base)
    screenshot = _evidence(
        2, artifact_id=102, source_format="image", trace_id="trace-1", service="ui",
        source_file="shot.png", ocr_confidence=0.77, first_seen=base + timedelta(milliseconds=5),
    )
    rows = [web, screenshot]
    run = run_correlation(analysis_id=1, evidence_rows=rows)

    payload = build_correlation_payload(run, rows)

    node = next(n for n in payload["components"][0]["nodes"] if n["artifact_id"] == 102)
    assert node["evidence"][0]["ocr_confidence"] == 0.77
    other_node = next(n for n in payload["components"][0]["nodes"] if n["artifact_id"] == 101)
    assert other_node["evidence"][0]["ocr_confidence"] is None


# --- both Gemini contexts ---------------------------------------------------


def test_simple_gemini_context_carries_ocr_confidence():
    rows = [_evidence(1, artifact_id=1, source_format="image", source_file="shot.png", ocr_confidence=0.55)]

    context = build_simple_llm_context(1, rows)

    assert context["evidence"][0]["ocr_confidence"] == 0.55


def test_correlated_gemini_context_carries_ocr_confidence_and_none_for_non_image():
    from datetime import timedelta

    base = datetime.now(timezone.utc)
    web = _evidence(1, artifact_id=101, source_format="web_server", trace_id="trace-1", service="api", first_seen=base)
    screenshot = _evidence(
        2, artifact_id=102, source_format="image", trace_id="trace-1", service="ui",
        source_file="shot.png", ocr_confidence=0.33, first_seen=base + timedelta(milliseconds=5),
    )
    rows = [web, screenshot]
    run = run_correlation(analysis_id=1, evidence_rows=rows)

    context = build_llm_context(run, rows)

    root_evidence_by_artifact = {
        item["artifact_id"]: item
        for component in context["components"]
        for item in component["root_evidence"]
    }
    assert root_evidence_by_artifact[102]["ocr_confidence"] == 0.33
    assert root_evidence_by_artifact[101]["ocr_confidence"] is None
