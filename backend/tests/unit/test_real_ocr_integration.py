"""Real-OCR integration regression: the mocked OCR tests in
test_image_ocr_record_reconstruction.py prove record-reconstruction logic
against hand-written OCR text, but that is not proof RapidOCR's actual
output on a real screenshot flows through the pipeline correctly - a
mocked `extract_text_from_image_with_confidence` return value agrees with
the parser by construction.

This exercises the real path end to end: image file -> real RapidOCR ->
real ocr_normalizer.normalize_ocr_text -> real record reconstruction
(diagnostic_adapters._stream_image_events) -> real Evidence-shaped
ParsedEvent objects. Nothing here monkeypatches `_ocr` or
extract_text_from_image_with_confidence.

The fixture (tests/fixtures/images/real_import_error_screenshot.jpeg) is a
sanitized deterministic terminal screenshot used to exercise the real OCR
ingestion path: a rendered terminal window showing a Python ImportError
traceback (`from app.services.email import send_password_reset_email`
failing because that function does not exist). It contains no real
user/host/path information and is committed as a permanent fixture so this
regression does not depend on ephemeral upload state.
"""
from pathlib import Path

import pytest

from app.services.artifact_detector import ArtifactFormat
from app.services.diagnostic_adapters import stream_artifact_events

FIXTURE = Path(__file__).parents[1] / "fixtures" / "images" / "real_import_error_screenshot.jpeg"


@pytest.mark.skipif(not FIXTURE.exists(), reason="real OCR fixture not present in this checkout")
def test_real_screenshot_produces_coherent_non_fragmented_evidence():
    records = list(
        stream_artifact_events(
            file_path=str(FIXTURE),
            artifact_format=ArtifactFormat.IMAGE,
            source_file=FIXTURE.name,
        )
    )
    events = [r.event for r in records if r.event is not None]

    # Real diagnostic content was genuinely retained (not evidence_count=0
    # despite OCR succeeding - the original bug this whole area guards
    # against).
    assert events

    # The full traceback (two "File ..." frames plus the ImportError line)
    # must not fragment into one singleton record per line - it is a
    # single coherent incident, not a pile of unrelated records.
    assert len(events) < 5

    combined_raw = "\n".join(e.raw_line for e in events)
    assert "auth.py" in combined_raw
    assert "13" in combined_raw
    assert "send_password_reset_email" in combined_raw
    assert "main.py" in combined_raw

    # Terminal-window chrome (the shell prompt line) must not survive
    # normalization into evidence.
    assert "python" not in combined_raw.lower()

    # The real ImportError line is present in this screenshot; the parser
    # must extract it from the genuinely captured OCR text, never fabricate
    # a different exception type or silently drop it.
    assert any(e.exception_type == "ImportError" for e in events)

    for event in events:
        assert event.source_format == "image"
        assert event.source_file == FIXTURE.name
        # Real RapidOCR confidence, never fabricated/defaulted.
        assert event.ocr_confidence is not None
        assert 0.0 <= event.ocr_confidence <= 1.0


@pytest.mark.skipif(not FIXTURE.exists(), reason="real OCR fixture not present in this checkout")
def test_real_screenshot_ocr_runs_exactly_once():
    from unittest.mock import patch

    from app.services import image_text_extractor

    engine = image_text_extractor._get_ocr_engine()
    with patch.object(
        image_text_extractor, "_ocr", wraps=engine
    ) as ocr_spy:
        list(
            stream_artifact_events(
                file_path=str(FIXTURE),
                artifact_format=ArtifactFormat.IMAGE,
                source_file=FIXTURE.name,
            )
        )

    assert ocr_spy.call_count == 1
