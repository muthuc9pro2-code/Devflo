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
real screenshot a Devflo user actually uploaded during manual testing of
this exact repository (a VS Code terminal showing a genuine ImportError:
`from app.services.email_service import send_password_reset_email` failing
because that function did not exist yet). It was available
locally (backend/uploads/) at the time this test was written and has been
committed as a permanent fixture so this regression does not depend on
ephemeral upload state.
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

    # The real "File ..." frame chain in this
    # screenshot must not fragment into one singleton record per frame
    # line. There are 5 frame lines in the real captured text (importlib
    # bootstrap x3, main.py, auth.py) - a handful of coherent records, not
    # one per frame.
    assert len(events) < 5

    combined_raw = "\n".join(e.raw_line for e in events)
    assert "auth.py" in combined_raw
    assert "13" in combined_raw
    assert "send_password_reset_email" in combined_raw
    assert "main.py" in combined_raw

    # Editor-chrome noise (VS Code panel tabs) must not survive
    # normalization into evidence.
    assert "PROBLEMS" not in combined_raw
    assert "TERMINAL" not in combined_raw

    # No exception line was actually captured in this screenshot (the crop
    # ends at the traceback frames) - nothing must invent one.
    assert all(e.exception_type is None for e in events)

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
