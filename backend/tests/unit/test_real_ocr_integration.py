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

    assert events

    assert len(events) < 5

    combined_raw = "\n".join(e.raw_line for e in events)
    assert "auth.py" in combined_raw
    assert "13" in combined_raw
    assert "send_password_reset_email" in combined_raw
    assert "main.py" in combined_raw

    assert "python" not in combined_raw.lower()

    assert any(e.exception_type == "ImportError" for e in events)

    for event in events:
        assert event.source_format == "image"
        assert event.source_file == FIXTURE.name
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
