from io import BytesIO

import ijson
import pytest

from app.core.processing_config import (
    EVIDENCE_TIMELINE_PAGE_SIZE,
    MAX_INVESTIGATION_UPLOAD_BYTES,
)
from app.services.artifact_detector import ArtifactFormat
from app.services.batch_processor import (
    MAX_BATCH_BYTES,
    MAX_BATCH_ITEMS,
    create_batches,
)
from app.services.diagnostic_adapters import stream_artifact_events
from app.services.otel_adapter import _OtelCapture
from app.utils.bounded_json import BoundedJsonStream, OversizedJsonScalarError
from app.utils.file_reader import stream_text_lines


def test_newline_free_input_is_split_at_the_record_bound(tmp_path):
    path = tmp_path / "newline-free.txt"
    path.write_bytes(b"x" * 35)

    records = list(
        stream_text_lines(
            str(path),
            read_chunk_bytes=7,
            max_line_bytes=16,
        )
    )

    assert [len(line) for line, _ in records] == [16, 16, 3]
    assert [offset for _, offset in records] == [16, 32, 35]


@pytest.mark.parametrize(
    ("artifact_format", "payload", "expected_sizes"),
    (
        (ArtifactFormat.GENERIC, "ERROR café\r\nERROR tea\n".encode(), [13, 10]),
        (ArtifactFormat.JSON, b'\xef\xbb\xbf{"level":"ERROR","message":"caf\xc3\xa9"}\r\n', [40]),
        (
            ArtifactFormat.STACK_TRACE,
            "RuntimeError: café\r\n  at run (/srv/app.js:2:3)\r\n".encode(),
            [49],
        ),
    ),
)
def test_artifact_batch_sizes_use_exact_captured_bytes(
    tmp_path, artifact_format, payload, expected_sizes
):
    path = tmp_path / "records.log"
    path.write_bytes(payload)

    records = list(
        stream_artifact_events(
            file_path=str(path),
            artifact_format=artifact_format,
            source_file=path.name,
        )
    )

    assert [record.batch_size_bytes for record in records] == expected_sizes
    assert sum(expected_sizes) == len(payload)


def test_centralized_batch_defaults_match_phase_one_targets():
    assert MAX_BATCH_BYTES == 10 * 1024 * 1024
    assert MAX_BATCH_ITEMS == 20_000
    assert EVIDENCE_TIMELINE_PAGE_SIZE == 10_000
    assert MAX_INVESTIGATION_UPLOAD_BYTES == 1024**3
    assert list(create_batches([("one", 3), ("two", 6)])) == [[("one", 3), ("two", 6)]]


def test_streaming_json_rejects_an_oversized_scalar_before_parser_allocation():
    source = BytesIO(b'["' + (b"x" * 128) + b'"]')

    with pytest.raises(OversizedJsonScalarError, match="16 bytes"):
        list(ijson.items(BoundedJsonStream(source, max_scalar_bytes=16), "item"))

    numeric_source = BytesIO(b"[" + (b"1" * 128) + b"]")
    with pytest.raises(OversizedJsonScalarError, match="16 bytes"):
        list(
            ijson.items(
                BoundedJsonStream(numeric_source, max_scalar_bytes=16),
                "item",
            )
        )


def test_otlp_capture_charges_empty_field_keys(monkeypatch):
    monkeypatch.setattr("app.services.otel_adapter.MAX_DIAGNOSTIC_RECORD_BYTES", 32)

    record = _OtelCapture(kind="log_record", prefix="record")
    for index in range(100):
        record.consume(f"record.field_{index}", "string", "")

    span = _OtelCapture(kind="span", prefix="span")
    span.consume("span.events.item", "start_map", None)
    for index in range(100):
        span.consume(f"span.events.item.field_{index}", "string", "")

    assert len(record.fields) < 100
    assert record._captured_bytes <= 32
    assert sum(len(key.encode()) for key in record.fields) <= record._captured_bytes
    assert span._event is not None
    assert len(span._event) < 100
    assert span._captured_bytes <= 32


def test_malformed_json_structured_checkpoint_does_not_switch_to_text(tmp_path):
    path = tmp_path / "partial.json"
    path.write_text('[{"level":"ERROR","message":"first"},{"level":', encoding="utf-8")

    with pytest.raises(ijson.JSONError):
        list(
            stream_artifact_events(
                file_path=str(path),
                artifact_format=ArtifactFormat.JSON,
                source_file=path.name,
                start_artifact_line=1,
            )
        )


def test_malformed_json_text_fallback_resumes_from_byte_checkpoint(tmp_path):
    first_line = b'{"level":\n'
    path = tmp_path / "fallback.json"
    path.write_bytes(first_line + b"ERROR remaining failure\n")

    records = list(
        stream_artifact_events(
            file_path=str(path),
            artifact_format=ArtifactFormat.JSON,
            source_file=path.name,
            start_offset=len(first_line),
            start_artifact_line=1,
            global_line_number=1,
        )
    )

    assert len(records) == 1
    assert records[0].event.raw_line == "ERROR remaining failure"
    assert records[0].event.line_number == 2
