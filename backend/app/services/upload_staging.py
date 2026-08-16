from pathlib import Path
from typing import BinaryIO
from app.core.processing_config import ARTIFACT_DETECTION_SAMPLE_BYTES

class UploadTooLarge(ValueError):
    pass

class UnsupportedUpload(ValueError):
    pass

def copy_upload(
    upload,
    target: Path,
    max_bytes: int,
    detail: str,
    chunk_bytes: int,
) -> tuple[int, bytes]:
    size = 0
    sample = bytearray()

    with open(target, "xb") as destination:
        while chunk := upload.file.read(chunk_bytes):
            size += len(chunk)

            if size > max_bytes:
                raise UploadTooLarge(detail)

            if len(sample) < ARTIFACT_DETECTION_SAMPLE_BYTES:
                remaining = ARTIFACT_DETECTION_SAMPLE_BYTES - len(sample)
                sample.extend(chunk[:remaining])

            destination.write(chunk)

    return size, bytes(sample)