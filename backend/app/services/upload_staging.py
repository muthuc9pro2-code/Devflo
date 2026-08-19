import hashlib
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
) -> tuple[int, bytes, str]:
    size = 0
    sample = bytearray()
    # Streaming digest over the same chunks already being read/written for
    # size and sample-detection purposes - no second pass over the file, no
    # whole-file buffering, bounded by chunk_bytes regardless of artifact
    # size. Content-identity only (duplicate detection); never a
    # diagnostic-content or correlation signal.
    digest = hashlib.sha256()

    with open(target, "xb") as destination:
        while chunk := upload.file.read(chunk_bytes):
            size += len(chunk)

            if size > max_bytes:
                raise UploadTooLarge(detail)

            if len(sample) < ARTIFACT_DETECTION_SAMPLE_BYTES:
                remaining = ARTIFACT_DETECTION_SAMPLE_BYTES - len(sample)
                sample.extend(chunk[:remaining])

            digest.update(chunk)
            destination.write(chunk)

    return size, bytes(sample), digest.hexdigest()