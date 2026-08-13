from pathlib import Path


class UploadTooLarge(ValueError):
    pass


def copy_upload(upload, target: Path, max_bytes: int, detail: str, chunk_bytes: int) -> int:
    size = 0
    with open(target, "xb") as destination:
        while chunk := upload.file.read(chunk_bytes):
            size += len(chunk)
            if size > max_bytes:
                raise UploadTooLarge(detail)
            destination.write(chunk)
    return size
