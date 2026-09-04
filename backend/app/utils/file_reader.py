from collections.abc import Iterator
from app.core.processing_config import (
    FILE_READ_CHUNK_BYTES,
    MAX_DIAGNOSTIC_RECORD_BYTES,
)

def stream_text_lines(
    file_path: str,
    start_offset: int = 0,
    read_chunk_bytes: int = FILE_READ_CHUNK_BYTES,
    max_line_bytes: int = MAX_DIAGNOSTIC_RECORD_BYTES,
) -> Iterator[tuple[str, int]]:

    if read_chunk_bytes <= 0 or max_line_bytes <= 0:
        raise ValueError("Reader limits must be positive")

    with open(file_path, "rb") as file:
        file.seek(start_offset)
        buffer = bytearray()
        current_offset = start_offset

        while True:
            chunk = file.read(read_chunk_bytes)
            if chunk:
                buffer.extend(chunk)

            consumed = 0
            while consumed < len(buffer):
                newline_index = buffer.find(b"\n", consumed)
                available_bytes = len(buffer) - consumed

                if newline_index >= 0 and newline_index - consumed <= max_line_bytes:
                    record_end = newline_index + 1
                elif available_bytes >= max_line_bytes:
                    record_end = consumed + max_line_bytes
                elif not chunk:
                    record_end = len(buffer)
                else:
                    break

                raw_line = bytes(buffer[consumed:record_end])
                current_offset += record_end - consumed
                consumed = record_end

                if raw_line.endswith(b"\n"):
                    raw_line = raw_line[:-1]
                    if raw_line.endswith(b"\r"):
                        raw_line = raw_line[:-1]

                yield (
                    raw_line.decode("utf-8", errors="replace"),
                    current_offset,
                )

            if consumed:
                del buffer[:consumed]

            if not chunk:
                break
