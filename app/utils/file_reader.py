from pathlib import Path
from collections.abc import Iterator

def stream_text_lines(
    file_path: str,
    start_offset: int = 0,
):
    with open(file_path, "rb") as file:
        file.seek(start_offset)

        while True:
            line = file.readline()

            if not line:
                break

            current_offset = file.tell()

            yield (
                line.decode("utf-8", errors="replace").rstrip("\r\n"),
                current_offset,
            )

    