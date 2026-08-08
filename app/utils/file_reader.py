from pathlib import Path
from collections.abc import Iterator

def stream_text_lines(file_path: str) -> Iterator[str]:
    path = Path(file_path)

    with path.open(
        "r",
        encoding="utf-8",
        errors="replace"
    ) as file:
        for line in file:
            yield line.rstrip("\r\n")

    