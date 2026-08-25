import re


_UI_NOISE = {
    "PROBLEMS",
    "OUTPUT",
    "TERMINAL",
    "PORTS",
    "DEBUG CONSOLE",
    "TIMELINE",
    "OUTLINE",
}


def normalize_ocr_text(text: str) -> str:
    normalized_lines: list[str] = []

    for raw_line in text.splitlines():
        line = raw_line.strip()

        if not line:
            continue

        if line.upper() in _UI_NOISE:
            continue

        line = (
            line.replace("（", "(")
            .replace("）", ")")
            .replace("，", ",")
            .replace("：", ":")
            .replace("“", '"')
            .replace("”", '"')
            .replace("‘", "'")
            .replace("’", "'")
        )

        line = re.sub(r"[ \t]+", " ", line)

        line = re.sub(
            r"\b[1lI]ine\s+(\d+)\b",
            r"line \1",
            line,
            flags=re.IGNORECASE,
        )

        normalized_lines.append(line)

    return "\n".join(normalized_lines)