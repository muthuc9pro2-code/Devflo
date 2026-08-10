import re
from app.services.log_praser import ParsedEvent

NUMBER_PATTERN = re.compile(r"\b\d+\b")
UUID_PATTERN = re.compile(
    r"\b[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}\b"
)
def normalize_exception_message(message: str) -> str:
    message = UUID_PATTERN.sub("<uuid>", message)
    message = NUMBER_PATTERN.sub("<number>", message)

    return message.lower().strip()

def build_exception_fingerprint(event: ParsedEvent) -> str:
    if event.exception_type is None:
        normalized_message = normalize_exception_message(event.raw_line)
        return f"{event.level or 'event'}:{normalized_message}"

    normalized_message = ""

    if event.exception_message:
        normalized_message = normalize_exception_message(
            event.exception_message
        )

    return f"{event.exception_type.lower()}:{normalized_message}"

