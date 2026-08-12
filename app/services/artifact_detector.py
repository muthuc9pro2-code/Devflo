import json
import re
from enum import StrEnum
from pathlib import Path
from typing import BinaryIO

from app.core.processing_config import ARTIFACT_DETECTION_SAMPLE_BYTES


class ArtifactFormat(StrEnum):
    GENERIC = "generic"
    JSON = "json"
    STACK_TRACE = "stack_trace"
    WEB_SERVER = "web_server"
    CONTAINER = "container"
    DATABASE = "database"
    CLOUD_GATEWAY = "cloud_gateway"
    CI_CD = "ci_cd"
    BROWSER = "browser"
    MESSAGE_BROKER = "message_broker"
    SERVERLESS = "serverless"
    SYSLOG = "syslog"
    OPENTELEMETRY = "opentelemetry"


SYSLOG_PATTERN = re.compile(
    r"^<\d{1,3}>(?:\d+\s+(?:\d{4}-\d{2}-\d{2}T|-\s+)|"
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})",
    re.MULTILINE,
)
WEB_ACCESS_PATTERN = re.compile(
    r'^\S+\s+\S+\s+\S+\s+\[[^\]]+\]\s+"'
    r"(?:GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\s+\S+\s+HTTP/\d(?:\.\d)?"
    r'"\s+\d{3}\s+',
    re.MULTILINE,
)
CRI_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\S+\s+(?:stdout|stderr)\s+[FP]\s+",
    re.MULTILINE,
)


def detect_artifact(
    file_path: str | Path,
    *,
    filename: str | None = None,
    mime_type: str | None = None,
) -> ArtifactFormat:
    with open(file_path, "rb") as stream:
        return detect_artifact_stream(
            stream,
            filename=filename or Path(file_path).name,
            mime_type=mime_type,
        )


def detect_artifact_stream(
    stream: BinaryIO,
    *,
    filename: str | None = None,
    mime_type: str | None = None,
    sample_bytes: int = ARTIFACT_DETECTION_SAMPLE_BYTES,
) -> ArtifactFormat:
    """Detect from a bounded content sample and restore the stream position."""

    if sample_bytes <= 0:
        raise ValueError("sample_bytes must be positive")

    original_position = stream.tell()
    try:
        sample = stream.read(sample_bytes)
    finally:
        stream.seek(original_position)

    return detect_artifact_sample(
        sample,
        filename=filename,
        mime_type=mime_type,
    )


def detect_artifact_sample(
    sample: bytes,
    *,
    filename: str | None = None,
    mime_type: str | None = None,
) -> ArtifactFormat:
    text = sample.decode("utf-8-sig", errors="replace")
    stripped = text.lstrip()
    lowered = text.lower()
    compact_lowered = re.sub(r"\s+", "", lowered)
    looks_json = _looks_like_json(stripped)

    # Structured formats are checked before generic JSON because their source
    # semantics materially improve normalization.
    if looks_json and any(
        marker in compact_lowered
        for marker in (
            '"resourcelogs":',
            '"resourcespans":',
            '"resource_logs":',
            '"resource_spans":',
        )
    ):
        return ArtifactFormat.OPENTELEMETRY

    if looks_json and (
        '"kubernetes":' in compact_lowered
        or (
            '"log":' in compact_lowered
            and any(
                marker in compact_lowered for marker in ('"stream":', '"container_id":')
            )
        )
    ):
        return ArtifactFormat.CONTAINER

    if looks_json and (
        '"entries":' in compact_lowered
        and ('"startedDateTime"'.lower() in lowered or '"request":' in compact_lowered)
    ):
        return ArtifactFormat.BROWSER

    # CloudFront standard logs are W3C-style TSV, and typically contain no
    # literal ``cloudfront`` marker. The schema comment is the reliable content
    # signature. Requiring an edge field avoids treating an arbitrary W3C log
    # as a CloudFront artifact.
    if _has_cloudfront_fields(text):
        return ArtifactFormat.CLOUD_GATEWAY

    if SYSLOG_PATTERN.search(text):
        return ArtifactFormat.SYSLOG

    if CRI_PATTERN.search(text):
        return ArtifactFormat.CONTAINER

    if (
        WEB_ACCESS_PATTERN.search(text)
        or re.search(
            r"^\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}\s+\[(?:warn|error|crit|alert|emerg)\]",
            text,
            re.MULTILINE | re.IGNORECASE,
        )
        or re.search(
            r"^\[[A-Z][a-z]{2}\s+[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}"
            r"(?:\.\d+)?\s+\d{4}\]\s+\[[^\]]*:(?:warn|error|crit|alert|emerg)\]",
            text,
            re.MULTILINE | re.IGNORECASE,
        )
    ):
        return ArtifactFormat.WEB_SERVER

    if any(
        marker in lowered
        for marker in (
            "traceback (most recent call last)",
            "exception in thread",
            "caused by:",
            "segmentation fault",
            "panic:",
            "core dumped",
            "crash report",
            "fatal signal",
        )
    ) or re.search(
        r"^\s+at\s+.*[^\s:]\:\d+(?::\d+)?\)?$",
        text,
        re.MULTILINE,
    ):
        return ArtifactFormat.STACK_TRACE

    if any(
        marker in lowered
        for marker in (
            "##[error]",
            "##[warning]",
            "github_actions",
            "gitlab-runner",
            "jenkins",
            "build failed",
            "deployment failed",
        )
    ):
        return ArtifactFormat.CI_CD

    if any(
        marker in lowered
        for marker in (
            "query_time:",
            "slow query",
            "sqlstate[",
            "deadlock detected",
            "ora-",
            "postgresql:",
            "mysql, version",
        )
    ):
        return ArtifactFormat.DATABASE

    if any(
        marker in lowered
        for marker in (
            "kafka",
            "rabbitmq",
            "amqp",
            "consumer group",
            "rebalance",
            "broker id",
        )
    ):
        return ArtifactFormat.MESSAGE_BROKER

    if any(
        marker in text
        for marker in ("START RequestId:", "END RequestId:", "REPORT RequestId:")
    ) or (looks_json and '"awsrequestid":' in compact_lowered):
        return ArtifactFormat.SERVERLESS

    if looks_json and any(
        marker in compact_lowered
        for marker in (
            '"logstream":',
            '"loggroup":',
            '"logevents":',
            '"@message":',
        )
    ):
        return ArtifactFormat.SERVERLESS

    if any(
        marker in lowered
        for marker in (
            "api gateway",
            "cloudfront",
            "elasticloadbalancing",
            " elb ",
        )
    ) or (
        looks_json
        and any(
            marker in compact_lowered
            for marker in ('"requestcontext":', '"elb_status_code":')
        )
    ):
        return ArtifactFormat.CLOUD_GATEWAY

    if re.search(
        r"^(?:http|https|h2|grpc|grpcs)\s+\d{4}-\d{2}-\d{2}T\S+\s+\S+\s+",
        text,
        re.MULTILINE,
    ):
        return ArtifactFormat.CLOUD_GATEWAY

    if looks_json:
        return ArtifactFormat.JSON

    # Extension and MIME are deliberately only weak hints. A misleading JSON
    # name cannot override non-JSON content.
    suffix = Path(filename or "").suffix.lower()
    normalized_mime = (mime_type or "").split(";", 1)[0].strip().lower()

    if suffix == ".har" and stripped.startswith(("{", "[")):
        return ArtifactFormat.BROWSER
    if normalized_mime in {
        "application/json",
        "application/x-ndjson",
    } and stripped.startswith(("{", "[")):
        return ArtifactFormat.JSON

    return ArtifactFormat.GENERIC


def _looks_like_json(text: str) -> bool:
    if not text.startswith(("{", "[")):
        return False

    first_line = next((line.strip() for line in text.splitlines() if line.strip()), "")
    if first_line:
        try:
            json.loads(first_line)
            return True
        except json.JSONDecodeError:
            pass

    # A fixed prefix of a large valid document is usually incomplete. Structural
    # opening tokens plus a quoted key are enough to route it to streaming JSON.
    return bool(re.match(r"^[{[]\s*(?:[}\]]|\"[^\"]+\"\s*:|[{[])", text))


def _has_cloudfront_fields(text: str) -> bool:
    for line in text.splitlines():
        if not line.lstrip("\ufeff").lower().startswith("#fields:"):
            continue

        fields = {field.lower() for field in line.split(":", 1)[1].split()}
        if {
            "date",
            "time",
            "sc-status",
            "cs-uri-stem",
        } <= fields and fields.intersection(
            {"x-edge-location", "x-edge-request-id", "x-edge-result-type"}
        ):
            return True

    return False
