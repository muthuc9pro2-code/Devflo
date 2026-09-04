import json
import re
from enum import StrEnum
from pathlib import Path
from typing import BinaryIO
from app.core.processing_config import ARTIFACT_DETECTION_SAMPLE_BYTES

class ArtifactFormat(StrEnum):
    GENERIC = 'generic'
    JSON = 'json'
    STACK_TRACE = 'stack_trace'
    WEB_SERVER = 'web_server'
    CONTAINER = 'container'
    DATABASE = 'database'
    CLOUD_GATEWAY = 'cloud_gateway'
    CI_CD = 'ci_cd'
    BROWSER = 'browser'
    MESSAGE_BROKER = 'message_broker'
    SERVERLESS = 'serverless'
    SYSLOG = 'syslog'
    OPENTELEMETRY = 'opentelemetry'
    IMAGE = 'image'
    UNSUPPORTED = 'unsupported'
SUPPORTED_IMAGE_SUFFIXES = frozenset({
    ".png",
    ".jpg",
    ".jpeg",
    ".webp",
})

SUPPORTED_TEXT_SUFFIXES = frozenset({
    ".log",
    ".txt",
    ".json",
    ".jsonl",
    ".ndjson",
    ".har",
    ".out",
    ".err",
})

SUPPORTED_TEXT_MIME_TYPES = frozenset({
    "text/plain",
    "application/json",
    "application/x-ndjson",
    "application/jsonlines",
})

SUPPORTED_IMAGE_MIME_TYPES = frozenset({
    "image/png",
    "image/jpeg",
    "image/webp",
})
SYSLOG_PATTERN = re.compile('^<\\d{1,3}>(?:\\d+\\s+(?:\\d{4}-\\d{2}-\\d{2}T|-\\s+)|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\s+\\d{1,2}\\s+\\d{2}:\\d{2}:\\d{2})', re.MULTILINE)
WEB_ACCESS_PATTERN = re.compile('^\\S+\\s+\\S+\\s+\\S+\\s+\\[[^\\]]+\\]\\s+"(?:GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\\s+\\S+\\s+HTTP/\\d(?:\\.\\d)?"\\s+\\d{3}\\s+', re.MULTILINE)
CRI_PATTERN = re.compile('^\\d{4}-\\d{2}-\\d{2}T\\S+\\s+(?:stdout|stderr)\\s+[FP]\\s+', re.MULTILINE)

def detect_artifact(file_path: str | Path, *, filename: str | None=None, mime_type: str | None=None) -> ArtifactFormat:
    with open(file_path, 'rb') as stream:
        return detect_artifact_stream(stream, filename=filename or Path(file_path).name, mime_type=mime_type)

def detect_artifact_stream(stream: BinaryIO, *, filename: str | None=None, mime_type: str | None=None, sample_bytes: int=ARTIFACT_DETECTION_SAMPLE_BYTES) -> ArtifactFormat:
    if sample_bytes <= 0:
        raise ValueError('sample_bytes must be positive')
    normalized_mime = (mime_type or "").split(";", 1)[0].strip().lower()
    suffix = Path(filename or "").suffix.lower()
    if normalized_mime in {
        "image/png",
        "image/jpeg",
        "image/webp",
    } or suffix in {
        ".png",
        ".jpg",
        ".jpeg",
        ".webp",
    }:
        return ArtifactFormat.IMAGE
    original_position = stream.tell()
    try:
        sample = stream.read(sample_bytes)
    finally:
        stream.seek(original_position)
    return detect_artifact_sample(sample, filename=filename, mime_type=mime_type)

_ROTATION_SUFFIX_PATTERN = re.compile(r"^\.\d+$")

def _effective_suffix(filename: str) -> str:
    path = Path(filename or "")
    suffixes = path.suffixes
    if len(suffixes) > 1 and _ROTATION_SUFFIX_PATTERN.match(suffixes[-1]):
        return suffixes[-2].lower()
    return path.suffix.lower()

def _sample_is_text_like(sample: bytes) -> bool:
    if not sample or b"\x00" in sample:
        return False
    try:
        sample.decode("utf-8-sig")
    except UnicodeDecodeError:
        return False
    return True

def is_supported_diagnostic_sample(
    sample: bytes,
    *,
    filename: str | None = None,
    mime_type: str | None = None,
) -> bool:
    suffix = _effective_suffix(filename or "")
    normalized_mime = (mime_type or "").split(";", 1)[0].strip().lower()

    if (
        suffix in SUPPORTED_IMAGE_SUFFIXES
        or normalized_mime in SUPPORTED_IMAGE_MIME_TYPES
    ):
        return True

    if (
        suffix in SUPPORTED_TEXT_SUFFIXES
        or normalized_mime in SUPPORTED_TEXT_MIME_TYPES
    ):
        return _sample_is_text_like(sample)
    
    if not _sample_is_text_like(sample):
        return False

    detected = detect_artifact_sample(sample, filename=filename, mime_type=mime_type)
    return detected not in (
        ArtifactFormat.GENERIC,
        ArtifactFormat.UNSUPPORTED,
        ArtifactFormat.IMAGE,
    )

def detect_artifact_sample(sample: bytes, *, filename: str | None=None, mime_type: str | None=None) -> ArtifactFormat:
    sample_suffix = Path(filename or '').suffix.lower()
    sample_mime = (mime_type or '').split(';', 1)[0].strip().lower()
    if sample_suffix in SUPPORTED_IMAGE_SUFFIXES or sample_mime in SUPPORTED_IMAGE_MIME_TYPES:
        return ArtifactFormat.IMAGE
    text = sample.decode('utf-8-sig', errors='replace')
    stripped = text.lstrip()
    lowered = text.lower()
    compact_lowered = re.sub('\\s+', '', lowered)
    looks_json = _looks_like_json(stripped)
    if looks_json and any((marker in compact_lowered for marker in ('"resourcelogs":', '"resourcespans":', '"resource_logs":', '"resource_spans":'))):
        return ArtifactFormat.OPENTELEMETRY
    if looks_json and ('"kubernetes":' in compact_lowered or ('"log":' in compact_lowered and any((marker in compact_lowered for marker in ('"stream":', '"container_id":'))))):
        return ArtifactFormat.CONTAINER
    if looks_json and ('"entries":' in compact_lowered and ('"startedDateTime"'.lower() in lowered or '"request":' in compact_lowered)):
        return ArtifactFormat.BROWSER
    if _has_cloudfront_fields(text):
        return ArtifactFormat.CLOUD_GATEWAY
    if SYSLOG_PATTERN.search(text):
        return ArtifactFormat.SYSLOG
    if CRI_PATTERN.search(text):
        return ArtifactFormat.CONTAINER
    if WEB_ACCESS_PATTERN.search(text) or re.search('^\\d{4}/\\d{2}/\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}\\s+\\[(?:warn|error|crit|alert|emerg)\\]', text, re.MULTILINE | re.IGNORECASE) or re.search('^\\[[A-Z][a-z]{2}\\s+[A-Z][a-z]{2}\\s+\\d{1,2}\\s+\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?\\s+\\d{4}\\]\\s+\\[[^\\]]*:(?:warn|error|crit|alert|emerg)\\]', text, re.MULTILINE | re.IGNORECASE):
        return ArtifactFormat.WEB_SERVER
    if any((marker in lowered for marker in ('traceback (most recent call last)', 'exception in thread', 'caused by:', 'segmentation fault', 'panic:', 'core dumped', 'crash report', 'fatal signal'))) or re.search('^\\s+at\\s+.*[^\\s:]\\:\\d+(?::\\d+)?\\)?$', text, re.MULTILINE):
        return ArtifactFormat.STACK_TRACE
    if any((marker in lowered for marker in ('##[error]', '##[warning]', 'github_actions', 'gitlab-runner', 'jenkins', 'build failed', 'deployment failed'))):
        return ArtifactFormat.CI_CD
    if any((marker in lowered for marker in ('query_time:', 'slow query', 'sqlstate[', 'deadlock detected', 'ora-', 'postgresql:', 'mysql, version'))):
        return ArtifactFormat.DATABASE
    if any((marker in lowered for marker in ('kafka', 'rabbitmq', 'amqp', 'consumer group', 'rebalance', 'broker id'))):
        return ArtifactFormat.MESSAGE_BROKER
    if any((marker in text for marker in ('START RequestId:', 'END RequestId:', 'REPORT RequestId:'))) or (looks_json and '"awsrequestid":' in compact_lowered):
        return ArtifactFormat.SERVERLESS
    if looks_json and any((marker in compact_lowered for marker in ('"logstream":', '"loggroup":', '"logevents":', '"@message":'))):
        return ArtifactFormat.SERVERLESS
    if any((marker in lowered for marker in ('api gateway', 'cloudfront', 'elasticloadbalancing', ' elb '))) or (looks_json and any((marker in compact_lowered for marker in ('"requestcontext":', '"elb_status_code":')))):
        return ArtifactFormat.CLOUD_GATEWAY
    if re.search('^(?:http|https|h2|grpc|grpcs)\\s+\\d{4}-\\d{2}-\\d{2}T\\S+\\s+\\S+\\s+', text, re.MULTILINE):
        return ArtifactFormat.CLOUD_GATEWAY
    if looks_json:
        return ArtifactFormat.JSON
    suffix = Path(filename or '').suffix.lower()
    normalized_mime = (mime_type or '').split(';', 1)[0].strip().lower()
    if suffix == '.har' and stripped.startswith(('{', '[')):
        return ArtifactFormat.BROWSER
    if normalized_mime in {'application/json', 'application/x-ndjson'} and stripped.startswith(('{', '[')):
        return ArtifactFormat.JSON
    return ArtifactFormat.GENERIC

def _looks_like_json(text: str) -> bool:
    if not text.startswith(('{', '[')):
        return False
    first_line = next((line.strip() for line in text.splitlines() if line.strip()), '')
    if first_line:
        try:
            json.loads(first_line)
            return True
        except json.JSONDecodeError:
            pass
    return bool(re.match('^[{[]\\s*(?:[}\\]]|\\"[^\\"]+\\"\\s*:|[{[])', text))

def _has_cloudfront_fields(text: str) -> bool:
    for line in text.splitlines():
        if not line.lstrip('\ufeff').lower().startswith('#fields:'):
            continue
        fields = {field.lower() for field in line.split(':', 1)[1].split()}
        if {'date', 'time', 'sc-status', 'cs-uri-stem'} <= fields and fields.intersection({'x-edge-location', 'x-edge-request-id', 'x-edge-result-type'}):
            return True
    return False
