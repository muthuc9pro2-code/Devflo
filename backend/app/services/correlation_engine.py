from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

class CorrelationSignal(str, Enum):
    PARENT_SPAN = "parent_span"
    SPAN_ID = "span_id"
    TRACE_ID = "trace_id"
    REQUEST_ID = "request_id"
    RESOLVED_IDENTITY = "resolved_identity"

    SERVICE = "service"
    MODULE = "module"
    HOST = "host"
    CONTAINER = "container"
    POD = "pod"

    ENDPOINT = "endpoint"
    HTTP_STATUS = "http_status"

    EXCEPTION = "exception"
    FINGERPRINT = "fingerprint"
    SOURCE = "source"
    TEMPORAL = "temporal"

class SignalStrength(float, Enum):
    VERY_HIGH = 1.0
    HIGH = 0.85
    MEDIUM = 0.60
    LOW = 0.30

@dataclass(frozen=True, slots=True)
class CorrelationSignalMatch:
    signal: CorrelationSignal
    strength: SignalStrength

FORMAT_SIGNAL_PRIORITY: dict[str, dict[CorrelationSignal, SignalStrength]] = {
    "opentelemetry": {
        CorrelationSignal.PARENT_SPAN: SignalStrength.VERY_HIGH,
        CorrelationSignal.SPAN_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.REQUEST_ID: SignalStrength.HIGH,
        CorrelationSignal.RESOLVED_IDENTITY: SignalStrength.HIGH,
        CorrelationSignal.SERVICE: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "json": {
        CorrelationSignal.SPAN_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.PARENT_SPAN: SignalStrength.VERY_HIGH,
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.RESOLVED_IDENTITY: SignalStrength.HIGH,
        CorrelationSignal.MODULE: SignalStrength.MEDIUM,
        CorrelationSignal.SERVICE: SignalStrength.MEDIUM,
        CorrelationSignal.EXCEPTION: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "web_server": {
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.ENDPOINT: SignalStrength.MEDIUM,
        CorrelationSignal.HOST: SignalStrength.MEDIUM,
        CorrelationSignal.HTTP_STATUS: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "container": {
        CorrelationSignal.SPAN_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.PARENT_SPAN: SignalStrength.VERY_HIGH,
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.POD: SignalStrength.HIGH,
        CorrelationSignal.CONTAINER: SignalStrength.HIGH,
        CorrelationSignal.MODULE: SignalStrength.MEDIUM,
        CorrelationSignal.SERVICE: SignalStrength.MEDIUM,
        CorrelationSignal.EXCEPTION: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "database": {
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.SERVICE: SignalStrength.HIGH,
        CorrelationSignal.EXCEPTION: SignalStrength.HIGH,
        CorrelationSignal.FINGERPRINT: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "cloud_gateway": {
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.ENDPOINT: SignalStrength.HIGH,
        CorrelationSignal.HTTP_STATUS: SignalStrength.MEDIUM,
        CorrelationSignal.HOST: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "serverless": {
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.SERVICE: SignalStrength.HIGH,
        CorrelationSignal.EXCEPTION: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "syslog": {
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.HOST: SignalStrength.HIGH,
        CorrelationSignal.SERVICE: SignalStrength.MEDIUM,
        CorrelationSignal.EXCEPTION: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "message_broker": {
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.SERVICE: SignalStrength.HIGH,
        CorrelationSignal.FINGERPRINT: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "browser": {
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.ENDPOINT: SignalStrength.HIGH,
        CorrelationSignal.HTTP_STATUS: SignalStrength.MEDIUM,
        CorrelationSignal.EXCEPTION: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "ci_cd": {
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.SERVICE: SignalStrength.MEDIUM,
        CorrelationSignal.EXCEPTION: SignalStrength.MEDIUM,
        CorrelationSignal.SOURCE: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "stack_trace": {
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.EXCEPTION: SignalStrength.HIGH,
        CorrelationSignal.SOURCE: SignalStrength.HIGH,
        CorrelationSignal.FINGERPRINT: SignalStrength.HIGH,
        CorrelationSignal.MODULE: SignalStrength.HIGH,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
    "generic": {
        CorrelationSignal.SPAN_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.PARENT_SPAN: SignalStrength.VERY_HIGH,
        CorrelationSignal.TRACE_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.REQUEST_ID: SignalStrength.VERY_HIGH,
        CorrelationSignal.RESOLVED_IDENTITY: SignalStrength.HIGH,
        CorrelationSignal.MODULE: SignalStrength.MEDIUM,
        CorrelationSignal.SERVICE: SignalStrength.MEDIUM,
        CorrelationSignal.EXCEPTION: SignalStrength.MEDIUM,
        CorrelationSignal.FINGERPRINT: SignalStrength.MEDIUM,
        CorrelationSignal.TEMPORAL: SignalStrength.LOW,
    },
}

def signal_strength(
    source_format: str | None,
    signal: CorrelationSignal,
) -> SignalStrength | None:
    if source_format is None:
        return None

    priorities = FORMAT_SIGNAL_PRIORITY.get(source_format)

    if priorities is None:
        return None

    return priorities.get(signal)

@dataclass(slots=True)
class CorrelationNode:
    id: str
    service: str | None
    fingerprint: str | None
    first_seen: datetime | None
    last_seen: datetime | None
    occurrence_count: int = 1
    evidence_ids: list[int] = field(default_factory=list)
    trace_id: str | None = None
    request_id: str | None = None
    span_id: str | None = None
    parent_span_id: str | None = None
    resolved_identity: str | None = None


@dataclass(slots=True)
class CorrelationEdge:
    source_id: str
    target_id: str
    score: float
    delta_ms: float | None
    signals: list[CorrelationSignal] = field(default_factory=list)


@dataclass(slots=True)
class CorrelationComponent:
    nodes: list[CorrelationNode] = field(default_factory=list)
    edges: list[CorrelationEdge] = field(default_factory=list)


@dataclass(slots=True)
class CorrelationResult:
    analysis_id: int
    components: list[CorrelationComponent] = field(default_factory=list)



def temporal_score(
    delta_ms: float,
    decay_ms: float = 1000.0,
) -> float:
    if delta_ms < 0:
        return 0.0

    return 1.0 / (1.0 + (delta_ms / decay_ms))