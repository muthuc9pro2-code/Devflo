from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import ijson

from app.core.processing_config import (
    JSON_STREAM_BUFFER_BYTES,
    MAX_DIAGNOSTIC_RECORD_BYTES,
)
from app.utils.bounded_json import BoundedJsonStream

from .artifact_detector import ArtifactFormat
from .diagnostic_parser import normalize_otel_severity, normalize_text_event
from .log_praser import estimate_parsed_event_size_bytes

if TYPE_CHECKING:
    from .diagnostic_adapters import ArtifactEvent

SCALAR_EVENTS = {"string", "number", "boolean", "null"}


@dataclass(slots=True)
class _OtelCapture:
    kind: str
    prefix: str
    depth: int = 1
    fields: dict[str, Any] = field(default_factory=dict)
    attributes: dict[str, Any] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)
    _attribute_key: str | None = None
    _attribute_value: Any = None
    _event: dict[str, Any] | None = None
    _event_attribute_key: str | None = None
    _event_attribute_value: Any = None
    _captured_bytes: int = 0

    def consume(self, prefix: str, event: str, value: Any) -> None:
        relative = prefix[len(self.prefix) :].lstrip(".")

        if event not in SCALAR_EVENTS and event != "end_map":
            if event == "start_map" and relative == "events.item":
                self._event = {"attributes": {}}
            return

        if relative == "attributes.item.key" and event == "string":
            self._attribute_key = str(value)
            return

        if relative.startswith("attributes.item.value.") and event in SCALAR_EVENTS:
            self._attribute_value = value
            return

        if relative == "attributes.item" and event == "end_map":
            self._store_attribute(
                self.attributes,
                self._attribute_key,
                self._attribute_value,
            )
            self._attribute_key = None
            self._attribute_value = None
            return

        if relative == "events.item.attributes.item.key" and event == "string":
            self._event_attribute_key = str(value)
            return

        if (
            relative.startswith("events.item.attributes.item.value.")
            and event in SCALAR_EVENTS
        ):
            self._event_attribute_value = value
            return

        if relative == "events.item.attributes.item" and event == "end_map":
            if self._event is not None:
                self._store_attribute(
                    self._event["attributes"],
                    self._event_attribute_key,
                    self._event_attribute_value,
                )
            self._event_attribute_key = None
            self._event_attribute_value = None
            return

        if relative == "events.item" and event == "end_map":
            if self._event is not None and self._room_for(self._event):
                self.events.append(self._event)
            self._event = None
            return

        if relative.startswith("events.item.") and event in SCALAR_EVENTS:
            if self._event is not None and ".attributes." not in relative:
                key = relative.removeprefix("events.item.")
                self._store_attribute(self._event, key, value)
            return

        if (
            event in SCALAR_EVENTS
            and relative
            and not relative.startswith("attributes.")
        ):
            self._store_attribute(self.fields, relative, value)

    def _store_attribute(
        self,
        target: dict[str, Any],
        key: str | None,
        value: Any,
    ) -> None:
        if key is not None and self._room_for(key) and self._room_for(value):
            target[key] = value

    def _room_for(self, value: Any) -> bool:
        size = len(str(value).encode("utf-8", errors="replace"))
        if self._captured_bytes + size > MAX_DIAGNOSTIC_RECORD_BYTES:
            return False
        self._captured_bytes += size
        return True


def stream_otlp_events(
    *,
    file_path: str,
    source_file: str,
    skip_records: int = 0,
    global_line_number: int = 0,
) -> Iterator[ArtifactEvent]:
    # Imported here to keep the adapter modules independently importable.
    from .diagnostic_adapters import ArtifactEvent

    log_resource: dict[str, Any] = {}
    span_resource: dict[str, Any] = {}
    log_scope: str | None = None
    span_scope: str | None = None
    capture: _OtelCapture | None = None
    local_record = 0
    global_line = global_line_number

    with open(file_path, "rb") as stream:
        prefix = stream.read(3)
        if prefix != b"\xef\xbb\xbf":
            stream.seek(0)

        for prefix, event, value in ijson.parse(
            BoundedJsonStream(stream),
            buf_size=JSON_STREAM_BUFFER_BYTES,
            use_float=True,
        ):
            if capture is not None:
                if event in {"start_map", "start_array"}:
                    capture.depth += 1

                capture.consume(prefix, event, value)

                if event in {"end_map", "end_array"}:
                    capture.depth -= 1

                if capture.depth != 0:
                    continue

                completed = capture
                capture = None

                if completed.kind == "log_resource":
                    log_resource = completed.attributes
                    continue
                if completed.kind == "span_resource":
                    span_resource = completed.attributes
                    continue

                local_record += 1
                if local_record <= skip_records:
                    continue

                global_line += 1
                if completed.kind == "log_record":
                    canonical_event = _normalize_log_record(
                        completed,
                        line_number=global_line,
                        source_file=source_file,
                        resource_attributes=log_resource,
                        scope_name=log_scope,
                    )
                else:
                    canonical_event = _normalize_span(
                        completed,
                        line_number=global_line,
                        source_file=source_file,
                        resource_attributes=span_resource,
                        scope_name=span_scope,
                    )

                retained_bytes = max(
                    completed._captured_bytes,
                    estimate_parsed_event_size_bytes(canonical_event),
                )
                yield ArtifactEvent(
                    event=canonical_event,
                    end_offset=0,
                    artifact_line_number=local_record,
                    global_end_line_number=global_line,
                    batch_size_bytes=retained_bytes,
                )
                continue

            if event == "start_map":
                kind = _capture_kind(prefix)
                if kind is not None:
                    capture = _OtelCapture(kind=kind, prefix=prefix)
                    continue

                normalized_prefix = prefix.replace("_", "").lower()
                if normalized_prefix.endswith("resourcelogs.item"):
                    log_resource = {}
                    log_scope = None
                elif normalized_prefix.endswith("resourcespans.item"):
                    span_resource = {}
                    span_scope = None
                elif normalized_prefix.endswith("scopelogs.item"):
                    log_scope = None
                elif normalized_prefix.endswith("scopespans.item"):
                    span_scope = None

            if event == "string" and prefix.endswith("scope.name"):
                normalized_prefix = prefix.replace("_", "").lower()
                if "resourcelogs." in normalized_prefix:
                    log_scope = str(value)
                elif "resourcespans." in normalized_prefix:
                    span_scope = str(value)


def _capture_kind(prefix: str) -> str | None:
    normalized = prefix.replace("_", "").lower()
    if normalized.endswith("resourcelogs.item.resource"):
        return "log_resource"
    if normalized.endswith("resourcespans.item.resource"):
        return "span_resource"
    if normalized.endswith("scopelogs.item.logrecords.item"):
        return "log_record"
    if normalized.endswith("scopespans.item.spans.item"):
        return "span"
    return None


def _normalize_log_record(
    capture: _OtelCapture,
    *,
    line_number: int,
    source_file: str,
    resource_attributes: dict[str, Any],
    scope_name: str | None,
):
    attributes = capture.attributes
    message = (
        _first(
            capture.fields,
            "body.stringValue",
            "body.string_value",
            "body.intValue",
            "body.int_value",
            "body.doubleValue",
            "body.double_value",
            "body.boolValue",
            "body.bool_value",
            "body",
        )
        or attributes.get("exception.message")
        or "OpenTelemetry log record"
    )

    defaults = _resource_defaults(resource_attributes)
    defaults.update(
        timestamp=_first(
            capture.fields,
            "timeUnixNano",
            "time_unix_nano",
            "observedTimeUnixNano",
            "observed_time_unix_nano",
        ),
        level=normalize_otel_severity(
            _first(
                capture.fields,
                "severityText",
                "severity_text",
                "severityNumber",
                "severity_number",
            )
        ),
        trace_id=_first(capture.fields, "traceId", "trace_id"),
        span_id=_first(capture.fields, "spanId", "span_id"),
        request_id=_first_attribute(
            attributes,
            "request.id",
            "http.request.id",
            "correlation.id",
            "aws.request_id",
        ),
        module=scope_name,
        exception_type=attributes.get("exception.type"),
        exception_message=attributes.get("exception.message"),
        endpoint=_first_attribute(
            attributes,
            "http.route",
            "url.path",
            "http.target",
        ),
        http_status=_first_attribute(
            attributes,
            "http.response.status_code",
            "http.status_code",
        ),
    )

    return normalize_text_event(
        str(message),
        line_number,
        source_file=source_file,
        source_format=ArtifactFormat.OPENTELEMETRY.value,
        defaults=defaults,
    )


def _normalize_span(
    capture: _OtelCapture,
    *,
    line_number: int,
    source_file: str,
    resource_attributes: dict[str, Any],
    scope_name: str | None,
):
    attributes = capture.attributes
    exception_event = next(
        (
            item
            for item in capture.events
            if str(item.get("name", "")).lower() == "exception"
        ),
        None,
    )
    exception_attributes = (
        exception_event.get("attributes", {}) if exception_event else {}
    )
    span_name = str(_first(capture.fields, "name") or "unnamed")
    status_code = _first(capture.fields, "status.code")
    status_message = _first(capture.fields, "status.message")
    exception_message = exception_attributes.get("exception.message")
    raw_message = (
        exception_message or status_message or f"OpenTelemetry span: {span_name}"
    )
    status_is_error = str(status_code).upper() in {
        "2",
        "ERROR",
        "STATUS_CODE_ERROR",
    }

    defaults = _resource_defaults(resource_attributes)
    defaults.update(
        timestamp=_first(capture.fields, "startTimeUnixNano", "start_time_unix_nano"),
        level="ERROR" if status_is_error or exception_event else "INFO",
        trace_id=_first(capture.fields, "traceId", "trace_id"),
        span_id=_first(capture.fields, "spanId", "span_id"),
        parent_span_id=_first(capture.fields, "parentSpanId", "parent_span_id"),
        request_id=_first_attribute(
            attributes,
            "request.id",
            "http.request.id",
            "correlation.id",
        ),
        module=scope_name,
        exception_type=exception_attributes.get("exception.type"),
        exception_message=exception_message,
        endpoint=_first_attribute(
            attributes,
            "http.route",
            "url.path",
            "http.target",
            "rpc.method",
        ),
        http_status=_first_attribute(
            attributes,
            "http.response.status_code",
            "http.status_code",
        ),
    )

    return normalize_text_event(
        str(raw_message),
        line_number,
        source_file=source_file,
        source_format=ArtifactFormat.OPENTELEMETRY.value,
        defaults=defaults,
    )


def _resource_defaults(attributes: dict[str, Any]) -> dict[str, Any]:
    return {
        "service": _first_attribute(
            attributes,
            "service.name",
            "faas.name",
        ),
        "host": _first_attribute(
            attributes,
            "host.name",
            "host.id",
            "cloud.availability_zone",
        ),
        "container": _first_attribute(
            attributes,
            "container.name",
            "container.id",
        ),
        "pod": _first_attribute(
            attributes,
            "k8s.pod.name",
            "k8s.pod.uid",
        ),
    }


def _first(values: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if values.get(key) is not None:
            return values[key]
    return None


def _first_attribute(attributes: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if attributes.get(key) is not None:
            return attributes[key]
    return None
