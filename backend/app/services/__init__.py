"""Public service API with lazy imports for lightweight parser use."""

from importlib import import_module
from typing import Any

_EXPORTS = {
    "parse_log_line": ("app.services.log_praser", "parse_log_line"),
    "filter_important_events": (
        "app.services.event_filter",
        "filter_important_events",
    ),
    "build_exception_fingerprint": (
        "app.services.exception_fingerprint",
        "build_exception_fingerprint",
    ),
    "create_batches": ("app.services.batch_processor", "create_batches"),
    "persist_evidence_batch": (
        "app.services.evidence_store",
        "persist_evidence_batch",
    ),
    "resolve_evidence_identity": (
        "app.services.persistent_identity_resolver",
        "resolve_evidence_identity",
    ),
    "persist_resolved_identities": (
        "app.services.identity_persister",
        "persist_resolved_identities",
    ),
    "stream_timeline_evidence": (
        "app.services.persistent_timeline",
        "stream_timeline_evidence",
    ),
    "process_persisted_timelines": (
        "app.services.timeline_processor",
        "process_persisted_timelines",
    ),
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    try:
        module_name, attribute_name = _EXPORTS[name]
    except KeyError as error:
        raise AttributeError(name) from error

    value = getattr(import_module(module_name), attribute_name)
    globals()[name] = value
    return value
