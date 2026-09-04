from importlib import import_module
from typing import Any

_EXPORTS = {
    "parse_log_line": ("app.services.log_praser", "parse_log_line"),
    "filter_important_events": (
        "app.services.event_filter",
        "filter_important_events",
    ),
    "is_evidence_worthy": (
        "app.services.event_filter",
        "is_evidence_worthy",
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
    "persist_resolved_identities": (
        "app.services.identity_persister",
        "persist_resolved_identities",
    ),
    "build_component_timeline": (
        "app.services.timeline_processor",
        "build_component_timeline",
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
