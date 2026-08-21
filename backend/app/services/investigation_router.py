from enum import Enum
from app.models.evidence import Evidence
from app.services.correlation_engine import has_genuine_correlatable_structure


class InvestigationPath(str, Enum):
    SIMPLE = "simple"
    CORRELATED = "correlated"


def choose_investigation_path(
    evidence_rows: list[Evidence],
) -> InvestigationPath:
    """CORRELATED is chosen only when has_genuine_correlatable_structure()
    (correlation_engine.py) finds at least one real relationship - the
    SAME relationship semantics build_correlation_edges itself uses, never
    a second, independently-drifting heuristic here.

    A thin policy wrapper only: no DB query of its own. The caller
    (_finalize_analysis_task) selects ONE bounded working Evidence set
    (select_bounded_evidence_from_db) before routing, and routing decides
    from exactly that same set - never a separate, unbounded materialize
    merely to route (Section 4 hardening).

    Two previous router-only signals were removed as unsound:
      - a shared correlation_key: that hash is generated from sentinel
        placeholders ("__none__") whenever trace_id/request_id/span_id are
        ALL missing, so every untraced evidence row in an analysis shares
        the identical hash regardless of whether they are actually
        related - a persistence/dedup grouping key, never trustworthy
        incident identity.
      - "this row has both span_id and parent_span_id set": that only
        proves the row itself looks like a child span: it says nothing
        about whether the actual PARENT row (matching span_id, compatible
        trace) exists at all. has_genuine_correlatable_structure() instead
        calls find_parent_span_candidate(), which verifies a real match.
    """
    if has_genuine_correlatable_structure(evidence_rows):
        return InvestigationPath.CORRELATED

    return InvestigationPath.SIMPLE
