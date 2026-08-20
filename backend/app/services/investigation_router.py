from enum import Enum
from sqlalchemy.orm import Session
from app.models.evidence import Evidence
from app.services.correlation_engine import has_genuine_correlatable_structure


class InvestigationPath(str, Enum):
    SIMPLE = "simple"
    CORRELATED = "correlated"


def choose_investigation_path(
    db: Session,
    analysis_id: int,
) -> InvestigationPath:
    """CORRELATED is chosen only when has_genuine_correlatable_structure()
    (correlation_engine.py) finds at least one real relationship - the
    SAME relationship semantics build_correlation_edges itself uses, never
    a second, independently-drifting heuristic here.

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
    evidence_rows = (
        db.query(Evidence)
        .filter(Evidence.analysis_id == analysis_id)
        .all()
    )

    if has_genuine_correlatable_structure(evidence_rows):
        return InvestigationPath.CORRELATED

    return InvestigationPath.SIMPLE
