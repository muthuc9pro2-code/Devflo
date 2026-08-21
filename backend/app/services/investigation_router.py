from enum import Enum
from app.models.evidence import Evidence
from app.services.correlation_engine import (
    CorrelationIndexes,
    CorrelationPreparation,
    has_genuine_correlatable_structure,
)


class InvestigationPath(str, Enum):
    SIMPLE = "simple"
    CORRELATED = "correlated"


def choose_investigation_path(
    evidence_rows: list[Evidence],
    *,
    indexes: CorrelationIndexes | None = None,
    preparation: CorrelationPreparation | None = None,
) -> InvestigationPath:
    """Choose SIMPLE/CORRELATED from the same correlation semantics.

    The production finalizer passes a fully prepared relationship set, so
    routing is O(1) and run_correlation() can reuse that exact set without a
    second candidate traversal.  The indexes/evidence fallback is retained
    for compatibility with direct callers and focused tests.
    """
    if preparation is not None:
        return (
            InvestigationPath.CORRELATED
            if preparation.has_relationships
            else InvestigationPath.SIMPLE
        )

    if has_genuine_correlatable_structure(evidence_rows, indexes=indexes):
        return InvestigationPath.CORRELATED

    return InvestigationPath.SIMPLE
