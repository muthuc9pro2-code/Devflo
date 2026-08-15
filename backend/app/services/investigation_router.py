from enum import Enum
from sqlalchemy import func
from sqlalchemy.orm import Session
from app.models.evidence import Evidence

class InvestigationPath(str, Enum):
    SIMPLE = "simple"
    CORRELATED = "correlated"

def choose_investigation_path(
    db: Session,
    analysis_id: int,
) -> InvestigationPath:
    evidence_count = (
        db.query(func.count(Evidence.id))
        .filter(Evidence.analysis_id == analysis_id)
        .scalar()
        or 0
    )

    if evidence_count <= 1:
        return InvestigationPath.SIMPLE

    has_parent_child_span = (
        db.query(Evidence.id)
        .filter(
            Evidence.analysis_id == analysis_id,
            Evidence.parent_span_id.is_not(None),
            Evidence.span_id.is_not(None),
        )
        .first()
        is not None
    )

    if has_parent_child_span:
        return InvestigationPath.CORRELATED

    shared_identity = (
        db.query(Evidence.resolved_identity)
        .filter(
            Evidence.analysis_id == analysis_id,
            Evidence.resolved_identity.is_not(None),
            Evidence.identity_strength > 0,
        )
        .group_by(Evidence.resolved_identity)
        .having(func.count(Evidence.id) > 1)
        .first()
    )

    if shared_identity is not None:
        return InvestigationPath.CORRELATED

    return InvestigationPath.SIMPLE