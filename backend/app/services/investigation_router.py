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

    shared_trace = (
        db.query(Evidence.trace_id)
        .filter(
            Evidence.analysis_id == analysis_id,
            Evidence.trace_id.is_not(None),
        )
        .group_by(Evidence.trace_id)
        .having(func.count(Evidence.id) > 1)
        .first()
    )

    if shared_trace is not None:
        return InvestigationPath.CORRELATED

    shared_request = (
        db.query(Evidence.request_id)
        .filter(
            Evidence.analysis_id == analysis_id,
            Evidence.request_id.is_not(None),
        )
        .group_by(Evidence.request_id)
        .having(func.count(Evidence.id) > 1)
        .first()
    )

    if shared_request is not None:
        return InvestigationPath.CORRELATED

    shared_correlation = (
        db.query(Evidence.correlation_key)
        .filter(
            Evidence.analysis_id == analysis_id,
            Evidence.correlation_key.is_not(None),
        )
        .group_by(Evidence.correlation_key)
        .having(func.count(Evidence.id) > 1)
        .first()
    )

    if shared_correlation is not None:
        return InvestigationPath.CORRELATED

    return InvestigationPath.SIMPLE
