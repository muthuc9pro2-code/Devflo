from sqlalchemy import String, and_, case, cast, func, update
from sqlalchemy.orm import Session

from app.models.evidence import Evidence


def persist_resolved_identities(
    db: Session,
    analysis_id: int,
) -> None:
    has_trace_id = and_(
        Evidence.trace_id.is_not(None),
        Evidence.trace_id != "__none__",
    )

    has_request_id = and_(
        Evidence.request_id.is_not(None),
        Evidence.request_id != "__none__",
    )

    statement = (
        update(Evidence)
        .where(
            Evidence.analysis_id == analysis_id,
            # persist_evidence_batch() already resolves identity at insert
            # time for every row where trace_id/request_id is known; only
            # rows it left NULL (resolved_identity needs the DB-assigned id,
            # which doesn't exist before the insert) still need this pass.
            Evidence.resolved_identity.is_(None),
        )
        .values(
            resolved_identity=case(
                (
                    has_trace_id,
                    func.concat("trace:", Evidence.trace_id),
                ),
                (
                    has_request_id,
                    func.concat("request:", Evidence.request_id),
                ),
                else_=func.concat(
                    "unresolved:",
                    cast(Evidence.id, String),
                ),
            ),
            identity_match_type=case(
                (
                    has_trace_id,
                    "trace_id",
                ),
                (
                    has_request_id,
                    "request_id",
                ),
                else_="unresolved",
            ),
            identity_strength=case(
                (
                    has_trace_id,
                    1.0,
                ),
                (
                    has_request_id,
                    0.9,
                ),
                else_=0.0,
            ),
        )
    )

    db.execute(statement)
    db.commit()
