from sqlalchemy import String, and_, case, cast, func, update
from sqlalchemy.orm import Session

from app.models.analysis import Analysis
from app.models.evidence import Evidence


def persist_resolved_identities(
    db: Session,
    analysis_id: int,
    *,
    generation: int | None = None,
) -> bool:
    """Set-based UPDATE, never a per-row loop - see BASELINE_PHASE1 for why.

    When `generation` is given (the finalizer's real call shape), the
    ownership check and the Evidence UPDATE happen inside ONE short
    transaction on the caller's own session: a locking read
    (SELECT ... FOR UPDATE) on the Analysis row first proves, against the
    truly current COMMITTED row - never a stale MySQL REPEATABLE READ
    snapshot, since a locking read always reads the latest committed data
    regardless of when this session's transaction began - that:

        Analysis.status == "processing"
        Analysis.processing_generation == generation
        Analysis.finalization_generation == generation

    before the Evidence UPDATE is even issued; both the check and the
    write commit (or roll back) together. The row lock is held only for
    this one short transaction, released immediately by the commit/
    rollback below - never across any CPU/network work the caller does
    afterward. This does not risk flushing unrelated dirty Analysis ORM
    state early: the finalizer keeps its own final-result values
    (processed_bytes/last_processed_line/ai_analysis/result_snapshot) in
    local variables, never set onto the ORM object, until its own later
    authoritative final-commit fence.

    Returns True if the update actually ran (ownership held at the time of
    the check), False if ownership was already lost - the caller must stop
    finalizing without persisting or publishing a result. When
    `generation` is omitted (a direct/legacy caller with no generation to
    scope to), the update runs unconditionally on the caller's own
    session/transaction, exactly as before this generation-scoping was
    added.
    """
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

    if generation is None:
        db.execute(statement)
        db.commit()
        return True

    current = (
        db.query(
            Analysis.status,
            Analysis.processing_generation,
            Analysis.finalization_generation,
        )
        .filter(Analysis.id == analysis_id)
        .with_for_update()
        .first()
    )
    if (
        current is None
        or current[0] != "processing"
        or current[1] != generation
        or current[2] != generation
    ):
        db.rollback()
        return False

    db.execute(statement)
    db.commit()
    return True
