from sqlalchemy.orm import Session
from app.services.evidence_reader import stream_evidence
from app.services.persistent_identity_resolver import resolve_evidence_identity

IDENTITY_COMMIT_SIZE = 1000

def persist_resolved_identities(
    db: Session,
    analysis_id: int,
) -> None:

    pending = 0

    for evidence in stream_evidence(db, analysis_id):
        identity = resolve_evidence_identity(evidence)

        evidence.resolved_identity = identity.identity
        evidence.identity_match_type = identity.match_type
        evidence.identity_strength = identity.strength

        pending += 1

        if pending >= IDENTITY_COMMIT_SIZE:
            db.commit()
            pending = 0

    if pending:
        db.commit()