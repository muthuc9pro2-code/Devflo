from sqlalchemy.orm import Session
from app.models.evidence import Evidence
from app.services.persistent_identity_resolver import resolve_evidence_identity

IDENTITY_PAGE_SIZE = 500

def persist_resolved_identities(
    db: Session,
    analysis_id: int,
) -> None:

    last_id = 0

    while True:
        evidence_rows = (
            db.query(Evidence)
            .filter(
                Evidence.analysis_id == analysis_id,
                Evidence.id > last_id,
            )
            .order_by(Evidence.id)
            .limit(IDENTITY_PAGE_SIZE)
            .all()
        )

        if not evidence_rows:
            break

        for evidence in evidence_rows:
            identity = resolve_evidence_identity(evidence)

            evidence.resolved_identity = identity.identity
            evidence.identity_match_type = identity.match_type
            evidence.identity_strength = identity.strength

        last_id = evidence_rows[-1].id

        db.commit()