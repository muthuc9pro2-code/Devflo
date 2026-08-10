from sqlalchemy.orm import Session

from app.models.evidence import Evidence


IDENTITY_CHUNK_SIZE = 500


def stream_resolved_identities(
    db: Session,
    analysis_id: int,
):
    last_id = 0

    while True:
        evidence_rows = (
            db.query(Evidence)
            .filter(
                Evidence.analysis_id == analysis_id,
                Evidence.id > last_id,
                Evidence.resolved_identity.isnot(None),
            )
            .order_by(Evidence.id)
            .limit(IDENTITY_CHUNK_SIZE)
            .all()
        )

        if not evidence_rows:
            break

        for evidence in evidence_rows:
            yield evidence.resolved_identity

        last_id = evidence_rows[-1].id