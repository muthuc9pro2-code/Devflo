from collections.abc import Iterator
from sqlalchemy.orm import Session
from app.models import Evidence

IDENTITY_CHUNK_SIZE = 500

def stream_resolved_identities(
    db: Session,
    analysis_id: int,
) -> Iterator[str]:

    query = (
        db.query(Evidence.resolved_identity)
        .filter(
            Evidence.analysis_id == analysis_id,
            Evidence.resolved_identity.isnot(None),
        )
        .distinct()
        .yield_per(IDENTITY_CHUNK_SIZE)
    )

    for (identity,) in query:
        yield identity

        