from collections.abc import Iterator
from sqlalchemy.orm import Session
from app.models import Evidence

TIMELINE_CHUNK_SIZE = 1000

def stream_identity_timeline(
    db: Session,
    analysis_id: int,
    resolved_identity: str,
) -> Iterator[Evidence]:

    query = (
        db.query(Evidence)
        .filter(
            Evidence.analysis_id == analysis_id,
            Evidence.resolved_identity == resolved_identity,
        )
        .order_by(
            Evidence.first_seen,
            Evidence.id,
        )
        .yield_per(TIMELINE_CHUNK_SIZE)
    )

    for evidence in query:
        yield evidence

