from collections.abc import Iterator

from sqlalchemy.orm import Session

from app.models import Evidence


EVIDENCE_CHUNK_SIZE = 1000


def stream_evidence(
    db: Session,
    analysis_id: int,
) -> Iterator[Evidence]:

    query = (
        db.query(Evidence)
        .filter(Evidence.analysis_id == analysis_id)
        .order_by(Evidence.id)
        .yield_per(EVIDENCE_CHUNK_SIZE)
    )

    for evidence in query:
        yield evidence