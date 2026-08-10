from collections.abc import Iterator
from sqlalchemy.orm import Session
from app.models import Evidence

TIMELINE_PAGE_SIZE = 5000

def stream_timeline_evidence(
    db: Session,
    analysis_id: int,
) -> Iterator[Evidence]:

    last_id = 0

    while True:
        evidence_rows = (
            db.query(Evidence)
            .filter(
                Evidence.analysis_id == analysis_id,
                Evidence.id > last_id,
            )
            .order_by(Evidence.id)
            .limit(TIMELINE_PAGE_SIZE)
            .all()
        )

        if not evidence_rows:
            break

        for evidence in evidence_rows:
            yield evidence

        last_id = evidence_rows[-1].id