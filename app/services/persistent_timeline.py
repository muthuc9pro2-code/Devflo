from collections.abc import Iterator

from sqlalchemy import and_, or_
from sqlalchemy.orm import Session

from app.core.processing_config import EVIDENCE_TIMELINE_PAGE_SIZE
from app.models import Evidence

TIMELINE_PAGE_SIZE = EVIDENCE_TIMELINE_PAGE_SIZE


def stream_timeline_evidence(
    db: Session,
    analysis_id: int,
) -> Iterator[Evidence]:

    last_null_id = 0

    while True:
        evidence_rows = (
            db.query(Evidence)
            .filter(
                Evidence.analysis_id == analysis_id,
                Evidence.first_seen.is_(None),
                Evidence.id > last_null_id,
            )
            .order_by(Evidence.id)
            .limit(TIMELINE_PAGE_SIZE)
            .all()
        )

        if not evidence_rows:
            break

        for evidence in evidence_rows:
            yield evidence

        last_null_id = evidence_rows[-1].id

    last_first_seen = None
    last_id = 0

    while True:
        query = db.query(Evidence).filter(
            Evidence.analysis_id == analysis_id,
            Evidence.first_seen.is_not(None),
        )

        if last_first_seen is not None:
            query = query.filter(
                or_(
                    Evidence.first_seen > last_first_seen,
                    and_(
                        Evidence.first_seen == last_first_seen,
                        Evidence.id > last_id,
                    ),
                )
            )

        evidence_rows = (
            query.order_by(
                Evidence.first_seen,
                Evidence.id,
            )
            .limit(TIMELINE_PAGE_SIZE)
            .all()
        )

        if not evidence_rows:
            break

        for evidence in evidence_rows:
            yield evidence

        last_evidence = evidence_rows[-1]

        last_first_seen = last_evidence.first_seen
        last_id = last_evidence.id
