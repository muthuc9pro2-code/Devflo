from sqlalchemy.orm import Session
from app.services.persistent_timeline import stream_timeline_evidence

def process_persisted_timelines(
    db: Session,
    analysis_id: int,
) -> None:

    for evidence in stream_timeline_evidence(
        db=db,
        analysis_id=analysis_id,
    ):
        # Timeline/correlation logic will be added here.
        pass