from sqlalchemy.orm import Session
from app.services.identity_reader import stream_resolved_identities
from app.services.persistent_timeline import stream_identity_timeline


def process_persisted_timelines(
    db: Session,
    analysis_id: int,
) -> None:

    for identity in stream_resolved_identities(db, analysis_id):

        for evidence in stream_identity_timeline(
            db=db,
            analysis_id=analysis_id,
            resolved_identity=identity,
        ):
            # Correlation logic will consume each ordered
            # evidence record here in the next stage.
            pass