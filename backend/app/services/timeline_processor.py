from sqlalchemy.orm import Session

def process_persisted_timelines(
        db: Session,
        analysis_id: int,
) -> None:
    return 