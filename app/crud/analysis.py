from sqlalchemy.orm import Session
from app.models.analysis import Analysis

def create_analysis(
        db: Session,
        user_id: int,
        filename: str,
        saved_file_path: str
) -> Analysis:
    analysis = Analysis(
        user_id=user_id,
        original_filename=filename,
        saved_file_path=saved_file_path
    )

    db.add(analysis)
    db.commit()
    db.refresh(analysis)

    return analysis

