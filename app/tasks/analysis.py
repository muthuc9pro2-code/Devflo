from app.core.celery_app import celery_app
from app.db.database import sessionLocal
from app.models.analysis import Analysis

@celery_app.task
def process_analysis(analysis_id: int):

    db = sessionLocal()

    try:
        analysis = db.query(Analysis).filter(
            Analysis.id == analysis_id
        ).first()

        if analysis is None:
            print(f"processing analysis {analysis_id} not found")
            return 

        analysis.status = "processing"
        db.commit()

        print(f"Processing analysis {analysis_id}")

        import time
        time.sleep(5)

        analysis.status = "completed"
        db.commit()

    finally: 
        db.close()