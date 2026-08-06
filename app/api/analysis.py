from fastapi import APIRouter, Depends, File, UploadFile
from sqlalchemy.orm import Session

from app.crud.analysis import create_analysis
from app.db.database import get_db
from app.api.dependencies import get_current_verified_user
from app.models.user import User
from app.schemas.analysis import AnalysisResponse

from pathlib import Path
import shutil
import uuid

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

router = APIRouter(prefix="/analysis", tags=["Analysis"])

@router.post(
    "/upload",
    response_model=AnalysisResponse
)
def upload_file(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_verified_user)
):
    unique_filename = f"{uuid.uuid4()}_{file.filename}"
    file_path = UPLOAD_DIR / unique_filename

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    analysis = create_analysis(
        db=db,
        user_id=current_user.id,
        filename=file.filename,
        saved_file_path=str(file_path)
    )

    return analysis






