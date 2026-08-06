from datetime import datetime
from pydantic import BaseModel

class AnalysisResponse(BaseModel):
    id: int
    original_filename: str
    status: str
    created_at: datetime

    model_config = {
        "from_attribute": True
    }