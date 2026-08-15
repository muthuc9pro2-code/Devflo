from pydantic import BaseModel

class ImageInvestigationResponse(BaseModel):
    extracted_text: str
    