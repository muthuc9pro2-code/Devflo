from datetime import datetime

from pydantic import BaseModel


class AnalysisResponse(BaseModel):
    id: int
    original_filename: str
    status: str
    created_at: datetime

    model_config = {"from_attributes": True}


class AnalysisHistoryItem(BaseModel):
    """One History row - deliberately narrow: never saved_file_path,
    source_reference, raw evidence, the correlation graph, or the complete
    Gemini payload. title/summary are Gemini's own (already bounded)
    output when available, never a persisted/fabricated short title."""

    analysis_id: int
    status: str
    created_at: datetime
    updated_at: datetime
    artifact_count: int
    original_filename: str
    title: str | None = None
    summary: str | None = None


class AnalysisHistoryPage(BaseModel):
    items: list[AnalysisHistoryItem]
    next_cursor: str | None = None
