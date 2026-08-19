from pydantic import BaseModel, Field

class ProbableRootCause(BaseModel):
    title: str
    explanation: str
    evidence_ids: list[int] = Field(default_factory=list)


class SourceCodeFinding(BaseModel):
    file: str
    line_number: int | None = None
    function: str | None = None
    explanation: str
    evidence_ids: list[int] = Field(default_factory=list)


class GeminiInvestigationResponse(BaseModel):
    title: str
    summary: str
    probable_root_causes: list[ProbableRootCause] = Field(default_factory=list)
    what_happened: list[str] = Field(default_factory=list)
    source_code_findings: list[SourceCodeFinding] = Field(default_factory=list)
    recommended_actions: list[str] = Field(default_factory=list)
    uncertainties: list[str] = Field(default_factory=list)