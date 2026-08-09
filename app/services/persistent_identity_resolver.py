from dataclasses import dataclass

from app.models import Evidence


@dataclass(slots=True)
class EvidenceIdentity:
    identity: str
    match_type: str
    strength: float


def resolve_evidence_identity(
    evidence: Evidence,
) -> EvidenceIdentity:

    if evidence.trace_id:
        return EvidenceIdentity(
            identity=f"trace:{evidence.trace_id}",
            match_type="trace_id",
            strength=1.0,
        )

    if evidence.request_id:
        return EvidenceIdentity(
            identity=f"request:{evidence.request_id}",
            match_type="request_id",
            strength=0.9,
        )

    return EvidenceIdentity(
        identity=f"unresolved:{evidence.id}",
        match_type="unresolved",
        strength=0.0,
    )