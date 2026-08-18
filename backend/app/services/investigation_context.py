from __future__ import annotations
from typing import Any
from app.models.evidence import Evidence
from app.services.correlation_engine import CorrelationRun

def build_correlation_payload(
    correlation_run: CorrelationRun,
    evidence_rows: list[Evidence],
) -> dict[str, Any]:
    evidence_by_id = {
        evidence.id: evidence
        for evidence in evidence_rows
    }

    components: list[dict[str, Any]] = []

    for component_index, component in enumerate(
        correlation_run.result.components
    ):
        root_candidates = correlation_run.root_causes.get(
            component_index,
            [],
        )

        nodes: list[dict[str, Any]] = []

        for node in component.nodes:
            evidence = [
                evidence_by_id[evidence_id]
                for evidence_id in node.evidence_ids
                if evidence_id in evidence_by_id
            ]

            nodes.append(
                {
                    "id": node.id,
                    "artifact_id": node.artifact_id,
                    "service": node.service,
                    "fingerprint": node.fingerprint,
                    "first_seen": (
                        node.first_seen.isoformat()
                        if node.first_seen is not None
                        else None
                    ),
                    "last_seen": (
                        node.last_seen.isoformat()
                        if node.last_seen is not None
                        else None
                    ),
                    "occurrence_count": node.occurrence_count,
                    "trace_id": node.trace_id,
                    "request_id": node.request_id,
                    "span_id": node.span_id,
                    "parent_span_id": node.parent_span_id,
                    "resolved_identity": node.resolved_identity,
                    "evidence": [
                        _evidence_payload(item)
                        for item in evidence
                    ],
                }
            )

        components.append(
            {
                "id": component_index,
                "root_causes": [
                    {
                        "node_id": candidate.node_id,
                        "score": round(candidate.score, 4),
                        "role": candidate.role,
                    }
                    for candidate in root_candidates
                ],
                "nodes": nodes,
                "edges": [
                    {
                        "source_id": edge.source_id,
                        "target_id": edge.target_id,
                        "score": round(edge.score, 4),
                        "delta_ms": edge.delta_ms,
                        "signals": [
                            signal.value
                            for signal in edge.signals
                        ],
                    }
                    for edge in component.edges
                ],
            }
        )

    return {
        "analysis_id": correlation_run.result.analysis_id,
        "components": components,
    }

def build_simple_llm_context(
    analysis_id: int,
    evidence_rows: list[Evidence],
) -> dict[str, Any]:
    return {
        "analysis_id": analysis_id,
        "investigation_path": "simple",
        "instruction": (
            "Explain the available diagnostic evidence and suggest "
            "debugging/investigation steps. Do not invent evidence "
            "or claim a definitive fix."
        ),
        "evidence": [
            _evidence_payload(
                evidence,
                compact=True,
            )
            for evidence in evidence_rows
        ],
    }

def build_llm_context(
    correlation_run: CorrelationRun,
    evidence_rows: list[Evidence],
    *,
    roots_per_component: int = 3,
) -> dict[str, Any]:
    evidence_by_id = {
        evidence.id: evidence
        for evidence in evidence_rows
    }

    components: list[dict[str, Any]] = []

    for component_index, component in enumerate(
        correlation_run.result.components
    ):
        root_candidates = correlation_run.root_causes.get(
            component_index,
            [],
        )[:roots_per_component]

        selected_node_ids = {
            candidate.node_id
            for candidate in root_candidates
        }

        selected_evidence_ids: set[int] = set()

        for node in component.nodes:
            if node.id not in selected_node_ids:
                continue

            selected_evidence_ids.update(node.evidence_ids)

        components.append(
            {
                "component_id": component_index,
                "root_candidates": [
                    {
                        "node_id": candidate.node_id,
                        "root_cause_strength": round(
                            candidate.score,
                            4,
                        ),
                        "role": candidate.role,
                    }
                    for candidate in root_candidates
                ],
                "propagation": [
                    {
                        "source": edge.source_id,
                        "target": edge.target_id,
                        "strength": round(edge.score, 4),
                        "delta_ms": edge.delta_ms,
                        "signals": [
                            signal.value
                            for signal in edge.signals
                        ],
                    }
                    for edge in component.edges
                ],
                "root_evidence": [
                    _evidence_payload(
                        evidence_by_id[evidence_id],
                        compact=True,
                    )
                    for evidence_id in selected_evidence_ids
                    if evidence_id in evidence_by_id
                ],
            }
        )

    return {
        "analysis_id": correlation_run.result.analysis_id,
        "instruction": (
            "Explain the deterministic findings and suggest "
            "debugging/investigation steps. Do not invent evidence "
            "or claim a definitive fix."
        ),
        "components": components,
    }


def _evidence_payload(
    evidence: Evidence,
    *,
    compact: bool = False,
) -> dict[str, Any]:
    payload = {
        "id": evidence.id,
        "artifact_id": evidence.artifact_id,
        "event_type": evidence.event_type,
        "severity": evidence.severity,
        "service": evidence.service,
        "module": evidence.module,
        "endpoint": evidence.endpoint,
        "http_status": evidence.http_status,
        "source_format": evidence.source_format,
        "source_file": evidence.source_file,
        "first_line_number": evidence.first_line_number,
        "last_line_number": evidence.last_line_number,
        "representative_line": evidence.representative_line,
        "occurrence_count": evidence.occurrence_count,
        "source_matches": evidence.source_matches,
    }

    if not compact:
        payload.update(
            {
                "trace_id": evidence.trace_id,
                "request_id": evidence.request_id,
                "span_id": evidence.span_id,
                "parent_span_id": evidence.parent_span_id,
                "resolved_identity": evidence.resolved_identity,
                "identity_match_type": evidence.identity_match_type,
                "identity_strength": evidence.identity_strength,
            }
        )

    return payload