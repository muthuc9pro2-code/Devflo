from __future__ import annotations
from typing import Any
from app.models.evidence import Evidence
from app.services.correlation_engine import CorrelationRun

# Zero retained evidence only proves Devflo did not extract meaningful
# diagnostic evidence from that artifact/analysis under the existing
# evidence rules - it must never be read as "this artifact/file is
# unrelated to the incident", corrupted, unsupported, or invalid.
_NO_EVIDENCE_MESSAGE = (
    "No meaningful diagnostic evidence was extracted from this artifact."
)
_NO_EVIDENCE_ANALYSIS_MESSAGE = "No meaningful diagnostic evidence found"
_UNSUPPORTED_MESSAGE = "This file type is not supported for diagnostic analysis."


def _count_evidence_by_artifact(evidence_rows: list[Evidence]) -> dict[int, int]:
    counts: dict[int, int] = {}
    for evidence in evidence_rows:
        counts[evidence.artifact_id] = counts.get(evidence.artifact_id, 0) + 1
    return counts


def _artifacts_outcome_list(
    artifacts: list[Any],
    evidence_counts_by_artifact: dict[int, int],
) -> list[dict[str, Any]]:
    # The same already-fetched artifacts list already contains the
    # canonical artifact a duplicate points to, so its filename can be
    # resolved here with no extra query.
    filename_by_artifact_id = {
        artifact.id: artifact.original_filename for artifact in artifacts
    }
    return [
        build_artifact_outcome_payload(
            artifact,
            evidence_counts_by_artifact.get(artifact.id, 0),
            filename_by_artifact_id,
        )
        for artifact in artifacts
    ]


def build_correlation_payload(
    correlation_run: CorrelationRun,
    evidence_rows: list[Evidence],
    artifacts: list[Any] | None = None,
) -> dict[str, Any]:
    evidence_by_id = {
        evidence.id: evidence
        for evidence in evidence_rows
    }
    evidence_counts_by_artifact = _count_evidence_by_artifact(evidence_rows)

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
                    "event_type": node.event_type,
                    "severity": node.severity,
                    "module": node.module,
                    "endpoint": node.endpoint,
                    "http_status": node.http_status,
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
                    "source_file": node.source_file,
                    "source_format": node.source_format,
                    "identity_match_type": node.identity_match_type,
                    "identity_strength": node.identity_strength,
                    "source_matches": node.source_matches,
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
                        "root_cause_strength": round(candidate.score, 4),
                        "role": candidate.role,
                        "graph_stats": {
                            "incoming_count": candidate.graph_stats.incoming_count,
                            "outgoing_count": candidate.graph_stats.outgoing_count,
                            "downstream_count": candidate.graph_stats.downstream_count,
                            "incoming_strength": round(
                                candidate.graph_stats.incoming_strength, 4
                            ),
                            "outgoing_strength": round(
                                candidate.graph_stats.outgoing_strength, 4
                            ),
                        },
                    }
                    for candidate in root_candidates
                ],
                "nodes": nodes,
                "edges": [
                    {
                        "source_id": edge.source_id,
                        "target_id": edge.target_id,
                        "correlation_strength": round(edge.score, 4),
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

    payload: dict[str, Any] = {
        "analysis_id": correlation_run.result.analysis_id,
        "investigation_path": "correlated",
        "evidence_count": len(evidence_rows),
        "component_count": len(correlation_run.result.components),
        # Distinct artifacts represented in RETAINED evidence - not the total
        # number of artifacts processed for this analysis. An artifact that
        # was processed but produced zero retained evidence contributes no
        # rows to evidence_rows and so is not counted here; see `artifacts`
        # (when provided) for the full per-artifact outcome, including
        # zero-evidence artifacts.
        "evidence_artifact_count": len(evidence_counts_by_artifact),
        "components": components,
    }

    if artifacts is not None:
        payload["artifacts"] = _artifacts_outcome_list(
            artifacts,
            evidence_counts_by_artifact,
        )

    return payload


def build_simple_payload(
    analysis_id: int,
    evidence_rows: list[Evidence],
    *,
    artifacts: list[Any] | None = None,
) -> dict[str, Any]:
    """Final frontend result for the SIMPLE investigation path - real
    retained evidence and (optionally) per-artifact outcomes only, never a
    fabricated components/edges/correlation-strength graph (that shape only
    exists where the deterministic correlation engine actually ran)."""
    evidence_counts_by_artifact = _count_evidence_by_artifact(evidence_rows)

    payload: dict[str, Any] = {
        "analysis_id": analysis_id,
        "investigation_path": "simple",
        "evidence_count": len(evidence_rows),
        "evidence_artifact_count": len(evidence_counts_by_artifact),
        "evidence": [
            _evidence_payload(evidence)
            for evidence in evidence_rows
        ],
    }

    if artifacts is not None:
        payload["artifacts"] = _artifacts_outcome_list(
            artifacts,
            evidence_counts_by_artifact,
        )

    return payload


def build_zero_evidence_payload(
    analysis_id: int,
    *,
    artifacts: list[Any] | None = None,
) -> dict[str, Any]:
    """Final frontend result when the WHOLE analysis retained zero evidence:
    processing completed successfully, it just found nothing meaningful -
    never framed as failure/unsupported/unrelated. Every artifact here is
    necessarily zero-evidence too (evidence_count for the whole analysis is
    0), so each entry in `artifacts` (when provided) carries the same
    neutral per-artifact message as build_correlation_payload/
    build_simple_payload would for a zero-evidence artifact."""
    payload: dict[str, Any] = {
        "analysis_id": analysis_id,
        "investigation_path": "zero_evidence",
        "evidence_count": 0,
        "evidence_artifact_count": 0,
        "message": _NO_EVIDENCE_ANALYSIS_MESSAGE,
    }

    if artifacts is not None:
        payload["artifacts"] = _artifacts_outcome_list(artifacts, {})

    return payload


def build_simple_llm_context(
    analysis_id: int,
    evidence_rows: list[Evidence],
    *,
    artifacts: list[Any] | None = None,
) -> dict[str, Any]:
    context: dict[str, Any] = {
        "analysis_id": analysis_id,
        "investigation_path": "simple",
        "instruction": (
            "Explain the available diagnostic evidence and suggest "
            "debugging/investigation steps. Do not invent evidence "
            "or claim a definitive fix."
        ),
        "evidence": [
            _evidence_payload(evidence)
            for evidence in evidence_rows
        ],
    }

    if artifacts is not None:
        context["artifacts"] = _artifacts_outcome_list(
            artifacts,
            _count_evidence_by_artifact(evidence_rows),
        )

    return context

def build_llm_context(
    correlation_run: CorrelationRun,
    evidence_rows: list[Evidence],
    *,
    roots_per_component: int = 3,
    max_additional_source_matched_evidence: int = 3,
    artifacts: list[Any] | None = None,
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

        # Root-cause ranking itself is untouched above - this only widens
        # what is allowed to reach Gemini's bounded context. A node with a
        # genuine deterministic source-code match (evidence.source_matches)
        # must not be silently invisible to Gemini merely because
        # correlation/root-cause scoring ranked it outside the top
        # roots_per_component candidates (e.g. a valid srv/worker.py:42
        # match on a 4th-ranked node). Bounded to a small, fixed number of
        # additional evidence rows per component - never the whole
        # component/graph.
        additional_selected = 0
        if max_additional_source_matched_evidence > 0:
            for node in component.nodes:
                if node.id in selected_node_ids:
                    continue
                if additional_selected >= max_additional_source_matched_evidence:
                    break
                for evidence_id in node.evidence_ids:
                    if evidence_id in selected_evidence_ids:
                        continue
                    evidence = evidence_by_id.get(evidence_id)
                    if evidence is not None and evidence.source_matches:
                        selected_evidence_ids.add(evidence_id)
                        additional_selected += 1
                        break

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
                        "correlation_strength": round(edge.score, 4),
                        "delta_ms": edge.delta_ms,
                        "signals": [
                            signal.value
                            for signal in edge.signals
                        ],
                    }
                    for edge in component.edges
                ],
                "root_evidence": [
                    _evidence_payload(evidence_by_id[evidence_id])
                    for evidence_id in selected_evidence_ids
                    if evidence_id in evidence_by_id
                ],
            }
        )

    context: dict[str, Any] = {
        "analysis_id": correlation_run.result.analysis_id,
        "investigation_path": "correlated",
        "instruction": (
            "Explain the deterministic findings and suggest "
            "debugging/investigation steps. Do not invent evidence "
            "or claim a definitive fix."
        ),
        "components": components,
    }

    if artifacts is not None:
        # Whole-analysis provenance summary ("nginx.log produced 8 evidence
        # rows") so Gemini can refer to findings by their real source
        # artifact - deliberately NOT scoped to just the selected root
        # evidence above; this is a small, bounded, one-row-per-artifact
        # list, not a raw-line dump.
        context["artifacts"] = _artifacts_outcome_list(
            artifacts,
            _count_evidence_by_artifact(evidence_rows),
        )

    return context


def _evidence_payload(evidence: Evidence) -> dict[str, Any]:
    return {
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
        "first_seen": (
            evidence.first_seen.isoformat()
            if evidence.first_seen is not None
            else None
        ),
        "last_seen": (
            evidence.last_seen.isoformat()
            if evidence.last_seen is not None
            else None
        ),
        "trace_id": evidence.trace_id,
        "request_id": evidence.request_id,
        "span_id": evidence.span_id,
        "parent_span_id": evidence.parent_span_id,
        "resolved_identity": evidence.resolved_identity,
        "identity_match_type": evidence.identity_match_type,
        "identity_strength": evidence.identity_strength,
        # Real RapidOCR confidence, preserved as-is - None for anything
        # that isn't source_format="image" evidence, never fabricated.
        "ocr_confidence": evidence.ocr_confidence,
    }


def build_artifact_outcome_payload(
    artifact: Any,
    evidence_count: int,
    filename_by_artifact_id: dict[int, str] | None = None,
) -> dict[str, Any]:
    status = artifact.status

    if status == "unsupported":
        # Genuinely unsupported by the existing artifact-format policy
        # (rejected before ingestion, at upload time) - distinct from a
        # supported artifact that simply retained zero evidence.
        return {
            "artifact_id": artifact.id,
            "source_file": artifact.original_filename,
            "source_format": artifact.detected_format,
            "evidence_count": 0,
            "status": "unsupported",
            "message": _UNSUPPORTED_MESSAGE,
        }

    duplicate_of_artifact_id = getattr(artifact, "duplicate_of_artifact_id", None)

    if status == "duplicate" and duplicate_of_artifact_id is not None:
        duplicate_of_source_file = (filename_by_artifact_id or {}).get(
            duplicate_of_artifact_id
        )
        return {
            "artifact_id": artifact.id,
            "source_file": artifact.original_filename,
            "source_format": artifact.detected_format,
            "evidence_count": 0,
            "status": "duplicate",
            "duplicate_of_artifact_id": duplicate_of_artifact_id,
            "duplicate_of_source_file": duplicate_of_source_file,
            "message": (
                f"Duplicate of {duplicate_of_source_file}; not processed "
                "as independent evidence."
                if duplicate_of_source_file
                else "Duplicate of another uploaded artifact; not "
                "processed as independent evidence."
            ),
        }

    payload: dict[str, Any] = {
        "artifact_id": artifact.id,
        "source_file": artifact.original_filename,
        "source_format": artifact.detected_format,
        "evidence_count": evidence_count,
        # Every artifact reaching this point has already been fully ingested
        # (finalize only runs once all artifacts are "completed"); "processed"
        # names that outcome without leaking the internal ingestion-status
        # vocabulary into the frontend contract.
        "status": "processed" if status == "completed" else status,
    }

    if evidence_count == 0:
        payload["message"] = _NO_EVIDENCE_MESSAGE

    return payload
