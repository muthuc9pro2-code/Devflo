from __future__ import annotations
from datetime import datetime, timezone
from typing import Any
from app.core.processing_config import (
    SIMPLE_LLM_MAX_CONTEXT_BYTES,
    SIMPLE_LLM_MAX_EVIDENCE_RECORDS,
)
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

# Reuses the same severity vocabulary normalize_level()/LEVEL_ALIASES
# already normalize evidence.severity into - not a new severity scale.
_SIMPLE_LLM_SEVERITY_RANK = {
    "CRITICAL": 3,
    "ERROR": 2,
    "WARNING": 1,
    "WARN": 1,
}
_DATETIME_MIN_UTC = datetime.min.replace(tzinfo=timezone.utc)


def _simple_llm_priority_key(evidence: Evidence) -> tuple:
    """Deterministic priority for SIMPLE-mode Gemini context selection -
    only already-computed signals (a real source-code match, severity,
    occurrence_count, then a stable timestamp/id tiebreak), no AI-based
    ranking and no second root-cause engine. Sorts ascending, so stronger
    candidates (True/higher numbers) are negated to sort first."""
    has_source_match = bool(evidence.source_matches)
    severity_rank = _SIMPLE_LLM_SEVERITY_RANK.get(
        (evidence.severity or "").upper(), 0
    )
    first_seen = evidence.first_seen
    if first_seen is not None and first_seen.tzinfo is None:
        first_seen = first_seen.replace(tzinfo=timezone.utc)
    return (
        0 if has_source_match else 1,
        -severity_rank,
        -(evidence.occurrence_count or 0),
        first_seen or _DATETIME_MIN_UTC,
        evidence.id,
    )


def _evidence_context_size_bytes(evidence: Evidence) -> int:
    """Approximate serialized size of one evidence row's free-text content
    in the Gemini context - the one part of _evidence_payload whose size is
    not already otherwise bounded (representative_line and source-match
    snippets can each be sizable free text)."""
    size = len((evidence.representative_line or "").encode("utf-8", errors="replace"))

    for match in evidence.source_matches or []:
        snippet = match.get("snippet") if isinstance(match, dict) else None
        if snippet:
            size += len(str(snippet).encode("utf-8", errors="replace"))

    return size


def _select_bounded_simple_evidence(
    evidence_rows: list[Evidence],
    *,
    max_records: int = SIMPLE_LLM_MAX_EVIDENCE_RECORDS,
    max_context_bytes: int = SIMPLE_LLM_MAX_CONTEXT_BYTES,
) -> list[Evidence]:
    """Deterministic, bounded evidence selection for build_simple_llm_context.
    Reused priorities only (source match > severity > occurrence_count >
    stable time/id order) - this is not a second root-cause engine, just a
    cap on what an uncorrelated investigation is allowed to hand to Gemini
    in one request. Round-robins across artifacts (in a stable, sorted
    artifact_id order) so one artifact with many rows cannot crowd out
    every other artifact's evidence merely by having more of it - this is
    the "artifact diversity" priority.
    """
    if len(evidence_rows) <= max_records:
        total_bytes = sum(_evidence_context_size_bytes(e) for e in evidence_rows)
        if total_bytes <= max_context_bytes:
            return evidence_rows

    by_artifact: dict[int, list[Evidence]] = {}
    for evidence in evidence_rows:
        by_artifact.setdefault(evidence.artifact_id, []).append(evidence)

    for rows in by_artifact.values():
        rows.sort(key=_simple_llm_priority_key)

    queues = {
        artifact_id: iter(rows)
        for artifact_id, rows in by_artifact.items()
    }
    active_artifact_ids = sorted(by_artifact.keys())

    selected: list[Evidence] = []
    selected_bytes = 0
    budget_reached = False

    while active_artifact_ids and not budget_reached:
        next_round: list[int] = []

        for artifact_id in active_artifact_ids:
            if len(selected) >= max_records or selected_bytes >= max_context_bytes:
                budget_reached = True
                break

            evidence = next(queues[artifact_id], None)
            if evidence is None:
                continue

            size = _evidence_context_size_bytes(evidence)
            # Always take at least one record if nothing has been selected
            # yet (a single oversized record must not empty the context
            # entirely) - otherwise stop adding once the byte budget would
            # be exceeded.
            if selected and selected_bytes + size > max_context_bytes:
                continue

            selected.append(evidence)
            selected_bytes += size
            next_round.append(artifact_id)

        active_artifact_ids = next_round

    return selected


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
                # Directed, causal relationships only - see
                # CorrelationComponent.edges' docstring in correlation_engine.py.
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
                # Non-directional "same incident" relationships (e.g. equal
                # timestamps, or a timestamp missing on either side) - real
                # correlation, but no real signal establishes which side
                # caused the other. node_a/node_b deliberately (not
                # source_id/target_id) so no direction is implied by the
                # field names themselves.
                "associations": [
                    {
                        "node_a": association.source_id,
                        "node_b": association.target_id,
                        "correlation_strength": round(association.score, 4),
                        "delta_ms": association.delta_ms,
                        "signals": [
                            signal.value
                            for signal in association.signals
                        ],
                    }
                    for association in component.associations
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
    # Bounded, deterministic selection (see _select_bounded_simple_evidence)
    # - the final frontend/database payload (build_simple_payload) still
    # carries every retained evidence row; only what reaches Gemini in one
    # request is capped.
    selected_evidence = _select_bounded_simple_evidence(evidence_rows)
    truncated = len(selected_evidence) < len(evidence_rows)

    instruction = (
        "Explain the available diagnostic evidence and suggest "
        "debugging/investigation steps. Do not invent evidence "
        "or claim a definitive fix."
    )
    if truncated:
        instruction += (
            " Only the most diagnostically relevant evidence records "
            "(evidence_count_included of evidence_count_total) are "
            "included below - do not assume this is the complete set."
        )

    context: dict[str, Any] = {
        "analysis_id": analysis_id,
        "investigation_path": "simple",
        "instruction": instruction,
        "evidence_count_total": len(evidence_rows),
        "evidence_count_included": len(selected_evidence),
        "evidence": [
            _evidence_payload(evidence)
            for evidence in selected_evidence
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
                # Directed, causal relationships only - never an equal-
                # timestamp/unknown-timestamp association (see associations
                # below). Gemini must not be told an association is
                # propagation.
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
                # Same-incident relationships with no established direction
                # (equal timestamps, or a timestamp missing on either side) -
                # kept separate from "propagation" so Gemini can describe
                # these as corroborating/associated evidence rather than a
                # causal chain.
                "associations": [
                    {
                        "node_a": association.source_id,
                        "node_b": association.target_id,
                        "correlation_strength": round(association.score, 4),
                        "delta_ms": association.delta_ms,
                        "signals": [
                            signal.value
                            for signal in association.signals
                        ],
                    }
                    for association in component.associations
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
