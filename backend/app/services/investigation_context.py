from __future__ import annotations
import heapq
import json
from datetime import datetime, timezone
from typing import Any
from sqlalchemy import func
from sqlalchemy.orm import Session
from app.core.processing_config import (
    BOUNDED_SELECTION_MAX_AGGREGATE_GROUPS,
    CORRELATED_GEMINI_CONTEXT_MAX_BYTES,
    SIMPLE_FALLBACK_MAX_TOTAL_CONTEXT_BYTES,
    SIMPLE_FRONTEND_MAX_CONTEXT_BYTES,
    SIMPLE_FRONTEND_MAX_EVIDENCE_RECORDS,
    SIMPLE_LLM_MAX_CONTEXT_BYTES,
    SIMPLE_LLM_MAX_EVIDENCE_RECORDS,
)
from app.models.evidence import Evidence
from app.services.correlation_engine import CorrelationRun
from app.services.timeline_processor import build_component_timeline

# Zero retained evidence only proves Devflo did not extract meaningful
# diagnostic evidence from that artifact/analysis under the existing
# evidence rules - it must never be read as "this artifact/file is
# unrelated to the incident", corrupted, unsupported, or invalid.
_NO_EVIDENCE_MESSAGE = (
    "No meaningful diagnostic evidence was extracted from this artifact."
)
_NO_EVIDENCE_ANALYSIS_MESSAGE = "No meaningful diagnostic evidence found"
_UNSUPPORTED_MESSAGE = "This file type is not supported for diagnostic analysis."
# Section 14: deliberately "not linked" rather than "unrelated" - Devflo
# only knows this artifact's real, retained evidence never deterministically
# connected into the correlated incident, not that the artifact itself is
# unrelated (it may still be a genuine, valid diagnostic artifact).
_NOT_LINKED_MESSAGE = "Not linked to the correlated incident evidence."
_PARTIALLY_LINKED_MESSAGE = (
    "Some of this artifact's evidence is linked to the correlated incident; "
    "the rest is not."
)
# Section 2: an artifact that retained zero STRUCTURED Evidence but did
# capture a useful fallback_context (see fallback_context.py) during its
# own ingestion pass is a materially different outcome from one with
# nothing useful at all - never call this "no meaningful diagnostic
# evidence", and never claim it was deterministically linked/unlinked to
# anything (relationship_status "low_structure" is neither "linked" nor
# "not_linked" - it was never structured enough to attempt linking).
_LOW_STRUCTURE_MESSAGE = (
    "Diagnostic content was extracted, but it could not be "
    "deterministically structured/linked."
)

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


def select_bounded_evidence(
    evidence_rows: list[Evidence],
    *,
    max_records: int = SIMPLE_LLM_MAX_EVIDENCE_RECORDS,
    max_context_bytes: int = SIMPLE_LLM_MAX_CONTEXT_BYTES,
) -> list[Evidence]:
    """Deterministic, bounded evidence selection - used both for
    build_simple_llm_context's Gemini bound and (Section 20) as the large-
    investigation safety net before CORRELATED even runs. Reused
    priorities only (source match > severity > occurrence_count > stable
    time/id order) - this is not a second root-cause engine, just a cap on
    how much evidence a single request/graph is allowed to carry.
    Round-robins across artifacts (in a stable, sorted artifact_id order)
    so one artifact with many rows cannot crowd out every other artifact's
    evidence merely by having more of it - this is the "artifact
    diversity" priority.
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


_SEVERITY_PRIORITY_RANK = {"CRITICAL": 4, "FATAL": 4, "ERROR": 3, "WARNING": 2, "WARN": 2}
# Bounded batch size for the streaming keyset scan below - large enough to
# make few round trips, small enough that memory stays O(max_records +
# this), never O(total Evidence rows).
_BOUNDED_EVIDENCE_SCAN_BATCH_SIZE = 2000


def _bounded_evidence_priority(
    evidence: Evidence,
    *,
    fingerprint_counts: dict[str, int],
    bridging_trace_ids: set[str],
    bridging_request_ids: set[str],
) -> float:
    """Section 4: a single-row, single-pass priority score (no second
    root-cause engine) biased toward the same signals that actually build
    incident structure downstream in correlation_engine.py, so bounded
    selection is more likely to preserve a coherent incident rather than
    just "whatever happened to be most severe"."""
    score = 0.0
    if evidence.span_id is not None or evidence.parent_span_id is not None:
        score += 5.0  # actual parent-span participant (potential)

    is_trace_bridge = evidence.trace_id is not None and evidence.trace_id in bridging_trace_ids
    is_request_bridge = evidence.request_id is not None and evidence.request_id in bridging_request_ids
    # persist_resolved_identities() writes resolved_identity="unresolved:
    # <this row's own id>"/identity_match_type="unresolved"/
    # identity_strength=0.0 for a row with neither a real trace_id nor
    # request_id (identity_persister.py). That value can never match any
    # OTHER row's resolved_identity - it is provenance, not a correlation
    # signal, and must not receive the identity-bearing bonus below.
    has_real_resolved_identity = (
        evidence.resolved_identity is not None
        and evidence.identity_match_type != "unresolved"
        and (evidence.identity_strength or 0) > 0
    )

    if is_trace_bridge or is_request_bridge:
        score += 4.0  # cross-artifact identity bridge evidence
    elif evidence.trace_id is not None or evidence.request_id is not None or has_real_resolved_identity:
        score += 3.0  # trace/request/identity-bearing evidence

    score += _SEVERITY_PRIORITY_RANK.get((evidence.severity or "").upper(), 0)
    if evidence.source_matches:
        score += 2.0  # source-code matched evidence
    fingerprint_count = fingerprint_counts.get(evidence.fingerprint, 1) if evidence.fingerprint else 1
    score += 1.0 / fingerprint_count  # rare/unique fingerprints score higher
    return score


def select_bounded_evidence_from_db(
    db: Session,
    *,
    analysis_id: int,
    max_records: int,
    max_context_bytes: int,
    batch_size: int = _BOUNDED_EVIDENCE_SCAN_BATCH_SIZE,
) -> tuple[list[Evidence], int]:
    """Section 4: genuinely end-to-end bounded evidence selection for a
    pathological single analysis with far more Evidence rows than
    max_records. Never loads the whole Evidence result set into memory
    merely to truncate it afterward - reads the table in bounded batches
    (keyset-paginated by id) and maintains only a bounded max_records-sized
    working set (a small max-heap) plus three small, capped
    (BOUNDED_SELECTION_MAX_AGGREGATE_GROUPS) aggregate lookups (repeated
    fingerprints, cross-artifact trace/request-id bridges), so memory stays
    O(max_records + batch_size + BOUNDED_SELECTION_MAX_AGGREGATE_GROUPS),
    never O(total Evidence rows or total distinct fingerprints/identities).
    Evidence itself is never touched/deleted in MySQL either way - this
    only bounds what one CORRELATED/SIMPLE graph/payload has to build.

    Returns (selected_rows, total_count) - total_count is the real,
    unbounded count for the whole analysis (never silently understated by
    the caller).
    """
    total_count = (
        db.query(func.count(Evidence.id))
        .filter(Evidence.analysis_id == analysis_id)
        .scalar()
        or 0
    )

    if total_count <= max_records:
        rows = (
            db.query(Evidence)
            .filter(Evidence.analysis_id == analysis_id)
            .order_by(Evidence.first_seen, Evidence.id)
            .all()
        )

        rows = select_bounded_evidence(
            rows,
            max_records=max_records,
            max_context_bytes=max_context_bytes,
        )
        rows.sort(
            key=lambda row: (
                row.first_seen or _DATETIME_MIN_UTC,
                row.id,
            )
        )
        return rows, total_count

    # Small, bounded (O(BOUNDED_SELECTION_MAX_AGGREGATE_GROUPS), never
    # O(distinct values in the table)) aggregate passes that inform
    # per-row scoring without a second full materialize. Only repeated
    # fingerprints need an exact penalty (a singleton already defaults to
    # count=1 via fingerprint_counts.get(..., 1) below) - HAVING count > 1
    # keeps the query itself from ever returning every distinct
    # fingerprint, and ordering by count DESC before capping keeps the
    # MOST-repeated fingerprints (the ones the repeat penalty matters most
    # for) when a pathological analysis has more distinct repeated
    # fingerprints than the cap.
    fingerprint_counts = dict(
        db.query(Evidence.fingerprint, func.count(Evidence.id))
        .filter(Evidence.analysis_id == analysis_id, Evidence.fingerprint.isnot(None))
        .group_by(Evidence.fingerprint)
        .having(func.count(Evidence.id) > 1)
        .order_by(func.count(Evidence.id).desc(), Evidence.fingerprint.asc())
        .limit(BOUNDED_SELECTION_MAX_AGGREGATE_GROUPS)
        .all()
    )

    def _bridging_ids(column) -> set[str]:
        distinct_artifacts = func.count(func.distinct(Evidence.artifact_id))
        rows = (
            db.query(column, distinct_artifacts)
            .filter(Evidence.analysis_id == analysis_id, column.isnot(None))
            .group_by(column)
            .having(distinct_artifacts > 1)
            .order_by(distinct_artifacts.desc(), column.asc())
            .limit(BOUNDED_SELECTION_MAX_AGGREGATE_GROUPS)
            .all()
        )
        return {value for value, _artifact_count in rows}

    bridging_trace_ids = _bridging_ids(Evidence.trace_id)
    bridging_request_ids = _bridging_ids(Evidence.request_id)

    # Stable temporal boundary/ordering: the earliest/latest few records by
    # first_seen are always reserved a seat regardless of score - cheap and
    # index-backed (ix_evidence_analysis_first_seen_id), O(boundary_reserve)
    # only, never a full-table sort.
    boundary_reserve = min(50, max(max_records // 20, 1))
    boundary_rows = (
        db.query(Evidence)
        .filter(Evidence.analysis_id == analysis_id, Evidence.first_seen.isnot(None))
        .order_by(Evidence.first_seen.asc(), Evidence.id.asc())
        .limit(boundary_reserve)
        .all()
    ) + (
        db.query(Evidence)
        .filter(Evidence.analysis_id == analysis_id, Evidence.first_seen.isnot(None))
        .order_by(Evidence.first_seen.desc(), Evidence.id.desc())
        .limit(boundary_reserve)
        .all()
    )
    reserved_by_id: dict[int, Evidence] = {row.id: row for row in boundary_rows}
    remaining_budget = max(max_records - len(reserved_by_id), 0)

    heap: list[tuple[float, int, Evidence]] = []  # (score, tiebreak, row) min-heap
    selected_count_by_artifact: dict[int, int] = {}
    tiebreak_counter = 0
    last_id = 0

    while True:
        batch = (
            db.query(Evidence)
            .filter(Evidence.analysis_id == analysis_id, Evidence.id > last_id)
            .order_by(Evidence.id)
            .limit(batch_size)
            .all()
        )
        if not batch:
            break
        last_id = batch[-1].id

        for row in batch:
            if row.id in reserved_by_id:
                continue
            if remaining_budget <= 0:
                continue
            diversity_penalty = 0.02 * selected_count_by_artifact.get(row.artifact_id, 0)
            score = (
                _bounded_evidence_priority(
                    row,
                    fingerprint_counts=fingerprint_counts,
                    bridging_trace_ids=bridging_trace_ids,
                    bridging_request_ids=bridging_request_ids,
                )
                - diversity_penalty
            )
            tiebreak_counter += 1
            if len(heap) < remaining_budget:
                heapq.heappush(heap, (score, tiebreak_counter, row))
                selected_count_by_artifact[row.artifact_id] = (
                    selected_count_by_artifact.get(row.artifact_id, 0) + 1
                )
            elif score > heap[0][0]:
                _evicted_score, _evicted_tiebreak, evicted_row = heapq.heapreplace(
                    heap, (score, tiebreak_counter, row)
                )
                selected_count_by_artifact[evicted_row.artifact_id] -= 1
                selected_count_by_artifact[row.artifact_id] = (
                    selected_count_by_artifact.get(row.artifact_id, 0) + 1
                )

        if len(batch) < batch_size:
            break

    # Final byte-budget assembly: reserved (boundary) rows always included,
    # everything else greedily by score until either max_records or
    # max_context_bytes is hit - the same "always take at least one" rule
    # select_bounded_evidence uses so a single oversized record can never
    # empty the selection entirely.
    scored_candidates: list[tuple[float, Evidence]] = [
        (float("inf"), row) for row in reserved_by_id.values()
    ] + [(score, row) for score, _tiebreak, row in heap]
    scored_candidates.sort(key=lambda item: item[0], reverse=True)

    selected: list[Evidence] = []
    selected_bytes = 0
    for _score, row in scored_candidates:
        if len(selected) >= max_records:
            break
        size = _evidence_context_size_bytes(row)
        if selected and selected_bytes + size > max_context_bytes:
            continue
        selected.append(row)
        selected_bytes += size

    selected.sort(key=lambda row: (row.first_seen or _DATETIME_MIN_UTC, row.id))
    return selected, total_count


def select_evidence_counts_by_artifact(
    db: Session,
    *,
    analysis_id: int,
) -> dict[int, int]:
    """REAL per-artifact Evidence counts from MySQL, independent of any
    bounded working-set selection (select_bounded_evidence_from_db) - a
    bounded 5000/20MiB working set may not include every row from every
    artifact, so deriving "how much Evidence does this artifact really
    have" (or worse, "does this artifact have any structured Evidence at
    all") from that subset alone would understate/misreport it. Bounded by
    MAX_DIAGNOSTIC_ARTIFACTS (at most one GROUP BY row per artifact in this
    analysis), so materializing this result is always safe."""
    rows = (
        db.query(Evidence.artifact_id, func.count(Evidence.id))
        .filter(Evidence.analysis_id == analysis_id)
        .group_by(Evidence.artifact_id)
        .all()
    )
    return dict(rows)


def _count_evidence_by_artifact(evidence_rows: list[Evidence]) -> dict[int, int]:
    counts: dict[int, int] = {}
    for evidence in evidence_rows:
        counts[evidence.artifact_id] = counts.get(evidence.artifact_id, 0) + 1
    return counts


def _artifacts_outcome_list(
    artifacts: list[Any],
    evidence_counts_by_artifact: dict[int, int],
    *,
    relationship_status_by_artifact: dict[int, str] | None = None,
) -> list[dict[str, Any]]:
    # The same already-fetched artifacts list already contains the
    # canonical artifact a duplicate points to, so its filename can be
    # resolved here with no extra query.
    filename_by_artifact_id = {
        artifact.id: artifact.original_filename for artifact in artifacts
    }
    relationship_status_by_artifact = relationship_status_by_artifact or {}
    return [
        build_artifact_outcome_payload(
            artifact,
            evidence_counts_by_artifact.get(artifact.id, 0),
            filename_by_artifact_id,
            relationship_status=relationship_status_by_artifact.get(artifact.id),
        )
        for artifact in artifacts
    ]


def _select_primary_component(components: list[Any]) -> Any | None:
    """The single PRIMARY correlated incident - the component with the
    most distinct artifacts represented (a genuine cross-artifact
    incident), falling back to whichever component has the most nodes when
    no component spans more than one artifact (e.g. correlation triggered
    only by a same-artifact burst) - deterministic, ties broken by
    component order. A component of a single node establishes nothing, so
    if even the largest component is a singleton, there is no primary
    incident at all (None) - every artifact with real evidence is equally
    "not part of anything larger". Shared by _artifact_relationship_statuses
    (frontend not_linked/partially_linked/linked labeling) and
    build_llm_context (Section 3/not_linked isolation: only the primary
    component's substantive content ever reaches Gemini's primary
    reasoning) so the two can never define "primary" differently.
    """
    if not components:
        return None

    def _component_key(component: Any) -> tuple[int, int]:
        artifact_ids = {
            node.artifact_id for node in component.nodes if node.artifact_id is not None
        }
        return (len(artifact_ids), len(component.nodes))

    primary = max(components, key=_component_key)
    if len(primary.nodes) <= 1:
        return None
    return primary


def _artifact_relationship_statuses(
    components: list[Any],
) -> dict[int, str]:
    """Section 14: per-artifact relationship to the PRIMARY correlated
    incident - linked / partially_linked / not_linked (no_evidence is
    handled separately in build_artifact_outcome_payload, before this is
    even consulted).
    """
    primary = _select_primary_component(components)
    if primary is None:
        return {}

    primary_nodes = primary.nodes

    primary_node_counts_by_artifact: dict[int, int] = {}
    for node in primary_nodes:
        if node.artifact_id is not None:
            primary_node_counts_by_artifact[node.artifact_id] = (
                primary_node_counts_by_artifact.get(node.artifact_id, 0) + 1
            )

    total_node_counts_by_artifact: dict[int, int] = {}
    for component in components:
        for node in component.nodes:
            if node.artifact_id is not None:
                total_node_counts_by_artifact[node.artifact_id] = (
                    total_node_counts_by_artifact.get(node.artifact_id, 0) + 1
                )

    statuses: dict[int, str] = {}
    for artifact_id, total_count in total_node_counts_by_artifact.items():
        linked_count = primary_node_counts_by_artifact.get(artifact_id, 0)
        if linked_count == 0:
            statuses[artifact_id] = "not_linked"
        elif linked_count == total_count:
            statuses[artifact_id] = "linked"
        else:
            statuses[artifact_id] = "partially_linked"

    return statuses


def build_correlation_payload(
    correlation_run: CorrelationRun,
    evidence_rows: list[Evidence],
    artifacts: list[Any] | None = None,
    *,
    total_evidence_count: int | None = None,
    evidence_counts_by_artifact: dict[int, int] | None = None,
    supplemental_artifacts: list[Any] | None = None,
) -> dict[str, Any]:
    """total_evidence_count is the REAL, un-bounded evidence count for the
    whole analysis (Section 20) - pass it whenever the caller may have
    already selected a bounded subset of evidence_rows before correlating
    (evidence itself is never deleted, only what one graph/payload has to
    carry is capped). When omitted, or equal to len(evidence_rows), no
    truncation occurred and evidence_count_total/evidence_truncated are
    simply not exposed.

    evidence_counts_by_artifact (Section 4 hardening) is the REAL
    per-artifact count for the whole analysis (select_evidence_counts_by_
    artifact) - pass it for the same reason: a bounded working set of
    evidence_rows may not include every row from every artifact, so
    deriving evidence_artifact_count/artifacts[].evidence_count from
    evidence_rows alone would understate them. When omitted, falls back to
    counting evidence_rows itself (existing direct callers/tests unchanged).
    """
    evidence_by_id = {
        evidence.id: evidence
        for evidence in evidence_rows
    }
    real_evidence_counts_by_artifact = (
        evidence_counts_by_artifact
        if evidence_counts_by_artifact is not None
        else _count_evidence_by_artifact(evidence_rows)
    )

    components: list[dict[str, Any]] = []
    total_component_count = len(correlation_run.result.components)

    # The frontend correlated graph represents the PRIMARY incident only -
    # the exact same _select_primary_component() definition already used
    # for relationship-status labeling (linked/partially_linked/not_linked)
    # and Gemini isolation, so "primary" can never mean something different
    # here. Any other component (isolated singleton or a smaller, genuinely
    # correlated but non-primary group) is excluded from components[] -
    # its artifacts remain fully visible via artifacts[] with
    # relationship_status="not_linked"/"partially_linked", and its Evidence
    # remains fully persisted; it simply never renders as extra incident
    # structure. A "partially_linked" artifact naturally keeps only the
    # subset of its nodes that belong to the primary component, since a
    # node can only ever live in the one component it was actually
    # assigned to.
    primary_component = _select_primary_component(correlation_run.result.components)
    primary_entry: tuple[int, Any] | None = None
    if primary_component is not None:
        for index, component in enumerate(correlation_run.result.components):
            if component is primary_component:
                primary_entry = (index, component)
                break

    for component_index, component in ([primary_entry] if primary_entry is not None else []):
        root_candidates = correlation_run.root_causes.get(
            component_index,
            [],
        )
        candidate_by_node_id = {
            candidate.node_id: candidate for candidate in root_candidates
        }

        nodes: list[dict[str, Any]] = []

        for node in component.nodes:
            evidence = [
                evidence_by_id[evidence_id]
                for evidence_id in node.evidence_ids
                if evidence_id in evidence_by_id
            ]
            candidate = candidate_by_node_id.get(node.id)

            nodes.append(
                {
                    "id": node.id,
                    "artifact_id": node.artifact_id,
                    # Same role/score already computed for root_causes[]
                    # below, attached here too so the frontend does not
                    # have to cross-reference a second array just to know
                    # what this node IS (root/propagation/victim/
                    # uncorrelated) - nodes remain the one canonical detail
                    # object; root_causes[] stays the ranked view.
                    "role": candidate.role if candidate is not None else "uncorrelated",
                    "root_cause_strength": (
                        round(candidate.score, 4) if candidate is not None else 0.0
                    ),
                    "service": node.service,
                    "fingerprint": node.fingerprint,
                    "event_type": node.event_type,
                    "severity": node.severity,
                    "module": node.module,
                    "host": node.host,
                    "container": node.container,
                    "pod": node.pod,
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
                    "representative_line": node.representative_line,
                    "diagnostic_attributes": node.diagnostic_attributes,
                    "evidence": [
                        _evidence_payload(item)
                        for item in evidence
                    ],
                }
            )

        components.append(
            {
                "id": component_index,
                # Section 9: root_causes is a truthful name - only actual
                # role=="root" candidates (no incoming directed edge, at
                # least one outgoing one) ever appear here. A component
                # with no qualifying root (e.g. association-only, or a
                # singleton) legitimately has an EMPTY root_causes list -
                # never fabricated to keep it non-empty. Every node's own
                # role/root_cause_strength remains available on the node
                # objects above regardless, for frontend ranking/detail.
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
                    if candidate.role == "root"
                ],
                "nodes": nodes,
                # Section 18: built purely from this component's already-
                # loaded nodes/roles (see timeline_processor.py) - never a
                # second DB scan, never fabricated ordering for missing/
                # equal timestamps.
                "timeline": build_component_timeline(component, root_candidates),
                # Directed relationships only - see CorrelationComponent.
                # edges' docstring in correlation_engine.py.
                # relationship_type distinguishes "explicit_parent_child"
                # (exact parent-span match - proven DIRECTION, not proven
                # physical failure causation) from "inferred_propagation"
                # (a real positive time delta between correlated records -
                # a directional HYPOTHESIS, also not proven causation).
                "edges": [
                    {
                        "source_id": edge.source_id,
                        "target_id": edge.target_id,
                        "relationship_type": edge.relationship_type,
                        "correlation_strength": round(edge.score, 4),
                        "direction_confidence": (
                            round(edge.direction_confidence, 4)
                            if edge.direction_confidence is not None
                            else None
                        ),
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

    # Section 20: evidence_count is always the REAL total for the whole
    # analysis (never silently understated) - when the caller bounded
    # evidence_rows below that total (select_bounded_evidence in
    # app/tasks/analysis.py), evidence_count_returned/evidence_truncated
    # make that honestly visible instead of discarding the fact that more
    # evidence exists (still fully persisted in MySQL, never deleted).
    real_total = total_evidence_count if total_evidence_count is not None else len(evidence_rows)

    payload: dict[str, Any] = {
        "analysis_id": correlation_run.result.analysis_id,
        "investigation_path": "correlated",
        "evidence_count": real_total,
        # The truthful count of components actually returned in
        # `components` below (0 or 1 - the primary incident only), never
        # overloaded to also mean "components found" - see
        # component_count_total/excluded_component_count for that.
        "component_count": len(components),
        "component_count_total": total_component_count,
        # Distinct artifacts represented in RETAINED evidence - not the total
        # number of artifacts processed for this analysis. An artifact that
        # was processed but produced zero retained evidence contributes no
        # rows to evidence_rows and so is not counted here; see `artifacts`
        # (when provided) for the full per-artifact outcome, including
        # zero-evidence artifacts.
        "evidence_artifact_count": len(real_evidence_counts_by_artifact),
        "components": components,
    }

    if total_component_count > len(components):
        # Never silently claim excluded components were returned.
        payload["excluded_component_count"] = total_component_count - len(components)

    if real_total > len(evidence_rows):
        payload["evidence_count_returned"] = len(evidence_rows)
        payload["evidence_truncated"] = True

    if artifacts is not None:
        payload["artifacts"] = _artifacts_outcome_list(
            artifacts,
            real_evidence_counts_by_artifact,
            relationship_status_by_artifact=_artifact_relationship_statuses(
                correlation_run.result.components
            ),
        )

    # Section 2: bounded, clearly-non-deterministic supplemental context
    # from artifacts that retained zero structured Evidence but did capture
    # useful fallback text/OCR content during their own ingestion pass
    # (never a second read/OCR - see fallback_context.py) - kept out of the
    # `components` graph entirely (never a correlation participant) so it
    # can never masquerade as linked incident structure.
    if supplemental_artifacts:
        payload["supplemental_low_structure_context"] = _bounded_fallback_context_list(
            supplemental_artifacts
        )

    return payload


def build_simple_payload(
    analysis_id: int,
    evidence_rows: list[Evidence],
    *,
    total_evidence_count: int | None = None,
    evidence_counts_by_artifact: dict[int, int] | None = None,
    artifacts: list[Any] | None = None,
    supplemental_artifacts: list[Any] | None = None,
) -> dict[str, Any]:
    """Final frontend result for the SIMPLE investigation path - real
    retained evidence and (optionally) per-artifact outcomes only, never a
    fabricated components/edges/correlation-strength graph (that shape only
    exists where the deterministic correlation engine actually ran).

    Section 21: Gemini's own SIMPLE context is already bounded
    (build_simple_llm_context); this bounds the SSE/frontend evidence array
    the same way for a giant uncorrelated investigation - Evidence itself
    stays fully persisted in MySQL regardless (evidence_count is always the
    real total, never understated).

    total_evidence_count/evidence_counts_by_artifact mirror build_
    correlation_payload's same-named optional overrides (Section 4
    hardening) - pass them whenever the caller already selected a bounded
    working set of evidence_rows before calling this
    (select_bounded_evidence_from_db/select_evidence_counts_by_artifact),
    so evidence_count/evidence_artifact_count stay truthful for the WHOLE
    analysis rather than just the working subset. When omitted, both fall
    back to deriving from evidence_rows itself (existing direct
    callers/tests unchanged)."""
    real_evidence_counts_by_artifact = (
        evidence_counts_by_artifact
        if evidence_counts_by_artifact is not None
        else _count_evidence_by_artifact(evidence_rows)
    )
    real_total_evidence_count = (
        total_evidence_count if total_evidence_count is not None else len(evidence_rows)
    )

    included_evidence_rows = evidence_rows
    if len(evidence_rows) > SIMPLE_FRONTEND_MAX_EVIDENCE_RECORDS:
        included_evidence_rows = select_bounded_evidence(
            evidence_rows,
            max_records=SIMPLE_FRONTEND_MAX_EVIDENCE_RECORDS,
            max_context_bytes=SIMPLE_FRONTEND_MAX_CONTEXT_BYTES,
        )

    payload: dict[str, Any] = {
        "analysis_id": analysis_id,
        "investigation_path": "simple",
        "evidence_count": real_total_evidence_count,
        "evidence_artifact_count": len(real_evidence_counts_by_artifact),
        "evidence": [
            _evidence_payload(evidence)
            for evidence in included_evidence_rows
        ],
    }

    if real_total_evidence_count > len(included_evidence_rows):
        payload["evidence_count_returned"] = len(included_evidence_rows)
        payload["evidence_truncated"] = True

    if artifacts is not None:
        payload["artifacts"] = _artifacts_outcome_list(
            artifacts,
            real_evidence_counts_by_artifact,
        )

    if supplemental_artifacts:
        payload["supplemental_low_structure_context"] = _bounded_fallback_context_list(
            supplemental_artifacts
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


def _bounded_fallback_context_list(fallback_artifacts: list[Any]) -> list[dict[str, Any]]:
    """Deterministic (stable artifact-id order), bounded to
    SIMPLE_FALLBACK_MAX_TOTAL_CONTEXT_BYTES across every contributing
    artifact combined - each individual artifact's own fallback_context is
    already bounded to SIMPLE_FALLBACK_MAX_TEXT_BYTES at capture time (see
    fallback_context.py); this only bounds the SUM when several small
    artifacts each contributed one. A single oversized entry is still
    included on its own (never emptied entirely) rather than dropped.
    """
    entries: list[dict[str, Any]] = []
    total_bytes = 0

    for artifact in sorted(fallback_artifacts, key=lambda a: a.id):
        context = artifact.fallback_context
        if not context:
            continue

        text = context.get("text", "")
        size = len(text.encode("utf-8", errors="replace"))

        if entries and total_bytes + size > SIMPLE_FALLBACK_MAX_TOTAL_CONTEXT_BYTES:
            continue

        entries.append(
            {
                "artifact_id": artifact.id,
                "source_file": artifact.original_filename,
                "kind": context.get("kind"),
                "text": text,
                "ocr_confidence": context.get("ocr_confidence"),
            }
        )
        total_bytes += size

    return entries


def build_fallback_payload(
    analysis_id: int,
    fallback_artifacts: list[Any],
    *,
    artifacts: list[Any] | None = None,
) -> dict[str, Any]:
    """Final frontend result for the zero-structured-evidence SIMPLE
    fallback (Sections 9-12): the whole analysis retained no structured
    Evidence, but at least one artifact captured useful bounded diagnostic
    text during its own ingestion pass (never a second read/re-OCR). Never
    a graph - context_kind distinguishes this from an ordinary SIMPLE
    (real Evidence) result, and from build_zero_evidence_payload (truly
    nothing useful at all, no Gemini call)."""
    payload: dict[str, Any] = {
        "analysis_id": analysis_id,
        "investigation_path": "simple",
        "context_kind": "unstructured_fallback",
        "evidence_count": 0,
        "evidence_artifact_count": 0,
        "fallback_context": _bounded_fallback_context_list(fallback_artifacts),
    }

    if artifacts is not None:
        payload["artifacts"] = _artifacts_outcome_list(artifacts, {})

    return payload


def build_fallback_llm_context(
    analysis_id: int,
    fallback_artifacts: list[Any],
    *,
    artifacts: list[Any] | None = None,
) -> dict[str, Any]:
    context: dict[str, Any] = {
        "analysis_id": analysis_id,
        "investigation_path": "simple",
        "context_kind": "unstructured_fallback",
        "instruction": (
            "This context was useful user-provided diagnostic text but "
            "Devflo did not promote it to deterministic structured "
            "Evidence. Explain the probable problem and suggest concrete "
            "debugging steps. Do not invent evidence, claim a definitive "
            "fix, or claim access to the raw file/image/repository beyond "
            "what is supplied here. OCR-derived text may contain "
            "recognition errors - treat it as possibly imperfect."
        ),
        "fallback_context": _bounded_fallback_context_list(fallback_artifacts),
    }

    if artifacts is not None:
        context["artifacts"] = _artifacts_outcome_list(artifacts, {})

    return context


def build_simple_llm_context(
    analysis_id: int,
    evidence_rows: list[Evidence],
    *,
    artifacts: list[Any] | None = None,
    supplemental_artifacts: list[Any] | None = None,
) -> dict[str, Any]:
    # Bounded, deterministic selection (see select_bounded_evidence) - the
    # final frontend/database payload (build_simple_payload) still carries
    # every retained evidence row; only what reaches Gemini in one request
    # is capped.
    selected_evidence = select_bounded_evidence(evidence_rows)
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

    if supplemental_artifacts:
        context["supplemental_low_structure_context"] = _bounded_fallback_context_list(
            supplemental_artifacts
        )
        context["supplemental_low_structure_instruction"] = (
            "The entries in supplemental_low_structure_context are useful "
            "diagnostic text Devflo captured but could NOT deterministically "
            "structure or link to the evidence above. You may explain them "
            "separately, but you must NOT treat them as proof of causality "
            "or a root cause, and must NOT merge them into the primary "
            "evidence-based reasoning unless the structured evidence itself "
            "already establishes the connection."
        )

    return context

def _context_size_bytes(value: Any) -> int:
    return len(json.dumps(value, separators=(",", ":"), default=str).encode("utf-8"))


def _llm_component_priority_key(component_dict: dict[str, Any]) -> tuple:
    """Section 10 ordering for the Gemini byte budget: real roots first,
    then more explicit_parent_child-relationship_type edges (explicit,
    proven trace topology - stronger evidence than an inferred propagation
    hypothesis), then more overall directed/associated structure. A pure
    function of the already-built component dict - never re-derives
    anything from the correlation engine."""
    root_count = len(component_dict["root_candidates"])
    explicit_parent_child_count = sum(
        1
        for edge in component_dict["propagation"]
        if edge["relationship_type"] == "explicit_parent_child"
    )
    total_structure = (
        len(component_dict["propagation"])
        + len(component_dict["associations"])
        + len(component_dict["root_evidence"])
    )
    return (root_count > 0, root_count, explicit_parent_child_count, total_structure)


def build_llm_context(
    correlation_run: CorrelationRun,
    evidence_rows: list[Evidence],
    *,
    roots_per_component: int = 3,
    max_additional_source_matched_evidence: int = 3,
    artifacts: list[Any] | None = None,
    supplemental_artifacts: list[Any] | None = None,
) -> dict[str, Any]:
    evidence_by_id = {
        evidence.id: evidence
        for evidence in evidence_rows
    }

    components: list[dict[str, Any]] = []
    excluded_isolated_component_count = 0

    # Section 3 / not_linked isolation: only the single PRIMARY correlated
    # component (same definition _artifact_relationship_statuses uses - a
    # component whose artifacts the frontend would label "linked") ever
    # reaches Gemini's PRIMARY incident reasoning with its substantive
    # content (nodes/edges/evidence). Every other component - whether a
    # true isolated singleton or a smaller, genuinely-correlated but
    # non-primary group - belongs to artifacts the frontend labels
    # not_linked/partially_linked; those artifacts' outcome (existed, was
    # not linked, why) still reaches Gemini via context["artifacts"] below,
    # but their diagnostic content never contaminates the primary
    # reasoning, and their nodes can never become a root/propagation/
    # victim/root-cause candidate for the primary incident.
    primary_component = _select_primary_component(correlation_run.result.components)

    for component_index, component in enumerate(
        correlation_run.result.components
    ):
        if component is not primary_component:
            excluded_isolated_component_count += 1
            continue

        # Used for EVIDENCE SELECTION (which nodes' evidence Gemini gets to
        # see) - the top-ranked structural candidates regardless of role,
        # unchanged from before, so a propagation/victim-heavy component
        # (no node classifies as "root") still gives Gemini real evidence
        # to explain rather than an empty context. Section 9's truthful
        # "root_causes only contains role=='root'" contract only narrows
        # what is DISPLAYED as root_candidates below, never what evidence
        # is included.
        top_ranked_candidates = correlation_run.root_causes.get(
            component_index,
            [],
        )[:roots_per_component]
        true_root_candidates = [
            candidate for candidate in top_ranked_candidates if candidate.role == "root"
        ]

        selected_node_ids = {
            candidate.node_id
            for candidate in top_ranked_candidates
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
                # Section 9: truthful naming - only actual role=="root"
                # candidates (never propagation/victim/uncorrelated), same
                # contract as build_correlation_payload's root_causes[].
                # Evidence selection above is intentionally broader (see
                # top_ranked_candidates) so this narrowing never starves
                # Gemini of evidence for a component with no true root.
                "root_candidates": [
                    {
                        "node_id": candidate.node_id,
                        "root_cause_strength": round(
                            candidate.score,
                            4,
                        ),
                        "role": candidate.role,
                    }
                    for candidate in true_root_candidates
                ],
                # Directed relationships only - never an equal-timestamp/
                # unknown-timestamp association (see associations below).
                # Gemini must not be told an association is propagation,
                # and must distinguish relationship_type
                # "explicit_parent_child" (exact parent-span match, proven
                # DIRECTION - not proven physical failure causation) from
                # "inferred_propagation" (a real time-ordered signal - a
                # directional hypothesis, also not proven causation).
                "propagation": [
                    {
                        "source": edge.source_id,
                        "target": edge.target_id,
                        "relationship_type": edge.relationship_type,
                        "correlation_strength": round(edge.score, 4),
                        "direction_confidence": (
                            round(edge.direction_confidence, 4)
                            if edge.direction_confidence is not None
                            else None
                        ),
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

    # Section 10: a final, explicit byte budget for the CORRELATED Gemini
    # context - separate from and smaller than the frontend/SSE graph
    # budget (CORRELATED_MAX_CONTEXT_BYTES), since a large bounded graph
    # (up to CORRELATED_MAX_EVIDENCE_RECORDS) can still be far more than
    # Gemini needs. Components are included whole, in priority order
    # (real roots first, then more explicit_parent_child edges, then more
    # overall structure), never split apart mid-component - simple deterministic
    # byte-size bin-filling, no tokenization, no LLM-based preselection.
    total_component_count = len(components)
    components.sort(key=_llm_component_priority_key, reverse=True)

    included_components: list[dict[str, Any]] = []
    included_bytes = 0
    for component_dict in components:
        size = _context_size_bytes(component_dict)
        if included_components and included_bytes + size > CORRELATED_GEMINI_CONTEXT_MAX_BYTES:
            break
        included_components.append(component_dict)
        included_bytes += size
    components = included_components
    context_truncated = len(components) < total_component_count

    # Whole-context signal (not per-component) so Gemini's causal-wording
    # rule (see the system instruction) is trivial to apply even without
    # re-deriving it from components[] itself: real reported cases showed
    # Gemini using "led to"/"cascaded into" wording for an analysis that
    # was 100% associations (0 directed edges) - the system instruction
    # alone was not a strong enough signal, so this makes the "any real
    # direction at all" fact explicit and impossible to miss.
    has_directed_relationships = any(
        component["propagation"] for component in components
    )

    context: dict[str, Any] = {
        "analysis_id": correlation_run.result.analysis_id,
        "investigation_path": "correlated",
        "instruction": (
            "Explain the deterministic findings and suggest "
            "debugging/investigation steps. Do not invent evidence "
            "or claim a definitive fix."
        ),
        "has_directed_relationships": has_directed_relationships,
        "components": components,
        "component_count_total": total_component_count,
        "component_count_included": len(components),
    }

    if context_truncated:
        # Never silently pretend the omitted component(s) do not exist.
        context["context_truncated"] = True

    if not has_directed_relationships:
        context["causal_language_instruction"] = (
            "has_directed_relationships is false: none of the components "
            "below contain an explicit_parent_child or inferred_propagation edge, only "
            "associations. Do not use causal/directional wording (\"caused\", "
            "\"led to\", \"resulted in\", \"propagated to\", \"cascaded "
            "into\", or equivalents) anywhere in this response, even if the "
            "co-occurring evidence looks intuitively causal. Describe "
            "relationships as coinciding/associated/observed-alongside, and "
            "state plainly that direction was not established."
        )

    if excluded_isolated_component_count:
        # Never silently pretend the omitted structure does not exist -
        # Gemini knows a count of non-primary components (isolated
        # singletons and any smaller, non-primary correlated groups) were
        # withheld from primary reasoning, without their content ever
        # reaching it. Per-artifact not_linked/partially_linked outcomes
        # for those components still reach Gemini via context["artifacts"].
        context["excluded_isolated_component_count"] = excluded_isolated_component_count

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

    if supplemental_artifacts:
        context["supplemental_low_structure_context"] = _bounded_fallback_context_list(
            supplemental_artifacts
        )
        context["supplemental_low_structure_instruction"] = (
            "The entries in supplemental_low_structure_context are useful "
            "diagnostic text Devflo captured but could NOT deterministically "
            "structure or link to the correlated components above. You may "
            "explain them separately, but you must NOT treat them as proof "
            "of causality or a root cause, and must NOT merge them into the "
            "primary incident reasoning unless the structured evidence "
            "itself already establishes the connection."
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
        "host": evidence.host,
        "container": evidence.container,
        "pod": evidence.pod,
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
        # Bounded, non-canonical structured fields (see
        # diagnostic_adapters.py) - diagnostic context only, never a
        # correlation signal and never promoted to causal proof on its own.
        "diagnostic_attributes": evidence.diagnostic_attributes,
    }


def build_artifact_outcome_payload(
    artifact: Any,
    evidence_count: int,
    filename_by_artifact_id: dict[int, str] | None = None,
    *,
    relationship_status: str | None = None,
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
        if getattr(artifact, "fallback_context", None):
            # A supported artifact with useful captured text/OCR content
            # but zero structured Evidence - distinct from true no_evidence
            # (below) and never framed as "no meaningful diagnostic
            # evidence" (see build_fallback_payload's own zero-evidence-
            # analysis-wide case for the same non-fabrication principle).
            payload["message"] = _LOW_STRUCTURE_MESSAGE
            payload["relationship_status"] = "low_structure"
        else:
            payload["message"] = _NO_EVIDENCE_MESSAGE
            # Section 14: a supported artifact that simply retained no
            # evidence is a distinct outcome from one that retained real
            # evidence but never linked into the correlated incident (see
            # relationship_status below) - both are honestly reported, never
            # conflated with "unrelated".
            payload["relationship_status"] = "no_evidence"
    elif relationship_status is not None:
        payload["relationship_status"] = relationship_status
        if relationship_status == "not_linked":
            payload["message"] = _NOT_LINKED_MESSAGE
        elif relationship_status == "partially_linked":
            payload["message"] = _PARTIALLY_LINKED_MESSAGE

    return payload
