import json
from datetime import datetime, timedelta, timezone
from app.models.evidence import Evidence
from app.services.correlation_engine import run_correlation
from app.services.investigation_context import (
    _select_primary_component,
    build_correlation_payload,
    build_llm_context,
    build_simple_llm_context,
)

def _evidence(evidence_id: int, **kwargs) -> Evidence:
    defaults = {
        "id": evidence_id,
        "analysis_id": 1,
        "artifact_id": 1,
        "fingerprint": f"fp-{evidence_id}",
        "event_type": None,
        "severity": "ERROR",
        "occurrence_count": 1,
        "first_seen": datetime.now(timezone.utc),
        "last_seen": datetime.now(timezone.utc),
        "source_format": "generic",
        "first_line_number": 10,
        "last_line_number": 12,
        "representative_line": "ERROR something failed",
    }
    defaults.update(kwargs)
    return Evidence(**defaults)

def _correlated_fixture():
    base = datetime.now(timezone.utc)
    database = _evidence(
        1, artifact_id=101, source_format="database", trace_id="trace-1",
        service="orders-db", event_type="ConnectionTimeout",
        source_file="db/pool.py",
        source_matches=[
            {
                "relative_path": "db/pool.py",
                "line_number": 42,
                "function": "acquire",
                "snippet": "def acquire():\n    ...",
                "match_method": "exact",
                "confidence": "high",
            }
        ],
        first_seen=base, last_seen=base,
    )
    payment = _evidence(
        2, artifact_id=102, source_format="opentelemetry", trace_id="trace-1",
        span_id="payment-span", service="payment", identity_strength=1.0,
        first_seen=base + timedelta(milliseconds=184),
        last_seen=base + timedelta(milliseconds=184),
    )
    api = _evidence(
        3, artifact_id=103, source_format="web_server", trace_id="trace-1",
        service="api", http_status=500, endpoint="/checkout",
        first_seen=base + timedelta(milliseconds=257),
        last_seen=base + timedelta(milliseconds=257),
    )
    run = run_correlation(analysis_id=7, evidence_rows=[database, payment, api])
    return run, [database, payment, api]

def test_correlated_frontend_payload_uses_explicit_strength_names():
    run, evidence_rows = _correlated_fixture()
    payload = build_correlation_payload(run, evidence_rows)

    component = payload["components"][0]
    assert component["root_causes"], "expected at least one root cause"
    for candidate in component["root_causes"]:
        assert "root_cause_strength" in candidate
        assert "score" not in candidate

    assert component["edges"], "expected at least one edge"
    for edge in component["edges"]:
        assert "correlation_strength" in edge
        assert "score" not in edge
        assert "strength" not in edge

    payload_text = json.dumps(payload)
    assert '"score"' not in payload_text

def test_node_carries_its_own_role_and_root_cause_strength():
    run, evidence_rows = _correlated_fixture()
    payload = build_correlation_payload(run, evidence_rows)

    component = payload["components"][0]
    assert component["nodes"], "expected nodes in the fixture"
    for node in component["nodes"]:
        assert "role" in node
        assert "root_cause_strength" in node

    roles = {node["id"]: node["role"] for node in component["nodes"]}
    assert "root" in roles.values()
    assert "victim" in roles.values()

    root_node_ids = {node["id"] for node in component["nodes"] if node["role"] == "root"}
    assert root_node_ids == {c["node_id"] for c in component["root_causes"]}

def test_correlated_gemini_context_uses_explicit_strength_names():
    run, evidence_rows = _correlated_fixture()
    context = build_llm_context(run, evidence_rows)

    component = context["components"][0]
    for candidate in component["root_candidates"]:
        assert "root_cause_strength" in candidate
        assert "score" not in candidate

    for edge in component["propagation"]:
        assert "correlation_strength" in edge
        assert "score" not in edge
        assert "strength" not in edge

    context_text = json.dumps(context)
    assert '"score"' not in context_text
    assert '"strength":' not in context_text

def test_identity_strength_survives_where_available():
    run, evidence_rows = _correlated_fixture()
    payload = build_correlation_payload(run, evidence_rows)

    node_by_id = {node["id"]: node for node in payload["components"][0]["nodes"]}
    payment_node = node_by_id["evidence-2"]
    assert payment_node["identity_strength"] == 1.0

def test_delta_ms_and_signals_preserved_from_the_correlation_edge():
    run, evidence_rows = _correlated_fixture()
    payload = build_correlation_payload(run, evidence_rows)

    raw_edges = {
        (e.source_id, e.target_id): e
        for c in run.result.components
        for e in c.edges
    }
    for edge in payload["components"][0]["edges"]:
        raw = raw_edges[(edge["source_id"], edge["target_id"])]
        assert edge["delta_ms"] == raw.delta_ms
        assert edge["signals"] == [s.value for s in raw.signals]
        assert edge["signals"]

def test_delta_ms_preserved_in_gemini_propagation_too():
    run, evidence_rows = _correlated_fixture()
    context = build_llm_context(run, evidence_rows)

    raw_edges = {
        (e.source_id, e.target_id): e
        for c in run.result.components
        for e in c.edges
    }
    for edge in context["components"][0]["propagation"]:
        raw = raw_edges[(edge["source"], edge["target"])]
        assert edge["delta_ms"] == raw.delta_ms

def test_evidence_provenance_survives_artifact_source_file_and_format():
    run, evidence_rows = _correlated_fixture()
    payload = build_correlation_payload(run, evidence_rows)

    nodes = payload["components"][0]["nodes"]
    artifact_ids = {node["artifact_id"] for node in nodes}
    assert artifact_ids == {101, 102, 103}

    db_node = next(n for n in nodes if n["id"] == "evidence-1")
    assert db_node["source_file"] == "db/pool.py"
    assert db_node["source_format"] == "database"

    for node in nodes:
        for evidence_item in node["evidence"]:
            assert evidence_item["artifact_id"] == node["artifact_id"]

def test_timestamps_survive_where_available_and_are_not_fabricated():
    run, evidence_rows = _correlated_fixture()
    payload = build_correlation_payload(run, evidence_rows)

    db_node = next(n for n in payload["components"][0]["nodes"] if n["id"] == "evidence-1")
    assert db_node["first_seen"] is not None
    assert db_node["last_seen"] is not None

    timestampless = _evidence(99, artifact_id=1, first_seen=None, last_seen=None)
    simple_context = build_simple_llm_context(analysis_id=1, evidence_rows=[timestampless])
    assert simple_context["evidence"][0]["first_seen"] is None
    assert simple_context["evidence"][0]["last_seen"] is None

def test_source_matches_survive_with_original_structure():
    run, evidence_rows = _correlated_fixture()
    payload = build_correlation_payload(run, evidence_rows)

    db_node = next(n for n in payload["components"][0]["nodes"] if n["id"] == "evidence-1")
    assert db_node["source_matches"] == evidence_rows[0].source_matches
    match = db_node["source_matches"][0]
    assert match["relative_path"] == "db/pool.py"
    assert match["line_number"] == 42
    assert match["function"] == "acquire"
    assert match["match_method"] == "exact"
    assert match["confidence"] == "high"

def test_gemini_context_still_reaches_a_source_match_outside_top_roots():
    from app.services.correlation_engine import (
        CorrelationComponent,
        CorrelationNode,
        CorrelationResult,
        CorrelationRun,
        NodeGraphStats,
        RootCauseCandidate,
    )

    evidence_rows = [
        _evidence(1, service="svc-a", representative_line="ERROR one"),
        _evidence(2, service="svc-b", representative_line="ERROR two"),
        _evidence(3, service="svc-c", representative_line="ERROR three"),
        _evidence(
            4,
            service="worker",
            representative_line="ERROR RuntimeError: queue unavailable",
            source_file="srv/worker.py",
            source_matches=[
                {
                    "relative_path": "srv/worker.py",
                    "line_number": 42,
                    "function": "run",
                    "match_method": "exact",
                    "confidence": "high",
                }
            ],
        ),
    ]

    nodes = [
        CorrelationNode(id=f"evidence-{i}", service=evidence_rows[i - 1].service, fingerprint=None, first_seen=None, last_seen=None, evidence_ids=[i])
        for i in range(1, 5)
    ]
    component = CorrelationComponent(nodes=nodes, edges=[])
    stats = NodeGraphStats()
    root_causes = {
        0: [
            RootCauseCandidate(node_id="evidence-1", score=0.9, graph_stats=stats, role="root"),
            RootCauseCandidate(node_id="evidence-2", score=0.8, graph_stats=stats, role="propagation"),
            RootCauseCandidate(node_id="evidence-3", score=0.7, graph_stats=stats, role="victim"),
            RootCauseCandidate(node_id="evidence-4", score=0.6, graph_stats=stats, role="uncorrelated"),
        ]
    }
    run = CorrelationRun(
        result=CorrelationResult(analysis_id=1, components=[component]),
        root_causes=root_causes,
    )

    context = build_llm_context(run, evidence_rows)

    root_evidence_ids = {item["id"] for item in context["components"][0]["root_evidence"]}
    assert root_evidence_ids == {1, 2, 3, 4}
    source_matched = next(item for item in context["components"][0]["root_evidence"] if item["id"] == 4)
    assert source_matched["source_matches"][0]["relative_path"] == "srv/worker.py"
    assert source_matched["source_matches"][0]["line_number"] == 42

    assert [c["node_id"] for c in context["components"][0]["root_candidates"]] == [
        "evidence-1",
    ]

def test_gemini_context_additional_source_matched_evidence_stays_bounded():
    from app.services.correlation_engine import (
        CorrelationComponent,
        CorrelationNode,
        CorrelationResult,
        CorrelationRun,
        NodeGraphStats,
        RootCauseCandidate,
    )

    def _match(n):
        return [{"relative_path": f"srv/mod_{n}.py", "line_number": n, "function": "f", "match_method": "exact", "confidence": "high"}]

    evidence_rows = [_evidence(1, service="root")] + [
        _evidence(n, service=f"svc-{n}", source_matches=_match(n)) for n in range(2, 8)
    ]
    nodes = [
        CorrelationNode(id=f"evidence-{e.id}", service=e.service, fingerprint=None, first_seen=None, last_seen=None, evidence_ids=[e.id])
        for e in evidence_rows
    ]
    component = CorrelationComponent(nodes=nodes, edges=[])
    stats = NodeGraphStats()
    root_causes = {0: [RootCauseCandidate(node_id="evidence-1", score=1.0, graph_stats=stats, role="root")]}
    run = CorrelationRun(result=CorrelationResult(analysis_id=1, components=[component]), root_causes=root_causes)

    context = build_llm_context(run, evidence_rows)

    assert len(context["components"][0]["root_evidence"]) <= 4

def test_gemini_correlated_context_contains_required_fields():
    run, evidence_rows = _correlated_fixture()
    context = build_llm_context(run, evidence_rows)

    assert context["analysis_id"] == 7
    assert context["investigation_path"] == "correlated"

    component = context["components"][0]
    assert component["root_candidates"]
    for candidate in component["root_candidates"]:
        assert "root_cause_strength" in candidate
        assert "role" in candidate

    for edge in component["propagation"]:
        assert "correlation_strength" in edge
        assert "delta_ms" in edge

    assert component["root_evidence"]
    for item in component["root_evidence"]:
        assert "source_file" in item
        assert "source_format" in item
        assert "artifact_id" in item

def test_simple_gemini_context_does_not_fabricate_correlation_concepts():
    evidence_rows = [_evidence(1, artifact_id=1, service="worker")]
    context = build_simple_llm_context(analysis_id=1, evidence_rows=evidence_rows)

    assert context["investigation_path"] == "simple"
    context_text = json.dumps(context)
    assert "correlation_strength" not in context_text
    assert "root_cause_strength" not in context_text
    assert "propagation" not in context_text
    assert "components" not in context

def test_simple_gemini_context_still_carries_available_evidence_fields():
    evidence_rows = [
        _evidence(
            1, artifact_id=5, service="worker", trace_id="trace-x",
            source_file="worker.py",
        )
    ]
    context = build_simple_llm_context(analysis_id=1, evidence_rows=evidence_rows)

    item = context["evidence"][0]
    assert item["artifact_id"] == 5
    assert item["service"] == "worker"
    assert item["trace_id"] == "trace-x"
    assert item["source_file"] == "worker.py"

def test_missing_optional_fields_do_not_break_serialization():
    base = datetime.now(timezone.utc)
    sparse = _evidence(
        1,
        artifact_id=1,
        service="svc",
        trace_id=None,
        request_id=None,
        span_id=None,
        parent_span_id=None,
        resolved_identity=None,
        identity_match_type=None,
        identity_strength=None,
        source_file=None,
        source_matches=None,
        endpoint=None,
        http_status=None,
        module=None,
        first_seen=base,
        last_seen=None,
    )
    companion = _evidence(2, artifact_id=1, service="svc", first_seen=base + timedelta(milliseconds=5))

    run = run_correlation(analysis_id=1, evidence_rows=[sparse, companion])
    payload = build_correlation_payload(run, [sparse, companion])
    llm_context = build_llm_context(run, [sparse, companion])
    simple_context = build_simple_llm_context(analysis_id=1, evidence_rows=[sparse, companion])

    json.dumps(payload)
    json.dumps(llm_context)
    json.dumps(simple_context)

    assert len(payload["components"]) == 1
    sparse_node = next(n for n in payload["components"][0]["nodes"] if n["id"] == "evidence-1")

    assert sparse_node["source_matches"] == []
    assert sparse_node["trace_id"] is None

    sparse_evidence_context = next(e for e in simple_context["evidence"] if e["id"] == 1)
    assert sparse_evidence_context["source_matches"] is None
    assert sparse_evidence_context["trace_id"] is None

def test_select_primary_component_ties_are_broken_by_earliest_line_not_list_order():
    from types import SimpleNamespace

    def _node(artifact_id, first_line_number):
        return SimpleNamespace(artifact_id=artifact_id, first_line_number=first_line_number)

    component_early = SimpleNamespace(
        nodes=[_node(1, 100), _node(1, 200)],
    )
    component_late = SimpleNamespace(
        nodes=[_node(2, 900), _node(2, 950)],
    )

    forward = _select_primary_component([component_early, component_late])
    reversed_order = _select_primary_component([component_late, component_early])

    assert forward is component_early
    assert reversed_order is component_early

def test_select_primary_component_exact_line_ties_use_complete_stable_node_key():
    from types import SimpleNamespace

    def _node(
        artifact_id,
        first_line_number,
        fingerprint,
    ):
        return SimpleNamespace(
            artifact_id=artifact_id,
            first_line_number=first_line_number,
            fingerprint=fingerprint,
            correlation_key=f"ck-{fingerprint}",
            source_file=f"{fingerprint}.log",
        )

    component_a = SimpleNamespace(
        nodes=[
            _node(1, 100, "a"),
            _node(1, 200, "z"),
        ],
    )

    component_b = SimpleNamespace(
        nodes=[
            _node(2, 100, "b"),
            _node(2, 250, "y"),
        ],
    )

    assert (
        _select_primary_component(
            [component_a, component_b]
        )
        is component_a
    )

    assert (
        _select_primary_component(
            [component_b, component_a]
        )
        is component_a
    )
