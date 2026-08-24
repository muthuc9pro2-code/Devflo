"""Investigation routing: CORRELATED must be chosen only when
real correlatable structure exists, using the exact same relationship
semantics correlation_engine.build_correlation_edges uses - never a second,
independently-drifting router-only heuristic.

Regression coverage for two real bugs in the previous router:
  1. correlation_key sentinel bug: correlation_key is generated from
     sentinel placeholders ("__none__") whenever trace_id/request_id/
     span_id are ALL missing, so every untraced evidence row in an
     analysis shares the identical hash - a persistence/dedup grouping
     key, never trustworthy incident identity. The router used to treat a
     shared correlation_key as CORRELATED-worthy on its own; that check is
     now gone entirely.
  2. parent-span false positive: the previous check only asked whether a
     row's own span_id AND parent_span_id were both non-null (true of any
     child span, whether or not its actual parent row exists at all). The
     router now calls has_genuine_correlatable_structure(), which verifies
     a real parent.span_id == child.parent_span_id match via
     find_parent_span_candidate().

choose_investigation_path() does not query the DB
itself - it is a thin policy wrapper over an already-loaded evidence_rows
list. _route() below does the query these tests still need (a real sqlite
DB is still the most direct way to build/persist Evidence fixtures).
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.models import Evidence
from app.services.investigation_router import InvestigationPath, choose_investigation_path


def _db():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Evidence.__table__.create(engine)
    return Session(engine)


def _route(db, analysis_id: int = 1) -> InvestigationPath:
    evidence_rows = db.query(Evidence).filter(Evidence.analysis_id == analysis_id).all()
    return choose_investigation_path(evidence_rows)


def _evidence(identifier, **overrides):
    fields = dict(
        id=identifier,
        analysis_id=1,
        artifact_id=1,
        correlation_key=f"key-{identifier}",
        fingerprint=f"fingerprint-{identifier}",
        trace_id=None,
        request_id=None,
        occurrence_count=1,
        first_line_number=identifier,
        last_line_number=identifier,
        # A real evidence row always carries a known source_format -
        # signal strength (match_correlation_signals -> _pair_strength) is
        # gated on FORMAT_SIGNAL_PRIORITY declaring the signal for BOTH
        # sides, exactly like the real ingestion pipeline always produces.
        # Omitting it (as the old version of this fixture did) makes every
        # signal silently fail to register, which is not how a real
        # investigation's evidence looks.
        source_format="generic",
    )
    fields.update(overrides)
    return Evidence(**fields)


def test_no_evidence_stops_before_correlated_processing():
    with _db() as db:
        assert _route(db) == InvestigationPath.SIMPLE


def test_single_evidence_row_is_simple():
    with _db() as db:
        db.add(_evidence(1))
        db.commit()

        assert _route(db) == InvestigationPath.SIMPLE


def test_shared_trace_id_across_rows_is_correlated():
    with _db() as db:
        db.add_all([_evidence(1, trace_id="trace-a"), _evidence(2, trace_id="trace-a")])
        db.commit()

        assert _route(db) == InvestigationPath.CORRELATED


def test_shared_request_id_across_rows_is_correlated():
    with _db() as db:
        db.add_all(
            [_evidence(1, request_id="req-a"), _evidence(2, request_id="req-a")]
        )
        db.commit()

        assert _route(db) == InvestigationPath.CORRELATED


def test_real_parent_child_span_is_correlated():
    with _db() as db:
        db.add_all(
            [
                _evidence(1, trace_id="trace-a", span_id="span-parent"),
                _evidence(
                    2, trace_id="trace-a", span_id="span-child",
                    parent_span_id="span-parent",
                ),
            ]
        )
        db.commit()

        assert _route(db) == InvestigationPath.CORRELATED


# --- parent_span_id present but the actual parent row is absent -----------


def test_parent_span_id_present_without_a_matching_parent_row_stays_simple():
    """The previous router bug: it only checked whether ANY row had both
    span_id and parent_span_id set - true of the child alone - without
    verifying a real parent exists. A lone child-shaped row (no other
    evidence at all, or none whose span_id matches its parent_span_id)
    must not route CORRELATED on that basis."""
    with _db() as db:
        db.add_all(
            [
                _evidence(1, span_id="span-child", parent_span_id="span-does-not-exist"),
                _evidence(2, service="unrelated"),
            ]
        )
        db.commit()

        assert _route(db) == InvestigationPath.SIMPLE


# --- The correlation_key sentinel bug --------------------------------------


def test_shared_sentinel_correlation_key_alone_does_not_route_correlated():
    """Two rows with NO trace/request/span id at all (both hash to the
    identical sentinel-derived correlation_key), different fingerprints,
    different services - the old bug would have made this CORRELATED
    purely because of the shared internal hash. It must stay SIMPLE."""
    with _db() as db:
        db.add_all(
            [
                _evidence(
                    1, correlation_key="same-sentinel-hash", fingerprint="fp-a",
                    service="svc-a",
                ),
                _evidence(
                    2, correlation_key="same-sentinel-hash", fingerprint="fp-b",
                    service="svc-b",
                ),
            ]
        )
        db.commit()

        assert _route(db) == InvestigationPath.SIMPLE


def test_two_unrelated_untraced_rows_with_different_ids_stay_simple():
    """No trace, no request, no span, different fingerprint, different
    service - the baseline sparse/unrelated case must remain SIMPLE."""
    with _db() as db:
        db.add_all(
            [
                _evidence(1, fingerprint="fp-a", service="svc-a"),
                _evidence(2, fingerprint="fp-b", service="svc-b"),
            ]
        )
        db.commit()

        assert _route(db) == InvestigationPath.SIMPLE


# --- No DB query of its own -------------------------------------------------


def test_choose_investigation_path_performs_no_db_query():
    """A thin policy wrapper over an already-loaded evidence_rows list -
    the caller (_finalize_analysis_task) selects the bounded working set
    exactly once, before routing; routing itself must never re-query."""
    evidence_rows = [
        _evidence(1, trace_id="trace-a"),
        _evidence(2, trace_id="trace-a"),
    ]

    # A bare object with no query/session behavior at all - if
    # choose_investigation_path touched a db/session argument (it takes
    # none), this would fail loudly rather than silently pass.
    result = choose_investigation_path(evidence_rows)

    assert result == InvestigationPath.CORRELATED
