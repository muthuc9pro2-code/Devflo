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

def test_parent_span_id_present_without_a_matching_parent_row_stays_simple():
    with _db() as db:
        db.add_all(
            [
                _evidence(1, span_id="span-child", parent_span_id="span-does-not-exist"),
                _evidence(2, service="unrelated"),
            ]
        )
        db.commit()

        assert _route(db) == InvestigationPath.SIMPLE

def test_shared_sentinel_correlation_key_alone_does_not_route_correlated():
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
    with _db() as db:
        db.add_all(
            [
                _evidence(1, fingerprint="fp-a", service="svc-a"),
                _evidence(2, fingerprint="fp-b", service="svc-b"),
            ]
        )
        db.commit()

        assert _route(db) == InvestigationPath.SIMPLE

def test_choose_investigation_path_performs_no_db_query():
    evidence_rows = [
        _evidence(1, trace_id="trace-a"),
        _evidence(2, trace_id="trace-a"),
    ]

    result = choose_investigation_path(evidence_rows)

    assert result == InvestigationPath.CORRELATED
