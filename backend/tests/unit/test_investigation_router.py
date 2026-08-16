from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.models import Evidence
from app.services.investigation_router import InvestigationPath, choose_investigation_path


def _db():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Evidence.__table__.create(engine)
    return Session(engine)


def _evidence(identifier, **overrides):
    fields = dict(
        id=identifier,
        analysis_id=1,
        artifact_id=1,
        correlation_key=f"key-{identifier}",
        fingerprint=f"fingerprint-{identifier}",
        trace_id="__none__",
        request_id="__none__",
        occurrence_count=1,
        first_line_number=identifier,
        last_line_number=identifier,
    )
    fields.update(overrides)
    return Evidence(**fields)


def test_no_evidence_stops_before_correlated_processing():
    with _db() as db:
        assert choose_investigation_path(db=db, analysis_id=1) == InvestigationPath.SIMPLE


def test_single_evidence_row_is_simple():
    with _db() as db:
        db.add(_evidence(1))
        db.commit()

        assert choose_investigation_path(db=db, analysis_id=1) == InvestigationPath.SIMPLE


def test_shared_trace_id_across_rows_is_correlated():
    with _db() as db:
        db.add_all([_evidence(1, trace_id="trace-a"), _evidence(2, trace_id="trace-a")])
        db.commit()

        assert choose_investigation_path(db=db, analysis_id=1) == InvestigationPath.CORRELATED


def test_parent_child_span_is_correlated():
    with _db() as db:
        db.add_all(
            [
                _evidence(1, span_id="span-parent"),
                _evidence(2, span_id="span-child", parent_span_id="span-parent"),
            ]
        )
        db.commit()

        assert choose_investigation_path(db=db, analysis_id=1) == InvestigationPath.CORRELATED
