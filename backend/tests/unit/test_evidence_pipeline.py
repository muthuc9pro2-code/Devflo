from datetime import datetime
from unittest.mock import Mock

import pytest
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy.dialects import mysql

from app.models import Evidence
from app.services.evidence_store import persist_evidence_batch
from app.services.log_praser import ParsedEvent


def test_evidence_batch_remains_one_bulk_upsert():
    db = Mock()
    events = [
        ParsedEvent(
            line_number=number,
            raw_line=f"ERROR failure {number}",
            timestamp=datetime(2026, 8, 12, 10, number),  # noqa: DTZ001
            level="ERROR",
            trace_id="trace-1",
            span_id=f"span-{number}",
            fingerprint="runtimeerror:failure",
            artifact_id=7,
        )
        for number in (1, 2)
    ]

    persist_evidence_batch(db=db, analysis_id=3, events=events)

    db.execute.assert_called_once()
    statement, rows = db.execute.call_args.args
    sql = str(
        statement.compile(
            dialect=mysql.dialect(),
            column_keys=list(rows[0]),
        )
    )
    assert "ON DUPLICATE KEY UPDATE" in sql
    assert "correlation_key" in sql
    assert len(rows) == 2


def test_missing_trace_request_span_ids_persist_as_real_null_not_sentinel():
    """Regression test: the internal "__none__" grouping/correlation_key
    sentinel must never leak into the stored trace_id/request_id/span_id
    columns - correlation_engine.py treats any non-None value as a real
    shared identifier, so two unrelated events that both lack an id would
    otherwise falsely trace-match/request-match each other.
    """
    db = Mock()
    events = [
        ParsedEvent(
            line_number=1,
            raw_line="ERROR unrelated failure A",
            level="ERROR",
            fingerprint="fp-a",
            service="checkout-service",
            artifact_id=7,
        ),
        ParsedEvent(
            line_number=2,
            raw_line="ERROR unrelated failure B",
            level="ERROR",
            fingerprint="fp-b",
            service="unrelated-batch-job",
            artifact_id=7,
        ),
    ]

    persist_evidence_batch(db=db, analysis_id=3, events=events)

    _, rows = db.execute.call_args.args
    assert len(rows) == 2
    for row in rows:
        assert row["trace_id"] is None
        assert row["request_id"] is None
        assert row["span_id"] is None
        # correlation_key is still a real, present hash - the sentinel is
        # legitimately used internally for that, not stored as an id.
        assert row["correlation_key"]


def test_missing_ids_still_group_separately_by_fingerprint():
    """The internal sentinel is still legitimately needed so that two
    different-fingerprint, both-untraced events don't collapse into one
    grouped row - only the stored id columns must be real NULL."""
    db = Mock()
    events = [
        ParsedEvent(line_number=1, raw_line="ERROR A", level="ERROR", fingerprint="fp-a", artifact_id=7),
        ParsedEvent(line_number=2, raw_line="ERROR B", level="ERROR", fingerprint="fp-b", artifact_id=7),
    ]

    persist_evidence_batch(db=db, analysis_id=3, events=events)

    _, rows = db.execute.call_args.args
    assert {row["fingerprint"] for row in rows} == {"fp-a", "fp-b"}
    assert len(rows) == 2


def test_real_trace_id_is_still_persisted_and_resolved():
    db = Mock()
    events = [
        ParsedEvent(
            line_number=1,
            raw_line="ERROR real trace",
            level="ERROR",
            fingerprint="fp",
            trace_id="trace-real-1",
            artifact_id=7,
        )
    ]

    persist_evidence_batch(db=db, analysis_id=3, events=events)

    _, rows = db.execute.call_args.args
    assert rows[0]["trace_id"] == "trace-real-1"
    assert rows[0]["resolved_identity"] == "trace:trace-real-1"


def test_evidence_artifact_foreign_key_is_scoped_to_the_analysis():
    foreign_keys = [
        constraint
        for constraint in Evidence.__table__.constraints
        if isinstance(constraint, ForeignKeyConstraint)
        and constraint.name == "fk_evidence_artifact_analysis"
    ]

    assert len(foreign_keys) == 1
    constraint = foreign_keys[0]
    assert [column.name for column in constraint.columns] == [
        "artifact_id",
        "analysis_id",
    ]
    assert [element.target_fullname for element in constraint.elements] == [
        "analysis_artifacts.id",
        "analysis_artifacts.analysis_id",
    ]
    assert Evidence.resolved_identity.type.length >= len("request:") + 255


def test_legacy_evidence_batch_resolves_its_artifact_once():
    db = Mock()
    db.query.return_value.filter.return_value.order_by.return_value.first.return_value = (
        17,
    )
    event = ParsedEvent(
        line_number=1,
        raw_line="ERROR legacy failure",
        level="ERROR",
        fingerprint="error:legacy failure",
    )

    persist_evidence_batch(db=db, analysis_id=3, events=[event])

    db.query.assert_called_once()
    db.execute.assert_called_once()


def test_explicit_batch_artifact_rejects_event_provenance_mismatch():
    db = Mock()
    event = ParsedEvent(
        line_number=1,
        raw_line="ERROR mismatched artifact",
        level="ERROR",
        fingerprint="error:mismatched artifact",
        artifact_id=8,
    )

    with pytest.raises(ValueError, match="does not match"):
        persist_evidence_batch(
            db=db,
            analysis_id=3,
            events=[event],
            artifact_id=7,
        )

    db.query.assert_not_called()
    db.execute.assert_not_called()


def test_evidence_batch_normalizes_mixed_timestamp_inputs():
    db = Mock()
    events = [
        ParsedEvent(
            line_number=1,
            raw_line="ERROR earlier",
            timestamp="2026-08-12T09:00:00Z",
            level="ERROR",
            fingerprint="error:mixed-time",
            artifact_id=7,
        ),
        ParsedEvent(
            line_number=2,
            raw_line="ERROR later",
            timestamp=datetime(2026, 8, 12, 10, 0),  # noqa: DTZ001
            level="ERROR",
            fingerprint="error:mixed-time",
            artifact_id=7,
        ),
    ]

    persist_evidence_batch(db=db, analysis_id=3, events=events, artifact_id=7)

    db.execute.assert_called_once()
    _, rows = db.execute.call_args.args
    assert len(rows) == 1
    assert rows[0]["first_seen"] == datetime(2026, 8, 12, 9, 0)  # noqa: DTZ001
    assert rows[0]["last_seen"] == datetime(2026, 8, 12, 10, 0)  # noqa: DTZ001


def test_evidence_batch_persists_first_truthy_source_matches():
    db = Mock()
    events = [
        ParsedEvent(
            line_number=number,
            raw_line="ERROR correlated failure",
            level="ERROR",
            fingerprint="error:correlated",
            artifact_id=7,
        )
        for number in (1, 2)
    ]
    events[0].source_matches = []
    events[1].source_matches = [{"relative_path": "app/main.py", "line_number": 9}]

    persist_evidence_batch(db=db, analysis_id=3, events=events, artifact_id=7)

    _, rows = db.execute.call_args.args
    assert rows[0]["source_matches"] == events[1].source_matches
