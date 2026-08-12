from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.database import Base


class Evidence(Base):
    __tablename__ = "evidence"
    __table_args__ = (
        UniqueConstraint(
            "analysis_id",
            "artifact_id",
            "fingerprint",
            "correlation_key",
            name="uq_evidence_artifact_fingerprint_correlation",
        ),
        Index("ix_evidence_analysis_span_id", "analysis_id", "span_id"),
        Index(
            "ix_evidence_analysis_parent_span_id",
            "analysis_id",
            "parent_span_id",
        ),
        Index(
            "ix_evidence_analysis_first_seen_id",
            "analysis_id",
            "first_seen",
            "id",
        ),
        Index(
            "ix_evidence_artifact_analysis",
            "artifact_id",
            "analysis_id",
        ),
        ForeignKeyConstraint(
            ["artifact_id", "analysis_id"],
            ["analysis_artifacts.id", "analysis_artifacts.analysis_id"],
            name="fk_evidence_artifact_analysis",
            ondelete="CASCADE",
        ),
    )

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        index=True,
    )

    analysis_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("analyses.id"),
        nullable=False,
        index=True,
    )

    artifact_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )

    correlation_key: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )

    fingerprint: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        index=True,
    )

    event_type: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
    )

    severity: Mapped[str | None] = mapped_column(
        String(50),
        nullable=True,
    )

    trace_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        index=True,
    )

    request_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        index=True,
    )

    span_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )

    parent_span_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )

    service: Mapped[str | None] = mapped_column(String(255), nullable=True)
    module: Mapped[str | None] = mapped_column(String(255), nullable=True)
    host: Mapped[str | None] = mapped_column(String(255), nullable=True)
    container: Mapped[str | None] = mapped_column(String(255), nullable=True)
    pod: Mapped[str | None] = mapped_column(String(255), nullable=True)
    endpoint: Mapped[str | None] = mapped_column(String(500), nullable=True)
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_file: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_format: Mapped[str | None] = mapped_column(String(50), nullable=True)

    first_seen: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
    )

    last_seen: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
    )

    occurrence_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
    )

    first_line_number: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
    )

    last_line_number: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
    )

    representative_line: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    resolved_identity: Mapped[str | None] = mapped_column(
        String(263),
        nullable=True,
        index=True,
    )

    identity_match_type: Mapped[str | None] = mapped_column(
        String(50),
        nullable=True,
    )

    identity_strength: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
    )

    