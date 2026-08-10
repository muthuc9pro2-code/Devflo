from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.database import Base


class Evidence(Base):
    __tablename__ = "evidence"
    __table_args__ = (
        UniqueConstraint(
            "analysis_id",
            "fingerprint",
            "trace_id",
            "request_id",
            name="uq_evidence_analysis_fingerprint_identity",
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
        String(255),
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