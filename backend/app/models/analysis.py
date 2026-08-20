from datetime import datetime

from sqlalchemy import JSON, BigInteger, DateTime, Enum, ForeignKey, Index, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.database import Base


class Analysis(Base):
    __tablename__ = "analyses"
    __table_args__ = (
        # Covers the History-list query - WHERE user_id = ? ORDER BY
        # created_at DESC, id DESC (keyset pagination) - with a single
        # index instead of a filter plus a separate sort. A bare user_id
        # index (implicit from the FK) would satisfy the filter but still
        # require sorting every matching row in memory.
        Index("ix_analyses_user_id_created_at_id", "user_id", "created_at", "id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    original_filename: Mapped[str] = mapped_column(String(255), nullable=False)
    saved_file_path: Mapped[str] = mapped_column(String(500), nullable=False)
    source_kind: Mapped[str | None] = mapped_column(String(20), nullable=True)
    source_reference: Mapped[str | None] = mapped_column(String(500), nullable=True)
    status: Mapped[str] = mapped_column(
        Enum("pending", "processing", "completed", "failed", name="analysis_status"),
        default="pending",
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    user = relationship("User", back_populates="analyses")
    artifacts = relationship(
        "AnalysisArtifact",
        back_populates="analysis",
        cascade="all, delete-orphan",
        order_by="AnalysisArtifact.position",
    )
    last_processed_line: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=0,
    )

    processed_bytes: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=0,
    )

    # The structured GeminiInvestigationResponse.model_dump() result
    # persisted only after Gemini returns a valid schema result (see
    # _finalize_analysis_task) - lets a client reconnecting after
    # completion see the exact same AI explanation the live SSE event
    # delivered, without a second Gemini call. Null for zero-evidence
    # investigations (no Gemini call is ever made for those) and for any
    # analysis finalized before this field existed.
    ai_analysis: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    # The exact same bounded final payload published as investigation_result
    # (correlated/simple/zero_evidence/fallback, ai_analysis already
    # attached) - written BEFORE that event is published (see
    # _finalize_analysis_task) so History/reconnect always has an
    # authoritative, immutable record of what was actually shown, even if
    # the live SSE event never reaches a client. Never recomputed after the
    # fact - a later change to correlation/scoring logic must not alter a
    # historical result. Null for analyses finalized before this column
    # existed and for analyses still pending/processing/failed;
    # reconstruct_current_investigation_result() remains the read-time
    # fallback for those legacy completed rows.
    result_snapshot: Mapped[dict | None] = mapped_column(JSON, nullable=True)
