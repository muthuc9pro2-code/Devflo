from datetime import datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.database import Base


class AnalysisArtifact(Base):
    __tablename__ = "analysis_artifacts"
    __table_args__ = (
        UniqueConstraint(
            "analysis_id",
            "position",
            name="uq_analysis_artifact_position",
        ),
        UniqueConstraint(
            "id",
            "analysis_id",
            name="uq_analysis_artifact_id_analysis",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    analysis_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("analyses.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    position: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )

    original_filename: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )

    saved_file_path: Mapped[str] = mapped_column(
        String(500),
        nullable=False,
    )

    content_type: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )

    size_bytes: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
    )

    detected_format: Mapped[str | None] = mapped_column(
        String(50),
        nullable=True,
    )

    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="pending",
    )

    # Short, user-safe reason for a terminal artifact-level processing
    # outcome such as resource_limited or processing_error.
    #
    # This is intentionally persisted rather than existing only in Redis/SSE:
    # reconnects, History, and the final investigation result must reconstruct
    # the same artifact outcome that was shown live.
    #
    # Never store a traceback, exception repr, filesystem path, secret,
    # diagnostic payload, or other unbounded/internal detail here.
    failure_reason: Mapped[str | None] = mapped_column(
        String(500),
        nullable=True,
    )

    # Streaming SHA-256 of the artifact's raw bytes (see upload_staging.
    # copy_upload), used only for content-identity duplicate detection
    # within the same analysis - never a diagnostic-content signal, never
    # consulted by correlation/scoring. Null for rows created before this
    # column existed.
    content_sha256: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        index=True,
    )

    # Set only when status == "duplicate": the canonical artifact (same
    # analysis, identical content_sha256, uploaded first) this one is a
    # duplicate of. That canonical artifact is the one actually processed;
    # this row is never dispatched for ingestion and never gets its own
    # Evidence set.
    duplicate_of_artifact_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("analysis_artifacts.id", ondelete="SET NULL"),
        nullable=True,
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

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    # Small, bounded unstructured-diagnostic-text fallback (Sections 9-11),
    # e.g. {"kind": "text"|"ocr", "text": "...", "ocr_confidence": 0.94} -
    # captured during this artifact's own (only) ingestion pass, used only
    # when the WHOLE analysis otherwise retains zero structured Evidence.
    # Never raw image bytes, never an unbounded slice of the artifact.
    fallback_context: Mapped[dict | None] = mapped_column(
        JSON,
        nullable=True,
    )

    analysis = relationship(
        "Analysis",
        back_populates="artifacts",
    )