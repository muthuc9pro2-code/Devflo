from datetime import datetime
from sqlalchemy import JSON, BigInteger, DateTime, Enum, ForeignKey, Index, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.db.database import Base

class Analysis(Base):
    __tablename__ = "analyses"
    __table_args__ = (
        Index("ix_analyses_user_id_created_at_id", "user_id", "created_at", "id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    original_filename: Mapped[str] = mapped_column(String(255), nullable=False)
    saved_file_path: Mapped[str] = mapped_column(String(500), nullable=False)
    source_kind: Mapped[str | None] = mapped_column(String(20), nullable=True)
    source_reference: Mapped[str | None] = mapped_column(String(500), nullable=True)
    source_status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    source_failure_reason: Mapped[str | None] = mapped_column(String(500), nullable=True)

    status: Mapped[str] = mapped_column(
        Enum(
            "pending", "processing", "completed", "failed", "cancelled",
            name="analysis_status",
        ),
        default="pending",
        nullable=False,
    )

    processing_heartbeat_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    processing_generation: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    finalization_generation: Mapped[int | None] = mapped_column(
        Integer, nullable=True
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
    ai_analysis: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    result_snapshot: Mapped[dict | None] = mapped_column(JSON, nullable=True)
