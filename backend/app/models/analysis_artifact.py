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

    failure_reason: Mapped[str | None] = mapped_column(
        String(500),
        nullable=True,
    )

    content_sha256: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        index=True,
    )

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

    fallback_context: Mapped[dict | None] = mapped_column(
        JSON,
        nullable=True,
    )

    analysis = relationship(
        "Analysis",
        back_populates="artifacts",
    )
