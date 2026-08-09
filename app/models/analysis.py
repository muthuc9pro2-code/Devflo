from datetime import datetime
from sqlalchemy import DateTime, Enum, ForeignKey, String, func, BigInteger
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.db.database import Base

class Analysis(Base):
    __tablename__ = "analyses"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id"),
        nullable=False
    )
    original_filename: Mapped[str] = mapped_column(
        String(255),
        nullable=False
    )
    saved_file_path: Mapped[str] = mapped_column(
        String(500),
        nullable=False
    )
    status: Mapped[str] = mapped_column(
        Enum(
            "pending",
            "processing",
            "completed",
            "failed",
            name="analysis_status"
            ),
        default="pending",
        nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now()
    )
    user = relationship(
        "User",
        back_populates="analyses"
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