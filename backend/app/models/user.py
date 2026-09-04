from sqlalchemy import Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.db.database import Base
from datetime import datetime

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String(50), unique=True)
    email: Mapped[str] = mapped_column(String(255), unique=True)
    hashed_password: Mapped[str] = mapped_column(String(255))
    is_verified: Mapped[bool] = mapped_column(default=False)
    token_version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    unverified_activity_at: Mapped[datetime | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    analyses = relationship("Analysis", back_populates="user", cascade="all, delete-orphan")
