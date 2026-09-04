from collections.abc import Sequence
import sqlalchemy as sa
from alembic import op

revision: str = "a1c8347ae37b"
down_revision: str | Sequence[str] | None = "a8b9c0d1e2f3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

def upgrade() -> None:
    op.alter_column(
        "analyses",
        "status",
        existing_type=sa.Enum(
            "pending", "processing", "completed", "failed", name="analysis_status"
        ),
        type_=sa.Enum(
            "pending", "processing", "completed", "failed", "cancelled",
            name="analysis_status",
        ),
        existing_nullable=False,
    )
    op.add_column(
        "analyses",
        sa.Column("processing_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
    )

def downgrade() -> None:
    op.drop_column("analyses", "processing_heartbeat_at")
    op.alter_column(
        "analyses",
        "status",
        existing_type=sa.Enum(
            "pending", "processing", "completed", "failed", "cancelled",
            name="analysis_status",
        ),
        type_=sa.Enum(
            "pending", "processing", "completed", "failed", name="analysis_status"
        ),
        existing_nullable=False,
    )
