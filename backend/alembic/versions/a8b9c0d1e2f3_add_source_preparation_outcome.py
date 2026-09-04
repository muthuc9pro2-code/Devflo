from collections.abc import Sequence
import sqlalchemy as sa
from alembic import op

revision: str = "a8b9c0d1e2f3"
down_revision: str | Sequence[str] | None = "f7a8b9c0d1e2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

def upgrade() -> None:
    op.add_column(
        "analyses",
        sa.Column("source_status", sa.String(length=20), nullable=True),
    )
    op.add_column(
        "analyses",
        sa.Column("source_failure_reason", sa.String(length=500), nullable=True),
    )

def downgrade() -> None:
    op.drop_column("analyses", "source_failure_reason")
    op.drop_column("analyses", "source_status")
