from collections.abc import Sequence
import sqlalchemy as sa
from alembic import op

revision: str = "e9f5a7b2c3d4"
down_revision: str | Sequence[str] | None = "d8e4f6a1b2c3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("unverified_activity_at", sa.DateTime(), nullable=True),
    )

def downgrade() -> None:
    op.drop_column("users", "unverified_activity_at")
