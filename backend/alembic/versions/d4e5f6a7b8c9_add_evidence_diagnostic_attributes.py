from collections.abc import Sequence
import sqlalchemy as sa
from alembic import op

revision: str = "d4e5f6a7b8c9"
down_revision: str | Sequence[str] | None = "c3d4e5f6a7b8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

def upgrade() -> None:
    op.add_column(
        "evidence", sa.Column("diagnostic_attributes", sa.JSON(), nullable=True)
    )

def downgrade() -> None:
    op.drop_column("evidence", "diagnostic_attributes")
