from collections.abc import Sequence
import sqlalchemy as sa
from alembic import op

revision: str = "a3f8c1d9e6b4"
down_revision: str | Sequence[str] | None = "e9f5a7b2c3d4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

def upgrade() -> None:
    op.add_column(
        "analyses",
        sa.Column(
            "processing_generation",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "analyses",
        sa.Column("finalization_generation", sa.Integer(), nullable=True),
    )

def downgrade() -> None:
    op.drop_column("analyses", "finalization_generation")
    op.drop_column("analyses", "processing_generation")
