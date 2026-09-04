from collections.abc import Sequence
import sqlalchemy as sa
from alembic import op

revision: str = "d8e4f6a1b2c3"
down_revision: str | Sequence[str] | None = "a1c8347ae37b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column(
            "token_version",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )

def downgrade() -> None:
    op.drop_column("users", "token_version")
