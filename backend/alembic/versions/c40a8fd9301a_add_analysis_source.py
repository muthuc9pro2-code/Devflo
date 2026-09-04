from collections.abc import Sequence
import sqlalchemy as sa
from alembic import op

revision: str = "c40a8fd9301a"
down_revision: str | Sequence[str] | None = "4a8c2f1d9e70"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

def upgrade() -> None:
    op.add_column("analyses", sa.Column("source_kind", sa.String(20)))
    op.add_column("analyses", sa.Column("source_reference", sa.String(500)))
    op.add_column("evidence", sa.Column("source_matches", sa.JSON()))

def downgrade() -> None:
    op.drop_column("evidence", "source_matches")
    op.drop_column("analyses", "source_reference")
    op.drop_column("analyses", "source_kind")
