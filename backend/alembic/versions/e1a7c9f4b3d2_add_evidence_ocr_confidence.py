"""add evidence ocr_confidence"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "e1a7c9f4b3d2"
down_revision: str | Sequence[str] | None = "c40a8fd9301a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("evidence", sa.Column("ocr_confidence", sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column("evidence", "ocr_confidence")
