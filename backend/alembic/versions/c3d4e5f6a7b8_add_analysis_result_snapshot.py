"""add analyses.result_snapshot and history list index"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c3d4e5f6a7b8"
down_revision: str | Sequence[str] | None = "b2e3d4c5f6a7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "analyses", sa.Column("result_snapshot", sa.JSON(), nullable=True)
    )
    op.create_index(
        "ix_analyses_user_id_created_at_id",
        "analyses",
        ["user_id", "created_at", "id"],
    )


def downgrade() -> None:
    op.drop_index("ix_analyses_user_id_created_at_id", table_name="analyses")
    op.drop_column("analyses", "result_snapshot")
