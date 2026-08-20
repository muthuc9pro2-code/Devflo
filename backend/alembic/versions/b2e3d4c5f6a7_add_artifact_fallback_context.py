"""add analysis_artifacts fallback_context"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "b2e3d4c5f6a7"
down_revision: str | Sequence[str] | None = "a1f2c3d4e5b6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "analysis_artifacts", sa.Column("fallback_context", sa.JSON(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("analysis_artifacts", "fallback_context")
