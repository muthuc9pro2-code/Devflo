"""add evidence (analysis_id, first_line_number, id) index for deterministic
bounded-selection keyset pagination

Revision ID: b7d2f4a9c1e6
Revises: a3f8c1d9e6b4
Create Date: 2026-08-29 00:00:00.000000

"""

from collections.abc import Sequence

from alembic import op

revision: str = "b7d2f4a9c1e6"
down_revision: str | Sequence[str] | None = "a3f8c1d9e6b4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ix_evidence_analysis_first_line_number_id",
        "evidence",
        ["analysis_id", "first_line_number", "id"],
    )


def downgrade() -> None:
    op.drop_index("ix_evidence_analysis_first_line_number_id", table_name="evidence")
