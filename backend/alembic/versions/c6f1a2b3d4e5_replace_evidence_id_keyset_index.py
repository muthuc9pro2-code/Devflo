from collections.abc import Sequence
from alembic import op

revision: str = "c6f1a2b3d4e5"
down_revision: str | Sequence[str] | None = "b7d2f4a9c1e6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

def upgrade() -> None:
    op.create_index(
        "ix_evidence_analysis_first_line_logical",
        "evidence",
        [
            "analysis_id",
            "first_line_number",
            "fingerprint",
            "correlation_key",
        ],
    )
    op.drop_index(
        "ix_evidence_analysis_first_line_number_id",
        table_name="evidence",
    )

def downgrade() -> None:
    op.create_index(
        "ix_evidence_analysis_first_line_number_id",
        "evidence",
        ["analysis_id", "first_line_number", "id"],
    )
    op.drop_index(
        "ix_evidence_analysis_first_line_logical",
        table_name="evidence",
    )
