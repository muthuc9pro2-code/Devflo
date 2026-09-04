from collections.abc import Sequence
import sqlalchemy as sa
from alembic import op

revision: str = "9b3d7a1c5f6e"
down_revision: str | Sequence[str] | None = "e1a7c9f4b3d2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

def upgrade() -> None:
    op.add_column(
        "analysis_artifacts", sa.Column("content_sha256", sa.String(length=64), nullable=True)
    )
    op.create_index(
        op.f("ix_analysis_artifacts_content_sha256"),
        "analysis_artifacts",
        ["content_sha256"],
        unique=False,
    )
    op.add_column(
        "analysis_artifacts",
        sa.Column("duplicate_of_artifact_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_analysis_artifacts_duplicate_of_artifact_id",
        "analysis_artifacts",
        "analysis_artifacts",
        ["duplicate_of_artifact_id"],
        ["id"],
        ondelete="SET NULL",
    )

def downgrade() -> None:
    op.drop_constraint(
        "fk_analysis_artifacts_duplicate_of_artifact_id",
        "analysis_artifacts",
        type_="foreignkey",
    )
    op.drop_column("analysis_artifacts", "duplicate_of_artifact_id")
    op.drop_index(
        op.f("ix_analysis_artifacts_content_sha256"), table_name="analysis_artifacts"
    )
    op.drop_column("analysis_artifacts", "content_sha256")
