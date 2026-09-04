from collections.abc import Sequence
import sqlalchemy as sa
from alembic import op

revision: str = "4a8c2f1d9e70"
down_revision: str | Sequence[str] | None = "9ca196c91d66"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

def upgrade() -> None:
    op.create_table(
        "analysis_artifacts",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("analysis_id", sa.Integer(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("original_filename", sa.String(length=255), nullable=False),
        sa.Column("saved_file_path", sa.String(length=500), nullable=False),
        sa.Column("content_type", sa.String(length=255), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("detected_format", sa.String(length=50), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("last_processed_line", sa.BigInteger(), nullable=False),
        sa.Column("processed_bytes", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["analysis_id"],
            ["analyses.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "analysis_id",
            "position",
            name="uq_analysis_artifact_position",
        ),
        sa.UniqueConstraint(
            "id",
            "analysis_id",
            name="uq_analysis_artifact_id_analysis",
        ),
    )
    op.create_index(
        op.f("ix_analysis_artifacts_analysis_id"),
        "analysis_artifacts",
        ["analysis_id"],
        unique=False,
    )

    op.execute(
        sa.text(
            "INSERT INTO analysis_artifacts "
            "(analysis_id, position, original_filename, saved_file_path, "
            "content_type, size_bytes, detected_format, status, "
            "last_processed_line, processed_bytes) "
            "SELECT id, 0, original_filename, saved_file_path, NULL, "
            "CASE WHEN status = 'completed' THEN processed_bytes ELSE 0 END, "
            "CASE WHEN status <> 'completed' "
            "THEN 'generic' ELSE NULL END, "
            "CASE WHEN status = 'completed' THEN 'completed' ELSE 'pending' END, "
            "last_processed_line, processed_bytes FROM analyses"
        )
    )

    op.add_column("evidence", sa.Column("artifact_id", sa.Integer(), nullable=True))
    op.add_column(
        "evidence", sa.Column("correlation_key", sa.String(64), nullable=True)
    )
    op.add_column("evidence", sa.Column("span_id", sa.String(255)))
    op.add_column("evidence", sa.Column("parent_span_id", sa.String(255)))
    op.add_column("evidence", sa.Column("service", sa.String(255)))
    op.add_column("evidence", sa.Column("module", sa.String(255)))
    op.add_column("evidence", sa.Column("host", sa.String(255)))
    op.add_column("evidence", sa.Column("container", sa.String(255)))
    op.add_column("evidence", sa.Column("pod", sa.String(255)))
    op.add_column("evidence", sa.Column("endpoint", sa.String(500)))
    op.add_column("evidence", sa.Column("http_status", sa.Integer()))
    op.add_column("evidence", sa.Column("source_file", sa.String(255)))
    op.add_column("evidence", sa.Column("source_format", sa.String(50)))
    op.alter_column(
        "evidence",
        "resolved_identity",
        existing_type=sa.String(length=255),
        type_=sa.String(length=263),
        existing_nullable=True,
    )

    op.execute(
        sa.text(
            "UPDATE evidence SET artifact_id = "
            "(SELECT MIN(analysis_artifacts.id) FROM analysis_artifacts "
            "WHERE analysis_artifacts.analysis_id = evidence.analysis_id)"
        )
    )
    op.execute(
        sa.text(
            "UPDATE evidence SET correlation_key = SHA2(CONCAT_WS('|', "
            "COALESCE(trace_id, '__none__'), "
            "COALESCE(request_id, '__none__'), '__none__'), 256)"
        )
    )
    op.alter_column(
        "evidence",
        "artifact_id",
        existing_type=sa.Integer(),
        nullable=False,
    )
    op.create_index(
        "ix_evidence_artifact_analysis",
        "evidence",
        ["artifact_id", "analysis_id"],
    )
    op.create_foreign_key(
        "fk_evidence_artifact_analysis",
        "evidence",
        "analysis_artifacts",
        ["artifact_id", "analysis_id"],
        ["id", "analysis_id"],
        ondelete="CASCADE",
    )
    op.alter_column(
        "evidence",
        "correlation_key",
        existing_type=sa.String(length=64),
        nullable=False,
    )

    op.execute(sa.text("DROP TEMPORARY TABLE IF EXISTS evidence_upgrade_rollup"))
    op.execute(
        sa.text(
            "CREATE TEMPORARY TABLE evidence_upgrade_rollup AS "
            "SELECT MIN(id) AS survivor_id, analysis_id, artifact_id, "
            "fingerprint, correlation_key, "
            "SUM(occurrence_count) AS occurrence_count, "
            "MIN(first_seen) AS first_seen, MAX(last_seen) AS last_seen, "
            "MIN(first_line_number) AS first_line_number, "
            "MAX(last_line_number) AS last_line_number "
            "FROM evidence GROUP BY analysis_id, artifact_id, fingerprint, "
            "correlation_key"
        )
    )
    op.execute(
        sa.text(
            "UPDATE evidence AS survivor "
            "INNER JOIN evidence_upgrade_rollup AS rollup "
            "ON survivor.id = rollup.survivor_id "
            "SET survivor.occurrence_count = rollup.occurrence_count, "
            "survivor.first_seen = rollup.first_seen, "
            "survivor.last_seen = rollup.last_seen, "
            "survivor.first_line_number = rollup.first_line_number, "
            "survivor.last_line_number = rollup.last_line_number"
        )
    )
    op.execute(
        sa.text(
            "DELETE duplicate FROM evidence AS duplicate "
            "INNER JOIN evidence_upgrade_rollup AS rollup ON "
            "duplicate.analysis_id = rollup.analysis_id AND "
            "duplicate.artifact_id = rollup.artifact_id AND "
            "duplicate.fingerprint = rollup.fingerprint AND "
            "duplicate.correlation_key = rollup.correlation_key AND "
            "duplicate.id <> rollup.survivor_id"
        )
    )
    op.execute(sa.text("DROP TEMPORARY TABLE evidence_upgrade_rollup"))

    op.drop_constraint(
        "uq_evidence_analysis_fingerprint_identity",
        "evidence",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_evidence_artifact_fingerprint_correlation",
        "evidence",
        ["analysis_id", "artifact_id", "fingerprint", "correlation_key"],
    )
    op.create_index(
        "ix_evidence_analysis_span_id",
        "evidence",
        ["analysis_id", "span_id"],
    )
    op.create_index(
        "ix_evidence_analysis_parent_span_id",
        "evidence",
        ["analysis_id", "parent_span_id"],
    )
    op.create_index(
        "ix_evidence_analysis_first_seen_id",
        "evidence",
        ["analysis_id", "first_seen", "id"],
    )

def downgrade() -> None:
    op.drop_index("ix_evidence_analysis_first_seen_id", table_name="evidence")
    op.drop_index("ix_evidence_analysis_parent_span_id", table_name="evidence")
    op.drop_index("ix_evidence_analysis_span_id", table_name="evidence")
    op.drop_constraint(
        "fk_evidence_artifact_analysis",
        "evidence",
        type_="foreignkey",
    )
    op.drop_index("ix_evidence_artifact_analysis", table_name="evidence")
    op.drop_constraint(
        "uq_evidence_artifact_fingerprint_correlation",
        "evidence",
        type_="unique",
    )
    op.execute(sa.text("DROP TEMPORARY TABLE IF EXISTS evidence_downgrade_rollup"))
    op.execute(
        sa.text(
            "CREATE TEMPORARY TABLE evidence_downgrade_rollup AS "
            "SELECT MIN(id) AS survivor_id, analysis_id, fingerprint, trace_id, "
            "request_id, SUM(occurrence_count) AS occurrence_count, "
            "MIN(first_seen) AS first_seen, MAX(last_seen) AS last_seen, "
            "MIN(first_line_number) AS first_line_number, "
            "MAX(last_line_number) AS last_line_number "
            "FROM evidence GROUP BY analysis_id, fingerprint, trace_id, request_id"
        )
    )
    op.execute(
        sa.text(
            "UPDATE evidence AS survivor "
            "INNER JOIN evidence_downgrade_rollup AS rollup "
            "ON survivor.id = rollup.survivor_id "
            "SET survivor.occurrence_count = rollup.occurrence_count, "
            "survivor.first_seen = rollup.first_seen, "
            "survivor.last_seen = rollup.last_seen, "
            "survivor.first_line_number = rollup.first_line_number, "
            "survivor.last_line_number = rollup.last_line_number"
        )
    )
    op.execute(
        sa.text(
            "DELETE duplicate FROM evidence AS duplicate "
            "INNER JOIN evidence_downgrade_rollup AS rollup ON "
            "duplicate.analysis_id = rollup.analysis_id AND "
            "duplicate.fingerprint = rollup.fingerprint AND "
            "duplicate.trace_id <=> rollup.trace_id AND "
            "duplicate.request_id <=> rollup.request_id AND "
            "duplicate.id <> rollup.survivor_id"
        )
    )
    op.execute(sa.text("DROP TEMPORARY TABLE evidence_downgrade_rollup"))
    op.create_unique_constraint(
        "uq_evidence_analysis_fingerprint_identity",
        "evidence",
        ["analysis_id", "fingerprint", "trace_id", "request_id"],
    )
    op.execute(
        sa.text(
            "UPDATE evidence SET resolved_identity = LEFT(resolved_identity, 255) "
            "WHERE CHAR_LENGTH(resolved_identity) > 255"
        )
    )
    op.alter_column(
        "evidence",
        "resolved_identity",
        existing_type=sa.String(length=263),
        type_=sa.String(length=255),
        existing_nullable=True,
    )

    op.drop_column("evidence", "source_format")
    op.drop_column("evidence", "source_file")
    op.drop_column("evidence", "http_status")
    op.drop_column("evidence", "endpoint")
    op.drop_column("evidence", "pod")
    op.drop_column("evidence", "container")
    op.drop_column("evidence", "host")
    op.drop_column("evidence", "module")
    op.drop_column("evidence", "service")
    op.drop_column("evidence", "parent_span_id")
    op.drop_column("evidence", "span_id")
    op.drop_column("evidence", "correlation_key")
    op.drop_column("evidence", "artifact_id")

    op.drop_index(
        op.f("ix_analysis_artifacts_analysis_id"),
        table_name="analysis_artifacts",
    )
    op.drop_table("analysis_artifacts")
