"""add cancelled status and processing heartbeat

Revision ID: a1c8347ae37b
Revises: a8b9c0d1e2f3
Create Date: 2026-08-23

Two closely-related lifecycle fields, deliberately in one migration since
Analysis.status is a real MySQL ENUM and altering it requires its own DDL
statement anyway:

- "cancelled" added to the existing analysis_status enum (pending/
  processing/completed/failed preserved exactly, nothing renamed).
- Analysis.processing_heartbeat_at: nullable, no default - a throttled
  liveness signal (see app/tasks/analysis.py's heartbeat helper) used ONLY
  for orphan/stale-analysis recovery. Also reused, unmodified, as the
  atomic conditional-UPDATE fence a recovery scan claims a stale analysis
  with, so no second/lease field is needed.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op


revision: str = "a1c8347ae37b"
down_revision: str | Sequence[str] | None = "a8b9c0d1e2f3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column(
        "analyses",
        "status",
        existing_type=sa.Enum(
            "pending", "processing", "completed", "failed", name="analysis_status"
        ),
        type_=sa.Enum(
            "pending", "processing", "completed", "failed", "cancelled",
            name="analysis_status",
        ),
        existing_nullable=False,
    )
    op.add_column(
        "analyses",
        sa.Column("processing_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("analyses", "processing_heartbeat_at")
    op.alter_column(
        "analyses",
        "status",
        existing_type=sa.Enum(
            "pending", "processing", "completed", "failed", "cancelled",
            name="analysis_status",
        ),
        type_=sa.Enum(
            "pending", "processing", "completed", "failed", name="analysis_status"
        ),
        existing_nullable=False,
    )
