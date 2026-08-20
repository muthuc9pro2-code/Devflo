"""widen evidence.first_seen/last_seen to microsecond precision on MySQL"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import mysql

from alembic import op

revision: str = "e5f6a7b8c9d0"
down_revision: str | Sequence[str] | None = "d4e5f6a7b8c9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column(
        "evidence",
        "first_seen",
        existing_type=sa.DateTime(),
        type_=mysql.DATETIME(fsp=6),
        existing_nullable=True,
    )
    op.alter_column(
        "evidence",
        "last_seen",
        existing_type=sa.DateTime(),
        type_=mysql.DATETIME(fsp=6),
        existing_nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "evidence",
        "first_seen",
        existing_type=mysql.DATETIME(fsp=6),
        type_=sa.DateTime(),
        existing_nullable=True,
    )
    op.alter_column(
        "evidence",
        "last_seen",
        existing_type=mysql.DATETIME(fsp=6),
        type_=sa.DateTime(),
        existing_nullable=True,
    )
