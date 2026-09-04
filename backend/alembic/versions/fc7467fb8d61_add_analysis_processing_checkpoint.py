from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = 'fc7467fb8d61'
down_revision: Union[str, Sequence[str], None] = '0fd4b5e60840'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    op.add_column('analyses', sa.Column('last_processed_line', sa.BigInteger(), nullable=False))
    op.add_column('analyses', sa.Column('processed_bytes', sa.BigInteger(), nullable=False))

def downgrade() -> None:
    op.drop_column('analyses', 'processed_bytes')
    op.drop_column('analyses', 'last_processed_line')
