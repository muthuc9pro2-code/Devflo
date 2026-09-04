from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = '46219a42ab55'
down_revision: Union[str, Sequence[str], None] = '7c0a72de692d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    op.create_table('analyses',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('user_id', sa.Integer(), nullable=False),
    sa.Column('original_filename', sa.String(length=255), nullable=False),
    sa.Column('status', sa.Enum('pending', 'processing', 'completed', 'failed', name='analysis_status'), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
    sa.PrimaryKeyConstraint('id')
    )

def downgrade() -> None:
    op.drop_table('analyses')
