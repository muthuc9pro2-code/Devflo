from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = '7d13a8f36c03'
down_revision: Union[str, Sequence[str], None] = '83c1b577957f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    op.add_column('analyses', sa.Column('saved_file_path', sa.String(length=500), nullable=False))

def downgrade() -> None:
    op.drop_column('analyses', 'saved_file_path')
