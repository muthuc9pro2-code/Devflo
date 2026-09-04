from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = '83c1b577957f'
down_revision: Union[str, Sequence[str], None] = '46219a42ab55'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    op.add_column('analyses', sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False))

def downgrade() -> None:
    op.drop_column('analyses', 'updated_at')
