from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = 'b31a40e834c7'
down_revision: Union[str, Sequence[str], None] = '7d13a8f36c03'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    pass

def downgrade() -> None:
    pass
