from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

revision: str = '8ad2f9a57815'
down_revision: Union[str, Sequence[str], None] = 'b31a40e834c7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    op.drop_column('users', 'role')

def downgrade() -> None:
    op.add_column('users', sa.Column('role', mysql.VARCHAR(length=20), nullable=False))
