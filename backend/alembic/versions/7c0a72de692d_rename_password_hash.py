from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

revision: str = '7c0a72de692d'
down_revision: Union[str, Sequence[str], None] = 'bbfd2bfc27ec'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    op.add_column('users', sa.Column('hashed_password', sa.String(length=255), nullable=False))
    op.drop_column('users', 'password_hash')

def downgrade() -> None:
    op.add_column('users', sa.Column('password_hash', mysql.VARCHAR(length=255), nullable=False))
    op.drop_column('users', 'hashed_password')
