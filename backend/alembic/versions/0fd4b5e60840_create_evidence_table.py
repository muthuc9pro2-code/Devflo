from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = '0fd4b5e60840'
down_revision: Union[str, Sequence[str], None] = '8ad2f9a57815'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    op.create_table('evidence',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('analysis_id', sa.Integer(), nullable=False),
    sa.Column('fingerprint', sa.String(length=255), nullable=False),
    sa.Column('event_type', sa.String(length=100), nullable=True),
    sa.Column('severity', sa.String(length=50), nullable=True),
    sa.Column('trace_id', sa.String(length=255), nullable=True),
    sa.Column('request_id', sa.String(length=255), nullable=True),
    sa.Column('first_seen', sa.DateTime(), nullable=True),
    sa.Column('last_seen', sa.DateTime(), nullable=True),
    sa.Column('occurrence_count', sa.Integer(), nullable=False),
    sa.Column('first_line_number', sa.BigInteger(), nullable=False),
    sa.Column('last_line_number', sa.BigInteger(), nullable=False),
    sa.Column('representative_line', sa.Text(), nullable=True),
    sa.ForeignKeyConstraint(['analysis_id'], ['analyses.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_evidence_analysis_id'), 'evidence', ['analysis_id'], unique=False)
    op.create_index(op.f('ix_evidence_fingerprint'), 'evidence', ['fingerprint'], unique=False)
    op.create_index(op.f('ix_evidence_id'), 'evidence', ['id'], unique=False)
    op.create_index(op.f('ix_evidence_request_id'), 'evidence', ['request_id'], unique=False)
    op.create_index(op.f('ix_evidence_trace_id'), 'evidence', ['trace_id'], unique=False)

def downgrade() -> None:
    op.drop_index(op.f('ix_evidence_trace_id'), table_name='evidence')
    op.drop_index(op.f('ix_evidence_request_id'), table_name='evidence')
    op.drop_index(op.f('ix_evidence_id'), table_name='evidence')
    op.drop_index(op.f('ix_evidence_fingerprint'), table_name='evidence')
    op.drop_index(op.f('ix_evidence_analysis_id'), table_name='evidence')
    op.drop_table('evidence')
