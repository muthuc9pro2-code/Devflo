from typing import Sequence, Union
from alembic import op

revision: str = '9ca196c91d66'
down_revision: Union[str, Sequence[str], None] = '1466d19bd2b0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    op.create_unique_constraint('uq_evidence_analysis_fingerprint_identity', 'evidence', ['analysis_id', 'fingerprint', 'trace_id', 'request_id'])

def downgrade() -> None:
    op.drop_constraint('uq_evidence_analysis_fingerprint_identity', 'evidence', type_='unique')
