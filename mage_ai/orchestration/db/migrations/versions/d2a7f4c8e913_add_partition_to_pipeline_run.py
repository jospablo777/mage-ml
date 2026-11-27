"""Add partition to pipeline_run

Revision ID: d2a7f4c8e913
Revises: c6f1a9e4d2b7
Create Date: 2026-11-27 04:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd2a7f4c8e913'
down_revision = 'c6f1a9e4d2b7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table('pipeline_run', schema=None) as batch_op:
        batch_op.add_column(sa.Column('partition', sa.String(length=255), nullable=True))
        batch_op.create_index(
            'ix_pipeline_run_pipeline_uuid_partition',
            ['pipeline_uuid', 'partition', 'id'],
            unique=False,
        )


def downgrade() -> None:
    with op.batch_alter_table('pipeline_run', schema=None) as batch_op:
        batch_op.drop_index('ix_pipeline_run_pipeline_uuid_partition')
        batch_op.drop_column('partition')
