"""Add attempt to block_run

Each claim of a block run by a worker increments its attempt; the worker's status writes
apply only while the attempt is still its own, so a superseded worker cannot overwrite
a newer state.

Revision ID: a7c31e9f0d42
Revises: 39d36f1dab73
Create Date: 2026-10-10 13:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a7c31e9f0d42'
down_revision = '39d36f1dab73'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'block_run',
        sa.Column('attempt', sa.Integer(), nullable=False, server_default='0'),
    )


def downgrade() -> None:
    with op.batch_alter_table('block_run') as batch_op:
        batch_op.drop_column('attempt')
