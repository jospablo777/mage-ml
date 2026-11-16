"""Add the audit_event table

Revision ID: c6f1a9e4d2b7
Revises: b4e8d2f61c09
Create Date: 2026-11-16 16:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c6f1a9e4d2b7'
down_revision = 'b4e8d2f61c09'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'audit_event',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(),
                  nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(),
                  nullable=True),
        sa.Column('user_id', sa.Integer(), nullable=True),
        sa.Column('user_name', sa.String(length=255), nullable=True),
        sa.Column('action', sa.String(length=32), nullable=True),
        sa.Column('object_type', sa.String(length=255), nullable=True),
        sa.Column('object_id', sa.String(length=255), nullable=True),
        sa.Column('parent', sa.String(length=255), nullable=True),
        sa.Column('parent_id', sa.String(length=255), nullable=True),
        sa.Column('outcome', sa.String(length=32), nullable=True),
        sa.Column('error', sa.Text(), nullable=True),
        sa.Column('changes', sa.JSON(), nullable=True),
        sa.Column('source_ip', sa.String(length=255), nullable=True),
        sa.Column('count', sa.Integer(), nullable=True),
        sa.Column('last_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(op.f('ix_audit_event_created_at'), 'audit_event', ['created_at'])
    op.create_index(op.f('ix_audit_event_object_type'), 'audit_event',
                    ['object_type', 'object_id'])
    op.create_index(op.f('ix_audit_event_user_id'), 'audit_event', ['user_id'])


def downgrade() -> None:
    op.drop_index(op.f('ix_audit_event_user_id'), table_name='audit_event')
    op.drop_index(op.f('ix_audit_event_object_type'), table_name='audit_event')
    op.drop_index(op.f('ix_audit_event_created_at'), table_name='audit_event')
    op.drop_table('audit_event')
