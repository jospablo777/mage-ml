"""Add the after_run schedule type

Revision ID: b4e8d2f61c09
Revises: a7c31e9f0d42
Create Date: 2026-10-11 17:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b4e8d2f61c09'
down_revision = 'a7c31e9f0d42'
branch_labels = None
depends_on = None

VALUES = ('TIME', 'API', 'EVENT')


def upgrade() -> None:
    # SQLAlchemy stores the enum's names. PostgreSQL and MySQL keep a native enum type;
    # other databases a VARCHAR as long as the longest name, EVENT, before AFTER_RUN.
    bind = op.get_bind()
    if bind.engine.name == 'postgresql':
        with op.get_context().autocommit_block():
            op.execute("ALTER TYPE scheduletype ADD VALUE IF NOT EXISTS 'AFTER_RUN'")
    elif bind.engine.name == 'mysql':
        names = ', '.join(f"'{v}'" for v in (*VALUES, 'AFTER_RUN'))
        op.execute(f'ALTER TABLE pipeline_schedule MODIFY COLUMN schedule_type ENUM({names})')
    elif bind.engine.name == 'mssql':
        op.alter_column(
            'pipeline_schedule', 'schedule_type',
            existing_type=sa.String(length=5), type_=sa.String(length=9),
        )


def downgrade() -> None:
    bind = op.get_bind()
    op.execute("DELETE FROM pipeline_schedule WHERE schedule_type = 'AFTER_RUN'")
    if bind.engine.name == 'postgresql':
        op.execute('ALTER TYPE scheduletype RENAME TO scheduletype_old')
        op.execute(f"CREATE TYPE scheduletype AS ENUM({', '.join(repr(v) for v in VALUES)})")
        op.execute(
            'ALTER TABLE pipeline_schedule ALTER COLUMN schedule_type TYPE scheduletype USING '
            'schedule_type::text::scheduletype'
        )
        op.execute('DROP TYPE scheduletype_old')
    elif bind.engine.name == 'mysql':
        names = ', '.join(f"'{v}'" for v in VALUES)
        op.execute(f'ALTER TABLE pipeline_schedule MODIFY COLUMN schedule_type ENUM({names})')
