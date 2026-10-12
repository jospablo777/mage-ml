"""
MageML's database migrations on PostgreSQL, which enforces foreign keys: upgrade and
downgrade with rows in the tables they touch.
"""
import os
import uuid

import pytest
from sqlalchemy import create_engine, text

AFTER_RUN = 'b4e8d2f61c09'
BEFORE_AFTER_RUN = 'a7c31e9f0d42'


def _alembic(url, action, revision):
    from alembic import command
    from alembic.config import Config

    import mage_ai.orchestration.db.database_manager as database_manager

    folder = os.path.dirname(database_manager.__file__)
    config = Config(os.path.join(folder, 'alembic.ini'), attributes={'configure_logger': False})
    config.set_main_option('script_location', os.path.join(folder, 'migrations'))
    config.set_main_option('sqlalchemy.url', url.replace('%', '%%'))
    getattr(command, action)(config, revision)


@pytest.fixture
def database(postgres_settings):
    s = postgres_settings
    server = f"postgresql://{s['user']}:{s['password']}@{s['host']}:{s['port']}"
    name = f'mage_migrations_{uuid.uuid4().hex[:8]}'
    admin = create_engine(f'{server}/postgres', isolation_level='AUTOCOMMIT')
    with admin.connect() as connection:
        connection.execute(text(f'CREATE DATABASE {name}'))
    yield f'{server}/{name}'
    with admin.connect() as connection:
        connection.execute(text(f'DROP DATABASE IF EXISTS {name} WITH (FORCE)'))
    admin.dispose()


def _trigger_with_a_run(connection, schedule_type):
    schedule = connection.execute(text(
        'INSERT INTO pipeline_schedule (name, pipeline_uuid, schedule_type) '
        "VALUES (:name, 'p', :type) RETURNING id"
    ), dict(name=f'trigger_{schedule_type.lower()}', type=schedule_type)).scalar()
    run = connection.execute(text(
        "INSERT INTO pipeline_run (pipeline_schedule_id, pipeline_uuid) VALUES (:id, 'p') "
        'RETURNING id'
    ), dict(id=schedule)).scalar()
    connection.execute(text(
        "INSERT INTO block_run (pipeline_run_id, block_uuid) VALUES (:id, 'load')"
    ), dict(id=run))


def test_the_after_run_migration_downgrades_with_runs_of_after_run_triggers(database):
    """
    The downgrade deleted the after_run triggers while pipeline runs still referenced them,
    and PostgreSQL refused it: a database with one after-run run could not downgrade.
    """
    _alembic(database, 'upgrade', 'head')
    engine = create_engine(database)
    with engine.begin() as connection:
        _trigger_with_a_run(connection, 'AFTER_RUN')
        _trigger_with_a_run(connection, 'TIME')

    _alembic(database, 'downgrade', BEFORE_AFTER_RUN)

    with engine.connect() as connection:
        types = connection.execute(text('SELECT unnest(enum_range(NULL::scheduletype))'))
        assert [row[0] for row in types] == ['TIME', 'API', 'EVENT']
        schedules = connection.execute(text('SELECT schedule_type FROM pipeline_schedule'))
        assert [row[0] for row in schedules] == ['TIME']
        assert connection.execute(text('SELECT count(*) FROM pipeline_run')).scalar() == 1
        assert connection.execute(text('SELECT count(*) FROM block_run')).scalar() == 1

    _alembic(database, 'upgrade', AFTER_RUN)
    with engine.connect() as connection:
        types = connection.execute(text('SELECT unnest(enum_range(NULL::scheduletype))'))
        assert 'AFTER_RUN' in [row[0] for row in types]
    engine.dispose()
