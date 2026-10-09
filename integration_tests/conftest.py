"""
Shared fixtures for integration tests against real services.

Services come from integration_tests/compose.yaml. Tests read their addresses from the
environment and skip when a service is not configured, so a plain run without services
reports skips. The Makefile sets the variables, so a run through it fails when a service
is unreachable.
"""

import os
import sys
import uuid
from pathlib import Path

# Mage uses a temporary test.db for its own metadata when ENV is test_mage. It has to be
# set before mage_ai is imported.
os.environ['ENV'] = 'test_mage'

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import psycopg2  # noqa: E402
import pytest  # noqa: E402

from integration_tests.data import postgres_dataset  # noqa: E402

# The Mage project fixture, shared by the pipeline tests.
pytest_plugins = ['integration_tests.mage_runner']

POSTGRES_ENV = {
    'dbname': 'MAGE_TEST_POSTGRES_DBNAME',
    'host': 'MAGE_TEST_POSTGRES_HOST',
    'password': 'MAGE_TEST_POSTGRES_PASSWORD',
    'port': 'MAGE_TEST_POSTGRES_PORT',
    'user': 'MAGE_TEST_POSTGRES_USER',
}
# Faker rows after the edge rows. 2,500 rows cross the default INSERT page size twice.
FAKER_ROW_COUNT = int(os.getenv('MAGE_TEST_FAKER_ROWS', '2500'))
# A statement waiting on a lock fails after this long, so a leaked transaction shows up as
# an error and not as a hung run.
LOCK_TIMEOUT = '-c lock_timeout=15s'


@pytest.fixture(scope='session')
def postgres_settings():
    settings = {key: os.getenv(variable) for key, variable in POSTGRES_ENV.items()}
    missing = [POSTGRES_ENV[key] for key, value in settings.items() if not value]
    if missing:
        pytest.skip(f'PostgreSQL is not configured: {", ".join(missing)} unset')
    return settings


@pytest.fixture
def pg(postgres_settings):
    """
    A psycopg2 connection that bypasses Mage, for setup and for reading results.
    """
    connection = psycopg2.connect(options=LOCK_TIMEOUT, **postgres_settings)
    yield connection
    connection.close()


@pytest.fixture
def schema(pg):
    """
    A schema used by one test and dropped after it.
    """
    name = f'it_{uuid.uuid4().hex[:12]}'
    with pg.cursor() as cursor:
        cursor.execute(f'CREATE SCHEMA {name}')
    pg.commit()
    yield name
    pg.rollback()
    with pg.cursor() as cursor:
        cursor.execute(f'DROP SCHEMA {name} CASCADE')
    pg.commit()


@pytest.fixture
def mage_postgres(postgres_settings, schema):
    """
    Mage's client. It depends on schema, so it closes before the schema is dropped.
    """
    from mage_ai.io.postgres import Postgres

    client = Postgres(verbose=False, options=LOCK_TIMEOUT, **postgres_settings)
    client.open()
    yield client
    client.close()


@pytest.fixture(scope='session')
def source_rows():
    return postgres_dataset.edge_rows() + postgres_dataset.faker_rows(FAKER_ROW_COUNT)


@pytest.fixture
def source_table(pg, schema, source_rows):
    postgres_dataset.create_source_table(pg, schema, 'src', source_rows)
    return 'src'


@pytest.fixture(scope='session')
def redis_url():
    url = os.getenv('MAGE_TEST_REDIS_URL')
    if not url:
        pytest.skip('Redis is not configured: MAGE_TEST_REDIS_URL unset')
    return url


@pytest.fixture(scope='session')
def api_url():
    url = os.getenv('MAGE_TEST_API_URL')
    if not url:
        pytest.skip('The test API is not configured: MAGE_TEST_API_URL unset')
    return url.rstrip('/')


@pytest.fixture
def collection():
    """A collection name on the test API, unique to the test."""
    return f'c_{uuid.uuid4().hex[:12]}'
