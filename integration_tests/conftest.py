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


@pytest.fixture(scope='session')
def feast_url():
    url = os.getenv('MAGE_TEST_FEAST_URL')
    if not url:
        pytest.skip('Feast is not configured: MAGE_TEST_FEAST_URL unset')
    return url.rstrip('/')


@pytest.fixture
def feast_offline(postgres_settings, feast_url):
    """
    Mage's PostgreSQL client on the database of Feast's offline store. Rows a test
    writes for its drivers are deleted afterwards.
    """
    from mage_ai.io.postgres import Postgres

    client = Postgres(
        verbose=False, options=LOCK_TIMEOUT, **dict(postgres_settings, dbname='feast'),
    )
    client.open()
    client.written_drivers = []
    yield client
    if client.written_drivers:
        client.execute(
            'DELETE FROM driver_hourly_stats WHERE driver_id = ANY(%(ids)s)',
            ids=list(client.written_drivers),
        )
    client.close()


@pytest.fixture(scope='session')
def mlflow_url():
    url = os.getenv('MAGE_TEST_MLFLOW_URL')
    if not url:
        pytest.skip('MLflow is not configured: MAGE_TEST_MLFLOW_URL unset')
    os.environ.setdefault('MLFLOW_DISABLE_AGENT_HINT', '1')
    return url.rstrip('/')


@pytest.fixture
def duckdb_path(tmp_path):
    return str(tmp_path / 'test.duckdb')


@pytest.fixture
def duckdb_client(duckdb_path):
    """Mage's DuckDB client on a new database file holding the source table src."""
    from integration_tests.data import duckdb_dataset
    from mage_ai.io.duckdb import DuckDB

    client = DuckDB(database=duckdb_path, verbose=False)
    duckdb_dataset.create_source_table(client.conn)
    yield client
    client.close()


S3_ENV = {
    'endpoint_url': 'MAGE_TEST_S3_ENDPOINT',
    'aws_access_key_id': 'MAGE_TEST_S3_ACCESS_KEY',
    'aws_secret_access_key': 'MAGE_TEST_S3_SECRET_KEY',
}


@pytest.fixture(scope='session')
def s3_settings():
    settings = {key: os.getenv(variable) for key, variable in S3_ENV.items()}
    missing = [S3_ENV[key] for key, value in settings.items() if not value]
    if missing:
        pytest.skip(f'S3 is not configured: {", ".join(missing)} unset')
    return dict(settings, region_name='us-east-1')


@pytest.fixture(scope='session')
def s3(s3_settings):
    """A boto3 client that bypasses Mage, for setup and for reading results."""
    import boto3

    return boto3.client('s3', **s3_settings)


@pytest.fixture
def bucket(s3):
    """A bucket used by one test, emptied and removed after it."""
    name = f'it-{uuid.uuid4().hex[:12]}'
    s3.create_bucket(Bucket=name)
    yield name
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=name):
        keys = [{'Key': item['Key']} for item in page.get('Contents', [])]
        if keys:
            s3.delete_objects(Bucket=name, Delete={'Objects': keys})
    s3.delete_bucket(Bucket=name)


@pytest.fixture
def mage_s3(s3_settings):
    """Mage's S3 client."""
    from mage_ai.io.s3 import S3

    return S3(verbose=False, **s3_settings)


@pytest.fixture
def s3_env(s3_settings, monkeypatch):
    """
    The AWS environment variables, which Mage's S3 block output storage reads through
    boto3.
    """
    monkeypatch.setenv('AWS_ENDPOINT_URL', s3_settings['endpoint_url'])
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', s3_settings['aws_access_key_id'])
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', s3_settings['aws_secret_access_key'])
    monkeypatch.setenv('AWS_DEFAULT_REGION', s3_settings['region_name'])
    return s3_settings


@pytest.fixture
def connector_config(s3_settings, bucket):
    """The S3 settings of the mage_integrations connectors, for the test bucket."""
    return dict(
        bucket=bucket,
        aws_access_key_id=s3_settings['aws_access_key_id'],
        aws_secret_access_key=s3_settings['aws_secret_access_key'],
        aws_endpoint=s3_settings['endpoint_url'],
        aws_region=s3_settings['region_name'],
    )


MYSQL_ENV = {
    'host': 'MAGE_TEST_MYSQL_HOST',
    'port': 'MAGE_TEST_MYSQL_PORT',
    'user': 'MAGE_TEST_MYSQL_USER',
    'password': 'MAGE_TEST_MYSQL_PASSWORD',
}


@pytest.fixture(scope='session')
def mysql_settings():
    settings = {key: os.getenv(variable) for key, variable in MYSQL_ENV.items()}
    missing = [MYSQL_ENV[key] for key, value in settings.items() if not value]
    if missing:
        pytest.skip(f'MySQL is not configured: {", ".join(missing)} unset')
    return dict(settings, port=int(settings['port']))


@pytest.fixture
def mysql_database(mysql_settings):
    """A database used by one test and dropped after it."""
    import mysql.connector

    name = f'it_{uuid.uuid4().hex[:12]}'
    connection = mysql.connector.connect(**mysql_settings)
    with connection.cursor() as cursor:
        cursor.execute(f'CREATE DATABASE `{name}`')
    yield name
    with connection.cursor() as cursor:
        cursor.execute(f'DROP DATABASE `{name}`')
    connection.close()


@pytest.fixture
def my(mysql_settings, mysql_database):
    """A mysql-connector connection that bypasses Mage, for setup and for reading results."""
    import mysql.connector

    connection = mysql.connector.connect(database=mysql_database, **mysql_settings)
    # Each read sees committed data; a transaction left open kept its snapshot.
    connection.autocommit = True
    yield connection
    connection.close()


@pytest.fixture
def mage_mysql(mysql_settings, mysql_database):
    """Mage's MySQL client on the test's database."""
    from mage_ai.io.mysql import MySQL

    client = MySQL(database=mysql_database, verbose=False, **mysql_settings)
    client.open()
    yield client
    client.close()


@pytest.fixture
def mysql_source(my):
    """The source table src, with one column per MySQL type."""
    from integration_tests.data import mysql_dataset

    with my.cursor() as cursor:
        mysql_dataset.create_source_table(cursor)
    my.commit()
    return 'src'


@pytest.fixture(scope='session')
def mongodb_url():
    url = os.getenv('MAGE_TEST_MONGODB_URL')
    if not url:
        pytest.skip('MongoDB is not configured: MAGE_TEST_MONGODB_URL unset')
    return url


@pytest.fixture
def mongo(mongodb_url):
    """A pymongo database used by one test and dropped after it."""
    from pymongo import MongoClient

    client = MongoClient(mongodb_url, uuidRepresentation='standard')
    name = f'it_{uuid.uuid4().hex[:12]}'
    yield client[name]
    client.drop_database(name)
    client.close()


@pytest.fixture
def mage_mongodb(mongodb_url, mongo):
    """Mage's MongoDB client on the test's database."""
    from mage_ai.io.mongodb import MongoDB

    client = MongoDB(connection_string=mongodb_url, database=mongo.name, verbose=False)
    yield client
    client.client.close()
