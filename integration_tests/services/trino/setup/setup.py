"""
Create what Trino's catalogs need before Trino starts: the PostgreSQL database and
tables of the Iceberg catalog, which Trino does not create, and the MinIO bucket of the
Iceberg and Delta Lake tables.
"""
import os
import time

import boto3
import psycopg2
from botocore.exceptions import ClientError, EndpointConnectionError

BUCKET = 'trino-warehouse'
# The tables of Iceberg's JDBC catalog, as Trino's documentation gives them.
CATALOG_TABLES = '''
CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
    catalog_name VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL,
    property_key VARCHAR(255),
    property_value VARCHAR(1000),
    PRIMARY KEY (catalog_name, namespace, property_key)
);
CREATE TABLE IF NOT EXISTS iceberg_tables (
    catalog_name VARCHAR(255) NOT NULL,
    table_namespace VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    metadata_location VARCHAR(1000),
    previous_metadata_location VARCHAR(1000),
    iceberg_type VARCHAR(5),
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);
'''


def retry(function):
    for _ in range(120):
        try:
            return function()
        except (psycopg2.OperationalError, EndpointConnectionError):
            time.sleep(1)
    raise RuntimeError(f'{function.__name__} did not succeed')


def create_database():
    connection = psycopg2.connect(
        host='postgres', port=5432, user='postgres', password='test', dbname='postgres',
    )
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'trino'")
        if cursor.fetchone() is None:
            cursor.execute('CREATE DATABASE trino')
    connection.close()

    connection = psycopg2.connect(
        host='postgres', port=5432, user='postgres', password='test', dbname='trino',
    )
    with connection, connection.cursor() as cursor:
        cursor.execute(CATALOG_TABLES)
    connection.close()


def create_bucket():
    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv('S3_ENDPOINT', 'http://minio:9000'),
        aws_access_key_id='mage-test',
        aws_secret_access_key='mage-test-secret',
        region_name='us-east-1',
    )
    try:
        s3.create_bucket(Bucket=BUCKET)
    except ClientError as error:
        if error.response['Error']['Code'] != 'BucketAlreadyOwnedByYou':
            raise


retry(create_database)
retry(create_bucket)
print('Trino setup done')
