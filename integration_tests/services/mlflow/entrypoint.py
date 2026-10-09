"""
Start an MLflow tracking server for the integration tests.

PostgreSQL is the backend store and the server proxies artifacts from a local
directory, so clients download them over HTTP. Once the server answers, seed.py adds an
experiment, runs and a registered model, then a marker file lets the health check pass.
"""
import os
import subprocess
import sys
import time
import urllib.request

import psycopg2

SETTINGS = dict(
    host=os.environ['MLFLOW_PG_HOST'],
    port=int(os.environ['MLFLOW_PG_PORT']),
    user=os.environ['MLFLOW_PG_USER'],
    password=os.environ['MLFLOW_PG_PASSWORD'],
)
DATABASE = os.environ['MLFLOW_PG_DB']


def create_database() -> None:
    for _ in range(120):
        try:
            connection = psycopg2.connect(dbname='postgres', **SETTINGS)
            break
        except psycopg2.OperationalError:
            time.sleep(1)
    else:
        raise RuntimeError('PostgreSQL did not start')
    connection.autocommit = True
    with connection.cursor() as cursor:
        # Artifacts live in this container, so a database kept from an earlier container
        # would point at files that are gone. Every start begins empty.
        cursor.execute(f'DROP DATABASE IF EXISTS {DATABASE} WITH (FORCE)')
        cursor.execute(f'CREATE DATABASE {DATABASE}')
    connection.close()


def wait_for(url: str) -> None:
    for _ in range(240):
        try:
            urllib.request.urlopen(url, timeout=2)
            return
        except Exception:
            time.sleep(0.5)
    raise RuntimeError(f'{url} did not answer')


if __name__ == '__main__':
    create_database()
    store = (
        f"postgresql+psycopg2://{SETTINGS['user']}:{SETTINGS['password']}"
        f"@{SETTINGS['host']}:{SETTINGS['port']}/{DATABASE}"
    )
    server = subprocess.Popen([
        'mlflow', 'server',
        '--backend-store-uri', store,
        '--artifacts-destination', '/mlflow/artifacts',
        '--serve-artifacts',
        '--host', '0.0.0.0',
        '--port', '5000',
        '--workers', '2',
    ])
    wait_for('http://127.0.0.1:5000/health')
    subprocess.run([sys.executable, 'seed.py'], check=True)
    sys.exit(server.wait())
