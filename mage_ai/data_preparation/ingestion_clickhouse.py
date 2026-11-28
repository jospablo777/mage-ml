"""
Guided ingestion into ClickHouse (ADR-0049): the destination's check, over ClickHouse's HTTP
interface (no driver), for ingestion.py. The stream engine writes the mirror.
"""
import base64
import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict

from mage_ai.data_preparation.ingestion import IngestionError, _check


def secure(connection: Dict[str, Any]) -> bool:
    """HTTPS unless TLS is turned off."""
    return (connection.get('sslmode') or 'prefer') != 'disable'


def port(connection: Dict[str, Any]) -> int:
    return int(connection.get('port') or (8443 if secure(connection) else 8123))


def _query(connection: Dict[str, Any], sql: str) -> Any:
    scheme = 'https' if secure(connection) else 'http'
    url = f"{scheme}://{connection['host']}:{port(connection)}/?" + urllib.parse.urlencode(
        dict(query=sql + ' FORMAT JSON'),
    )
    request = urllib.request.Request(url, method='POST', data=b'')
    credentials = f"{connection.get('user') or 'default'}:{connection.get('password') or ''}"
    request.add_header('Authorization',
                       'Basic ' + base64.b64encode(credentials.encode()).decode())
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.loads(response.read())['data']
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors='replace').strip().splitlines()[0][:300]
        raise IngestionError(f'ClickHouse answered {error.code}: {detail}') from None
    except Exception as error:
        raise IngestionError(f'Could not connect: {str(error).strip()}') from None


def check_destination(connection: Dict[str, Any]) -> Dict[str, Any]:
    """Whether ClickHouse answers, and whether the database exists or will be made."""
    for key in ('host', 'database'):
        if not connection.get(key):
            raise IngestionError(f'The connection needs {key}.')
    database = connection['database']
    version = _query(connection, 'SELECT version() AS v')[0]['v']
    exists = bool(_query(
        connection, 'SELECT count() AS n FROM system.databases WHERE name = '
        + "'" + database.replace('\\', '\\\\').replace("'", "\\'") + "'",
    )[0]['n'])
    checks = [
        _check('connect', True, f'Connected to ClickHouse {version}.'),
        _check('database', True,
               f'Tables will be made in {database}.' if exists else
               f'{database} does not exist; it will be made (CREATE DATABASE).'),
    ]
    parts = [int(p) for p in str(version).split('.')[:2] if p.isdigit()]
    recent = tuple(parts) >= (23, 2)
    checks.append(_check(
        'version', recent,
        'ReplacingMergeTree drops deleted keys.' if recent else
        f'ClickHouse {version}: mirrors need 23.2 or later, whose ReplacingMergeTree takes an '
        'is_deleted column.',
    ))
    return dict(checks=checks, ready=all(c['ok'] for c in checks))
