"""
Guided ingestion from MySQL (ADR-0048): the source's checks, tables and sample, for
ingestion.py. The stream engine reads the binary log; these only read the server's settings,
the user's grants and the tables (mysql-connector-python, the mysql extra).
"""
from typing import Any, Dict, List, Optional

from mage_ai.data_preparation.ingestion import IngestionError, _check

# What change capture needs of the server's binary log, and what each setting must be.
LOG_SETTINGS = (
    ('binlog_format', 'ROW', 'each change is written as rows'),
    ('binlog_row_image', 'FULL', 'an update carries the whole row'),
    ('binlog_row_metadata', 'FULL', 'each change names its columns'),
)


def _connect(connection: Dict[str, Any]):
    try:
        import mysql.connector
    except ImportError:
        raise IngestionError(
            'Checking a MySQL source needs mysql-connector-python: install mage-ml[mysql].'
        ) from None
    for key in ('host', 'database', 'user'):
        if not connection.get(key):
            raise IngestionError(f'The connection needs {key}.')
    options = dict(
        host=connection['host'],
        port=int(connection.get('port') or 3306),
        database=connection['database'],
        user=connection['user'],
        password=connection.get('password') or '',
        connection_timeout=10,
    )
    mode = connection.get('sslmode') or 'prefer'
    # The checks use TLS whenever the server has it, also with disable: without TLS this
    # connector cannot log in with caching_sha2_password, MySQL 8's default. The stream
    # engine follows sslmode as given.
    if mode in ('verify-ca', 'verify-full'):
        options['ssl_verify_cert'] = True
        options['ssl_verify_identity'] = mode == 'verify-full'
    try:
        return mysql.connector.connect(**options)
    except Exception as error:
        # The connector names the host and the reason; never the password.
        raise IngestionError(f'Could not connect: {str(error).strip()}') from None


def tables(cursor, database: str, names: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """The database's tables (or those named, as database.table) with their primary keys and
    row estimates; a table without a key cannot be mirrored."""
    cursor.execute(
        """
        SELECT t.table_schema, t.table_name, t.table_rows,
               GROUP_CONCAT(k.column_name ORDER BY k.ordinal_position SEPARATOR '\\n')
        FROM information_schema.tables t
        LEFT JOIN information_schema.key_column_usage k
            ON k.table_schema = t.table_schema AND k.table_name = t.table_name
           AND k.constraint_name = 'PRIMARY'
        WHERE t.table_schema = %s AND t.table_type = 'BASE TABLE'
        GROUP BY t.table_schema, t.table_name, t.table_rows
        ORDER BY t.table_name
        """,
        (database,),
    )
    found = []
    for schema, name, rows, key in cursor.fetchall():
        table = f'{schema}.{name}'
        if names is not None and table not in names:
            continue
        key = [k for k in (key or '').split('\n') if k]
        found.append(dict(
            table=table, rows=int(rows or 0), key=key, replica_identity='full', owned=True,
            blocked=None if key else 'no primary key: a mirror upserts and deletes by one',
        ))
    return found


def check_source(connection: Dict[str, Any]) -> Dict[str, Any]:
    """The source's readiness for change capture, and its tables with keys and sizes."""
    db = _connect(connection)
    try:
        cursor = db.cursor()
        cursor.execute(
            'SELECT @@log_bin, @@binlog_format, @@binlog_row_image, @@binlog_row_metadata, '
            '@@binlog_transaction_compression, @@binlog_row_value_options'
        )
        log_bin, *values, compression, value_options = cursor.fetchone()
        cursor.execute('SHOW GRANTS FOR CURRENT_USER()')
        grants = ' '.join(str(row[0]) for row in cursor.fetchall()).upper()
        found = tables(cursor, connection['database'])
        cursor.close()
    finally:
        db.close()
    checks = [
        _check('connect', True, f"Connected to {connection['database']} as {connection['user']}."),
        _check('binary log', bool(int(log_bin or 0)),
               'The binary log is on.' if int(log_bin or 0) else
               'The binary log is off: start the server with log_bin.'),
    ]
    for (name, wanted, why), value in zip(LOG_SETTINGS, values):
        ok = str(value).upper() == wanted
        checks.append(_check(name, ok, f'{name} is {wanted}: {why}.' if ok else
                             f'{name} is {value}; set it to {wanted} ({why}).'))
    checks.append(_check(
        'transaction compression', not int(compression or 0),
        'Transactions are not compressed.' if not int(compression or 0) else
        'binlog_transaction_compression is ON; set it to OFF.',
    ))
    checks.append(_check(
        'row value options', not value_options,
        'Changes carry whole JSON values.' if not value_options else
        f"binlog_row_value_options is {value_options}; set it to ''.",
    ))
    replicates = any(g in grants for g in ('REPLICATION SLAVE', 'ALL PRIVILEGES'))
    reads_log = any(g in grants for g in ('REPLICATION CLIENT', 'ALL PRIVILEGES'))
    checks.append(_check(
        'replication grants', replicates and reads_log,
        f"{connection['user']} may read the binary log." if replicates and reads_log else
        f"{connection['user']} needs REPLICATION SLAVE and REPLICATION CLIENT: GRANT "
        f"REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{connection['user']}'.",
    ))
    return dict(checks=checks, ready=all(c['ok'] for c in checks), tables=found)


def sample(connection: Dict[str, Any], table: str, rows: int) -> Dict[str, Any]:
    """The first rows of a table, values as text."""
    database, _, name = table.partition('.')
    if not name:
        raise IngestionError('Name the table with its database, such as shop.orders.')
    db = _connect(connection)
    try:
        cursor = db.cursor()
        quoted = '.'.join('`' + part.replace('`', '``') + '`' for part in (database, name))
        cursor.execute(f'SELECT * FROM {quoted} LIMIT %s', (min(max(int(rows), 1), 100),))
        columns = [d[0] for d in cursor.description]
        values = [[None if v is None else str(v) for v in row] for row in cursor.fetchall()]
        cursor.close()
    except IngestionError:
        raise
    except Exception as error:
        raise IngestionError(f'Could not read {table}: {str(error).strip()}') from None
    finally:
        db.close()
    return dict(table=table, columns=columns, rows=values)


def chosen(connection: Dict[str, Any], names: List[str]) -> Dict[str, Dict[str, Any]]:
    """The named tables as the source has them now."""
    db = _connect(connection)
    try:
        cursor = db.cursor()
        found = {t['table']: t for t in tables(cursor, connection['database'], names)}
        cursor.close()
    finally:
        db.close()
    return found
