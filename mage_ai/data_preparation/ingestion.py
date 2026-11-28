"""
Guided ingestion (ADR-0045): a PostgreSQL database's tables copied into another and kept in
sync, set up after checks and a review, as a streaming pipeline the project keeps.

1. check_source: can the source be read by change capture (logical replication, a role
   that may replicate, a free slot), and its tables with their keys and sizes.
2. check_destination: can the destination be written (the schema exists or can be made).
3. sample: a few rows of a table, to see what will be copied.
4. review: what the sync will do, in sentences, for the choices made.
5. create: the pipeline: a change capture source that copies the tables first
   (snapshot: initial), and a PostgreSQL sink that mirrors each table (upsert by key,
   deletes applied or marked). Passwords are kept as project secrets and read with
   mage_secret_var(); no file holds them.
"""
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

SAMPLE_ROWS = 20
NAME = re.compile(r'[^a-z0-9_]+')


class IngestionError(Exception):
    pass


def _connect(connection: Dict[str, Any]):
    import psycopg2

    for key in ('host', 'database', 'user'):
        if not connection.get(key):
            raise IngestionError(f'The connection needs {key}.')
    try:
        return psycopg2.connect(
            host=connection['host'],
            port=int(connection.get('port') or 5432),
            dbname=connection['database'],
            user=connection['user'],
            password=connection.get('password') or None,
            sslmode=connection.get('sslmode') or 'prefer',
            connect_timeout=10,
        )
    except Exception as error:
        # psycopg2 names the host and the reason; never the password.
        raise IngestionError(f'Could not connect: {str(error).strip()}') from None


def _check(name: str, ok: bool, detail: str) -> Dict[str, Any]:
    return dict(name=name, ok=bool(ok), detail=detail)

_IDENTITY = {'d': 'key', 'f': 'full', 'i': 'index', 'n': 'none'}


def _blocked(table: str, key: List[str], identity: str) -> Optional[str]:
    """Why a table cannot be mirrored, or None."""
    if not key:
        return 'no primary key: a mirror upserts and deletes by one'
    if identity == 'none':
        # Published, its UPDATE and DELETE would fail on the source.
        return (f'REPLICA IDENTITY NOTHING: once published, its updates and deletes would '
                f'fail on the source. ALTER TABLE {table} REPLICA IDENTITY DEFAULT')
    if identity == 'index':
        return (f'REPLICA IDENTITY USING INDEX: its deletes carry the index columns, not the '
                f'primary key. ALTER TABLE {table} REPLICA IDENTITY DEFAULT')
    return None


def _tables(cursor, names: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """The source's tables (or those named) with their primary keys, sizes and identity."""
    cursor.execute('''
        SELECT n.nspname, c.relname, greatest(c.reltuples, 0)::bigint, c.relreplident,
               pg_get_userbyid(c.relowner) = current_user OR
                   (SELECT rolsuper FROM pg_roles WHERE rolname = current_user),
               coalesce((
                   SELECT array_agg(a.attname ORDER BY array_position(i.indkey, a.attnum))
                   FROM pg_index i JOIN pg_attribute a
                       ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                   WHERE i.indrelid = c.oid AND i.indisprimary
               ), '{}')
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r', 'p')
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND n.nspname NOT LIKE 'pg_toast%%'
          AND (%s::text[] IS NULL OR n.nspname || '.' || c.relname = ANY(%s::text[]))
        ORDER BY n.nspname, c.relname
    ''', (names, names))
    found = []
    for schema, name, rows, identity, owned, key in cursor.fetchall():
        table, identity = f'{schema}.{name}', _IDENTITY.get(identity, identity)
        found.append(dict(
            table=table, rows=rows, key=list(key or []), replica_identity=identity,
            owned=bool(owned), blocked=_blocked(table, list(key or []), identity),
        ))
    return found


def check_source(connection: Dict[str, Any]) -> Dict[str, Any]:
    """The source's readiness for change capture, and its tables with keys and sizes."""
    db = _connect(connection)
    try:
        with db.cursor() as cursor:
            cursor.execute('SHOW wal_level')
            wal_level = cursor.fetchone()[0]
            cursor.execute(
                'SELECT rolreplication OR rolsuper FROM pg_roles WHERE rolname = current_user'
            )
            replicates = bool(cursor.fetchone()[0])
            cursor.execute(
                "SELECT current_setting('max_replication_slots')::int, "
                '(SELECT count(*) FROM pg_replication_slots)'
            )
            slots, used = cursor.fetchone()
            tables = _tables(cursor)
    finally:
        db.close()
    checks = [
        _check('connect', True, f"Connected to {connection['database']} as {connection['user']}."),
        _check(
            'logical replication', wal_level == 'logical',
            'wal_level is logical.' if wal_level == 'logical' else
            f'wal_level is {wal_level}; set it to logical and restart the server.',
        ),
        _check(
            'replication role', replicates,
            f"{connection['user']} may replicate." if replicates else
            f"{connection['user']} needs REPLICATION: ALTER ROLE {connection['user']} "
            'WITH REPLICATION.',
        ),
        _check(
            'free slot', used < slots,
            f'{slots - used} of {slots} replication slots are free.' if used < slots else
            f'All {slots} replication slots are taken; raise max_replication_slots or drop one.',
        ),
    ]
    return dict(checks=checks, ready=all(c['ok'] for c in checks), tables=tables)


def check_destination(connection: Dict[str, Any], schema: str) -> Dict[str, Any]:
    """Whether the destination's schema exists or can be made, and tables written in it."""
    db = _connect(connection)
    try:
        with db.cursor() as cursor:
            cursor.execute('SELECT 1 FROM pg_namespace WHERE nspname = %s', (schema,))
            exists = cursor.fetchone() is not None
            if exists:
                cursor.execute("SELECT has_schema_privilege(%s, 'CREATE')", (schema,))
            else:
                cursor.execute(
                    "SELECT has_database_privilege(current_database(), 'CREATE')",
                )
            can = bool(cursor.fetchone()[0])
    finally:
        db.close()
    detail = (
        (f'Tables can be made in {schema}.' if can else
         f"{connection['user']} cannot make tables in {schema}.")
        if exists else
        (f'{schema} does not exist; it will be made.' if can else
         f"{schema} does not exist, and {connection['user']} cannot make it.")
    )
    checks = [
        _check('connect', True, f"Connected to {connection['database']} as {connection['user']}."),
        _check('schema', can, detail),
    ]
    return dict(checks=checks, ready=can)


def sample(connection: Dict[str, Any], table: str, rows: int = SAMPLE_ROWS) -> Dict[str, Any]:
    """The first rows of a table, values as text."""
    from psycopg2 import sql

    schema, _, name = table.partition('.')
    if not name:
        raise IngestionError('Name the table with its schema, such as public.orders.')
    db = _connect(connection)
    try:
        with db.cursor() as cursor:
            cursor.execute(
                sql.SQL('SELECT * FROM {}.{} LIMIT %s').format(
                    sql.Identifier(schema), sql.Identifier(name),
                ),
                (min(max(int(rows), 1), 100),),
            )
            columns = [d.name for d in cursor.description]
            values = [[None if v is None else str(v) for v in row] for row in cursor.fetchall()]
    except IngestionError:
        raise
    except Exception as error:
        raise IngestionError(f'Could not read {table}: {str(error).strip()}') from None
    finally:
        db.close()
    return dict(table=table, columns=columns, rows=values)


def review(tables: List[Dict[str, Any]], deletes: str, schema: str, prefix: str) -> List[str]:
    """What the sync will do, in sentences."""
    names = ', '.join(t['table'] for t in tables)
    lines = [
        f'Copies every row of {names} first, then keeps each table in step with its changes '
        f'as they happen (logical replication).',
        f'Writes each table to {schema}.{prefix}<table>, upserting rows by '
        + '; '.join(f"{t['table']} by {', '.join(t['key'])}" for t in tables) + '.',
        'A row deleted in the source is deleted in the copy.' if deletes == 'delete' else
        'A row deleted in the source stays in the copy, with _mage_deleted_at set.',
        'A column added in the source is added to the copy with its next change. A column '
        'dropped or retyped in the source is not changed in the copy.',
        'After a stop, the sync continues from the last change the copy committed: nothing is '
        'missed, and a change it repeats writes the same row.',
        'TRUNCATE is not replicated.',
    ]
    return lines


def _validate(tables: List[Dict[str, Any]], deletes: str) -> None:
    if not tables:
        raise IngestionError('Choose at least one table.')
    for table in tables:
        if '.' not in str(table.get('table') or ''):
            raise IngestionError('Name each table with its schema, such as public.orders.')
        if not table.get('key'):
            raise IngestionError(
                f"{table['table']} has no key: a mirror upserts and deletes by one. Give the "
                'table a primary key.'
            )
    if deletes not in ('delete', 'soft'):
        raise IngestionError('deletes is delete or soft.')
    by_name: Dict[str, str] = {}
    for table in tables:
        name = table['table'].split('.', 1)[1]
        if name in by_name and by_name[name] != table['table']:
            raise IngestionError(
                f"{by_name[name]} and {table['table']} would both be written to the table "
                f'{name}: choose one of them per sync.'
            )
        by_name[name] = table['table']


def _from_source(source: Dict[str, Any], tables: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """The chosen tables as the source has them now; refused when one cannot be mirrored."""
    names = [t['table'] for t in tables]
    db = _connect(source)
    try:
        with db.cursor() as cursor:
            found = {t['table']: t for t in _tables(cursor, names)}
    finally:
        db.close()
    for name in names:
        if name not in found:
            raise IngestionError(f'{name} is not a table of the source.')
        if found[name]['blocked']:
            raise IngestionError(f"{name} cannot be copied: {found[name]['blocked']}.")
    return [found[name] for name in dict.fromkeys(names)]


def _identifier(text: str) -> str:
    return NAME.sub('_', text.lower()).strip('_')[:40] or 'ingestion'


def create(
    repo_path: str,
    name: str,
    source: Dict[str, Any],
    destination: Dict[str, Any],
    tables: List[Dict[str, Any]],
    deletes: str = 'delete',
    schema: str = 'public',
    prefix: str = '',
    user: Optional[str] = None,
) -> Dict[str, Any]:
    """The sync as a streaming pipeline; the passwords become project secrets."""
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.data_preparation.shared.secrets import create_secret, get_secret_value

    if not name or not str(name).strip():
        raise IngestionError('Name the pipeline.')
    _validate(tables, deletes)
    # Keys and replica identities from the source, not from the request.
    tables = _from_source(source, tables)
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=repo_path)
    ident = _identifier(pipeline.uuid)

    def password(role: str, connection: Dict[str, Any]) -> Optional[str]:
        if not connection.get('password'):
            return None
        secret = f'{ident}_{role}_password'
        if get_secret_value(secret, suppress_warning=True) is None:
            create_secret(secret, connection['password'])
        return "{{ mage_secret_var('" + secret + "') }}"

    def connection_yaml(connection: Dict[str, Any], role: str, user_key: str) -> Dict:
        settings = {
            'host': connection['host'],
            'port': int(connection.get('port') or 5432),
            'database': connection['database'],
            user_key: connection['user'],
        }
        secret = password(role, connection)
        if secret:
            settings['password'] = secret
        if connection.get('sslmode'):
            settings['sslmode'] = connection['sslmode']
        return settings

    def write_block(suffix: str, kind: str, settings: Dict, upstream=()) -> Any:
        block = Block.create(f'{pipeline.uuid}_{suffix}', kind, repo_path, language='yaml',
                             pipeline=pipeline)
        Path(block.file_path).write_text(''.join(
            f'{key}: {json.dumps(value)}\n' for key, value in settings.items()
        ))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    read = write_block('source', 'data_loader', dict(
        connector_type='postgres',
        **connection_yaml(source, 'source', 'user'),
        replication_slot=f'mageml_{ident}',
        publication_name=f'mageml_{ident}',
        tables=[t['table'] for t in tables],
        snapshot='initial',
        batch_size=5000,
        batch_timeout=1,
    ))
    write = write_block('copy', 'data_exporter', dict(
        connector_type='postgres',
        **connection_yaml(destination, 'destination', 'username'),
        schema=schema,
        table=f'{prefix}{{_mage_table}}',
        mirror=dict(
            key={t['table'].split('.', 1)[1]: list(t['key']) for t in tables},
            deletes=deletes,
        ),
    ), [read])
    return dict(
        pipeline_uuid=pipeline.uuid,
        # For the run panel, which names the sinks a start writes to.
        blocks=[dict(uuid=b.uuid, type=getattr(b.type, 'value', b.type))
                for b in (read, write)],
        review=review(tables, deletes, schema, prefix),
        created_by=user,
    )
