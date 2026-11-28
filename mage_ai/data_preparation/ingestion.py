"""
Guided ingestion (ADR-0045): a PostgreSQL or MySQL database's tables copied into PostgreSQL
or ClickHouse and kept in sync, set up after checks and a review, as a streaming pipeline the
project keeps. A source's `kind` is postgres (the default) or mysql (ADR-0048,
ingestion_mysql.py); a destination's is postgres or clickhouse (ADR-0049,
ingestion_clickhouse.py).

1. check_source: can the source be read by change capture (logical replication, a role
   that may replicate, a free slot), and its tables with their keys and sizes.
2. check_destination: can the destination be written (the schema exists or can be made).
3. sample: a few rows of a table, to see what will be copied.
4. review: what the sync will do, in sentences, for the choices made.
5. create: the pipeline: a change capture source that copies the tables first
   (snapshot: initial), and a PostgreSQL sink that mirrors each table (upsert by key,
   deletes applied or marked). Passwords are kept as project secrets and read with
   mage_secret_var(); no file holds them.
6. syncs, sync_tables, add_tables: the syncs made, a sync's source tables, and tables added
   to a sync, which its next start copies, then follows.
"""
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

SAMPLE_ROWS = 20
NAME = re.compile(r'[^a-z0-9_]+')


class IngestionError(Exception):
    pass


SOURCE_KINDS = ('postgres', 'mysql')
DESTINATION_KINDS = ('postgres', 'clickhouse')


def _kind(connection: Dict[str, Any], kinds=SOURCE_KINDS, role: str = 'source') -> str:
    kind = (connection or {}).get('kind') or 'postgres'
    if kind not in kinds:
        raise IngestionError(f'A {role} is one of {", ".join(kinds)}, not {kind}.')
    return kind


def _destination_kind(connection: Dict[str, Any]) -> str:
    return _kind(connection, DESTINATION_KINDS, 'destination')


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
    if _kind(connection) == 'mysql':
        from mage_ai.data_preparation import ingestion_mysql

        return ingestion_mysql.check_source(connection)
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
    if _destination_kind(connection) == 'clickhouse':
        from mage_ai.data_preparation import ingestion_clickhouse

        return ingestion_clickhouse.check_destination(connection)
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
    if _kind(connection) == 'mysql':
        from mage_ai.data_preparation import ingestion_mysql

        return ingestion_mysql.sample(connection, table, rows)
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


def review(tables: List[Dict[str, Any]], deletes: str, schema: str, prefix: str,
           kind: str = 'postgres', destination: str = 'postgres') -> List[str]:
    """What the sync will do, in sentences."""
    names = ', '.join(t['table'] for t in tables)
    how = 'the binary log' if kind == 'mysql' else 'logical replication'
    keys = '; '.join(f"{t['table']} by {', '.join(t['key'])}" for t in tables)
    if destination == 'clickhouse':
        writes = [
            f'Writes each table to the ClickHouse table {schema}.{prefix}<table>, a '
            f'ReplacingMergeTree by its key ({keys}): each key keeps its latest change.',
            'Read it with FINAL (SELECT ... FROM table FINAL) for one row per key before '
            'ClickHouse merges.',
        ]
    else:
        writes = [f'Writes each table to {schema}.{prefix}<table>, upserting rows by {keys}.']
    lines = [
        f'Copies every row of {names} first, then keeps each table in step with its changes '
        f'as they happen ({how}).',
        *writes,
        'A row deleted in the source is deleted in the copy.' if deletes == 'delete' else
        'A row deleted in the source stays in the copy, with _mage_deleted_at set.',
        'A column added in the source is added to the copy with its next change. A column '
        'dropped or retyped in the source is not changed in the copy.',
        'After a stop, the sync continues from the last change the copy committed: nothing is '
        'missed, and a change it repeats writes the same row.',
        'TRUNCATE is not replicated.',
    ]
    return lines


def _validate(tables: List[Dict[str, Any]], deletes: str, destination: str = 'postgres') -> None:
    if destination == 'clickhouse' and deletes != 'delete':
        raise IngestionError(
            'A ClickHouse copy deletes the rows deleted in the source: ClickHouse replaces '
            'whole rows, and a kept row would lose the columns a delete does not carry.'
        )
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
    if _kind(source) == 'mysql':
        from mage_ai.data_preparation import ingestion_mysql

        found = ingestion_mysql.chosen(source, names)
    else:
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
    target = _destination_kind(destination)
    _validate(tables, deletes, target)
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

    kind = _kind(source)

    def connection_yaml(connection: Dict[str, Any], role: str, user_key: str,
                        port: int = 5432) -> Dict:
        settings = {
            'host': connection['host'],
            'port': int(connection.get('port') or port),
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

    if kind == 'mysql':
        # The stream keeps its binary log position in its state (ADR-0048).
        read = write_block('source', 'data_loader', dict(
            connector_type='mysql',
            **connection_yaml(source, 'source', 'user', port=3306),
            tables=[t['table'] for t in tables],
            snapshot='initial',
            batch_size=5000,
            batch_timeout=1,
        ))
    else:
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
    mirror = dict(
        key={t['table'].split('.', 1)[1]: list(t['key']) for t in tables},
        deletes=deletes,
    )
    if target == 'clickhouse':
        from mage_ai.data_preparation import ingestion_clickhouse

        settings = dict(
            connector_type='clickhouse',
            host=destination['host'],
            port=ingestion_clickhouse.port(destination),
            secure=ingestion_clickhouse.secure(destination),
            username=destination.get('user') or 'default',
            database=destination['database'],
        )
        secret = password('destination', destination)
        if secret:
            settings['password'] = secret
        write = write_block('copy', 'data_exporter', dict(
            settings, config=dict(table_name=f'{prefix}{{_mage_table}}'), mirror=mirror,
        ), [read])
    else:
        write = write_block('copy', 'data_exporter', dict(
            connector_type='postgres',
            **connection_yaml(destination, 'destination', 'username'),
            schema=schema,
            table=f'{prefix}{{_mage_table}}',
            mirror=mirror,
        ), [read])
    return dict(
        pipeline_uuid=pipeline.uuid,
        # For the run panel, which names the sinks a start writes to.
        blocks=[dict(uuid=b.uuid, type=getattr(b.type, 'value', b.type))
                for b in (read, write)],
        review=review(tables, deletes,
                      destination['database'] if target == 'clickhouse' else schema, prefix,
                      kind, target),
        created_by=user,
    )


# Adding tables to a sync made here: the source's tables and the sink's keys are written in
# its blocks, and the next start copies the tables named since, then follows them with the
# others (ADR-0045).


def _yaml(block) -> Dict[str, Any]:
    import yaml

    try:
        found = yaml.safe_load(block.content or '')
    except yaml.YAMLError:
        return {}
    return found if isinstance(found, dict) else {}


def _is(block, kind: str) -> bool:
    return str(getattr(block.type, 'value', block.type)) == kind and \
        str(getattr(block.language, 'value', block.language)) == 'yaml'


def _sync_blocks(pipeline) -> Optional[tuple]:
    """A sync's source and sink blocks: a change capture source (snapshot: initial) and a
    mirror sink after it; None for any other pipeline."""
    for block in pipeline.blocks_by_uuid.values():
        if not _is(block, 'data_loader'):
            continue
        source = _yaml(block)
        if source.get('connector_type') not in SOURCE_KINDS or \
                not isinstance(source.get('tables'), list):
            continue
        for after in block.downstream_blocks:
            if _is(after, 'data_exporter') and isinstance(_yaml(after).get('mirror'), dict):
                return block, after
    return None


def _sync(repo_path: str, pipeline_uuid: str) -> tuple:
    from mage_ai.data_preparation.models.pipeline import Pipeline

    try:
        pipeline = Pipeline.get(pipeline_uuid, repo_path=repo_path)
    except Exception:
        raise IngestionError(f'There is no pipeline {pipeline_uuid}.') from None
    found = _sync_blocks(pipeline)
    if not found:
        raise IngestionError(
            f'{pipeline_uuid} is not a sync: it has no change capture source with a mirror '
            'sink after it.'
        )
    return (pipeline, *found)


def _source_connection(pipeline, block) -> Dict[str, Any]:
    """The source's connection, with its password rendered from the block."""
    from mage_ai.data_preparation.models.block.streaming import streaming_config

    config = streaming_config(block.content or '', pipeline.variables or {})
    return dict(
        kind=config['connector_type'],
        host=config.get('host'),
        port=config.get('port'),
        database=config.get('database'),
        user=config.get('user') or config.get('username'),
        password=config.get('password'),
        sslmode=config.get('sslmode'),
    )


def syncs(repo_path: str) -> List[Dict[str, Any]]:
    """The project's syncs: each streaming pipeline with a change capture source and a mirror
    sink, with its source and tables."""
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    found = []
    for uuid in sorted(Pipeline.get_all_pipelines(repo_path)):
        try:
            pipeline = Pipeline.get(uuid, repo_path=repo_path)
        except Exception:
            continue
        if pipeline.type != PipelineType.STREAMING:
            continue
        blocks = _sync_blocks(pipeline)
        if not blocks:
            continue
        source, sink = (_yaml(b) for b in blocks)
        found.append(dict(
            pipeline_uuid=uuid,
            kind=source['connector_type'],
            source=f"{source.get('host')}:{source.get('port')}/{source.get('database')}",
            destination=sink.get('connector_type'),
            tables=source['tables'],
        ))
    return found


def sync_tables(repo_path: str, pipeline_uuid: str) -> Dict[str, Any]:
    """The tables of a sync's source, each marked synced or not."""
    pipeline, source, _ = _sync(repo_path, pipeline_uuid)
    synced = set(_yaml(source)['tables'])
    found = check_source(_source_connection(pipeline, source))
    for table in found['tables']:
        table['synced'] = table['table'] in synced
    return dict(found, pipeline_uuid=pipeline_uuid)


def _set(content: str, key: str, value: Any) -> str:
    """`content` with the top-level `key` set to `value` (one line of JSON, as create writes);
    the lines of its old value go."""
    line = f'{key}: {json.dumps(value)}'
    pattern = re.compile(rf'^{re.escape(key)}:[^\n]*(?:\n(?:[ \t]+|-)[^\n]*)*', re.M)
    if pattern.search(content):
        return pattern.sub(lambda _: line, content, count=1)
    return content.rstrip('\n') + f'\n{line}\n'


def add_tables(repo_path: str, pipeline_uuid: str, tables: List[Any]) -> Dict[str, Any]:
    """Adds tables to a sync: the source reads them and the sink mirrors them by their keys.
    The next start copies them, then follows them with the others."""
    pipeline, source_block, sink_block = _sync(repo_path, pipeline_uuid)
    source, sink = _yaml(source_block), _yaml(sink_block)
    kind = source['connector_type']
    target = sink.get('connector_type') or 'postgres'
    synced = list(source['tables'])
    names = [t['table'] if isinstance(t, dict) else str(t) for t in tables or []]
    names = [n for n in dict.fromkeys(names) if n not in synced]
    if not names:
        raise IngestionError('Choose a table the sync does not have yet.')
    connection = _source_connection(pipeline, source_block)
    added = _from_source(connection, [dict(table=n) for n in names])
    mirror = dict(sink['mirror'])
    deletes = mirror.get('deletes') or 'delete'
    _validate(added, deletes, target)
    by_name: Dict[str, str] = {}
    for name in synced + names:
        short = name.split('.', 1)[-1]
        if short in by_name:
            raise IngestionError(
                f'{by_name[short]} and {name} would both be written to the table {short}: '
                'a sync has one of them.'
            )
        by_name[short] = name
    key = mirror.get('key')
    if isinstance(key, dict):
        key = dict(key, **{t['table'].split('.', 1)[1]: list(t['key']) for t in added})
    else:
        same = [key] if isinstance(key, str) else list(key or [])
        if any(list(t['key']) != same for t in added):
            # One key for every table no longer fits: each table by its own.
            every = _from_source(connection, [dict(table=n) for n in synced]) + added
            key = {t['table'].split('.', 1)[1]: list(t['key']) for t in every}
    mirror['key'] = key
    Path(source_block.file_path).write_text(
        _set(source_block.content or '', 'tables', synced + names),
    )
    Path(sink_block.file_path).write_text(_set(sink_block.content or '', 'mirror', mirror))
    template = sink.get('table') or (sink.get('config') or {}).get('table_name') or ''
    prefix = template.split('{_mage_table}')[0]
    schema = sink.get('database') if target == 'clickhouse' else sink.get('schema') or 'public'
    lines = review(added, deletes, schema, prefix, kind, target)
    lines[0] = (
        f"Adds {', '.join(names)}: the next start copies every row of "
        f"{'them' if len(names) > 1 else 'it'}, then keeps "
        f"{'them' if len(names) > 1 else 'it'} in step with the other tables. The tables "
        'synced already are not copied again.'
    )
    if kind == 'postgres':
        lines.insert(1, "The next start adds the tables to the sync's publication.")
    return dict(
        pipeline_uuid=pipeline_uuid,
        added=names,
        blocks=[dict(uuid=b.uuid, type=getattr(b.type, 'value', b.type))
                for b in (source_block, sink_block)],
        review=lines,
    )
