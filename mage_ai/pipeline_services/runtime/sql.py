"""
SQL blocks in pipeline services, run as Mage runs them in a pipeline run
(mage_ai/data_preparation/models/block/sql/__init__.py, the PostgreSQL, MySQL, DuckDB,
ClickHouse, Trino, BigQuery and Snowflake branches):

1. Upstream outputs that the query names as {{ df_N }} are written to tables, unless the
   upstream is a SQL block on the same database, whose table is used as it is.
2. {{ df_N }} becomes that table's name: "database".schema.table on PostgreSQL,
   catalog.schema.table on Trino, project.dataset.table on BigQuery, "DATABASE"."SCHEMA".
   "TABLE" in capitals on Snowflake, the table alone on MySQL, DuckDB and ClickHouse.
3. The query is rendered with Jinja twice, with the run's variables, variables(),
   env_var(), mage_secret_var() and json_value().
4. Without raw SQL, CREATE TABLE schema.table AS <query> (or INSERT INTO, or DROP first, by
   the export write policy); the block's output is the table, up to 10 million rows. With
   raw SQL, the statements run in order and the output is the last SELECT.

Mage's functions for this take Mage's Block, which a service does not have; this module
follows them step by step and uses Mage's io classes for the database work.
"""
import datetime
import importlib.util
import json
import math
import os
import re
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

QUERY_ROW_LIMIT = 10_000_000
CONFIG_KEY_UPSTREAM = 'upstream_block_configuration'
# Per database, what Mage's branch for it does differently:
# - profile_keys: the io_config.yaml settings that make two SQL blocks use the same database;
# - no_schema: upstream tables are named without a schema;
# - full_names: {{ df_N }} becomes "database".schema.table instead of the table alone.
PROVIDERS = {
    'postgres': dict(
        profile_keys=('POSTGRES_DBNAME', 'POSTGRES_HOST', 'POSTGRES_PORT'),
        no_schema=False,
        full_names=True,
    ),
    'mysql': dict(
        profile_keys=('MYSQL_DATABASE', 'MYSQL_HOST', 'MYSQL_PORT'),
        no_schema=True,
        full_names=False,
    ),
    'duckdb': dict(
        profile_keys=('DUCKDB_DATABASE', 'DUCKDB_SCHEMA'),
        no_schema=True,
        full_names=False,
    ),
    'clickhouse': dict(
        profile_keys=('CLICKHOUSE_DATABASE', 'CLICKHOUSE_HOST', 'CLICKHOUSE_PORT'),
        no_schema=True,
        full_names=False,
    ),
    'trino': dict(
        profile_keys=('TRINO_CATALOG', 'TRINO_HOST', 'TRINO_PORT', 'TRINO_SCHEMA'),
        no_schema=False,
        full_names=True,
    ),
    # Two profiles reach the same BigQuery tables when they are the same profile or name
    # the same project and location (_bigquery_project), not by comparing settings.
    'bigquery': dict(
        profile_keys=(),
        no_schema=False,
        full_names=True,
    ),
    'snowflake': dict(
        profile_keys=('SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_DEFAULT_WH', 'SNOWFLAKE_DEFAULT_DB'),
        no_schema=False,
        full_names=True,
    ),
}
# Tries of the read after BigQuery's CREATE TABLE AS: the new table can answer 404 for a
# moment, as Mage's branch waits for.
BIGQUERY_READ_TRIES = 5
SUPPORTED_PROVIDERS = tuple(PROVIDERS)

_statements = None


class SqlBlockError(Exception):
    pass


def statements():
    """Mage's SQL statement helpers, loaded without importing Mage's Block."""
    global _statements
    if _statements is None:
        spec = importlib.util.find_spec('mage_ai')
        path = os.path.join(
            os.path.dirname(spec.origin),
            'data_preparation', 'models', 'block', 'sql', 'utils', 'statements.py',
        )
        module_spec = importlib.util.spec_from_file_location('mage_service_sql_statements', path)
        module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(module)
        _statements = module
    return _statements


def variable_pattern(index: int) -> str:
    return r'{}[ ]*df_{}[ ]*{}'.format(r'\{\{', index, r'\}\}')


def _template_vars(include_python_libraries: bool) -> Dict:
    from mage_ai.data_preparation.shared.utils import get_template_vars

    if include_python_libraries:
        import inflection

        return get_template_vars(include_python_libraries=dict(
            datetime=datetime, inflection=inflection, json=json, math=math,
        ))
    return get_template_vars()


def render(text: str, variables: Dict) -> str:
    """Mage's two renders: template_render, then interpolate_vars with StrictUndefined."""
    from jinja2 import StrictUndefined, Template

    from mage_ai.data_preparation.templates.utils import get_variable_for_template

    def lookup(name, parse=None, values=variables):
        return get_variable_for_template(name, parse=parse, variables=values)

    first = Template(text).render(
        variables=lookup, **{**variables, **_template_vars(True)},
    )
    return Template(first, undefined=StrictUndefined).render(
        variables=lookup, **{**variables, **_template_vars(False)},
    )


def render_strict(text: Optional[str], variables: Dict) -> Optional[str]:
    if not text:
        return text
    from jinja2 import StrictUndefined, Template

    from mage_ai.data_preparation.templates.utils import get_variable_for_template

    return Template(text, undefined=StrictUndefined).render(
        variables=lambda name, parse=None: get_variable_for_template(
            name, parse=parse, variables=variables,
        ),
        **{**variables, **_template_vars(False)},
    )


def table_name_parts(
    configuration: Dict, upstream: Dict, no_schema: bool = False,
) -> Tuple[Any, Any, Any]:
    """(database, schema, table) of an upstream block's table, as Mage's table_name_parts."""
    database = schema = table = None
    full = ((configuration.get(CONFIG_KEY_UPSTREAM) or {}).get(upstream['uuid']) or {}).get(
        'table_name',
    )
    parts = (full or upstream['table_name']).split('.')
    if len(parts) == 3:
        database, schema, table = parts
    elif len(parts) == 2:
        if no_schema:
            database, table = parts
        else:
            schema, table = parts
    elif len(parts) == 1:
        table = parts[0]
    if not schema and not no_schema:
        upstream_configuration = upstream.get('configuration') or {}
        if (
            upstream_configuration
            and configuration.get('data_provider') == upstream_configuration.get('data_provider')
            and configuration.get('data_provider_profile')
            == upstream_configuration.get('data_provider_profile')
            and upstream_configuration.get('data_provider_schema')
        ):
            schema = upstream_configuration.get('data_provider_schema')
        else:
            schema = configuration.get('data_provider_schema')
    return database, schema, table


def should_upload(
    configuration: Dict, upstream: Dict, io_config: str, profile_keys: Tuple[str, ...],
) -> bool:
    """Whether an upstream output has to be written to a table first."""
    from mage_ai.io.config import ConfigFileLoader

    if upstream.get('type') == 'sensor':
        return False
    if upstream.get('language') != 'sql':
        return True
    upstream_configuration = upstream.get('configuration') or {}
    block_loader = ConfigFileLoader(io_config, configuration.get('data_provider_profile'))
    upstream_loader = ConfigFileLoader(
        io_config, upstream_configuration.get('data_provider_profile'),
    )
    same_provider = all(
        configuration.get(k) and upstream_configuration.get(k)
        and configuration.get(k) == upstream_configuration.get(k)
        for k in ('data_provider',)
    )
    if configuration.get('data_provider') == 'bigquery':
        return not same_provider or not _bigquery_same_tables(
            io_config, configuration.get('data_provider_profile'),
            upstream_configuration.get('data_provider_profile'),
        )
    # As Mage's same_profile_settings: one profile is one database, and a key unset in both
    # profiles is the same (I-267).
    same_database = (
        configuration.get('data_provider_profile')
        == upstream_configuration.get('data_provider_profile')
        or not profile_keys
        or (
            all(block_loader.config.get(k) == upstream_loader.config.get(k) for k in profile_keys)
            and any(block_loader.config.get(k) for k in profile_keys)
        )
    )
    return not same_provider or not same_database


def _bigquery_project(io_config: str, profile: Optional[str]) -> Tuple[Optional[str], Any]:
    """
    As bigquery_project in Mage's BigQuery branch: the project_id of the profile's service
    account key (the key, or the file it names) and GOOGLE_LOCATION.
    """
    from mage_ai.io.config import ConfigFileLoader, ConfigKey

    config = ConfigFileLoader(io_config, profile)
    key = config[ConfigKey.GOOGLE_SERVICE_ACC_KEY]
    if not key and config[ConfigKey.GOOGLE_SERVICE_ACC_KEY_FILEPATH]:
        try:
            with open(config[ConfigKey.GOOGLE_SERVICE_ACC_KEY_FILEPATH]) as file:
                key = json.load(file)
        except (OSError, ValueError):
            key = None
    if isinstance(key, str):
        try:
            key = json.loads(key)
        except ValueError:
            key = None
    project = key.get('project_id') if isinstance(key, dict) else None
    return project, config[ConfigKey.GOOGLE_LOCATION]


def _bigquery_same_tables(io_config: str, profile: Optional[str],
                          upstream_profile: Optional[str]) -> bool:
    """As reads_upstream_table in Mage's BigQuery branch."""
    if profile == upstream_profile:
        return True
    project, location = _bigquery_project(io_config, profile)
    return project is not None and (
        (project, location) == _bigquery_project(io_config, upstream_profile)
    )


def _no_data(value: Any) -> bool:
    import pandas as pd

    # An empty frame is uploaded as a table with its columns and no rows, as Mage does.
    if isinstance(value, pd.DataFrame) or type(value).__name__ == 'DataFrame':
        return False
    if hasattr(value, '__len__'):
        return len(value) == 0
    return not value


def upload_upstream_tables(
    loader, configuration: Dict, upstreams: List[Dict], inputs: List, query: str, io_config: str,
    provider: Dict, database: Optional[str] = None, table_suffix: Optional[str] = None,
) -> Dict[str, Tuple[Optional[str], str]]:
    """
    Writes the upstream outputs the query reads to tables. Returns the (schema, table) of
    each table written, by upstream block UUID.
    """
    import inspect

    written = {}
    default_schema = None if provider['no_schema'] else loader.default_schema()
    for index, upstream in enumerate(upstreams):
        if query and not re.findall(variable_pattern(index + 1), query):
            continue
        if not should_upload(configuration, upstream, io_config, provider['profile_keys']):
            continue
        value = inputs[index] if index < len(inputs) else None
        if _no_data(value):
            print(f"\n\nNo data in upstream block {upstream['uuid']}.")
            continue
        _, schema, table = table_name_parts(
            configuration, upstream, no_schema=provider['no_schema'],
        )
        if not schema and not provider['no_schema']:
            schema = default_schema
        if table_suffix:
            table = f'{table}_{table_suffix}'
        full_name = '.'.join(p for p in (schema, table) if p)
        print(f"\n\nExporting data from upstream block {upstream['uuid']} to {full_name}.")
        options = dict(
            table_name=table,
            schema_name=schema,
            cascade_on_drop=False,
            drop_table_on_replace=True,
            if_exists='replace',
            index=False,
            verbose=False,
        )
        if database:
            options['database'] = database
        parameters = inspect.signature(loader.export).parameters
        if 'allow_reserved_words' in parameters:
            options['allow_reserved_words'] = True
        # The frame's column names as they are, as Mage uploads them.
        if 'auto_clean_name' in parameters:
            options['auto_clean_name'] = False
        loader.export(value, **options)
        written[upstream['uuid']] = (schema, table)
    return written


def interpolate_input(
    loader, configuration: Dict, upstreams: List[Dict], query: str, full_names: bool = True,
    quote_database: bool = True, table_suffix: Optional[str] = None, written: Dict = None,
    default_schema: bool = True, name=None,
) -> str:
    """
    {{ df_N }} becomes the upstream table's name, as Mage's interpolate_input. With
    table_suffix, the tables in written are named with it. Without default_schema, a name
    with no schema configured has none (Mage's BigQuery branch). name(database, schema,
    table) writes the name, as Mage's replace_func.
    """
    helpers = statements()
    for index, upstream in enumerate(upstreams):
        matcher = '{} df_{} {}'.format('{{', index + 1, '}}')
        pattern = variable_pattern(index + 1)
        if re.search(pattern, query) is None:
            continue
        upstream_configuration = upstream.get('configuration') or {}
        is_sql = upstream.get('language') == 'sql'
        config = upstream_configuration if is_sql else configuration
        provider = configuration.get('data_provider')
        upstream_provider = upstream_configuration.get('data_provider')
        same_providers = provider == upstream_provider
        if configuration.get('use_raw_sql') and is_sql and provider != upstream_provider:
            raise SqlBlockError(
                f'Variable interpolation when using raw SQL for {matcher} is not supported '
                f"because upstream block {upstream['uuid']} is using a different data "
                f'provider ({upstream_provider}) than this block\'s data provider '
                f'({provider}). Please disable using raw SQL and try again.'
            )
        database, schema, table = table_name_parts(configuration, upstream)
        table = table or upstream['table_name']
        if table_suffix and upstream['uuid'] in (written or {}):
            table = f'{table}_{table_suffix}'
        if full_names:
            config_to_use = config if same_providers else configuration
            database = database or config_to_use.get('data_provider_database')
            schema = schema or config_to_use.get('data_provider_schema')
            if not database:
                database = loader.default_database()
                if quote_database and name is None:
                    database = f'"{database}"'
            if not schema and default_schema:
                schema = loader.default_schema()
            if name is not None:
                replace_with = name(database, schema, table)
            else:
                replace_with = '.'.join(p for p in (database, schema, table) if p)
        else:
            replace_with = table

        content = upstream.get('content') or ''
        if (
            is_sql and config.get('use_raw_sql') and same_providers
            and not helpers.has_create_or_insert_statement(content)
        ):
            # A raw SELECT upstream on the same database is inlined as a subquery.
            if re.search(r'\{\{[ ]*df_\d+[ ]*\}\}', content):
                raise SqlBlockError(
                    f"Upstream block {upstream['uuid']} is a raw SQL query that reads its own "
                    'upstream tables; pipeline services do not inline such queries yet.'
                )
            upstream_query = content
            match = 1
            while match is not None:
                match = None
                for inline_pattern in (
                    r'{}[\n\r\s]+as[\n\r\s]+'.format(pattern),
                    r'{}[\n\r\s]+["|\'].+["|\']'.format(pattern),
                    r'{}[\n\r\s]+\S+[\n\r\s]+ON'.format(pattern),
                ):
                    if match:
                        continue
                    match = re.search(inline_pattern, query, re.IGNORECASE)
                if not match:
                    continue
                start, end = match.span()
                query = ''.join([
                    query[:start],
                    re.sub(pattern, f'({upstream_query})', query[start:end]),
                    query[end:],
                ])
            replace_with = f'(\n{upstream_query.strip()}\n) AS {table}'

        query = re.sub(pattern, replace_with, query)
        query = query.replace(matcher, replace_with)
    return query


def execute_raw_sql(loader, query: str, configuration: Dict, limit: int = 1000) -> List:
    helpers = statements()
    if configuration.get('disable_query_preprocessing'):
        result = loader.execute_query_raw(query, configuration=configuration)
        return [] if result is None else [result]
    has_create_or_insert = helpers.has_create_or_insert_statement(query)
    has_drop = helpers.has_drop_statement(query)
    has_update = helpers.has_update_statement(query)
    queries, fetch = [], []
    for statement in helpers.split_query_string(query):
        queries.append(statement)
        fetch.append(not (has_create_or_insert or has_drop or has_update))
    if has_create_or_insert or has_update:
        full_name = helpers.extract_full_table_name(query)
        if full_name:
            queries.append(f'SELECT * FROM {full_name} LIMIT {int(limit)}')
            fetch.append(full_name)
    results = loader.execute_queries(queries, commit=True, fetch_query_at_indexes=fetch)
    last = results[-1] if results else None
    return [] if last is None else [last]


def _loader(provider: str, config_loader):
    if provider == 'postgres':
        from mage_ai.io.postgres import Postgres

        return Postgres.with_config(config_loader)
    if provider == 'mysql':
        from mage_ai.io.mysql import MySQL

        return MySQL.with_config(config_loader)
    if provider == 'clickhouse':
        from mage_ai.io.clickhouse import ClickHouse

        return ClickHouse.with_config(config_loader)
    if provider == 'trino':
        from mage_ai.io.trino import Trino

        return Trino.with_config(config_loader)
    if provider == 'bigquery':
        from mage_ai.io.bigquery import BigQuery

        return BigQuery.with_config(config_loader)
    from mage_ai.io.duckdb import DuckDB

    return DuckDB.with_config(config_loader)


def run(block: Dict, inputs: List, variables: Dict, repo_path: str) -> List:
    """The block's outputs, as a SQL block in a Mage pipeline run returns them."""
    from mage_ai.io.config import ConfigFileLoader

    configuration = block.get('configuration') or {}
    details = block.get('sql') or {}
    name = configuration.get('data_provider')
    if name not in PROVIDERS:
        raise SqlBlockError(
            f'SQL blocks on {name} are not supported by pipeline services yet; '
            f'{", ".join(SUPPORTED_PROVIDERS)} are.'
        )
    provider = PROVIDERS[name]
    with open(block['file'], encoding='utf-8') as file:
        query = file.read()

    variables = dict(variables)
    if isinstance(variables.get('execution_date'), datetime.datetime):
        variables['ds'] = variables['execution_date'].strftime('%Y-%m-%d')
    io_config = os.path.join(repo_path, 'io_config.yaml')
    profile = render_strict(configuration.get('data_provider_profile'), variables)
    config_loader = ConfigFileLoader(io_config, profile)
    upstreams = details.get('upstream') or []
    schema = configuration.get('data_provider_schema')
    table = details['table_name']
    limit = min(
        int(configuration.get('limit_in_pipeline_run') or QUERY_ROW_LIMIT), QUERY_ROW_LIMIT,
    )
    options = dict(
        drop_table_on_replace=True,
        if_exists=configuration.get('export_write_policy', 'append'),
        index=False,
        verbose=block.get('type') == 'data_exporter',
    )

    if name == 'snowflake':
        return _run_on_snowflake(config_loader, provider, configuration, upstreams, inputs,
                                 query, io_config, variables, schema, table, limit, options)
    loader = _loader(name, config_loader)
    if name == 'clickhouse':
        # Mage's ClickHouse branch: no with block, upstream tables in the block's database.
        database = configuration.get('data_provider_database') or loader.default_database()
        upload_upstream_tables(
            loader, configuration, upstreams, inputs, query, io_config, provider, database,
        )
        query = interpolate_input(loader, configuration, upstreams, query, full_names=False)
        query = render(query, variables)
        if configuration.get('use_raw_sql'):
            return execute_raw_sql(loader, query, configuration, limit)
        loader.export(None, table_name=table, database=database, query_string=query, **options)
        return [loader.load(f'SELECT * FROM {database}.{table}', verbose=False)]
    if name == 'duckdb':
        # Mage's DuckDB branch uses the loader without a with block, and the schema
        # falls back to the profile's before the upstream tables are written.
        schema = schema or loader.default_schema()
        return _run_on(loader, name, provider, configuration, upstreams, inputs, query,
                       io_config, variables, schema, table, limit, options)
    if name == 'bigquery':
        return _run_on_bigquery(loader, provider, configuration, upstreams, inputs, query,
                                io_config, variables, schema, table, limit, options)
    if name == 'trino':
        with loader:
            return _run_on_trino(loader, provider, configuration, upstreams, inputs, query,
                                 io_config, variables, schema, table, limit, options)
    with loader:
        if name == 'postgres':
            upload_upstream_tables(
                loader, configuration, upstreams, inputs, query, io_config, provider,
            )
            query = interpolate_input(loader, configuration, upstreams, query)
            query = render(query, variables)
            schema = schema or loader.default_schema()
            exact = bool(configuration.get('exact_numbers'))
            if configuration.get('use_raw_sql'):
                loader.exact_numbers = exact
                return execute_raw_sql(loader, query, configuration, limit)
            loader.export(None, schema, table, query_string=query, **options)
            from mage_ai.io.postgres_types import with_float_numbers

            frame = loader.load(
                f'SELECT * FROM {loader._full_table_name(schema, table)}', limit=limit,
                verbose=False, exact_types=True,
            )
            return [frame if exact else with_float_numbers(frame)]
        return _run_on(loader, name, provider, configuration, upstreams, inputs, query,
                       io_config, variables, schema, table, limit, options)


def _run_on(loader, name, provider, configuration, upstreams, inputs, query, io_config,
            variables, schema, table, limit, options) -> List:
    """MySQL and DuckDB: upstream tables and {{ df_N }} without a schema."""
    upload_upstream_tables(loader, configuration, upstreams, inputs, query, io_config, provider)
    query = interpolate_input(loader, configuration, upstreams, query, full_names=False)
    query = render(query, variables)
    if configuration.get('use_raw_sql'):
        return execute_raw_sql(loader, query, configuration, limit)
    if name == 'mysql':
        loader.export(None, None, table, query_string=query, **options)
        return [loader.load(
            f'SELECT * FROM {table}', limit=limit, verbose=False, nullable_integers=True,
        )]
    loader.export(None, schema, table_name=table, query_string=query, **options)
    if configuration.get('exact_numbers'):
        return [loader.load(f'SELECT * FROM {schema}.{table}', verbose=False, exact_types=True)]
    return [loader.load(f'SELECT * FROM {schema}.{table}', verbose=False, nullable_integers=True)]


def unique_table_name_suffix(configuration: Dict) -> Optional[str]:
    """With unique_upstream_table_name, the suffix of this run's upstream tables on Trino."""
    if not configuration.get('unique_upstream_table_name', False):
        return None
    return f'{int(time.time())}_{uuid.uuid4().hex[:8]}'


def _run_on_trino(loader, provider, configuration, upstreams, inputs, query, io_config,
                  variables, schema, table, limit, options) -> List:
    """
    Trino: {{ df_N }} is catalog.schema.table. With unique_upstream_table_name, the upstream
    tables are written under names of this run and dropped when the block ends.
    """
    from mage_ai.io.trino_types import quote

    database = configuration.get('data_provider_database') or loader.default_database()
    schema = schema or loader.default_schema()
    suffix = unique_table_name_suffix(configuration)
    written = upload_upstream_tables(
        loader, configuration, upstreams, inputs, query, io_config, provider,
        table_suffix=suffix,
    )
    try:
        query = interpolate_input(
            loader, configuration, upstreams, query, quote_database=False,
            table_suffix=suffix, written=written,
        )
        query = render(query, variables)
        if configuration.get('use_raw_sql'):
            return execute_raw_sql(loader, query, configuration, limit)
        loader.export(None, schema, table, query_string=query, **options)
        full_name = '.'.join(f'"{name}"' for name in (database, schema, table) if name)
        return [loader.load(
            f'SELECT * FROM {full_name}', limit=limit, verbose=False, nullable_integers=True,
        )]
    finally:
        if suffix:
            for upstream_schema, upstream_table in written.values():
                full_name = '.'.join(
                    quote(name) for name in (loader.default_database(), upstream_schema,
                                             upstream_table)
                )
                cursor = loader.conn.cursor()
                try:
                    # A Trino statement finishes when its results are read.
                    cursor.execute(f'DROP TABLE IF EXISTS {full_name}')
                    cursor.fetchall()
                except Exception as error:
                    print(f'Could not drop the upstream table {full_name}: {error}')


def _run_on_bigquery(loader, provider, configuration, upstreams, inputs, query, io_config,
                     variables, schema, table, limit, options) -> List:
    """
    BigQuery: tables are project.dataset.table, the loader is used without a with block, an
    empty upstream frame is not written (Mage's branch has not tested one), and the new
    table is read with a few tries while BigQuery answers 404 for it.
    """
    database = configuration.get('data_provider_database') or loader.default_database()
    for index, upstream in enumerate(upstreams):
        if query and not re.findall(variable_pattern(index + 1), query):
            continue
        if not should_upload(configuration, upstream, io_config, provider['profile_keys']):
            continue
        value = inputs[index] if index < len(inputs) else None
        empty_frame = type(value).__name__ == 'DataFrame' and len(value) == 0
        if empty_frame or _no_data(value):
            print(f"\n\nNo data in upstream block {upstream['uuid']}.")
            continue
        upstream_database, upstream_schema, upstream_table = table_name_parts(
            configuration, upstream,
        )
        upstream_database = upstream_database or database
        full_name = f'{upstream_schema}.{upstream_table}'
        print(f"\n\nExporting data from upstream block {upstream['uuid']} to "
              f'{upstream_database}.{full_name}.')
        loader.export(value, full_name, database=upstream_database, if_exists='replace',
                      verbose=False)
    query = interpolate_input(
        loader, configuration, upstreams, query, quote_database=False, default_schema=False,
    )
    query = render(query, variables)
    if configuration.get('use_raw_sql'):
        return execute_raw_sql(loader, query, configuration, limit)
    loader.export(None, f'{schema}.{table}', database=database, if_exists=options['if_exists'],
                  query_string=query, verbose=options['verbose'])
    for attempt in range(1, BIGQUERY_READ_TRIES + 1):
        time.sleep(attempt - 1)
        try:
            return [loader.load(f'SELECT * FROM {database}.{schema}.{table}', limit=limit,
                                verbose=False)]
        except Exception as error:
            if '404' not in str(error) or attempt == BIGQUERY_READ_TRIES:
                raise


def table_name_parts_from_query(query: str) -> Optional[Tuple[str, str, str]]:
    """As Mage's: database, schema and table of a SELECT's first FROM db.schema.table."""
    match = re.search(r'[^.+]from[\s](\S+)\.(\S+)\.(\S+[^;\s]+)', query, re.IGNORECASE)
    return None if match is None else match.groups()


def _upper(name: Optional[str]) -> Optional[str]:
    return name.upper() if name else name


def _run_on_snowflake(config_loader, provider, configuration, upstreams, inputs, query,
                      io_config, variables, schema, table, limit, options) -> List:
    """
    Snowflake: names are in capitals and quoted, "DATABASE"."SCHEMA"."TABLE"; the client
    opens on the block's database and schema, which a FROM database.schema.table in the
    query gives when the block has none; an empty upstream frame is not written.
    """
    from mage_ai.io.snowflake import Snowflake

    database = configuration.get('data_provider_database')
    if not configuration.get('use_raw_sql'):
        parts = table_name_parts_from_query(query)
        if parts is not None:
            database = parts[0] or database
            schema = parts[1] or schema
    table = _upper(table)
    with Snowflake.with_config(config_loader, database=database, schema=schema) as loader:
        database = _upper(database or loader.default_database())
        schema = _upper(schema or loader.default_schema())
        database_default = _upper(
            configuration.get('data_provider_database') or loader.default_database(),
        )
        schema_default = _upper(
            configuration.get('data_provider_schema') or loader.default_schema(),
        )
        for index, upstream in enumerate(upstreams):
            if query and not re.findall(variable_pattern(index + 1), query):
                continue
            if not should_upload(configuration, upstream, io_config, provider['profile_keys']):
                continue
            value = inputs[index] if index < len(inputs) else None
            empty_frame = type(value).__name__ == 'DataFrame' and len(value) == 0
            if empty_frame or _no_data(value):
                print(f"\n\nNo data in upstream block {upstream['uuid']}.")
                continue
            upstream_database, upstream_schema, upstream_table = table_name_parts(
                configuration, upstream,
            )
            loader.export(
                value,
                _upper(upstream_table),
                _upper(upstream_database) or database_default,
                _upper(upstream_schema) or schema_default,
                if_exists='replace',
                verbose=True,
            )
        query = interpolate_input(
            loader, configuration, upstreams, query,
            name=lambda db, sc, tn: f'"{db.upper()}"."{sc.upper()}"."{tn.upper()}"',
        )
        query = render(query, variables)
        if configuration.get('use_raw_sql'):
            return execute_raw_sql(loader, query, configuration, limit)
        loader.export(None, table, database, schema, if_exists=options['if_exists'],
                      query_string=query, verbose=options['verbose'])
        return [loader.load(
            f'SELECT * FROM "{database}"."{schema}"."{table}"',
            database=database, schema=schema, table_name=table, limit=limit, verbose=False,
        )]
