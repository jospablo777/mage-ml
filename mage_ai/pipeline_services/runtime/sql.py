"""
SQL blocks in pipeline services, run as Mage runs them in a pipeline run
(mage_ai/data_preparation/models/block/sql/__init__.py, the PostgreSQL branch):

1. Upstream outputs that the query names as {{ df_N }} are written to tables, unless the
   upstream is a SQL block on the same database, whose table is used as it is.
2. {{ df_N }} becomes that table's "database".schema.table.
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
from typing import Any, Dict, List, Optional, Tuple

QUERY_ROW_LIMIT = 10_000_000
SUPPORTED_PROVIDERS = ('postgres',)
# Profile settings that make two SQL blocks use the same PostgreSQL database.
POSTGRES_PROFILE_KEYS = ('POSTGRES_DBNAME', 'POSTGRES_HOST', 'POSTGRES_PORT')
CONFIG_KEY_UPSTREAM = 'upstream_block_configuration'

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


def table_name_parts(configuration: Dict, upstream: Dict) -> Tuple[Any, Any, Any]:
    """(database, schema, table) of an upstream block's table, as Mage's table_name_parts."""
    database = schema = table = None
    full = ((configuration.get(CONFIG_KEY_UPSTREAM) or {}).get(upstream['uuid']) or {}).get(
        'table_name',
    )
    parts = (full or upstream['table_name']).split('.')
    if len(parts) == 3:
        database, schema, table = parts
    elif len(parts) == 2:
        schema, table = parts
    elif len(parts) == 1:
        table = parts[0]
    if not schema:
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


def should_upload(configuration: Dict, upstream: Dict, io_config: str) -> bool:
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
    same_database = all(
        block_loader.config.get(k) and upstream_loader.config.get(k)
        and block_loader.config.get(k) == upstream_loader.config.get(k)
        for k in POSTGRES_PROFILE_KEYS
    )
    return not same_provider or not same_database


def _no_data(value: Any) -> bool:
    import pandas as pd

    if isinstance(value, pd.DataFrame):
        return len(value.index) == 0
    if hasattr(value, '__len__'):
        return len(value) == 0
    return not value


def upload_upstream_tables(
    loader, configuration: Dict, upstreams: List[Dict], inputs: List, query: str, io_config: str,
) -> None:
    default_schema = loader.default_schema()
    for index, upstream in enumerate(upstreams):
        if query and not re.findall(variable_pattern(index + 1), query):
            continue
        if not should_upload(configuration, upstream, io_config):
            continue
        value = inputs[index] if index < len(inputs) else None
        if _no_data(value):
            print(f"\n\nNo data in upstream block {upstream['uuid']}.")
            continue
        _, schema, table = table_name_parts(configuration, upstream)
        schema = schema or default_schema
        full_name = '.'.join(p for p in (schema, table) if p)
        print(f"\n\nExporting data from upstream block {upstream['uuid']} to {full_name}.")
        loader.export(
            value,
            table_name=table,
            schema_name=schema,
            cascade_on_drop=False,
            drop_table_on_replace=True,
            if_exists='replace',
            index=False,
            verbose=False,
            allow_reserved_words=True,
        )


def interpolate_input(loader, configuration: Dict, upstreams: List[Dict], query: str) -> str:
    """{{ df_N }} becomes the upstream table's name, as Mage's interpolate_input."""
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
        config_to_use = config if same_providers else configuration
        database = database or config_to_use.get('data_provider_database')
        schema = schema or config_to_use.get('data_provider_schema')
        if not database:
            database = f'"{loader.default_database()}"'
        if not schema:
            schema = loader.default_schema()
        table = table or upstream['table_name']
        replace_with = '.'.join(p for p in (database, schema, table) if p)

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


def execute_raw_sql(loader, query: str, configuration: Dict) -> List:
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
            queries.append(f'SELECT * FROM {full_name} LIMIT 1000')
            fetch.append(full_name)
    results = loader.execute_queries(queries, commit=True, fetch_query_at_indexes=fetch)
    last = results[-1] if results else None
    return [] if last is None else [last]


def run(block: Dict, inputs: List, variables: Dict, repo_path: str) -> List:
    """The block's outputs, as a SQL block in a Mage pipeline run returns them."""
    from mage_ai.io.config import ConfigFileLoader
    from mage_ai.io.postgres import Postgres
    from mage_ai.io.postgres_types import with_float_numbers

    configuration = block.get('configuration') or {}
    details = block.get('sql') or {}
    provider = configuration.get('data_provider')
    if provider not in SUPPORTED_PROVIDERS:
        raise SqlBlockError(
            f'SQL blocks on {provider} are not supported by pipeline services yet; '
            'PostgreSQL is.'
        )
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

    with Postgres.with_config(config_loader) as loader:
        upload_upstream_tables(loader, configuration, upstreams, inputs, query, io_config)
        query = interpolate_input(loader, configuration, upstreams, query)
        query = render(query, variables)
        schema = schema or loader.default_schema()
        if configuration.get('use_raw_sql'):
            return execute_raw_sql(loader, query, configuration)
        loader.export(
            None,
            schema,
            table,
            query_string=query,
            drop_table_on_replace=True,
            if_exists=configuration.get('export_write_policy', 'append'),
            index=False,
            verbose=block.get('type') == 'data_exporter',
        )
        return [with_float_numbers(loader.load(
            f'SELECT * FROM {schema}.{table}', limit=limit, verbose=False, exact_types=True,
        ))]
