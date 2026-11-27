"""
The database tables a SQL block reads and writes, as OpenLineage datasets named the way its
naming spec names them (https://openlineage.io/docs/spec/naming), so a catalog joins MageML's
lineage with the lineage other tools report for the same tables:

| Database | Namespace | Name |
| --- | --- | --- |
| PostgreSQL | postgres://host:port | database.schema.table |
| MySQL | mysql://host:port | database.table |
| ClickHouse | clickhouse://host:port | database.table |
| Snowflake | snowflake://account | database.schema.table |
| BigQuery | bigquery | project.dataset.table |
| DuckDB | duckdb://database file | schema.table |

The tables come from the block's query (sqlglot): the ones it reads (FROM, JOIN, not its own
CTEs or its upstream blocks, which are MageML datasets already) and, for a block with raw SQL,
the ones it writes (INSERT, CREATE, MERGE); a block whose output Mage writes as a table has
that table as its output. The host, port and database come from the block's io_config
profile. Anything that cannot be named (another database, a query that does not parse)
gives no dataset: lineage then has the blocks' outputs only.
"""
import os
import re
from typing import Dict, List, Optional, Tuple

DIALECTS = dict(
    bigquery='bigquery', clickhouse='clickhouse', duckdb='duckdb', mysql='mysql',
    postgres='postgres', snowflake='snowflake',
)


def _setting(loader, key: str) -> Optional[str]:
    from mage_ai.io.config import ConfigKey

    try:
        value = loader.get(getattr(ConfigKey, key))
    except Exception:
        return None
    return str(value) if value not in (None, '') else None


def _namespace(provider: str, loader) -> Optional[Tuple[str, Dict]]:
    """The dataset namespace, and the defaults table names are completed with."""
    if provider == 'postgres':
        host = _setting(loader, 'POSTGRES_HOST')
        if not host:
            return None
        port = _setting(loader, 'POSTGRES_PORT') or '5432'
        return f'postgres://{host}:{port}', dict(
            database=_setting(loader, 'POSTGRES_DBNAME'),
            schema=_setting(loader, 'POSTGRES_SCHEMA') or 'public',
        )
    if provider == 'mysql':
        host = _setting(loader, 'MYSQL_HOST')
        if not host:
            return None
        port = _setting(loader, 'MYSQL_PORT') or '3306'
        return f'mysql://{host}:{port}', dict(database=_setting(loader, 'MYSQL_DATABASE'))
    if provider == 'clickhouse':
        host = _setting(loader, 'CLICKHOUSE_HOST')
        if not host:
            return None
        port = _setting(loader, 'CLICKHOUSE_PORT') or '8123'
        return f'clickhouse://{host}:{port}', dict(
            database=_setting(loader, 'CLICKHOUSE_DATABASE') or 'default',
        )
    if provider == 'snowflake':
        account = _setting(loader, 'SNOWFLAKE_ACCOUNT')
        if not account:
            return None
        return f'snowflake://{account.lower()}', dict(
            database=_setting(loader, 'SNOWFLAKE_DEFAULT_DB'),
            schema=_setting(loader, 'SNOWFLAKE_DEFAULT_SCHEMA'),
        )
    if provider == 'bigquery':
        return 'bigquery', dict()
    if provider == 'duckdb':
        database = _setting(loader, 'DUCKDB_DATABASE')
        if not database:
            return None
        return f'duckdb://{os.path.abspath(database)}', dict(schema='main')
    return None


def _name(provider: str, parts: List[str], defaults: Dict) -> Optional[str]:
    """`database.schema.table` (or the provider's form) from a table's own parts."""
    parts = [p for p in parts if p]
    if provider in ('mysql', 'clickhouse'):
        if len(parts) == 1 and defaults.get('database'):
            parts = [defaults['database']] + parts
        return '.'.join(parts[-2:]) if len(parts) >= 2 else None
    if provider == 'bigquery':
        return '.'.join(parts) if len(parts) == 3 else None
    if provider == 'duckdb':
        if len(parts) == 1:
            parts = [defaults['schema']] + parts
        return '.'.join(parts[-2:])
    # PostgreSQL and Snowflake: database.schema.table.
    if len(parts) == 1 and defaults.get('schema'):
        parts = [defaults['schema']] + parts
    if len(parts) == 2 and defaults.get('database'):
        parts = [defaults['database']] + parts
    return '.'.join(parts) if len(parts) == 3 else None


# A block refers to its upstream blocks with {{ df_1 }} or {{ block_uuid }}: MageML datasets.
_TEMPLATE = re.compile(r'\{\{.*?\}\}|\{%.*?%\}', re.DOTALL)
UPSTREAM = '__mage_upstream__'


def tables(query: str, provider: str) -> Tuple[List[List[str]], List[List[str]]]:
    """The tables the query reads and writes, each as its parts, from sqlglot."""
    import sqlglot
    from sqlglot import exp

    text = _TEMPLATE.sub(UPSTREAM, query)
    try:
        statements = sqlglot.parse(text, read=DIALECTS.get(provider))
    except Exception:
        return [], []
    reads, writes = [], []
    for statement in statements:
        if statement is None:
            continue
        ctes = {cte.alias_or_name.lower() for cte in statement.find_all(exp.CTE)}
        targets = set()
        for kind in (exp.Insert, exp.Create, exp.Merge):
            for node in statement.find_all(kind):
                target = node.this
                if isinstance(target, exp.Schema):
                    target = target.this
                if isinstance(target, exp.Table):
                    targets.add(id(target))
                    writes.append(_parts(target))
        for table in statement.find_all(exp.Table):
            if id(table) in targets or table.name.lower() in ctes or table.name == UPSTREAM \
                    or not table.name:
                continue
            reads.append(_parts(table))
    return _unique(reads), _unique(writes)


def _parts(table) -> List[str]:
    return [table.catalog, table.db, table.name]


def _unique(items: List[List[str]]) -> List[List[str]]:
    """Each table once, sorted: the same query names its tables in the same order."""
    return [list(key) for key in sorted({tuple(item) for item in items})]


def sql_datasets(block) -> Tuple[List[Dict], List[Dict]]:
    """The input and output datasets of a SQL block's tables; empty when not known."""
    from mage_ai.io.config import ConfigFileLoader

    if str(getattr(block.language, 'value', block.language)) != 'sql':
        return [], []
    configuration = block.configuration or {}
    if configuration.get('data_provider') not in DIALECTS:
        return [], []
    try:
        loader = ConfigFileLoader(
            os.path.join(block.repo_path, 'io_config.yaml'),
            configuration.get('data_provider_profile') or 'default',
        )
        query = block.content or ''
        table = block.table_name
    except Exception:
        return [], []
    return datasets(configuration, loader, query, table)


def datasets(configuration: Dict, loader, query: str, table: str) -> Tuple[List[Dict], List[Dict]]:
    """
    The tables a SQL block with this configuration and query reads and writes, named from
    its io_config profile (`loader`, a ConfigFileLoader); `table` is the block's output table.
    Exported services' SQL runner calls it too.
    """
    provider = configuration.get('data_provider')
    if provider not in DIALECTS:
        return [], []
    try:
        named = _namespace(provider, loader)
    except Exception:
        return [], []
    if named is None:
        return [], []
    namespace, defaults = named
    if configuration.get('data_provider_database'):
        defaults = dict(defaults, database=configuration['data_provider_database'])
    if configuration.get('data_provider_schema'):
        defaults = dict(defaults, schema=configuration['data_provider_schema'])
    reads, writes = tables(query, provider)
    if not configuration.get('use_raw_sql'):
        # Mage writes the block's output as its table.
        writes = [[None, None, table]]

    def named_datasets(items):
        found = []
        for parts in items:
            name = _name(provider, parts, defaults)
            if name:
                found.append(dict(namespace=namespace, name=name))
        return found

    return named_datasets(reads), named_datasets(writes)
