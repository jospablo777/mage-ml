"""
SQL Server through pymssql (FreeTDS, no ODBC driver), on Babelfish for PostgreSQL, which
speaks SQL Server's protocol: SQL blocks run by Mage and by an exported service leave the
same tables. Babelfish lacks catalog objects SQLAlchemy reads when it connects
(sys.system_views, fn_listextendedproperty), so frames written with pandas' to_sql are
tested with a stand-in (mage_ai/tests/pipeline_services/test_mssql_sql.py); these
pipelines run SQL on SQL only.
"""
import pytest

from integration_tests.exported_services import run_exported
from integration_tests.mage_runner import execute_pipeline
from integration_tests.postgres.test_sql_block_matrix import build, error_of, output

MSSQL = dict(data_provider='sqlserver', data_provider_profile='mssql')

ORDERS = '''SELECT * FROM (VALUES
    (1, 'north', 10.50, 'Zürich'),
    (2, 'south', 5.25, 'São Paulo'),
    (3, 'north', 2.25, NULL),
    (4, 'east', 7.00, 'Bern')
) AS orders(id, region, amount, city)'''
TOTALS = '''SELECT region, SUM(amount) AS total, COUNT(*) AS orders
FROM {{ df_1 }}
GROUP BY region'''
BIG = 'SELECT region, total FROM {{ df_1 }} WHERE total > 7'


@pytest.fixture
def loader(mssql_settings):
    from mage_ai.io.mssql import MSSQL as Loader

    with Loader(**mssql_settings, verbose=False) as connected:
        yield connected


def rows(connection, sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return [tuple(r) for r in cursor.fetchall()]


def pipeline_of_sql_blocks(project):
    return build(project, [
        dict(uuid='orders', type='data_loader', language='sql',
             config=dict(MSSQL, export_write_policy='replace'), code=ORDERS),
        dict(uuid='totals', type='transformer', language='sql', upstream=['orders'],
             config=dict(MSSQL, export_write_policy='replace'), code=TOTALS),
        dict(uuid='big', type='data_exporter', language='sql', upstream=['totals'],
             config=dict(MSSQL, data_provider_table='big_regions',
                         export_write_policy='replace'), code=BIG),
    ])


def snapshot(connection, schema):
    tables = [t for (t,) in rows(connection, (
        'SELECT table_name FROM information_schema.tables '
        f"WHERE table_schema = '{schema}' ORDER BY table_name"
    ))]
    return {t: sorted(rows(connection, f'SELECT * FROM {schema}.{t}'), key=repr) for t in tables}


def test_a_query_is_written_to_a_table_and_read_back(mssql_schema, loader):
    # The loader closes first: its open transaction holds locks the schema's drop waits on.
    schema, connection = mssql_schema
    loader.export(None, schema, 'orders', query_string=ORDERS, if_exists='replace',
                  drop_table_on_replace=True)
    loader.export(None, schema, 'orders', query_string=ORDERS, if_exists='append')

    frame = loader.load(f'SELECT TOP 3 * FROM {schema}.orders ORDER BY id', verbose=False)

    assert frame['id'].tolist() == [1, 1, 2]
    assert list(frame['city'][:3]) == ['Zürich', 'Zürich', 'São Paulo']
    assert rows(connection, f'SELECT COUNT(*) FROM {schema}.orders') == [(8,)]


def test_raw_sql_reads_with_top(mage_project, mssql_schema):
    schema, connection = mssql_schema
    pipeline = build(mage_project, [
        dict(uuid='raw', type='data_loader', language='sql',
             config=dict(MSSQL, use_raw_sql=True, limit=2),
             code=f'SELECT * INTO {schema}.raw_orders FROM ({ORDERS}) AS o;'),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    assert rows(connection, f'SELECT COUNT(*) FROM {schema}.raw_orders') == [(4,)]


def test_sql_blocks_run_by_mage_and_by_a_service_leave_the_same_tables(
    mage_project, mssql_schema, tmp_path,
):
    schema, connection = mssql_schema
    pipeline = pipeline_of_sql_blocks(mage_project)

    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    totals = output(pipeline, run, 'totals')
    assert sorted(zip(totals['region'], totals['orders'])) == [
        ('east', 1), ('north', 2), ('south', 1),
    ]
    expected = snapshot(connection, schema)
    assert [(r, float(t)) for r, t in expected['big_regions']] == [('north', 12.75)]
    for table in expected:
        with connection.cursor() as cursor:
            cursor.execute(f'DROP TABLE {schema}.{table}')

    run_exported(mage_project, pipeline, tmp_path)

    assert snapshot(connection, schema) == expected
