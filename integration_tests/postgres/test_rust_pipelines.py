"""
Pipelines of Rust blocks only, on PostgreSQL: a Rust loader reads with binary COPY, a
Rust transformer works on the frame, a Rust exporter writes with binary COPY. Every type
of the source table round trips; appends cast by column name; failures say why.
"""
import pytest

from integration_tests.mage_runner import execute_pipeline, run_pipeline
from integration_tests.postgres.test_sql_block_matrix import build, error_of, query

RUST_PROFILE = dict(data_provider_profile='test_schema')

LOAD = '''
use mage::postgres::Postgres;
use mage::prelude::*;

fn load_data(vars: Vars) -> Result<DataFrame> {
    let schema: String = vars.get("schema")?;
    let table: String = vars.get_or("source", "src".to_string())?;
    Postgres::connect()?.query(&format!("SELECT * FROM {schema}.{table} ORDER BY id"))
}
'''

EXPORT = '''
use mage::postgres::{Postgres, WritePolicy};
use mage::prelude::*;

fn export_data(rows: DataFrame, vars: Vars) -> Result<()> {
    let schema: String = vars.get("schema")?;
    let policy = match vars.get_or("policy", "replace".to_string())?.as_str() {
        "append" => WritePolicy::Append,
        "fail" => WritePolicy::Fail,
        _ => WritePolicy::Replace,
    };
    let written = Postgres::connect()?.write(&rows, &format!("{schema}.dst"), policy)?;
    println!("wrote {written} rows");
    Ok(())
}
'''


def rust_pipeline(project, *blocks):
    return build(project, [dict(block, language='rust', config=RUST_PROFILE) for block in blocks])


# How a source column compares with what the Rust exporter wrote: types Rust reads as
# text come back as text, so they compare cast to the source type.
def round_trip_problems(pg, schema):
    from integration_tests.data import postgres_dataset

    source_types = dict(postgres_dataset._format_types(pg, schema, 'src'))
    target_types = dict(postgres_dataset._format_types(pg, schema, 'dst'))
    problems = {}
    for name in postgres_dataset.COLUMN_NAMES:
        source_type = source_types[name]
        if name not in target_types:
            problems[name] = ['<missing column>']
            continue
        if name in ('c_json', 'c_jsonb'):
            comparison = f'd.{name}::jsonb = s.{name}::jsonb'
        elif target_types[name] == source_type:
            comparison = f'd.{name} = s.{name}'
        elif name == 'c_real':
            comparison = f'd.{name}::text::double precision = s.{name}::text::double precision'
        elif name in ('c_numeric', 'c_numeric_free'):
            # Values past 38 digits, NaN or Infinity read as their exact text.
            comparison = f'd.{name}::numeric = s.{name}'
        else:
            comparison = f'd.{name}::text::{source_type} = s.{name}'
        rows = query(pg, f'''
            SELECT s.id, s.{name}::text, d.{name}::text
            FROM {schema}.src s JOIN {schema}.dst d USING (id)
            WHERE (s.{name} IS NULL) <> (d.{name} IS NULL)
               OR NOT coalesce({comparison}, s.{name} IS NULL)
            ORDER BY s.id LIMIT 3
        ''')
        if rows:
            problems[name] = rows
    return problems


def test_every_type_round_trips_through_rust(
    mage_project, pg, profile_schema, source_table, source_rows,
):
    pipeline = rust_pipeline(
        mage_project,
        dict(uuid='load', type='data_loader', code=LOAD),
        dict(uuid='export', type='data_exporter', code=EXPORT, upstream=['load']),
    )
    run = execute_pipeline(pipeline, schema=profile_schema)
    assert run.status.value == 'completed', error_of(run)
    assert query(pg, f'SELECT count(*) FROM {profile_schema}.dst')[0][0] == len(source_rows)
    assert round_trip_problems(pg, profile_schema) == {}
    types = dict(query(
        pg,
        'SELECT column_name, data_type FROM information_schema.columns '
        'WHERE table_schema = %s AND table_name = %s', (profile_schema, 'dst'),
    ))
    assert types['c_bigint'] == 'bigint'
    assert types['c_timestamptz'] == 'timestamp with time zone'
    assert types['c_int_array'] == 'ARRAY'


def test_append_casts_into_the_existing_table_by_name(
    mage_project, pg, profile_schema, source_table, source_rows,
):
    from integration_tests.data import postgres_dataset

    postgres_dataset.create_like_table(pg, profile_schema, 'src', 'dst')
    pipeline = rust_pipeline(
        mage_project,
        dict(uuid='load', type='data_loader', code=LOAD),
        dict(uuid='export', type='data_exporter', code=EXPORT, upstream=['load']),
    )
    run = execute_pipeline(pipeline, schema=profile_schema, policy='append')
    assert run.status.value == 'completed', error_of(run)
    assert postgres_dataset.mismatches(pg, profile_schema, 'src', 'dst') == {}

    failed = execute_pipeline(pipeline, schema=profile_schema, policy='fail')
    assert failed.status.value == 'failed'
    assert 'already exists' in error_of(failed, 'export')


def test_a_rust_transformer_between_rust_loader_and_exporter(mage_project, pg, profile_schema):
    query(pg, 'SELECT 1')
    with pg.cursor() as cursor:
        cursor.execute(f'''
            CREATE TABLE {profile_schema}.orders AS
            SELECT g AS id, (g % 7) AS customer, (g % 13) * 1.25 AS amount,
                   timestamptz '2026-01-01 00:00:00+00' + g * interval '1 minute' AS at
            FROM generate_series(1, 50000) g
        ''')
    pg.commit()
    pipeline = rust_pipeline(
        mage_project,
        dict(uuid='load', type='data_loader', code=LOAD),
        dict(uuid='totals', type='transformer', upstream=['load'], code='''
            use mage::prelude::*;

            fn transform(orders: LazyFrame) -> Result<LazyFrame> {
                Ok(orders
                    .group_by([col("customer")])
                    .agg([col("amount").sum().alias("total"), len().alias("orders")])
                    .sort(["customer"], Default::default()))
            }
        '''),
        dict(uuid='export', type='data_exporter', code=EXPORT, upstream=['totals']),
    )
    run_pipeline(pipeline, schema=profile_schema, source='orders')
    expected = query(pg, f'''
        SELECT customer, sum(amount), count(*) FROM {profile_schema}.orders
        GROUP BY customer ORDER BY customer
    ''')
    actual = query(
        pg, f'SELECT customer, total, orders FROM {profile_schema}.dst ORDER BY customer',
    )
    assert [(c, float(t), n) for c, t, n in actual] == [(c, float(t), n) for c, t, n in expected]


def test_errors_name_the_problem(mage_project, pg, profile_schema):
    pipeline = rust_pipeline(mage_project, dict(uuid='load', type='data_loader', code='''
        use mage::postgres::Postgres;
        use mage::prelude::*;

        fn load_data() -> Result<DataFrame> {
            Postgres::connect()?.query("SELECT nmae FROM pg_class")
        }
    '''))
    run = execute_pipeline(pipeline)
    assert run.status.value == 'failed'
    assert 'nmae' in error_of(run, 'load')

    duplicate = rust_pipeline(mage_project, dict(uuid='load', type='data_loader', code='''
        use mage::postgres::Postgres;
        use mage::prelude::*;

        fn load_data() -> Result<DataFrame> {
            Postgres::connect()?.query("SELECT 1 AS a, 2 AS a")
        }
    '''))
    run = execute_pipeline(duplicate)
    assert run.status.value == 'failed'
    assert 'two columns named a' in error_of(run, 'load')


@pytest.fixture(autouse=True)
def _rust_installed():
    import shutil

    if shutil.which('cargo') is None:
        pytest.skip('Rust is not installed')
