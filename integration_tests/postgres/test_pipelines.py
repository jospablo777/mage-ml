"""
Mage pipelines that read from PostgreSQL, transform, and write back.

The pipelines live in integration_tests/project. The loader checks what it fetched in
its @test functions, the transformer adds columns that SQL can compute the same way, and
the exporter writes every column back. Values cross Mage's variable storage between the
blocks. The expected table is built from the source with the same transformation in SQL.
"""

import pytest

from integration_tests.data import postgres_dataset
from integration_tests.mage_runner import run_pipeline

DERIVED_COLUMNS = [
    'text_len',
    'not_bool',
    'int_diff',
    'date_year',
    'int_array_len',
    'json_kind',
    'bytea_len',
]
EXPECTED_SQL = """
CREATE TABLE {schema}.expected AS
SELECT
    s.*,
    char_length(c_text)::bigint AS text_len,
    NOT c_bool AS not_bool,
    c_integer::bigint - c_smallint::bigint AS int_diff,
    extract(year FROM c_date)::bigint AS date_year,
    cardinality(c_int_array)::bigint AS int_array_len,
    jsonb_typeof(c_jsonb) AS json_kind,
    octet_length(c_bytea)::bigint AS bytea_len
FROM {schema}.src s
"""


def build_expected(pg, schema):
    with pg.cursor() as cursor:
        cursor.execute(EXPECTED_SQL.format(schema=schema))
    pg.commit()


@pytest.mark.parametrize('upsert', [False, True], ids=['replace', 'upsert'])
@pytest.mark.parametrize('engine', ['pandas', 'polars'])
def test_pipeline_round_trip(mage_project, pg, schema, source_table, source_rows, engine, upsert):
    build_expected(pg, schema)
    if upsert:
        # The upsert goes into an existing table with the source column types.
        postgres_dataset.create_like_table(pg, schema, 'expected', 'dst')
        with pg.cursor() as cursor:
            cursor.execute(f'ALTER TABLE {schema}.dst ADD PRIMARY KEY (id)')
        pg.commit()

    run_pipeline(
        f'postgres_{engine}',
        schema=schema,
        expected_rows=len(source_rows),
        upsert=upsert,
        if_exists='append' if upsert else 'replace',
    )

    columns = postgres_dataset.COLUMN_NAMES + DERIVED_COLUMNS
    assert postgres_dataset.mismatches(pg, schema, 'expected', 'dst', columns=columns) == {}
