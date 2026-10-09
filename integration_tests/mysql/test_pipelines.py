"""
A Mage pipeline on MySQL: mysql_polars loads the source table into Polars, transforms it
with a LazyFrame and exports the result with Mage's MySQL client.
"""
from integration_tests.data import mysql_dataset
from integration_tests.mage_runner import run_pipeline

COLUMNS = [
    c for c in mysql_dataset.COLUMN_NAMES if c not in ('c_decimal_wide', 'c_set')
]


def test_polars_pipeline(mage_project, my, mysql_source, mysql_database, monkeypatch):
    monkeypatch.setenv('MAGE_TEST_MYSQL_DATABASE', mysql_database)
    with my.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) FROM src')
        count = cursor.fetchone()[0]

    run_pipeline('mysql_polars', expected_rows=count)

    expected = mysql_dataset.rows(my.cursor(), 'src', COLUMNS)
    result = mysql_dataset.rows(
        my.cursor(), 'polars_result', COLUMNS + ['varchar_length', 'bigint_plus_one'],
    )
    assert mysql_dataset.mismatches(expected, result, COLUMNS) == {}
    for key, row in expected.items():
        text, number = row['c_varchar'], row['c_bigint']
        assert result[key]['varchar_length'] == (None if text is None else len(text))
        assert result[key]['bigint_plus_one'] == (None if number is None else number + 1)
