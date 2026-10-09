"""
Mage pipelines on MySQL:

- mysql_polars loads the source table into Polars, transforms it with a LazyFrame and
  exports the result with Mage's MySQL client.
- mysql_sql loads it with a SQL block, checks the frame in a Python block and exports it
  with a SQL block.
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


# SQL blocks read DECIMAL as float and TIMESTAMP as naive in the session time zone, as
# pandas read_sql does.
SQL_BLOCK_COLUMNS = [
    c for c in COLUMNS if not c.startswith('c_decimal') and c != 'c_timestamp'
]


def test_sql_blocks(mage_project, my, mysql_source, mysql_database, monkeypatch):
    """
    The SQL loader's frame kept integers: pandas read_sql turned integer columns with
    NULLs into float64, so 9223372036854775807 became 9.223372036854776e18.
    """
    monkeypatch.setenv('MAGE_TEST_MYSQL_DATABASE', mysql_database)
    with my.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) FROM src')
        count = cursor.fetchone()[0]

    run_pipeline('mysql_sql', expected_rows=count)

    expected = mysql_dataset.rows(my.cursor(), 'src', SQL_BLOCK_COLUMNS)
    result = mysql_dataset.rows(my.cursor(), 'mysql_sql_dst', SQL_BLOCK_COLUMNS)
    assert mysql_dataset.mismatches(expected, result, SQL_BLOCK_COLUMNS) == {}
    with my.cursor() as cursor:
        # Mage's session has the server's time zone; the test connection uses UTC. MySQL
        # compares a TIMESTAMP with a DATETIME in the session time zone.
        cursor.execute('SET time_zone = @@global.time_zone')
        cursor.execute(
            'SELECT COUNT(*) FROM src s JOIN mysql_sql_dst d USING (id) '
            'WHERE NOT (s.c_timestamp <=> d.c_timestamp)',
        )
        assert cursor.fetchone()[0] == 0
        cursor.execute("SET time_zone = '+00:00'")
