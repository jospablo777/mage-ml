"""
A MySQL SQL block with query preprocessing off sends its text as written: every statement
runs, in order, and the block gets the last statement's rows (upstream #5696).
"""
from mage_ai.data_preparation.models.block.sql import execute_raw_sql


def test_every_statement_of_a_raw_block_runs(mage_mysql, my):
    result = execute_raw_sql(
        mage_mysql, None,
        'CREATE TABLE t (id INT); INSERT INTO t VALUES (1), (2); '
        "-- a comment; with a semicolon\nINSERT INTO t VALUES (3); SELECT COUNT(*) FROM t;",
        disable_query_preprocessing=True,
    )

    with my.cursor() as cursor:
        cursor.execute('SELECT id FROM t ORDER BY id')
        assert [row[0] for row in cursor.fetchall()] == [1, 2, 3]
    assert result == [(3,)]
