"""
A ClickHouse SQL block between Python blocks: Mage writes the Python block's frame to a
table, the SQL block queries it, and the next block checks the result.
"""
from integration_tests.mage_runner import run_pipeline


def test_sql_block(mage_project, clickhouse_database, ch):
    run_pipeline('clickhouse_sql', rows=1000)

    engines = dict(ch.query(
        'SELECT name, engine FROM system.tables WHERE database = currentDatabase()',
    ).result_rows)
    # The upstream table and the SQL block's table keep their data on disk.
    assert engines and set(engines.values()) == {'MergeTree'}, engines
