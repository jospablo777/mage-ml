"""
A Trino SQL block between Python blocks, in each catalog: Mage writes the Python block's
frame to a table, the SQL block queries it, and the next block checks the result.
"""
from integration_tests.mage_runner import run_pipeline


def test_sql_block(mage_project, trino_schema, tr):
    run_pipeline('trino_sql', rows=1000)

    tables = {row[0] for row in tr('SHOW TABLES')}
    assert any('trino_sql_load' in table for table in tables), tables
    assert any('trino_sql_transform' in table for table in tables), tables
