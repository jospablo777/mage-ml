"""
clickhouse_sql (a Python loader, a ClickHouse SQL block and a Python check) exported with
`mage export service` and run by mage-service: it must leave the tables that the pipeline
run by Mage leaves, column by column.
"""
from integration_tests.exported_services import differences, run_exported
from integration_tests.mage_runner import run_pipeline


def snapshot(ch) -> dict:
    result = {}
    names = [row[0] for row in ch.query(
        'SELECT name FROM system.tables WHERE database = currentDatabase()',
    ).result_rows]
    for name in names:
        columns = ch.query(
            'SELECT name, type FROM system.columns '
            'WHERE database = currentDatabase() AND table = %(table)s ORDER BY position',
            parameters={'table': name},
        ).result_rows
        summary = {'rows': ch.query(f'SELECT count() FROM `{name}`').result_rows[0][0]}
        for column, column_type in columns:
            # An order-independent hash of the column's values.
            digest = ch.query(
                f'SELECT sum(cityHash64(toString(`{column}`))) FROM `{name}`',
            ).result_rows[0][0]
            summary[column] = (column_type, digest)
        result[name] = summary
    return result


def test_an_exported_clickhouse_sql_pipeline_leaves_the_tables_mage_leaves(
    mage_project, clickhouse_database, ch, tmp_path,
):
    run_pipeline('clickhouse_sql', rows=1000)
    expected = snapshot(ch)
    assert expected
    for name in expected:
        ch.command(f'DROP TABLE IF EXISTS `{name}`')

    run_exported(mage_project, 'clickhouse_sql', tmp_path, dict(rows=1000))

    assert differences(expected, snapshot(ch)) == {}
