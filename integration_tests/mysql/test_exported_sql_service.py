"""
mysql_sql (a MySQL SQL loader, a Python check and a MySQL SQL exporter) exported with
`mage export service` and run by mage-service: it must leave the tables that the pipeline
run by Mage leaves, column by column.
"""
from integration_tests.exported_services import differences, run_exported
from integration_tests.mage_runner import run_pipeline


def snapshot(my, database: str, keep=('src',)) -> dict:
    result = {}
    with my.cursor() as cursor:
        cursor.execute('SET SESSION group_concat_max_len = 1073741824')
        cursor.execute(
            'SELECT table_name FROM information_schema.tables WHERE table_schema = %s',
            (database,),
        )
        names = [row[0] for row in cursor.fetchall()]
        for name in names:
            if name in keep:
                continue
            cursor.execute(
                'SELECT column_name, column_type FROM information_schema.columns '
                'WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position',
                (database, name),
            )
            columns = cursor.fetchall()
            cursor.execute(f'SELECT COUNT(*) FROM `{name}`')
            summary = {'rows': cursor.fetchone()[0]}
            for column, column_type in columns:
                value = f"COALESCE(CAST(`{column}` AS CHAR), 'NULL')"
                cursor.execute(
                    f"SELECT MD5(COALESCE(GROUP_CONCAT({value} ORDER BY {value} SEPARATOR '|'), "
                    f"'')) FROM `{name}`"
                )
                summary[column] = (column_type, cursor.fetchone()[0])
            result[name] = summary
    my.commit()
    return result


def test_an_exported_mysql_sql_pipeline_leaves_the_tables_mage_leaves(
    mage_project, my, mysql_source, mysql_database, monkeypatch, tmp_path,
):
    monkeypatch.setenv('MAGE_TEST_MYSQL_DATABASE', mysql_database)
    with my.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) FROM src')
        count = cursor.fetchone()[0]
    run_pipeline('mysql_sql', expected_rows=count)
    expected = snapshot(my, mysql_database)
    assert expected
    with my.cursor() as cursor:
        for name in expected:
            cursor.execute(f'DROP TABLE IF EXISTS `{name}`')
    my.commit()

    run_exported(mage_project, 'mysql_sql', tmp_path, dict(expected_rows=count))

    assert differences(expected, snapshot(my, mysql_database)) == {}
