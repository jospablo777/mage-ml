"""
Guided ingestion from MySQL (ADR-0048) against a real server: the binary log's settings and
the user's grants checked, the tables with their keys, a sample, and the pipeline it
creates, which reads the binary log and exports as a PostgreSQL mirror with an initial copy.
"""
import uuid

from mage_ai.data_preparation import ingestion


def connection(mysql_settings, database, **extra):
    return dict(
        kind='mysql', host=mysql_settings['host'], port=mysql_settings['port'],
        database=database, user=mysql_settings['user'],
        password=mysql_settings['password'], sslmode='disable', **extra,
    )


def test_checks_tables_sample_and_the_pipeline_it_creates(
    mage_project, mysql_settings, mysql_database, my, tmp_path, monkeypatch,
):
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.pipeline_services import export

    # The secrets' key goes in a data folder of the test's, not the checkout.
    monkeypatch.setenv('MAGE_DATA_DIR', str(tmp_path / 'mage_data'))
    with my.cursor() as cursor:
        for statement in (
            'CREATE TABLE orders (id INT PRIMARY KEY, item VARCHAR(20))',
            'CREATE TABLE order_lines (order_id INT, line INT, qty INT, '
            'PRIMARY KEY (order_id, line))',
            'CREATE TABLE notes (body TEXT)',
            "INSERT INTO orders VALUES (1, 'tea'), (2, 'cake')",
        ):
            cursor.execute(statement)
    source = connection(mysql_settings, mysql_database)

    found = ingestion.check_source(source)
    assert found['ready'], found['checks']
    names = {c['name'] for c in found['checks']}
    assert {'binary log', 'binlog_row_metadata', 'replication grants'} <= names
    tables = {t['table']: t for t in found['tables']}
    assert tables[f'{mysql_database}.orders']['key'] == ['id']
    assert tables[f'{mysql_database}.order_lines']['key'] == ['order_id', 'line']
    assert tables[f'{mysql_database}.notes']['blocked'].startswith('no primary key')

    sampled = ingestion.sample(source, f'{mysql_database}.orders')
    assert sampled['columns'] == ['id', 'item']
    assert sampled['rows'] == [['1', 'tea'], ['2', 'cake']]

    chosen = [tables[f'{mysql_database}.orders'], tables[f'{mysql_database}.order_lines']]
    lines = ingestion.review(chosen, 'delete', 'raw', 'copy_', 'mysql')
    assert '(the binary log)' in lines[0]
    destination = dict(host='pg.example.com', port=5432, database='warehouse',
                       user='loader', password='secret', sslmode='disable')
    try:
        ingestion.create(mage_project, 'nope', source, destination,
                         [tables[f'{mysql_database}.notes']])
        raise AssertionError('a table without a key was accepted')
    except ingestion.IngestionError as error:
        assert 'no primary key' in str(error) or 'has no key' in str(error), error

    created = ingestion.create(
        mage_project, f'sync_{mysql_database}', source, destination, chosen,
        schema='raw', prefix='copy_',
    )
    pipeline = Pipeline.get(created['pipeline_uuid'], repo_path=mage_project)
    try:
        captured = export.capture(mage_project, [pipeline.uuid])
        stream = captured.manifest['pipelines'][0]['stream']
        assert stream['source']['kind'] == 'mysql'
        config = stream['source']['config']
        assert config['snapshot'] == 'initial'
        assert config['port'] == mysql_settings['port']
        assert config['tables'] == [f'{mysql_database}.orders', f'{mysql_database}.order_lines']
        sink = stream['sinks'][0]['config']
        assert sink['mirror'] == dict(key=dict(orders=['id'], order_lines=['order_id', 'line']),
                                      deletes='delete')
        assert 'mysql' in captured.engine_features
    finally:
        pipeline.delete()


def test_a_user_without_replication_grants_is_told_what_to_grant(
    mysql_settings, mysql_database, my,
):
    user = f'it_{uuid.uuid4().hex[:8]}'
    with my.cursor() as cursor:
        cursor.execute(f"CREATE USER '{user}'@'%' IDENTIFIED BY 'pw'")  # pragma: allowlist secret
        cursor.execute(f"GRANT SELECT ON `{mysql_database}`.* TO '{user}'@'%'")
    try:
        found = ingestion.check_source(dict(
            connection(mysql_settings, mysql_database), user=user, password='pw',
        ))
        assert not found['ready']
        grants = next(c for c in found['checks'] if c['name'] == 'replication grants')
        assert not grants['ok']
        assert 'GRANT REPLICATION SLAVE, REPLICATION CLIENT' in grants['detail']
    finally:
        with my.cursor() as cursor:
            cursor.execute(f"DROP USER '{user}'@'%'")


def test_a_table_with_another_key_is_added_to_a_sync_with_one_key(
    mage_project, mysql_settings, mysql_database, my, tmp_path, monkeypatch,
):
    """A sink keyed by one list for every table (as written by hand) gets keys by table when
    a table with another key is added; tables written as a YAML list are read and rewritten."""
    from pathlib import Path

    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.pipeline_services import export

    monkeypatch.setenv('MAGE_DATA_DIR', str(tmp_path / 'mage_data'))
    with my.cursor() as cursor:
        cursor.execute('CREATE TABLE orders (id INT PRIMARY KEY, item VARCHAR(20))')
        cursor.execute('CREATE TABLE order_lines (order_id INT, line INT, qty INT, '
                       'PRIMARY KEY (order_id, line))')
    source = connection(mysql_settings, mysql_database)
    tables = {t['table']: t for t in ingestion.check_source(source)['tables']}
    created = ingestion.create(mage_project, f'add_{mysql_database}', source,
                               dict(kind='postgres', host='db', database='copy', user='u'),
                               [tables[f'{mysql_database}.orders']], schema='raw')
    pipeline_uuid = created['pipeline_uuid']
    pipeline = Pipeline.get(pipeline_uuid, repo_path=mage_project)
    try:
        blocks = pipeline.blocks_by_uuid
        source_path = Path(blocks[f'{pipeline_uuid}_source'].file_path)
        sink_path = Path(blocks[f'{pipeline_uuid}_copy'].file_path)
        source_path.write_text(ingestion._set(
            source_path.read_text(), 'tables', None,
        ).replace('tables: null', f'tables:\n  - {mysql_database}.orders  # by hand'))
        sink_path.write_text(ingestion._set(
            sink_path.read_text(), 'mirror', dict(key=['id'], deletes='delete'),
        ))

        added = ingestion.add_tables(mage_project, pipeline_uuid,
                                     [dict(table=f'{mysql_database}.order_lines')])
        assert added['added'] == [f'{mysql_database}.order_lines']
        assert not any('publication' in line for line in added['review'])
        stream = export.capture(mage_project, [pipeline_uuid]).manifest['pipelines'][0]['stream']
        assert stream['source']['config']['tables'] == [
            f'{mysql_database}.orders', f'{mysql_database}.order_lines',
        ]
        assert stream['sinks'][0]['config']['mirror'] == dict(
            key=dict(orders=['id'], order_lines=['order_id', 'line']), deletes='delete',
        )
        assert source_path.read_text().count('tables:') == 1
    finally:
        Pipeline.get(pipeline_uuid, repo_path=mage_project).delete()
