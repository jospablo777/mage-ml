"""
Guided ingestion into ClickHouse (ADR-0049) against real servers: the destination's check
(a database that will be made), soft deletes refused, and the pipeline it creates from a
MySQL source, which exports as a ClickHouse mirror with its password kept as a secret.
"""
import uuid

from mage_ai.data_preparation import ingestion


def test_a_clickhouse_destination_is_checked_and_receives_a_mirror(
    mage_project, mysql_settings, mysql_database, my, clickhouse_settings, tmp_path,
    monkeypatch,
):
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.pipeline_services import export

    # The secrets' key goes in a data folder of the test's, not the checkout.
    monkeypatch.setenv('MAGE_DATA_DIR', str(tmp_path / 'mage_data'))
    with my.cursor() as cursor:
        cursor.execute('CREATE TABLE orders (id INT PRIMARY KEY, item VARCHAR(20))')
    source = dict(kind='mysql', host=mysql_settings['host'], port=mysql_settings['port'],
                  database=mysql_database, user=mysql_settings['user'],
                  password=mysql_settings['password'], sslmode='disable')
    database = f'it_{uuid.uuid4().hex[:12]}'
    destination = dict(kind='clickhouse', host=clickhouse_settings['host'],
                       port=clickhouse_settings['port'], user=clickhouse_settings['username'],
                       password=clickhouse_settings['password'], database=database,
                       sslmode='disable')

    found = ingestion.check_destination(destination, 'ignored')
    assert found['ready'], found['checks']
    details = {c['name']: c['detail'] for c in found['checks']}
    assert details['database'] == f'{database} does not exist; it will be made (CREATE DATABASE).'
    assert details['connect'].startswith('Connected to ClickHouse')

    tables = {t['table']: t for t in ingestion.check_source(source)['tables']}
    chosen = [tables[f'{mysql_database}.orders']]
    try:
        ingestion.create(mage_project, 'nope', source, destination, chosen, deletes='soft')
        raise AssertionError('soft deletes were accepted')
    except ingestion.IngestionError as error:
        assert 'A ClickHouse copy deletes the rows deleted in the source' in str(error)

    created = ingestion.create(mage_project, f'ch_{mysql_database}', source, destination,
                               chosen, prefix='copy_')
    assert any('ReplacingMergeTree' in line for line in created['review'])
    assert any('FINAL' in line for line in created['review'])
    pipeline = Pipeline.get(created['pipeline_uuid'], repo_path=mage_project)
    try:
        files = ''.join(open(b.file_path).read() for b in pipeline.blocks_by_uuid.values())
        assert clickhouse_settings['password'] not in files
        captured = export.capture(mage_project, [pipeline.uuid])
        sink = captured.manifest['pipelines'][0]['stream']['sinks'][0]
        assert sink['kind'] == 'clickhouse'
        assert sink['config']['database'] == database
        assert sink['config']['table'] == 'copy_{_mage_table}'
        assert sink['config']['secure'] is False
        assert sink['config']['mirror'] == dict(key=dict(orders=['id']), deletes='delete')
        assert '$parts' in str(sink['config']['password'])
    finally:
        pipeline.delete()
