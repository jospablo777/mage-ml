"""
Guided ingestion (ADR-0045) against a real server: the source's checks and tables with
their keys, a destination that can be written, a sample, and the pipeline it creates, which
keeps the passwords as secrets and exports as a mirror with an initial copy.
"""
import json

from mage_ai.data_preparation import ingestion


def connection(postgres_settings, **extra):
    return dict(
        host=postgres_settings['host'], port=postgres_settings['port'],
        database=postgres_settings['dbname'], user=postgres_settings['user'],
        password=postgres_settings['password'], sslmode='disable', **extra,
    )


def test_checks_tables_sample_and_the_pipeline_it_creates(
    mage_project, postgres_settings, pg, schema, tmp_path, monkeypatch, request,
):
    from mage_ai.pipeline_services import export

    # The secrets' key goes in a data folder of the test's, not the checkout.
    monkeypatch.setenv('MAGE_DATA_DIR', str(tmp_path / 'mage_data'))

    with pg.cursor() as cursor:
        cursor.execute(f'''
            CREATE TABLE {schema}.orders (id integer PRIMARY KEY, item text);
            CREATE TABLE {schema}.lines (order_id integer, line integer, qty integer,
                                         PRIMARY KEY (order_id, line));
            CREATE TABLE {schema}.notes (body text);
            CREATE TABLE {schema}.audit (id integer PRIMARY KEY);
            ALTER TABLE {schema}.audit REPLICA IDENTITY NOTHING;
            CREATE SCHEMA {schema}_other;
            CREATE TABLE {schema}_other.orders (id integer PRIMARY KEY);
            INSERT INTO {schema}.orders VALUES (1, 'tea'), (2, 'cake');
        ''')
    pg.commit()

    def drop_other():
        pg.rollback()
        with pg.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA IF EXISTS {schema}_other CASCADE')
        pg.commit()

    request.addfinalizer(drop_other)
    source = connection(postgres_settings)

    found = ingestion.check_source(source)
    assert found['ready'], found['checks']
    tables = {t['table']: t for t in found['tables'] if t['table'].startswith(schema)}
    assert tables[f'{schema}.orders']['key'] == ['id']
    assert tables[f'{schema}.lines']['key'] == ['order_id', 'line']
    assert tables[f'{schema}.notes']['key'] == []
    assert tables[f'{schema}.notes']['blocked'].startswith('no primary key')
    assert tables[f'{schema}.orders']['blocked'] is None
    # Published, a table without a replica identity fails its updates on the source.
    assert 'REPLICA IDENTITY NOTHING' in tables[f'{schema}.audit']['blocked']

    assert ingestion.check_destination(source, f'{schema}_copy')['ready']
    rows = ingestion.sample(source, f'{schema}.orders')
    assert rows['columns'] == ['id', 'item'] and sorted(rows['rows']) == [['1', 'tea'],
                                                                          ['2', 'cake']]

    keyed = [tables[f'{schema}.orders'], tables[f'{schema}.lines']]
    try:
        ingestion.create(mage_project, 'nope', source, source, [tables[f'{schema}.notes']])
        raise AssertionError('a table without a key was accepted')
    except ingestion.IngestionError as error:
        assert 'has no key' in str(error)

    # The request's keys are not trusted: the source's identity refuses the table.
    for chosen, message in [
        ([dict(tables[f'{schema}.audit'], blocked=None)], 'REPLICA IDENTITY NOTHING'),
        ([dict(table=f'{schema}.missing', key=['id'])], 'is not a table of the source'),
        ([tables[f'{schema}.orders'], dict(table=f'{schema}_other.orders', key=['id'])],
         'would both be written to the table orders'),
    ]:
        try:
            ingestion.create(mage_project, 'nope', source, source, chosen)
            raise AssertionError(f'{chosen} was accepted')
        except ingestion.IngestionError as error:
            assert message in str(error), error

    created = ingestion.create(
        mage_project, f'sync_{schema}', source, source, keyed, deletes='soft',
        schema=f'{schema}_copy', prefix='copy_',
    )
    assert any('_mage_deleted_at' in line for line in created['review'])
    assert created['blocks'] == [
        dict(uuid=f'sync_{schema}_source', type='data_loader'),
        dict(uuid=f'sync_{schema}_copy', type='data_exporter'),
    ]
    from mage_ai.data_preparation.models.pipeline import Pipeline

    pipeline = Pipeline.get(created['pipeline_uuid'], repo_path=mage_project)
    try:
        files = ''.join(
            open(block.file_path).read() for block in pipeline.blocks_by_uuid.values()
        )
        passwords = [line for line in files.splitlines() if line.startswith('password:')]
        assert len(passwords) == 2 and all('mage_secret_var' in p for p in passwords), passwords
        captured = export.capture(mage_project, [pipeline.uuid])
        stream = captured.manifest['pipelines'][0]['stream']
        assert stream['source']['config']['snapshot'] == 'initial'
        sink = stream['sinks'][0]['config']
        assert sink['table'] == 'copy_{_mage_table}'
        assert sink['mirror'] == dict(key=dict(orders=['id'], lines=['order_id', 'line']),
                                      deletes='soft')
        assert json.dumps(sink['password']).count('$parts') == 1
    finally:
        pipeline.delete()
