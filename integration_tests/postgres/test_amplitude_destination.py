"""
A PostgreSQL to Amplitude data integration pipeline, run by Mage twice against a local
server that answers as Amplitude's Batch Event Upload API does: orders arrive as events
with their times and customers, scores as user properties, and the second run, with a
changed order, adds no event twice.
"""
import json
import textwrap
import uuid

from integration_tests.mage_runner import run_integration_pipeline
from mage_ai.tests.io.fixtures_amplitude import API_KEY, FakeAmplitude


def build(project, fake_url, postgres_settings, schema):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_integrations.sources.postgresql import PostgreSQL

    name = f'to_amplitude_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.INTEGRATION, repo_path=project)
    config = dict(
        database=postgres_settings['dbname'],
        host=postgres_settings['host'],
        port=int(postgres_settings['port']),
        username=postgres_settings['user'],
        password=postgres_settings['password'],
        schema=schema,
    )
    catalog = PostgreSQL(config=config).discover(streams=['orders', 'customer_scores'])
    streams = []
    for stream in catalog.to_dict()['streams']:
        for entry in stream['metadata']:
            entry['metadata']['selected'] = True
        stream['replication_method'] = 'FULL_TABLE'
        stream['key_properties'] = (
            ['order_id'] if stream['tap_stream_id'] == 'orders' else ['customer_id']
        )
        streams.append(stream)
    with open(pipeline.catalog_config_path, 'w') as file:
        json.dump(dict(catalog=dict(streams=streams)), file, default=list)

    source = Block.create(f'{name}_source', 'data_loader', project, language='yaml')
    with open(source.file_path, 'w') as file:
        file.write('source: postgresql\nconfig:\n' + textwrap.indent(
            ''.join(f'{key}: {value}\n' for key, value in config.items()), '  ',
        ))
    destination = Block.create(f'{name}_destination', 'data_exporter', project, language='yaml')
    with open(destination.file_path, 'w') as file:
        file.write(textwrap.dedent(f'''\
            destination: amplitude
            config:
              api_key: {API_KEY}
              host: {fake_url}
              streams:
                orders:
                  event_type: Order Completed
                  time: ordered_at
                  user_id: customer_id
                customer_scores:
                  user_properties: true
                  user_id: customer_id
        '''))
    pipeline.add_block(source)
    pipeline.add_block(destination, upstream_block_uuids=[source.uuid])
    return pipeline.uuid


def test_postgres_to_amplitude_sends_each_order_once(
    mage_project, pg, profile_schema, postgres_settings,
):
    with pg.cursor() as cursor:
        cursor.execute(f'''
            CREATE TABLE {profile_schema}.orders (
                order_id integer PRIMARY KEY,
                customer_id text NOT NULL,
                ordered_at timestamptz NOT NULL,
                amount numeric(10, 2),
                status text
            );
            CREATE TABLE {profile_schema}.customer_scores (
                customer_id text PRIMARY KEY,
                churn_risk double precision,
                segment text
            );
            INSERT INTO {profile_schema}.orders VALUES
                (1, 'customer-1', '2024-06-01 10:00+00', 19.50, 'paid'),
                (2, 'customer-2', '2024-06-01 11:30+00', 35.00, 'paid'),
                (3, 'customer-1', '2024-06-02 09:15+00', 9.50, 'paid');
            INSERT INTO {profile_schema}.customer_scores VALUES
                ('customer-1', 0.82, 'at risk'),
                ('customer-2', 0.05, NULL);
        ''')
    pg.commit()

    with FakeAmplitude() as fake:
        pipeline = build(mage_project, fake.url, postgres_settings, profile_schema)
        run_integration_pipeline(pipeline)

        orders = sorted(
            (e for e in fake.uploaded if e['event_type'] == 'Order Completed'),
            key=lambda e: e['event_properties']['order_id'],
        )
        assert [e['user_id'] for e in orders] == ['customer-1', 'customer-2', 'customer-1']
        assert orders[0]['time'] == 1717236000000
        assert orders[1]['event_properties']['status'] == 'paid'
        assert 'ordered_at' not in orders[0]['event_properties']
        assert fake.users['customer-1'] == {'churn_risk': 0.82, 'segment': 'at risk'}
        assert fake.users['customer-2'] == {'churn_risk': 0.05}

        # A refund changes an order; the next run sends every row again and Amplitude
        # keeps one event per order, by the insert_id of its key.
        with pg.cursor() as cursor:
            cursor.execute(
                f"UPDATE {profile_schema}.orders SET status = 'refunded' WHERE order_id = 2",
            )
        pg.commit()
        run_integration_pipeline(pipeline)
        assert len([e for e in fake.uploaded if e['event_type'] == 'Order Completed']) == 3
