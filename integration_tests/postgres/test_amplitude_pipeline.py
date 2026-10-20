"""
An Amplitude to PostgreSQL data integration pipeline, the kind the editor creates, run by
Mage twice against a local server that answers as Amplitude's APIs do: the events, the
daily metrics and the tracking plan arrive with their types, and the second run adds what
is new without repeating a row.
"""
import datetime as dt
import json
import textwrap
import uuid

from integration_tests.mage_runner import run_integration_pipeline
from integration_tests.postgres.test_sql_block_matrix import query
from mage_ai.tests.io.fixtures_amplitude import (
    API_KEY,
    SECRET_KEY,
    FakeAmplitude,
    event,
)

DAY = dt.datetime(2024, 6, 1)
STREAMS = ('events', 'active_users', 'event_types')


def events(first_hour, hours, per_hour=3, day=DAY):
    return [
        event(hour * 100 + n, day + dt.timedelta(hours=hour, minutes=n))
        for hour in range(first_hour, first_hour + hours) for n in range(per_hour)
    ]


def build(project, fake_url, postgres_settings, schema):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_integrations.sources.amplitude import Amplitude

    name = f'amplitude_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.INTEGRATION, repo_path=project)
    catalog = Amplitude(config=dict(api_key='a', secret_key='b')).discover(streams=STREAMS)
    streams = []
    for stream in catalog.to_dict()['streams']:
        for entry in stream['metadata']:
            entry['metadata']['selected'] = True
        stream['destination_table'] = f"amplitude_{stream['tap_stream_id']}"
        streams.append(stream)
    with open(pipeline.catalog_config_path, 'w') as file:
        json.dump(dict(catalog=dict(streams=streams)), file, default=list)

    source = Block.create(f'{name}_source', 'data_loader', project, language='yaml')
    with open(source.file_path, 'w') as file:
        # window_hours: the range up to today in few requests to the local server.
        file.write(textwrap.dedent(f'''\
            source: amplitude
            config:
              api_key: {API_KEY}
              secret_key: {SECRET_KEY}
              region: US
              host: {fake_url}
              start_date: '2024-06-01'
              window_hours: 2400
              availability_delay_hours: 10
        '''))
    destination = Block.create(f'{name}_destination', 'data_exporter', project, language='yaml')
    with open(destination.file_path, 'w') as file:
        file.write(textwrap.dedent(f'''\
            destination: postgresql
            config:
              database: {postgres_settings['dbname']}
              host: {postgres_settings['host']}
              port: {postgres_settings['port']}
              username: {postgres_settings['user']}
              password: {postgres_settings['password']}
              schema: {schema}
        '''))
    pipeline.add_block(source)
    pipeline.add_block(destination, upstream_block_uuids=[source.uuid])
    return pipeline.uuid, source.file_path


def test_amplitude_to_postgres_runs_twice_without_repeats(
    mage_project, pg, profile_schema, postgres_settings,
):
    with FakeAmplitude(events(0, 24)) as fake:
        # The first run reads up to 11 hours ago; the second, with Amplitude's usual delay,
        # up to 3 hours ago, as if eight hours had passed.
        pipeline, source_path = build(mage_project, fake.url, postgres_settings, profile_schema)
        run_integration_pipeline(pipeline)

        table = f'{profile_schema}.amplitude_events'
        assert query(pg, f'SELECT count(*), count(DISTINCT uuid) FROM {table}') == [(72, 72)]
        types = dict(query(
            pg,
            'SELECT column_name, data_type FROM information_schema.columns '
            'WHERE table_schema = %s AND table_name = %s', (profile_schema, 'amplitude_events'),
        ))
        assert types['event_properties'] == 'jsonb', types
        assert types['amplitude_id'] == 'bigint', types
        assert query(pg, f"SELECT event_properties->>'song' FROM {table} WHERE event_id = 0") \
            == [('song 0',)]
        assert query(
            pg, f"SELECT event_time FROM {table} WHERE event_id = 2301",
        )[0][0] == dt.datetime(2024, 6, 1, 23, 1, tzinfo=dt.timezone.utc)
        assert query(pg, f'SELECT count(*) FROM {profile_schema}.amplitude_event_types') == [(1,)]
        metrics = query(pg, f'SELECT count(*) FROM {profile_schema}.amplitude_active_users')[0][0]
        assert metrics > 300

        # Events Amplitude received since, then a second run: only those are added, and
        # the metrics read again replace their rows.
        recent = dt.datetime.now(dt.timezone.utc).replace(
            minute=0, second=0, microsecond=0, tzinfo=None,
        ) - dt.timedelta(hours=6)
        fake.events += events(24, 2, day=recent - dt.timedelta(hours=24))
        with open(source_path) as file:
            text = file.read()
        with open(source_path, 'w') as file:
            file.write(text.replace('availability_delay_hours: 10', 'availability_delay_hours: 2'))
        run_integration_pipeline(pipeline)
        assert query(pg, f'SELECT count(*), count(DISTINCT uuid) FROM {table}') == [(78, 78)]
        assert query(pg, f'SELECT count(*) FROM {profile_schema}.amplitude_active_users')[0][0] \
            == metrics
        exports = [r for r in fake.requests if r.startswith('/api/2/export')]
        # The second run starts where the first ended, not at start_date.
        assert 'start=2024' not in exports[-1], exports[-3:]
