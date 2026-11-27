"""
Streams into Apache Iceberg tables through a REST catalog (ADR-0042), with the tables in
MinIO: an exported service creates a partitioned table from its first messages and appends
every message, read back with pyiceberg; with exactly-once delivery the engine is killed
three times and the table ends with each message once and the source's positions in its
properties.
"""
import json
import os
import textwrap
import urllib.request
import uuid
from pathlib import Path

import pytest
from kafka.admin import KafkaAdminClient, NewTopic

from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)
from integration_tests.kafka.test_broker_restart import broker  # noqa: F401 (fixture)
from integration_tests.kafka.test_exported_stream import produce
from integration_tests.kafka.test_stream_conformance import wait_for


@pytest.fixture(scope='module')
def iceberg_uri(s3):
    uri = os.getenv('MAGE_TEST_ICEBERG_URI', 'http://127.0.0.1:18181')
    try:
        urllib.request.urlopen(f'{uri}/v1/config', timeout=3).read()
    except OSError:
        pytest.skip(f'No Iceberg REST catalog at {uri}')
    try:
        s3.create_bucket(Bucket='iceberg')
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    return uri


@pytest.fixture
def iceberg_env(iceberg_uri, s3_settings):
    """What the service and pyiceberg reach the catalog and MinIO with."""
    return dict(
        ICEBERG_URI=iceberg_uri,
        ICEBERG_S3_ENDPOINT=s3_settings['endpoint_url'],
        ICEBERG_S3_KEY=s3_settings['aws_access_key_id'],
        ICEBERG_S3_SECRET=s3_settings['aws_secret_access_key'],
    )


def catalog(env):
    from pyiceberg.catalog import load_catalog

    return load_catalog('it', **{
        'type': 'rest',
        'uri': env['ICEBERG_URI'],
        's3.endpoint': env['ICEBERG_S3_ENDPOINT'],
        's3.access-key-id': env['ICEBERG_S3_KEY'],
        's3.secret-access-key': env['ICEBERG_S3_SECRET'],
        's3.region': 'us-east-1',
        's3.path-style-access': 'true',
    })


def make_pipeline(project, topic, group, table, delivery='at_least_once'):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'ice_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=project)

    def add(suffix, kind, code, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, project, language='yaml')
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', f'''
        connector_type: kafka
        bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
        topic: {topic}
        consumer_group: {group}
        auto_offset_reset: earliest
        batch_size: 500
        timeout_ms: 200
        delivery: {delivery}
    ''')
    add('lake', 'data_exporter', f'''
        connector_type: iceberg
        catalog:
          type: rest
          uri: "{{{{ env_var('ICEBERG_URI') }}}}"
          s3.endpoint: "{{{{ env_var('ICEBERG_S3_ENDPOINT') }}}}"
          s3.access-key-id: "{{{{ env_var('ICEBERG_S3_KEY') }}}}"
          s3.secret-access-key: "{{{{ env_var('ICEBERG_S3_SECRET') }}}}"
          s3.region: us-east-1
          s3.path-style-access: "true"
        table: {table}
        create_table: true
        partition_by: [bucket(4, id)]
    ''', [source])
    return pipeline


def ids_in(env, table):
    from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

    try:
        loaded = catalog(env).load_table(table)
    except (NoSuchTableError, NoSuchNamespaceError):
        return []
    return loaded.scan(selected_fields=('id',)).to_arrow().column('id').to_pylist()


def topic_with_messages(bootstrap, total, partitions=2):
    topic = f'it_ice_{uuid.uuid4().hex[:10]}'
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
    admin.close()
    produce(bootstrap, topic, range(total))
    return topic


def test_a_stream_creates_a_partitioned_table_and_appends_every_message(
    kafka_bootstrap, iceberg_env, mage_project, tmp_path,
):
    total = 5000
    topic = topic_with_messages(kafka_bootstrap, total)
    table = f'it.events_{uuid.uuid4().hex[:8]}'
    pipeline = make_pipeline(mage_project, topic, f'g_{uuid.uuid4().hex[:8]}', table)
    service = None
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        manifest = json.loads((directory / 'service.json').read_text())
        assert manifest['pipelines'][0]['stream']['sinks'][0]['kind'] == 'iceberg'
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data',
                                 KAFKA_BOOTSTRAP=kafka_bootstrap, **iceberg_env)
        service.start()
        wait_for(lambda: len(ids_in(iceberg_env, table)) >= total, timeout=240)

        ids = ids_in(iceberg_env, table)
        assert sorted(ids) == list(range(total))
        loaded = catalog(iceberg_env).load_table(table)
        assert [f.name for f in loaded.schema().fields] == ['id', 'amount']
        assert [f.name for f in loaded.spec().fields] == ['id_bucket']
        streams = {s.summary.additional_properties.get('mageml.stream')
                   for s in loaded.snapshots()}
        assert streams == {f'{pipeline.uuid}.{pipeline.uuid}_lake'}, streams
    except AssertionError:
        if service and service.log.is_file():
            print(service.log.read_text()[-6000:])
        raise
    finally:
        if service and service.process and service.process.poll() is None:
            service.stop()
        pipeline.delete()


def positions_kept(env, table, stream):
    """The sum of the next offsets the table keeps for the stream's partitions."""
    from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

    try:
        properties = catalog(env).load_table(table).properties
    except (NoSuchTableError, NoSuchNamespaceError):
        return 0
    prefix = f'mageml.stream.{stream}.position.'
    return sum(int(v) for k, v in properties.items() if k.startswith(prefix))


def test_exactly_once_keeps_each_message_once_through_killed_engines(
    broker, iceberg_env, mage_project, tmp_path,  # noqa: F811
):
    total, kills = 20000, 3
    _, bootstrap = broker
    topic = topic_with_messages(bootstrap, total)
    table = f'it.exact_{uuid.uuid4().hex[:8]}'
    pipeline = make_pipeline(mage_project, topic, f'g_{uuid.uuid4().hex[:8]}', table,
                             delivery='exactly_once')
    service = None
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        manifest = json.loads((directory / 'service.json').read_text())
        assert manifest['pipelines'][0]['stream']['delivery'] == 'exactly_once'
        binary = stream_service_binary()
        reached = 0
        for kill in range(kills):
            service = RunningService(binary, directory, tmp_path / 'data',
                                     KAFKA_BOOTSTRAP=bootstrap, **iceberg_env)
            service.start()
            target = min(total - 1, reached + 2000)
            wait_for(
                lambda target=target: positions_kept(iceberg_env, table, pipeline.uuid)
                >= target,
                timeout=180,
            )
            reached = positions_kept(iceberg_env, table, pipeline.uuid)
            service.process.kill()
            service.process.wait()
            print(f'killed {kill + 1}: {reached} of {total} kept')
        service = RunningService(binary, directory, tmp_path / 'data',
                                 KAFKA_BOOTSTRAP=bootstrap, **iceberg_env)
        service.start()
        wait_for(lambda: positions_kept(iceberg_env, table, pipeline.uuid) == total,
                 timeout=300)

        ids = ids_in(iceberg_env, table)
        assert reached < total, 'the last kill came after the stream had kept all'
        assert sorted(set(ids)) == list(range(total)), 'a message was lost'
        assert len(ids) == total, f'{len(ids) - total} messages written twice'
    except AssertionError:
        if service and service.log.is_file():
            print(service.log.read_text()[-6000:])
        raise
    finally:
        if service and service.process and service.process.poll() is None:
            service.stop()
        pipeline.delete()
