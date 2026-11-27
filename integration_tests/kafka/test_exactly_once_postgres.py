"""
Kafka to PostgreSQL exactly once (delivery: exactly_once, EXACTLY_ONCE_POSTGRES.md): the
engine is killed (SIGKILL) three times while it streams 50,000 messages into a table without
a unique constraint, so a message written twice would be a second row. The table ends with
each message's row once, and mageml_stream_positions with every partition's next offset at
its end. The test runs its own broker.
"""
import json
import textwrap
import uuid
from pathlib import Path

from kafka.admin import KafkaAdminClient, NewTopic

from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)
from integration_tests.kafka.test_broker_restart import broker  # noqa: F401 (fixture)
from integration_tests.kafka.test_exported_stream import produce
from integration_tests.kafka.test_stream_conformance import wait_for

TOTAL = 50000
KILLS = 3


def make_pipeline(project, topic, group, schema):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'eo_pg_{uuid.uuid4().hex[:8]}'
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
        batch_size: 100
        timeout_ms: 200
        delivery: exactly_once
    ''')
    add('rows', 'data_exporter', f'''
        connector_type: postgres
        database: "{{{{ env_var('MAGE_TEST_POSTGRES_DBNAME') }}}}"
        host: "{{{{ env_var('MAGE_TEST_POSTGRES_HOST') }}}}"
        password: "{{{{ env_var('MAGE_TEST_POSTGRES_PASSWORD') }}}}"
        port: "{{{{ env_var('MAGE_TEST_POSTGRES_PORT') }}}}"
        username: "{{{{ env_var('MAGE_TEST_POSTGRES_USER') }}}}"
        schema: {schema}
        table: events
    ''', [source])
    return pipeline


def kept(pg, schema, stream):
    """The sum of the next offsets kept for the stream's partitions: 0 before any commit."""
    with pg.cursor() as cursor:
        cursor.execute('SELECT to_regclass(%s)', (f'{schema}.mageml_stream_positions',))
        if cursor.fetchone()[0] is None:
            pg.rollback()
            return 0
        cursor.execute(
            f'SELECT coalesce(sum(next_offset), 0) FROM {schema}.mageml_stream_positions '
            'WHERE stream = %s', (stream,),
        )
        found = int(cursor.fetchone()[0])
    pg.rollback()
    return found


def test_each_message_is_one_row_through_killed_engines(
    broker, mage_project, pg, schema, tmp_path,  # noqa: F811
):
    _, bootstrap = broker
    topic = f'it_eo_pg_{uuid.uuid4().hex[:10]}'
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=1)])
    admin.close()
    pipeline = make_pipeline(mage_project, topic, f'g_{uuid.uuid4().hex[:8]}', schema)
    service = None
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        manifest = json.loads((directory / 'service.json').read_text())
        assert manifest['pipelines'][0]['stream']['delivery'] == 'exactly_once'
        produce(bootstrap, topic, range(TOTAL))
        binary = stream_service_binary()
        reached = 0
        for kill in range(KILLS):
            service = RunningService(binary, directory, tmp_path / 'data',
                                     KAFKA_BOOTSTRAP=bootstrap)
            service.start()
            # Killed once more has committed: mid-stream, with a transaction open.
            target = min(TOTAL - 1, reached + 3000)
            wait_for(lambda target=target: kept(pg, schema, pipeline.uuid) >= target,
                     timeout=180)
            reached = kept(pg, schema, pipeline.uuid)
            service.process.kill()
            service.process.wait()
            print(f'killed {kill + 1}: {reached} of {TOTAL} kept')
        service = RunningService(binary, directory, tmp_path / 'data',
                                 KAFKA_BOOTSTRAP=bootstrap)
        service.start()
        wait_for(lambda: kept(pg, schema, pipeline.uuid) == TOTAL, timeout=300)

        with pg.cursor() as cursor:
            cursor.execute(f'SELECT count(*), count(DISTINCT id), min(id), max(id) '
                           f'FROM {schema}.events')
            rows, distinct, low, high = cursor.fetchone()
        pg.rollback()

        assert reached < TOTAL, 'the last kill came after the stream had kept all'
        assert (distinct, low, high) == (TOTAL, 0, TOTAL - 1), 'a message was lost'
        assert rows == TOTAL, f'{rows - TOTAL} messages written twice'
    except AssertionError:
        if service and service.log.is_file():
            print(service.log.read_text()[-6000:])
        raise
    finally:
        if service and service.process and service.process.poll() is None:
            service.stop()
        pipeline.delete()
