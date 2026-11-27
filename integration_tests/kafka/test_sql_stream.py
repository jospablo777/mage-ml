"""
A SQL block in an exported service: 1,000 orders read from Kafka, the query keeps those of
50 or more, doubles the amount and upper-cases the country, in the engine, without Python.
The stream is bounded (stop_at: end), so it finishes.
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
from integration_tests.kafka.test_bounded_stream import finished_stream
from integration_tests.kafka.test_exactly_once import read_committed
from integration_tests.kafka.test_stream_conformance import wait_for

ORDERS = 1000
QUERY = '''
SELECT id, amount * 2 AS doubled, upper(country) AS country
FROM {{ df_1 }}
WHERE amount >= 50;
'''


def make_pipeline(project, topics, group):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'sql_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=project)

    def add(suffix, kind, code, language, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, project, language=language)
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', f'''
        connector_type: kafka
        bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
        topic: {topics[0]}
        consumer_group: {group}
        auto_offset_reset: earliest
        batch_size: 100
        timeout_ms: 200
        stop_at: end
    ''', 'yaml')
    picked = add('pick', 'transformer', QUERY, 'sql', [source])
    add('out', 'data_exporter', f'''
        connector_type: kafka
        bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
        topic: {topics[1]}
    ''', 'yaml', [picked])
    return pipeline


def test_a_sql_block_filters_and_computes_each_batch_in_the_engine(
    kafka_bootstrap, mage_project, tmp_path,
):
    from kafka import KafkaProducer

    topics = [f'it_sql_in_{uuid.uuid4().hex[:8]}', f'it_sql_out_{uuid.uuid4().hex[:8]}']
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(t, num_partitions=2, replication_factor=1) for t in topics])
    admin.close()
    pipeline = make_pipeline(mage_project, topics, f'g_{uuid.uuid4().hex[:8]}')
    service = None
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        manifest = json.loads((directory / 'service.json').read_text())
        [stream] = [p for p in manifest['pipelines'] if p.get('stream')]
        assert [b['language'] for b in stream['blocks']] == ['sql']

        producer = KafkaProducer(bootstrap_servers=kafka_bootstrap,
                                 value_serializer=lambda v: json.dumps(v).encode())
        for i in range(ORDERS):
            producer.send(topics[0], dict(id=i, amount=i % 100,
                                          country='cr' if i % 2 else 'us'))
        producer.flush()
        producer.close()

        service = RunningService(stream_service_binary(), directory, tmp_path / 'data',
                                 KAFKA_BOOTSTRAP=kafka_bootstrap)
        service.start()
        wait_for(lambda: finished_stream(service), timeout=180)
        service.stop()

        out = {m['id']: m for m in read_committed(kafka_bootstrap, topics[1])}
        assert sorted(out) == [i for i in range(ORDERS) if i % 100 >= 50]
        assert all(m['doubled'] == (i % 100) * 2 for i, m in out.items())
        assert {m['country'] for m in out.values()} == {'CR', 'US'}
        assert list(out[51]) == ['id', 'doubled', 'country'], 'fields in the query\'s order'
    except AssertionError:
        if service and service.log.is_file():
            print(service.log.read_text()[-6000:])
        raise
    finally:
        if service and service.process and service.process.poll() is None:
            service.stop()
        pipeline.delete()
