"""
A stream on the local engine from the editor (C2): started with its sinks named, it reads
with a development consumer group, reports rates and lag, pauses, resumes and stops; the
production group never moves.
"""
import json
import textwrap
import time
import uuid
from pathlib import Path

import pytest
from kafka import KafkaAdminClient, KafkaProducer


@pytest.fixture
def out_topic(kafka_bootstrap):
    from kafka.admin import NewTopic

    name = f'it_out_{uuid.uuid4().hex[:10]}'
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1)])
    yield name
    admin.delete_topics([name])
    admin.close()


def produce(bootstrap, topic, start, count):
    producer = KafkaProducer(bootstrap_servers=bootstrap)
    for i in range(start, start + count):
        producer.send(topic, json.dumps(dict(n=i)).encode())
    producer.flush()
    producer.close()


def wait_for(condition, timeout=90, every=0.5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        found = condition()
        if found:
            return found
        time.sleep(every)
    raise AssertionError('timed out')


def test_a_stream_runs_pauses_and_stops_on_the_local_engine(
    mage_project, kafka_bootstrap, kafka_topic, out_topic, monkeypatch,
):
    from integration_tests.exported_services import stream_service_binary
    from integration_tests.streaming.test_pipelines import read_topic
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.streaming import dev_run

    monkeypatch.setenv('MAGE_ENGINE_PATH', str(stream_service_binary()))
    group = f'it_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(f'dev_run_{uuid.uuid4().hex[:8]}',
                               pipeline_type=PipelineType.STREAMING, repo_path=mage_project)

    def add(name, kind, content, upstream=(), language='yaml'):
        block = Block.create(f'{pipeline.uuid}_{name}', kind, mage_project,
                             language=language, pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(content))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', f'''
        connector_type: kafka
        bootstrap_server: {kafka_bootstrap}
        topic: {kafka_topic}
        consumer_group: {group}
        auto_offset_reset: earliest
    ''')
    double = add('double', 'transformer', '''
        @transformer
        def double(messages, *args, **kwargs):
            return [dict(m, twice=m['n'] * 2) for m in messages]
    ''', upstream=[source], language='python')
    sink = add('sink', 'data_exporter', f'''
        connector_type: kafka
        bootstrap_server: {kafka_bootstrap}
        topic: {out_topic}
    ''', upstream=[double])
    try:
        produce(kafka_bootstrap, kafka_topic, 0, 10)
        with pytest.raises(dev_run.DevRunError, match='Name them to start it'):
            dev_run.start(mage_project, pipeline.uuid, [], user='ada')

        started = dev_run.start(mage_project, pipeline.uuid, [sink.uuid], user='ada')

        assert started['running'] is True
        assert any(f'{group}-dev-ada' in note for note in started['notes'])
        values = wait_for(lambda: read_topic(kafka_bootstrap, out_topic, 10))
        assert sorted(v['twice'] for v in values) == [i * 2 for i in range(10)]
        status = wait_for(lambda: (lambda s: s if s.get('messages_in') == 10 else None)(
            dev_run.status(mage_project, pipeline.uuid)))
        assert status['state'] == 'running'
        assert len(status['rate_in']) >= 1
        # Each block's numbers: the transformer saw the 10 messages and kept them.
        [block] = [b for b in status['blocks'] if b['block'] == double.uuid]
        assert (block['messages_in'], block['messages_out']) == (10, 10)
        assert block['batches'] >= 1 and block['p95_ms'] >= block['p50_ms'] >= 0
        assert any('serves' in line or 'stream' in line for line in status['logs'])

        paused = dev_run.control(mage_project, pipeline.uuid, 'pause')
        assert paused['paused'] is True
        produce(kafka_bootstrap, kafka_topic, 10, 5)
        time.sleep(3)
        assert dev_run.status(mage_project, pipeline.uuid)['messages_in'] == 10
        assert dev_run.control(mage_project, pipeline.uuid, 'resume')['paused'] is False
        wait_for(lambda: dev_run.status(mage_project, pipeline.uuid)['messages_in'] == 15)

        stopped = dev_run.stop(mage_project, pipeline.uuid)
        assert (stopped['running'], stopped['state']) == (False, 'stopped')

        # Only the development group moved.
        admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
        assert admin.list_consumer_group_offsets(group) == {}
        assert admin.list_consumer_group_offsets(f'{group}-dev-ada')
        admin.close()
    finally:
        dev_run.stop(mage_project, pipeline.uuid)
        pipeline.delete()
