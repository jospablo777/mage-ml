"""
A stream's dead letters from the editor (C3): read from the Kafka dead-letter sink with their
errors, opened as a sample that reproduces the failure, and replayed to the source once the
block is fixed.
"""
import json
import textwrap
import time
import uuid
from pathlib import Path

import pytest
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic


@pytest.fixture
def topics(kafka_bootstrap):
    names = {kind: f'it_{kind}_{uuid.uuid4().hex[:8]}' for kind in ('in', 'out', 'dead')}
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(n, num_partitions=1, replication_factor=1)
                         for n in names.values()])
    yield names
    admin.delete_topics(list(names.values()))
    admin.close()


def wait_for(condition, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        found = condition()
        if found:
            return found
        time.sleep(0.5)
    raise AssertionError('timed out')


def test_dead_letters_are_read_reproduced_and_replayed(
    mage_project, kafka_bootstrap, topics, monkeypatch,
):
    from integration_tests.exported_services import stream_service_binary
    from integration_tests.streaming.test_pipelines import read_topic
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.streaming import dead_letters, dev_run, sample_runner, samples

    monkeypatch.setenv('MAGE_ENGINE_PATH', str(stream_service_binary()))
    pipeline = Pipeline.create(f'dead_letters_{uuid.uuid4().hex[:8]}',
                               pipeline_type=PipelineType.STREAMING, repo_path=mage_project)

    def add(name, kind, content, upstream=(), language='yaml'):
        block = Block.create(f'{pipeline.uuid}_{name}', kind, mage_project, language=language)
        Path(block.file_path).write_text(textwrap.dedent(content))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    def kafka(topic, extra=''):
        return f'connector_type: kafka\nbootstrap_server: {kafka_bootstrap}\ntopic: {topic}\n' \
            + extra

    source = add('source', 'data_loader', kafka(
        topics['in'], 'consumer_group: dl\nauto_offset_reset: earliest\n'))
    check = add('check', 'transformer', '''
        @transformer
        def check(messages, *args, **kwargs):
            for m in messages:
                if m['n'] == 3:
                    raise ValueError('three is not allowed')
            return messages
    ''', upstream=[source], language='python')
    sink = add('sink', 'data_exporter', kafka(topics['out']), upstream=[check])
    dead = add('dead', 'data_exporter', kafka(topics['dead'], 'dead_letter: true\n'),
               upstream=[check])
    try:
        producer = KafkaProducer(bootstrap_servers=kafka_bootstrap)
        for i in range(6):
            producer.send(topics['in'], json.dumps(dict(n=i)).encode())
        producer.flush()
        producer.close()
        dev_run.start(mage_project, pipeline.uuid, [sink.uuid, dead.uuid], user='ada')
        wait_for(lambda: read_topic(kafka_bootstrap, topics['out'], 5))

        found = wait_for(lambda: (lambda f: f if f['letters'] else None)(
            dead_letters.read_dead_letters(mage_project, pipeline.uuid)))

        [letter] = found['letters']
        assert letter['value'] == {'n': 3}
        assert 'three is not allowed' in letter['error']
        assert found['source_topic'] == topics['in']

        sample = dead_letters.open_as_sample(mage_project, pipeline.uuid, 'failed')
        assert sample['message_count'] == 1
        result = sample_runner.run_on_sample(
            mage_project, pipeline.uuid, check.uuid,
            samples.read_sample(mage_project, pipeline.uuid, 'failed'))
        assert 'three is not allowed' in result['error']['message']

        # The fix, then the replay, which names the topic it writes to.
        Path(check.file_path).write_text(textwrap.dedent('''
            @transformer
            def check(messages, *args, **kwargs):
                return messages
        '''))
        dev_run.stop(mage_project, pipeline.uuid)
        with pytest.raises(dead_letters.DeadLetterError, match='Type the source topic'):
            dead_letters.replay(mage_project, pipeline.uuid, 'orders')
        assert dead_letters.replay(mage_project, pipeline.uuid, topics['in'])['replayed'] == 1
        dev_run.start(mage_project, pipeline.uuid, [sink.uuid, dead.uuid], user='ada')

        values = wait_for(lambda: read_topic(kafka_bootstrap, topics['out'], 6))
        assert sorted(v['n'] for v in values) == [0, 1, 2, 3, 4, 5]
    finally:
        dev_run.stop(mage_project, pipeline.uuid)
        pipeline.delete()


def test_late_events_go_to_the_dead_letter_topic_with_the_watermark(
    mage_project, kafka_bootstrap, topics, monkeypatch,
):
    """Stateful streams T1: event time from a field, a watermark per partition, and late
    events out of the stream before the blocks, with the reason."""
    from integration_tests.exported_services import stream_service_binary
    from integration_tests.streaming.test_pipelines import read_topic
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.streaming import dead_letters, dev_run

    monkeypatch.setenv('MAGE_ENGINE_PATH', str(stream_service_binary()))
    pipeline = Pipeline.create(f'late_{uuid.uuid4().hex[:8]}',
                               pipeline_type=PipelineType.STREAMING, repo_path=mage_project)

    def add(name, kind, content, upstream=(), language='yaml'):
        block = Block.create(f'{pipeline.uuid}_{name}', kind, mage_project, language=language)
        Path(block.file_path).write_text(textwrap.dedent(content))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    def kafka(topic, extra=''):
        return f'connector_type: kafka\nbootstrap_server: {kafka_bootstrap}\ntopic: {topic}\n' \
            + extra

    source = add('source', 'data_loader', kafka(
        topics['in'], 'consumer_group: late\nauto_offset_reset: earliest\nbatch_size: 1\n'
        'event_time:\n  field: at\n  format: epoch_s\n  lateness_seconds: 10\n'))
    keep = add('keep', 'transformer', '''
        @transformer
        def keep(messages, *args, **kwargs):
            return messages
    ''', upstream=[source], language='python')
    sink = add('sink', 'data_exporter', kafka(topics['out']), upstream=[keep])
    dead = add('dead', 'data_exporter', kafka(topics['dead'], 'dead_letter: true\n'),
               upstream=[keep])
    try:
        producer = KafkaProducer(bootstrap_servers=kafka_bootstrap)
        # 1,000 then 1,020 moves the watermark to 1,010: 1,005 is late, 1,015 is not.
        for at in (1000, 1020, 1005, 1015):
            producer.send(topics['in'], json.dumps(dict(at=at)).encode())
        producer.flush()
        producer.close()
        dev_run.start(mage_project, pipeline.uuid, [sink.uuid, dead.uuid], user='ada')
        values = wait_for(lambda: read_topic(kafka_bootstrap, topics['out'], 3))
        assert sorted(v['at'] for v in values) == [1000, 1015, 1020]

        found = wait_for(lambda: (lambda f: f if f['letters'] else None)(
            dead_letters.read_dead_letters(mage_project, pipeline.uuid)))
        [letter] = found['letters']
        assert letter['value'] == {'at': 1005}
        assert "before partition 0's watermark 1970-01-01T00:16:50.000Z" in letter['error']

        status = dev_run.status(mage_project, pipeline.uuid)
        assert status['late'] == 1
        assert status['watermarks'] == {'0': '1970-01-01T00:16:50.000Z'}
    finally:
        dev_run.stop(mage_project, pipeline.uuid)
        pipeline.delete()
