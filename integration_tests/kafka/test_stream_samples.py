"""
Capturing a stream sample from Kafka 4.1 for the editor: the newest or the first messages,
read without a consumer group, so the position a running stream reads from never moves.
"""
import json
import textwrap
import uuid
from pathlib import Path

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient

from mage_ai.streaming import samples


def test_a_capture_reads_the_newest_or_first_messages_and_commits_nothing(
    mage_project, kafka_bootstrap, kafka_topic,
):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    group = f'sample_group_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(f'sample_stream_{uuid.uuid4().hex[:8]}',
                               pipeline_type=PipelineType.STREAMING, repo_path=mage_project)
    try:
        block = Block.create(f'{pipeline.uuid}_source', 'data_loader', mage_project,
                             language='yaml', pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(f'''
            connector_type: kafka
            bootstrap_server: {kafka_bootstrap}
            topic: {kafka_topic}
            consumer_group: {group}
        '''))
        pipeline.add_block(block)
        producer = KafkaProducer(bootstrap_servers=kafka_bootstrap)
        for i in range(40):
            producer.send(kafka_topic, json.dumps({'id': i}).encode(), partition=0)
        producer.send(kafka_topic, b'not json', partition=0)
        producer.flush()

        newest = samples.capture_sample(mage_project, pipeline.uuid, 'newest', count=5)
        first = samples.capture_sample(mage_project, pipeline.uuid, 'first', count=5,
                                       start='earliest')

        assert newest['message_count'] == 5
        assert samples.read_sample(mage_project, pipeline.uuid, 'first') == [
            {'id': i} for i in range(5)
        ]
        # The newest come from the end of each partition; text that is not JSON stays text.
        assert 'not json' in samples.read_sample(mage_project, pipeline.uuid, 'newest')
        assert first['partitions'] == {f'{kafka_topic}[0]': 5}
        groups = [g[0] for g in KafkaAdminClient(
            bootstrap_servers=kafka_bootstrap).list_consumer_groups()]
        assert group not in groups
    finally:
        pipeline.delete()


def test_a_capture_redacts_secret_fields(mage_project, kafka_bootstrap, kafka_topic):
    """Captured samples can be committed; fields marked secret never reach them."""
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    pipeline = Pipeline.create(f'redact_stream_{uuid.uuid4().hex[:8]}',
                               pipeline_type=PipelineType.STREAMING, repo_path=mage_project)
    try:
        block = Block.create(f'{pipeline.uuid}_source', 'data_loader', mage_project,
                             language='yaml', pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(f'''
            connector_type: kafka
            bootstrap_server: {kafka_bootstrap}
            topic: {kafka_topic}
            consumer_group: redact
            secret_fields: [customer.email]
        '''))
        pipeline.add_block(block)
        producer = KafkaProducer(bootstrap_servers=kafka_bootstrap)
        for i in range(3):
            producer.send(kafka_topic, json.dumps({
                'id': i, 'customer': {'email': f'c{i}@example.com', 'country': 'CR'},
                'api_key': 'k-123',
            }).encode(), partition=0)
        producer.flush()
        producer.close()

        summary = samples.capture_sample(mage_project, pipeline.uuid, 'redacted', count=3,
                                         start='earliest')

        assert summary['redacted'] == {'customer.email': 3, 'api_key': 3}
        text = Path(samples.samples_dir(mage_project, pipeline.uuid), 'redacted.jsonl') \
            .read_text()
        assert 'example.com' not in text and 'k-123' not in text
        first = json.loads(text.splitlines()[0])
        assert first == {'id': 0, 'customer': {'email': '<redacted>', 'country': 'CR'},
                         'api_key': '<redacted>'}
    finally:
        pipeline.delete()
