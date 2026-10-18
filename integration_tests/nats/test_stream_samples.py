"""
Capturing a stream sample from NATS JetStream for the editor: the newest or the first
messages, read by an ephemeral consumer that acknowledges nothing, so a durable consumer's
position never moves.
"""
import textwrap
import uuid
from pathlib import Path

import pytest

from integration_tests.nats.test_streaming import consumer_state, jetstream, publish
from mage_ai.streaming import samples


@pytest.fixture
def nats_stream(nats_url):
    """A stream with one subject, deleted after the test."""
    name = f'it_{uuid.uuid4().hex[:12]}'
    subject = f'{name}.events'
    jetstream(nats_url, lambda js: js.add_stream(name=name, subjects=[subject]))
    yield name, subject
    jetstream(nats_url, lambda js: js.delete_stream(name))


def test_a_capture_reads_the_newest_or_first_messages_and_moves_no_consumer(
    mage_project, nats_url, nats_stream,
):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name, subject = nats_stream
    jetstream(nats_url, lambda js: js.add_consumer(name, durable_name='orders'))
    publish(nats_url, subject, *[{'n': i, 'password': 'p'} for i in range(30)], b'not json')
    pipeline = Pipeline.create(f'nats_sample_{uuid.uuid4().hex[:8]}',
                               pipeline_type=PipelineType.STREAMING, repo_path=mage_project)
    try:
        block = Block.create(f'{pipeline.uuid}_source', 'data_loader', mage_project,
                             language='yaml')
        Path(block.file_path).write_text(textwrap.dedent(f'''
            connector_type: nats
            server_url: {nats_url}
            stream_name: {name}
            subject: {subject}
            consumer_name: orders
        '''))
        pipeline.add_block(block)
        before = consumer_state(nats_url, name, 'orders')

        newest = samples.capture_sample(mage_project, pipeline.uuid, 'newest', count=5)
        first = samples.capture_sample(mage_project, pipeline.uuid, 'first', count=3,
                                       start='earliest')
        assert first['message_count'] == 3

        assert newest['message_count'] == 5
        assert newest['partitions'] == {subject: 5}
        assert newest['redacted'] == {'password': 4}
        newest_values = samples.read_sample(mage_project, pipeline.uuid, 'newest')
        assert newest_values[-1] == 'not json'
        assert [m['n'] for m in newest_values[:-1]] == [26, 27, 28, 29]
        assert [m['n'] for m in samples.read_sample(mage_project, pipeline.uuid, 'first')] \
            == [0, 1, 2]
        # The durable consumer still has every message to read.
        assert consumer_state(nats_url, name, 'orders') == before
    finally:
        pipeline.delete()
