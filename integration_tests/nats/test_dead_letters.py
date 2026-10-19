"""
A stream's dead letters on NATS JetStream (I-185): read from the dead-letter subject without
moving any consumer, opened as a sample that reproduces the failure, and replayed to the
source subject once the block is fixed.
"""
import textwrap
import time
import uuid
from pathlib import Path

import pytest

from integration_tests.nats.test_partition import count
from integration_tests.nats.test_streaming import jetstream, publish


@pytest.fixture
def streams(nats_url):
    names = {kind: f'it_{kind}_{uuid.uuid4().hex[:8]}' for kind in ('in', 'out', 'dead')}
    for name in names.values():
        jetstream(nats_url, lambda js, n=name: js.add_stream(name=n, subjects=[f'{n}.events']))
    yield names
    for name in names.values():
        jetstream(nats_url, lambda js, n=name: js.delete_stream(n))


def wait_for(condition, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        found = condition()
        if found:
            return found
        time.sleep(0.5)
    raise AssertionError('timed out')


def test_dead_letters_are_read_reproduced_and_replayed(
    mage_project, nats_url, streams, monkeypatch,
):
    from integration_tests.exported_services import stream_service_binary
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.streaming import dead_letters, dev_run, sample_runner, samples

    monkeypatch.setenv('MAGE_ENGINE_PATH', str(stream_service_binary()))
    pipeline = Pipeline.create(f'nats_dead_letters_{uuid.uuid4().hex[:8]}',
                               pipeline_type=PipelineType.STREAMING, repo_path=mage_project)

    def add(name, kind, content, upstream=(), language='yaml'):
        block = Block.create(f'{pipeline.uuid}_{name}', kind, mage_project, language=language)
        Path(block.file_path).write_text(textwrap.dedent(content))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    def sink(stream, extra=''):
        return f'connector_type: nats\nserver_url: {nats_url}\nsubject: {stream}.events\n' \
            + extra

    source_subject = f'{streams["in"]}.events'
    source = add('source', 'data_loader', f'''
        connector_type: nats
        server_url: {nats_url}
        stream_name: {streams["in"]}
        subject: {source_subject}
        consumer_name: dl
    ''')
    check = add('check', 'transformer', '''
        @transformer
        def check(messages, *args, **kwargs):
            for m in messages:
                if m['n'] == 3:
                    raise ValueError('three is not allowed')
            return messages
    ''', upstream=[source], language='python')
    out = add('out', 'data_exporter', sink(streams['out']), upstream=[check])
    dead = add('dead', 'data_exporter', sink(streams['dead'], 'dead_letter: true\n'),
               upstream=[check])
    try:
        publish(nats_url, source_subject, *[dict(n=i) for i in range(6)])
        dev_run.start(mage_project, pipeline.uuid, [out.uuid, dead.uuid], user='ada')
        wait_for(lambda: count(nats_url, streams['out']) == 5)

        found = wait_for(lambda: (lambda f: f if f['letters'] else None)(
            dead_letters.read_dead_letters(mage_project, pipeline.uuid)))

        [letter] = found['letters']
        assert letter['value'] == {'n': 3}
        assert 'three is not allowed' in letter['error']
        assert (found['topic'], found['source_topic'], found['total']) == (
            f'{streams["dead"]}.events', source_subject, 1)
        assert (found['kind'], found['source_kind']) == ('nats', 'nats')
        # Reading moved no consumer: the dead-letter stream has none.
        assert jetstream(nats_url, lambda js: js.consumers_info(streams['dead'])) == []

        sample = dead_letters.open_as_sample(mage_project, pipeline.uuid, 'failed')
        assert sample['message_count'] == 1
        result = sample_runner.run_on_sample(
            mage_project, pipeline.uuid, check.uuid,
            samples.read_sample(mage_project, pipeline.uuid, 'failed'))
        assert 'three is not allowed' in result['error']['message']

        # The fix, then the replay, which names the subject it publishes to.
        Path(check.file_path).write_text(textwrap.dedent('''
            @transformer
            def check(messages, *args, **kwargs):
                return messages
        '''))
        dev_run.stop(mage_project, pipeline.uuid)
        with pytest.raises(dead_letters.DeadLetterError, match='Type the source subject'):
            dead_letters.replay(mage_project, pipeline.uuid, 'orders.events')
        assert dead_letters.replay(mage_project, pipeline.uuid, source_subject)['replayed'] == 1
        dev_run.start(mage_project, pipeline.uuid, [out.uuid, dead.uuid], user='ada')

        wait_for(lambda: count(nats_url, streams['out']) == 6)
    finally:
        dev_run.stop(mage_project, pipeline.uuid)
        pipeline.delete()
