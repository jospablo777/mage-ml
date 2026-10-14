"""
IBM MQ streaming pipelines run by MageML, from a trigger and from the notebook, on the Python
source and sink (`ibmmq`): every message is transformed with its values exact and committed,
and a batch the pipeline fails on is backed out, not lost, also when the run stops.
"""
import importlib.util
import json
import os
import textwrap
import uuid
from pathlib import Path

import pytest

from integration_tests.streaming_runner import MODES, streaming_pipeline, wait_until

CONNECTION = '''
connector_type: ibm_mq
queue_manager: QM1
channel: DEV.APP.SVRCONN
connection_name: "127.0.0.1(%(port)s)"
queue: %(queue)s
user: %(user)s
password: "{{ env_var('MAGE_TEST_IBM_MQ_PASSWORD') }}"
'''
TRANSFORM = '''
from typing import Dict, List


@transformer
def double(messages: List[Dict], *args, **kwargs):
    if any(m.get('poison') for m in messages):
        raise ValueError('a poison message')
    return [dict(m, doubled=m['amount'] * 2) for m in messages]
'''


@pytest.fixture
def mq_children(ibm_mq, monkeypatch):
    """The pipeline runs in spawned processes, which load the MQ client from this path."""
    # Installed: the test process does not load it, the runs do.
    if importlib.util.find_spec('ibmmq') is None:
        pytest.skip('ibmmq is not installed: uv sync --extra ibm-mq')
    libraries = os.path.dirname(ibm_mq.library)
    monkeypatch.setenv('LD_LIBRARY_PATH', libraries)
    monkeypatch.setenv('DYLD_LIBRARY_PATH', libraries)
    return ibm_mq


@pytest.fixture
def queues(ibm_mq):
    names = [ibm_mq.define(), ibm_mq.define()]
    yield names
    for name in names:
        ibm_mq.delete(name)


@pytest.fixture
def pipeline(mage_project, ibm_mq, queues):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'mq_py_{uuid.uuid4().hex[:8]}'
    created = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=mage_project)

    def add(suffix, kind, code, language, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, mage_project, language=language,
                             pipeline=created)
        Path(block.file_path).write_text(textwrap.dedent(code))
        created.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    settings = dict(port=ibm_mq.port, user=ibm_mq.user)
    source = add('source', 'data_loader', CONNECTION % dict(settings, queue=queues[0])
                 + 'batch_size: 20\nbatch_timeout: 0.5\n', 'yaml')
    double = add('double', 'transformer', TRANSFORM, 'python', [source])
    add('sink', 'data_exporter', CONNECTION % dict(settings, queue=queues[1]), 'yaml', [double])
    yield created
    created.delete()


def drain(ibm_mq, queue):
    values = []
    while (got := ibm_mq.get(queue)) is not None:
        values.append(json.loads(got[0]))
    return values


@pytest.mark.parametrize('mode', MODES)
def test_messages_are_transformed_and_committed(mode, mq_children, pipeline, queues):
    ibm_mq = mq_children
    for i in range(40):
        ibm_mq.put(queues[0], json.dumps(dict(id=i, amount=i, big=2**70 + i, city='Zürich')))
    out = []
    with streaming_pipeline(pipeline.uuid, mode) as run:
        wait_until(lambda: out.extend(drain(ibm_mq, queues[1])) or len(out) >= 40, run,
                   message='the output queue')
        wait_until(lambda: ibm_mq.depth(queues[0]) == (0, 'NO'), run,
                   message='the input committed')

    assert sorted(out, key=lambda v: v['id']) == [
        dict(id=i, amount=i, big=2**70 + i, city='Zürich', doubled=2 * i) for i in range(40)
    ]


@pytest.mark.parametrize('mode', MODES)
def test_a_failed_batch_is_backed_out_and_stays_on_the_queue(
    mode, mq_children, pipeline, queues,
):
    ibm_mq = mq_children
    for i in range(5):
        ibm_mq.put(queues[0], json.dumps(dict(id=i, amount=i, poison=i == 2)))
    with streaming_pipeline(pipeline.uuid, mode) as run:
        # The run reads the batch, fails on it and stops; the stop backs out.
        run.process.join(90)
        text = run.text()
    assert 'a poison message' in text, text[-3000:]
    assert ibm_mq.depth(queues[0]) == (5, 'NO')
    assert drain(ibm_mq, queues[1]) == []
