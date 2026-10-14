"""
A Mage streaming pipeline (an IBM MQ source, a Python transformer, an IBM MQ sink) exported
with `mage export service` and run by mage-service built with IBM MQ, against IBM MQ 9.4:
messages are read under syncpoint and committed only after the sink committed their output,
so a restart, a kill, a failing sink and parallel connections lose nothing; a stopped stream
backs out instead of letting the disconnect commit.
"""
import json
import os
import textwrap
import time
import uuid
from pathlib import Path

import pytest

from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)

CONNECTION = '''
connector_type: ibm_mq
queue_manager: QM1
channel: DEV.APP.SVRCONN
connection_name: "127.0.0.1(%(port)s)"
queue: %(queue)s
user: %(user)s
password: "{{ env_var('MQ_PASSWORD') }}"
'''
SOURCE = CONNECTION + '''
batch_size: 50
batch_timeout: 0.2
connections: %(connections)s
'''
TRANSFORM = '''
import time
from typing import Dict, List


@transformer
def double(messages: List[Dict], *args, **kwargs):
    time.sleep(float(kwargs.get('delay', 0)))
    return [dict(m, doubled=m['amount'] * 2) for m in messages if m['id'] % 10 != 0]
'''


@pytest.fixture
def queues(ibm_mq):
    names = [ibm_mq.define(), ibm_mq.define()]
    yield names
    for name in names:
        ibm_mq.delete(name)


def make_pipeline(mage_project, ibm_mq, queues, connections=1):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'mq_stream_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING,
                               repo_path=mage_project)

    def add(suffix, kind, code, language, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, mage_project, language=language,
                             pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    settings = dict(port=ibm_mq.port, user=ibm_mq.user, connections=connections)
    source = add('source', 'data_loader', SOURCE % dict(settings, queue=queues[0]), 'yaml')
    double = add('double', 'transformer', TRANSFORM, 'python', [source])
    add('sink', 'data_exporter', CONNECTION % dict(settings, queue=queues[1]), 'yaml', [double])
    return pipeline


@pytest.fixture
def stream_pipeline(mage_project, ibm_mq, queues):
    pipeline = make_pipeline(mage_project, ibm_mq, queues)
    yield pipeline
    pipeline.delete()


def service_for(mage_project, pipeline, tmp_path, ibm_mq, name='data'):
    directory = export_service(mage_project, pipeline.uuid, tmp_path)
    libraries = os.path.dirname(ibm_mq.library)
    return RunningService(
        stream_service_binary(), directory, tmp_path / name,
        MQ_PASSWORD=ibm_mq.password, MAGE_SERVICE_MQ_LIBRARY=ibm_mq.library,
        LD_LIBRARY_PATH=libraries, DYLD_LIBRARY_PATH=libraries,
        MQ_OVERRIDE_DATA_PATH=str(tmp_path / 'mq-data'),
    )


def publish(ibm_mq, queue, ids):
    for i in ids:
        # 2**70 + i: integers past 64 bits keep every digit.
        ibm_mq.put(queue, json.dumps(dict(id=i, amount=i, big=2**70 + i)))


def receive(ibm_mq, queue, count, timeout=120):
    messages = []
    deadline = time.time() + timeout
    while len(messages) < count and time.time() < deadline:
        got = ibm_mq.get(queue, wait_ms=1000)
        if got is not None:
            body, headers = got
            messages.append((json.loads(body), headers))
    return messages


def wait_failed(service, timeout=90):
    deadline = time.time() + timeout
    status = None
    while time.time() < deadline:
        try:
            status = service.streams()[0]
        except OSError:  # not listening yet
            status = None
        if status and status['state'] == 'failed':
            break
        time.sleep(0.5)
    return status


def test_each_message_is_transformed_once_and_committed_across_a_restart(
    mage_project, stream_pipeline, queues, ibm_mq, tmp_path,
):
    service = service_for(mage_project, stream_pipeline, tmp_path, ibm_mq)
    publish(ibm_mq, queues[0], range(200))
    service.start()
    try:
        service.wait_running()
        first = receive(ibm_mq, queues[1], 180)
    finally:
        service.stop()

    rows = [row for row, _ in first]
    assert sorted(r['id'] for r in rows) == [i for i in range(200) if i % 10]
    assert all(r['doubled'] == r['amount'] * 2 and r['big'] == 2**70 + r['id'] for r in rows)
    # The sink sets each MsgId from the stream and position, not the queue manager's "AMQ ".
    ids = [headers['ibm-mq-md-messageid'] for _, headers in first]
    assert len(set(ids)) == len(ids)
    assert not any(i.startswith('ID:414d5120') for i in ids), ids[:3]
    assert all(h['ibm-mq-md-persistence'] == 'persistent' for _, h in first)
    assert ibm_mq.depth(queues[0]) == (0, 'NO')

    publish(ibm_mq, queues[0], range(200, 250))
    service.start()
    try:
        status = service.wait_running()
        second = receive(ibm_mq, queues[1], 45)
        time.sleep(2)
        second += receive(ibm_mq, queues[1], 1, timeout=2)
    finally:
        service.stop()

    assert status['last_error'] is None
    assert sorted(r['id'] for r, _ in second) == [i for i in range(200, 250) if i % 10]
    assert ibm_mq.depth(queues[0]) == (0, 'NO')


def test_a_killed_service_loses_nothing(
    mage_project, stream_pipeline, queues, ibm_mq, tmp_path,
):
    """
    kill -9 in the middle of a stream: the queue manager backs out the dead connection's unit
    of work, so the restarted service reads those messages again; only the batch in flight at
    the kill is written twice.
    """
    stream_pipeline.variables = dict(delay=0.2)
    stream_pipeline.save()
    service = service_for(mage_project, stream_pipeline, tmp_path, ibm_mq)
    expected = [i for i in range(400) if i % 10]
    publish(ibm_mq, queues[0], range(400))
    service.start()
    try:
        service.wait_running()
        received = receive(ibm_mq, queues[1], 90)
        service.process.kill()
        service.process.wait(timeout=30)
        service.start()
        service.wait_running()
        # Until every message arrived: the batch written twice counts in received, so a
        # count of what is left stopped reading short of it.
        deadline = time.time() + 180
        while {r['id'] for r, _ in received} != set(expected) and time.time() < deadline:
            received += receive(ibm_mq, queues[1], len(expected), timeout=5)
        time.sleep(3)
        received += receive(ibm_mq, queues[1], len(expected), timeout=3)
    finally:
        if service.process.poll() is None:
            service.stop()

    ids = [r['id'] for r, _ in received]
    assert sorted(set(ids)) == expected
    # One batch in flight per connection: at most 50 messages, 45 written, repeat.
    assert len(ids) - len(set(ids)) <= 45, f'{len(ids) - len(set(ids))} repeated'
    assert ibm_mq.depth(queues[0]) == (0, 'NO')


def test_a_failing_sink_commits_nothing_and_the_stop_backs_out(
    mage_project, stream_pipeline, queues, ibm_mq, tmp_path,
):
    """
    The output queue refuses puts: the stream stops with the reason. A normal disconnect
    would commit the messages read under syncpoint; the source backs out first, so after the
    service stops every message is still on the input queue.
    """
    ibm_mq.put_inhibit(queues[1])
    service = service_for(mage_project, stream_pipeline, tmp_path, ibm_mq)
    publish(ibm_mq, queues[0], range(1, 31))
    service.start()
    try:
        # The stream can fail before it is seen running.
        status = wait_failed(service)
    finally:
        service.stop()

    assert status['state'] == 'failed', status
    assert '2051' in status['last_error'] or 'PUT_INHIBITED' in status['last_error'], status
    assert ibm_mq.depth(queues[0]) == (30, 'NO')


def test_parallel_connections_read_every_message_once(
    mage_project, ibm_mq, queues, tmp_path,
):
    pipeline = make_pipeline(mage_project, ibm_mq, queues, connections=3)
    try:
        service = service_for(mage_project, pipeline, tmp_path, ibm_mq)
        publish(ibm_mq, queues[0], range(600))
        service.start()
        try:
            service.wait_running()
            received = receive(ibm_mq, queues[1], 540)
            time.sleep(2)
            received += receive(ibm_mq, queues[1], 1, timeout=2)
            partitions = service.streams()[0].get('partitions')
        finally:
            service.stop()
    finally:
        pipeline.delete()

    ids = [r['id'] for r, _ in received]
    assert sorted(ids) == [i for i in range(600) if i % 10]
    assert ibm_mq.depth(queues[0]) == (0, 'NO')
    if partitions is not None:
        assert len(partitions) == 3, partitions


def test_a_refused_connection_is_reported_with_the_reason(
    mage_project, stream_pipeline, queues, ibm_mq, tmp_path,
):
    """A wrong password does not leave the stream silent: its status names MQ's reason."""
    service = service_for(mage_project, stream_pipeline, tmp_path, ibm_mq)
    service.env['MQ_PASSWORD'] = 'not the password'  # pragma: allowlist secret
    service.start()
    try:
        deadline = time.time() + 60
        error = None
        while time.time() < deadline and not error:
            time.sleep(1)
            try:
                error = service.streams()[0].get('last_error')
            except OSError:
                continue
    finally:
        service.stop()

    assert error and '2035' in error and 'CHLAUTH' in error, error
    assert 'not the password' not in error
