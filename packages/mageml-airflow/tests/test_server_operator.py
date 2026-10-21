"""
MageMLPipelineOperator against a running MageML server (MAGEML_TEST_SERVER_URL, signed in as
MAGEML_TEST_SERVER_EMAIL / MAGEML_TEST_SERVER_PASSWORD, admin@admin.com / admin by default):
a run completes, a retry finds its run, a failed run names its block, a kill cancels.
"""
import json
import os
import threading
import time
import uuid

import pytest
from mageml_airflow.hooks import TRIGGER_NAME, MageMLServerHook
from mageml_airflow.operators import AirflowException, MageMLPipelineOperator

from .test_operator import context

URL = os.getenv('MAGEML_TEST_SERVER_URL')
pytestmark = pytest.mark.skipif(not URL, reason='MAGEML_TEST_SERVER_URL is not set')
SUFFIX = uuid.uuid4().hex[:8]
LOAD = """
@data_loader
def load(*args, **kwargs):
    print('loading for', kwargs.get('day'))
    return {'day': kwargs.get('day')}
"""
BROKEN = """
@transformer
def broken(data, *args, **kwargs):
    raise RuntimeError('this block always fails')
"""
SLOW = """
import time


@transformer
def slow(data, *args, **kwargs):
    time.sleep(120)
    return data
"""


@pytest.fixture(scope='module')
def hook():
    os.environ['AIRFLOW_CONN_MAGEML_SERVER_DEFAULT'] = json.dumps(dict(
        conn_type='mageml_server',
        host=URL,
        login=os.getenv('MAGEML_TEST_SERVER_EMAIL', 'admin@admin.com'),
        password=os.getenv('MAGEML_TEST_SERVER_PASSWORD', 'admin'),
    ))
    return MageMLServerHook()


@pytest.fixture(scope='module')
def pipelines(hook):
    made = {}
    for name, second in (('ok', None), ('fails', BROKEN), ('slow', SLOW)):
        uuid_ = hook._call('POST', 'pipelines', json={
            'pipeline': {'name': f'airflow_{name}_{SUFFIX}', 'type': 'python'},
        })['pipeline']['uuid']
        hook._call('POST', f'pipelines/{uuid_}/blocks', json={'block': {
            'content': LOAD, 'language': 'python', 'name': f'load_{name}_{SUFFIX}',
            'type': 'data_loader',
        }})
        if second:
            hook._call('POST', f'pipelines/{uuid_}/blocks', json={'block': {
                'content': second, 'language': 'python', 'name': f'second_{name}_{SUFFIX}',
                'type': 'transformer', 'upstream_blocks': [f'load_{name}_{SUFFIX}'],
            }})
        made[name] = uuid_
    yield made
    for uuid_ in made.values():
        try:
            hook._call('DELETE', f'pipelines/{uuid_}')
        except Exception:
            pass


def test_a_run_completes_and_a_retry_finds_it(hook, pipelines, capsys):
    operator = MageMLPipelineOperator(task_id='ok', pipeline=pipelines['ok'],
                                      variables={'day': '2026-10-21'}, poll_interval=1)
    ctx = context(task_id='ok', run_id=f'manual__{SUFFIX}')
    result = operator.execute(ctx)
    assert result == {'run_id': operator.run_id, 'status': 'completed'}
    assert ctx['ti'].pushed['run_id'] == operator.run_id
    printed = capsys.readouterr()
    assert f'[load_ok_{SUFFIX}] loading for 2026-10-21' in printed.out + printed.err

    again = MageMLPipelineOperator(task_id='ok', pipeline=pipelines['ok'], poll_interval=1)
    again.execute(context(task_id='ok', run_id=f'manual__{SUFFIX}', try_number=2))
    assert again.run_id == operator.run_id

    schedules = hook._call('GET', f"pipelines/{pipelines['ok']}/pipeline_schedules")
    triggers = [s for s in schedules['pipeline_schedules'] if s['name'] == TRIGGER_NAME]
    assert [(t['schedule_type'], t['status']) for t in triggers] == [('api', 'active')]


def test_a_failed_run_fails_the_task_with_its_failed_block(pipelines):
    operator = MageMLPipelineOperator(task_id='fails', pipeline=pipelines['fails'],
                                      poll_interval=1)
    with pytest.raises(AirflowException) as raised:
        operator.execute(context(task_id='fails', run_id=f'manual__{SUFFIX}'))
    message = str(raised.value)
    assert 'failed' in message
    assert f'second_fails_{SUFFIX}' in message


def test_killing_the_task_cancels_the_run(hook, pipelines):
    operator = MageMLPipelineOperator(task_id='slow', pipeline=pipelines['slow'],
                                      poll_interval=1)
    errors = []

    def execute():
        try:
            operator.execute(context(task_id='slow', run_id=f'manual__{SUFFIX}'))
        except AirflowException as error:
            errors.append(str(error))

    thread = threading.Thread(target=execute, daemon=True)
    thread.start()
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and (
        operator.run_id is None or hook.run(operator.run_id)['status'] != 'running'
    ):
        time.sleep(1)
    operator.on_kill()
    thread.join(timeout=120)
    assert hook.run(operator.run_id)['status'] == 'cancelled'
    assert errors and 'cancelled' in errors[0]
