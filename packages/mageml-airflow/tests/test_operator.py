"""MageMLServiceRunOperator against a running service: runs, logs, outputs, failures,
cancel and deferral; then a whole DAG run in Airflow."""
import asyncio
from types import SimpleNamespace

import pytest
from mageml_airflow.hooks import MageMLServiceHook
from mageml_airflow.operators import MageMLServiceRunOperator, idempotency_key
from mageml_airflow.triggers import MageMLRunTrigger


class FakeTaskInstance(SimpleNamespace):
    def xcom_push(self, key, value):
        self.pushed[key] = value


def context(task_id='run', run_id='manual__1', try_number=1):
    ti = FakeTaskInstance(dag_id='mageml_test', run_id=run_id, task_id=task_id, map_index=-1,
                          try_number=try_number, pushed={})
    return dict(ti=ti)


def test_a_run_completes_with_its_logs_and_its_output_as_the_return_value(service, capsys):
    operator = MageMLServiceRunOperator(
        task_id='sum', pipeline='sum', variables={'n': 5}, output_block='total',
        poll_interval=0.2,
    )
    ctx = context(task_id='sum')
    value = operator.execute(ctx)
    assert value == {'total': 10}
    assert ctx['ti'].pushed['run_id'] == operator.run_id
    # Airflow 3 writes the task log with structlog, to stdout here.
    printed = capsys.readouterr()
    assert '[load] loading 5' in printed.out + printed.err


def test_a_retry_of_a_task_never_starts_a_second_run(service, api):
    first = MageMLServiceRunOperator(task_id='once', pipeline='sum', poll_interval=0.2)
    first.execute(context(task_id='once', run_id='manual__retry'))
    # The next try of the same task instance: the run completed, so it is the answer.
    again = MageMLServiceRunOperator(task_id='once', pipeline='sum', poll_interval=0.2)
    again.execute(context(task_id='once', run_id='manual__retry', try_number=2))
    assert again.run_id == first.run_id
    key = idempotency_key(context(task_id='once', run_id='manual__retry'))
    assert key == 'airflow/mageml_test/manual__retry/once/-1'


def test_a_failed_run_fails_the_task_with_the_blocks_error(service):
    from mageml_airflow.operators import AirflowException

    operator = MageMLServiceRunOperator(task_id='fails', pipeline='fails', poll_interval=0.2)
    with pytest.raises(AirflowException, match='broken: .*this block always fails'):
        operator.execute(context(task_id='fails'))


def test_killing_the_task_cancels_the_run(service, api):
    import threading
    import time

    operator = MageMLServiceRunOperator(task_id='slow', pipeline='slow', poll_interval=0.2)
    errors = []
    thread = threading.Thread(
        target=lambda: _capture(errors, operator.execute, context(task_id='slow')),
        daemon=True,
    )
    thread.start()
    deadline = time.monotonic() + 30
    while operator.run_id is None and time.monotonic() < deadline:
        time.sleep(0.05)
    operator.on_kill()
    thread.join(timeout=30)
    assert api('GET', f'/v1/runs/{operator.run_id}')['status'] == 'cancelled'
    assert errors and 'cancelled' in str(errors[0])


def _capture(errors, function, *args):
    try:
        function(*args)
    except Exception as error:
        errors.append(error)


def test_a_deferred_task_waits_in_the_trigger(service):
    try:
        from airflow.sdk.exceptions import TaskDeferred
    except ImportError:
        from airflow.exceptions import TaskDeferred

    operator = MageMLServiceRunOperator(
        task_id='deferred', pipeline='sum', variables={'n': 3}, output_block='total',
        deferrable=True, poll_interval=0.2,
    )
    with pytest.raises(TaskDeferred) as deferred:
        operator.execute(context(task_id='deferred'))
    trigger = deferred.value.trigger
    assert isinstance(trigger, MageMLRunTrigger)
    assert trigger.serialize()[1]['run_id'] == operator.run_id

    async def first_event():
        async for event in trigger.run():
            return event.payload

    event = asyncio.run(first_event())
    assert event == {'run_id': operator.run_id, 'status': 'completed'}
    assert operator.execute_complete(context(task_id='deferred'), event) == {'total': 3}


def test_the_connection_says_whether_the_service_answers(service):
    assert MageMLServiceHook().test_connection() == (True, 'The service answered.')
