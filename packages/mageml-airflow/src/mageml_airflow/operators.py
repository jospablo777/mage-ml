"""Runs a pipeline of a MageML service from an Airflow task."""
import hashlib
import time
from typing import Any, Dict, Optional, Sequence

from mageml_airflow.hooks import MageMLServiceError, MageMLServiceHook
from mageml_airflow.triggers import FINISHED, MageMLRunTrigger

try:  # Airflow 3
    from airflow.sdk import BaseOperator
except ImportError:  # Airflow 2
    from airflow.models import BaseOperator
try:  # Airflow 3
    from airflow.sdk.exceptions import AirflowException
except ImportError:  # Airflow 2
    from airflow.exceptions import AirflowException


def idempotency_key(context: Dict[str, Any]) -> str:
    """
    The task instance's key, the same in every try. The service answers a try with the run
    the key started while it runs or once it completed, so a retry after a worker died never
    starts a second run; after a failed or cancelled run it starts a new one (retry_of).
    """
    ti = context['ti']
    parts = ['airflow', ti.dag_id, ti.run_id, ti.task_id, str(getattr(ti, 'map_index', -1))]
    key = '/'.join(parts)
    if len(key) > 200:
        key = 'airflow/' + hashlib.sha256(key.encode()).hexdigest()
    return key


class MageMLServiceRunOperator(BaseOperator):
    """
    Runs a pipeline of a MageML service and waits for it: the run's log lines go to the
    task log as they come, a failed or cancelled run fails the task with its error, and with
    `output_block` the block's output is the task's return value (XCom).

    Args:
        pipeline: The service's pipeline.
        variables: The run's variables (templated).
        output_block: A block whose output becomes the XCom; JSON, or a frame as rows.
        output_max_bytes: A larger output becomes a reference to read it from the service.
        mageml_conn_id: The MageML service connection.
        poll_interval: Seconds between checks of the run.
        deferrable: Wait in the triggerer, holding no worker slot.
    """

    template_fields: Sequence[str] = ('pipeline', 'variables', 'output_block')
    ui_color = '#6b50d7'

    def __init__(
        self,
        *,
        pipeline: str,
        variables: Optional[Dict[str, Any]] = None,
        output_block: Optional[str] = None,
        output_max_bytes: int = 1_000_000,
        mageml_conn_id: str = MageMLServiceHook.default_conn_name,
        poll_interval: float = 5.0,
        deferrable: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.pipeline = pipeline
        self.variables = variables or {}
        self.output_block = output_block
        self.output_max_bytes = output_max_bytes
        self.mageml_conn_id = mageml_conn_id
        self.poll_interval = poll_interval
        self.deferrable = deferrable
        self.run_id: Optional[str] = None
        self._log_next = 0

    def _hook(self) -> MageMLServiceHook:
        return MageMLServiceHook(self.mageml_conn_id)

    def execute(self, context: Dict[str, Any]) -> Any:
        hook = self._hook()
        run = hook.start_run(self.pipeline, self.variables, idempotency_key(context))
        self.run_id = run['id']
        context['ti'].xcom_push(key='run_id', value=self.run_id)
        self.log.info('MageML run %s of pipeline %s: %s', self.run_id, self.pipeline,
                      run.get('status'))
        if run.get('status') not in FINISHED and self.deferrable:
            self.defer(
                trigger=MageMLRunTrigger(self.run_id, self.mageml_conn_id, self.poll_interval),
                method_name='execute_complete',
            )
        while run.get('status') not in FINISHED:
            self._print_logs(hook)
            time.sleep(self.poll_interval)
            run = hook.run(self.run_id)
        return self._finish(hook, run)

    def execute_complete(self, context: Dict[str, Any], event: Dict[str, Any]) -> Any:
        self.run_id = event['run_id']
        if event.get('status') == 'error':
            raise AirflowException(event.get('message') or f'MageML run {self.run_id} is gone.')
        hook = self._hook()
        return self._finish(hook, hook.run(self.run_id))

    def _print_logs(self, hook: MageMLServiceHook) -> None:
        try:
            lines, self._log_next = hook.logs(self.run_id, self._log_next)
        except MageMLServiceError as error:
            self.log.warning('Could not read the run log: %s', error)
            return
        for line in lines:
            block = line.get('block')
            message = line.get('message', '')
            self.log.info('[%s] %s', block, message) if block else self.log.info('%s', message)

    def _finish(self, hook: MageMLServiceHook, run: Dict[str, Any]) -> Any:
        self._print_logs(hook)
        status = run.get('status')
        if status != 'completed':
            failed = [b for b in run.get('blocks') or [] if b.get('status') == 'failed']
            reason = '; '.join(f"{b.get('block')}: {b.get('error') or 'failed'}" for b in failed)
            reason = reason or run.get('error') or ''
            raise AirflowException(
                f'MageML run {self.run_id} of pipeline {self.pipeline} {status}'
                + (f': {reason}' if reason else '.')
            )
        if not self.output_block:
            return dict(run_id=self.run_id, status=status)
        value, size = hook.output(self.run_id, self.output_block)
        if size > self.output_max_bytes:
            self.log.info('The output of %s has %d bytes, more than output_max_bytes: the '
                          'XCom holds where to read it.', self.output_block, size)
            return dict(run_id=self.run_id, block=self.output_block,
                        output_url=f'/v1/runs/{self.run_id}/output?block={self.output_block}')
        return value

    def on_kill(self) -> None:
        if self.run_id:
            try:
                self._hook().cancel(self.run_id)
                self.log.info('Cancelled MageML run %s.', self.run_id)
            except MageMLServiceError as error:
                self.log.warning('Could not cancel MageML run %s: %s', self.run_id, error)
